#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <base/sort.h>
#include <Columns/ColumnConst.h>

#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSource.h>
#include <Access/ContextAccess.h>
#include <Analyzer/QueryNode.h>
#include <Core/Names.h>
#include <Core/ProtocolDefines.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/distributedIndexAnalysis.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/PredicateStatisticsLog.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/parseIdentifiersOrStringLiteralsWithSettings.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/MergeTreeFinalMerge.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Processors/Transforms/VirtualRowTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ConditionTemplate.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>
#include <Storages/MergeTree/MergeTreePrefetchedReadPool.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/IndexReadRangesRefiner.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>
#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicasInOrder.h>
#include <Storages/MergeTree/MergeTreeReadPoolProjectionIndex.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/JSONBuilder.h>
#include <Common/Logger.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>
#include <Common/checkStackSize.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <set>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <limits>

#include <city.h>

#include <boost/functional/hash.hpp>

#include <fmt/ranges.h>

#include "config.h"

using namespace DB;

namespace
{
template <typename Container, typename Getter>
size_t countPartitions(const Container & parts, Getter get_partition_id)
{
    if (parts.empty())
        return 0;

    String cur_partition_id = get_partition_id(parts[0]);
    size_t unique_partitions = 1;
    for (size_t i = 1; i < parts.size(); ++i)
    {
        if (get_partition_id(parts[i]) != cur_partition_id)
        {
            ++unique_partitions;
            cur_partition_id = get_partition_id(parts[i]);
        }
    }
    return unique_partitions;
}

size_t countPartitions(const RangesInDataParts & parts_with_ranges)
{
    auto get_partition_id = [](const RangesInDataPart & rng) { return rng.data_part->info.getPartitionId(); };
    return countPartitions(parts_with_ranges, get_partition_id);
}

/// check if a DAG node only depends on sorting key columns
/// (ActionsDAG version of isExpressionOverSortingKey)
bool isNodeOverSortingKey(const ActionsDAG::Node * node, const NameSet & sorting_key_set)
{
    if (sorting_key_set.contains(node->result_name))
        return true;
    if (node->type == ActionsDAG::ActionType::COLUMN)
        return true; // constants are fine
    if (node->type == ActionsDAG::ActionType::INPUT || node->type == ActionsDAG::ActionType::PLACEHOLDER)
        return false; // already checked result_name
    for (const auto * child : node->children)
        if (!isNodeOverSortingKey(child, sorting_key_set))
            return false;
    return true;
}

bool isNodeDeterministic(const ActionsDAG::Node * node)
{
    /// A `COLUMN` node is not always an innocent literal. `node->isDeterministic` covers the function of a
    /// `FUNCTION` node and a constant that was folded from a non-deterministic expression such as `now`.
    /// `allNodeFunctions` additionally looks inside a lambda that constant folding turned into a `COLUMN`
    /// node holding a `ColumnFunction`: a call on the lambda argument lives in the lambda's own
    /// `ActionsDAG`, so it is invisible to a walk over this DAG alone.
    if (!node->isDeterministic())
        return false;
    if (!allNodeFunctions(*node, [](const IFunctionBase & function) { return function.isDeterministic(); }))
        return false;
    for (const auto * child : node->children)
        if (!isNodeDeterministic(child))
            return false;
    return true;
}

/// Like `VirtualColumnUtils::isDeterministic`, but treats `__topKFilter` as deterministic.
/// Mirrors `isDeterministicAllowingTopKFilter` in `updateQueryConditionCache.cpp` — both
/// gates must agree, otherwise QCC writes and reads diverge on TopK plans.
///
/// Unlike `isNodeDeterministic`, this also rejects non-deterministic `COLUMN` nodes (such
/// as query-time constants `now()` / `today()`). Without that check, queries whose filter
/// captures such constants could write QCC entries and reuse them later when the constant's
/// value has changed.
bool isDeterministicAllowingTopKFilter(const ActionsDAG::Node * node)
{
    for (const auto * child : node->children)
        if (!isDeterministicAllowingTopKFilter(child))
            return false;

    if (node->type == ActionsDAG::ActionType::COLUMN)
        return node->isDeterministic();

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministic())
        return node->function_base->getName() == "__topKFilter";

    return true;
}

bool restoreDAGInputs(ActionsDAG & dag, const NameSet & inputs)
{
    std::unordered_set<const ActionsDAG::Node *> outputs(dag.getOutputs().begin(), dag.getOutputs().end());
    bool added = false;
    for (const auto * input : dag.getInputs())
    {
        if (inputs.contains(input->result_name) && !outputs.contains(input))
        {
            dag.getOutputs().push_back(input);
            added = true;
        }
    }

    return added;
}

/// Same as above, but for a filter DAG whose result column is erased by name after the DAG runs.
/// A requested column that is already an output is still erased when it happens to BE that filter
/// column (a bare `PREWHERE c`, or a row policy `USING b`), so there the remove-filter flag is
/// cleared instead: after filtering the column keeps its original values for the surviving rows.
/// Only the filter column itself is exempted, so a computed filter column keeps being removed.
/// Same reasoning and shape as `reexpose_in_filter` in `addStartingPartOffsetAndPartOffset`.
bool restoreFilterDAGInputs(ActionsDAG & dag, const String & filter_column_name, bool & remove_filter_column, const NameSet & inputs)
{
    std::unordered_set<const ActionsDAG::Node *> outputs(dag.getOutputs().begin(), dag.getOutputs().end());
    bool added = false;
    for (const auto * input : dag.getInputs())
    {
        if (!inputs.contains(input->result_name))
            continue;

        if (!outputs.contains(input))
        {
            dag.getOutputs().push_back(input);
            added = true;
        }
        else if (remove_filter_column && input->result_name == filter_column_name)
        {
            remove_filter_column = false;
            added = true;
        }
    }

    return added;
}

bool restorePrewhereInputs(FilterDAGInfo * row_level_filter, PrewhereInfo * info, const NameSet & inputs)
{
    bool added = false;
    /// Both DAGs must be visited: `||` would short-circuit and skip the prewhere restore.
    if (row_level_filter)
        added |= restoreFilterDAGInputs(
            row_level_filter->actions, row_level_filter->column_name, row_level_filter->do_remove_column, inputs);

    if (info)
        added |= restoreFilterDAGInputs(
            info->prewhere_actions, info->prewhere_column_name, info->remove_prewhere_column, inputs);

    return added;
}

}

namespace ProfileEvents
{
    extern const Event IndexAnalysisRounds;
    extern const Event SelectedParts;
    extern const Event SelectedPartsTotal;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
    extern const Event SelectedMarksTotal;
    extern const Event SelectQueriesWithPrimaryKeyUsage;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_asynchronous_read_from_io_pool_for_merge_tree;
    extern const SettingsBool allow_calculating_subcolumns_sizes_for_merge_tree_reading;
    extern const SettingsBool allow_prefetched_read_pool_for_local_filesystem;
    extern const SettingsBool allow_prefetched_read_pool_for_remote_filesystem;
    extern const SettingsBool compile_sort_description;
    extern const SettingsBool distributed_plan_prefer_replicas_over_workers;
    extern const SettingsBool do_not_merge_across_partitions_select_final;
    extern const SettingsBool enable_automatic_decision_for_merging_across_partitions_for_final;
    extern const SettingsBool enable_vertical_final;
    extern const SettingsBool force_aggregate_partitions_independently;
    extern const SettingsBool force_creating_set_partitions_independently;
    extern const SettingsBool force_distinct_partitions_independently;
    extern const SettingsBool force_window_partitions_independently;
    extern const SettingsBool force_primary_key;
    extern const SettingsString ignore_data_skipping_indices;
    extern const SettingsUInt64 max_number_of_partitions_for_independent_aggregation;
    extern const SettingsUInt64 max_number_of_partitions_for_independent_distinct;
    extern const SettingsUInt64 max_number_of_partitions_for_independent_window;
    extern const SettingsInt64 max_partitions_to_read;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsMaxThreads max_final_threads;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_streams_for_merge_tree_reading;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 merge_tree_max_bytes_to_use_cache;
    extern const SettingsUInt64 merge_tree_max_rows_to_use_cache;
    extern const SettingsUInt64 merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem;
    extern const SettingsUInt64 merge_tree_min_rows_for_concurrent_read_for_remote_filesystem;
    extern const SettingsUInt64 merge_tree_min_bytes_for_concurrent_read;
    extern const SettingsUInt64 merge_tree_min_bytes_per_read_stream;
    extern const SettingsUInt64 merge_tree_min_rows_for_concurrent_read;
    extern const SettingsFloat merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
    extern const SettingsBool optimize_read_in_reverse_order_final;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsBool parallel_replicas_index_analysis_only_on_coordinator;
    extern const SettingsBool parallel_replicas_support_projection;
    extern const SettingsBool distributed_index_analysis;
    extern const SettingsBool distributed_index_analysis_for_non_shared_merge_tree;
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 preferred_max_column_in_block_size_bytes;
    extern const SettingsUInt64 read_in_order_two_level_merge_threshold;
    extern const SettingsUInt64 read_in_order_split_by_key_prefix_in_read_ahead_rows;
    extern const SettingsBool split_parts_ranges_into_intersecting_and_non_intersecting_final;
    extern const SettingsBool split_intersecting_parts_ranges_into_layers_final;
    extern const SettingsBool use_constant_folding_in_index_analysis;
    extern const SettingsBool use_primary_key;
    extern const SettingsBool use_partition_pruning;
    extern const SettingsBool use_statistics;
    extern const SettingsBool use_skip_indexes;
    extern const SettingsBool use_skip_indexes_if_final;
    extern const SettingsBool use_skip_indexes_for_disjunctions;
    extern const SettingsBool use_uncompressed_cache;
    extern const SettingsNonZeroUInt64 merge_tree_min_read_task_size;
    extern const SettingsBool read_in_order_use_virtual_row;
    extern const SettingsBool read_in_order_use_virtual_row_per_block;
    extern const SettingsBool use_skip_indexes_if_final_exact_mode;
    extern const SettingsBool use_skip_indexes_on_data_read;
    extern const SettingsBool use_indexes_refiner_in_read_pools;
    extern const SettingsUInt64 join_runtime_filter_exact_values_limit;
    extern const SettingsBool use_skip_indexes_for_top_k;
    extern const SettingsBool use_top_k_dynamic_filtering;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool use_query_condition_cache_for_top_k;
    extern const SettingsUInt64 predicate_statistics_sample_rate;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 query_plan_max_step_description_length;
    extern const SettingsBool apply_row_policy_after_final;
    extern const SettingsBool apply_prewhere_after_final;
    extern const SettingsBool defer_partition_pruning_after_final;
    extern const SettingsBool distributed_index_analysis_only_on_coordinator;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsUInt64 max_concurrent_queries;
    extern const MergeTreeSettingsInt64 max_partitions_to_read;
    extern const MergeTreeSettingsUInt64 min_marks_to_honor_max_concurrent_queries;
    extern const MergeTreeSettingsBool share_nested_offsets;
    extern const MergeTreeSettingsUInt64 distributed_index_analysis_min_parts_to_activate;
    extern const MergeTreeSettingsUInt64 distributed_index_analysis_min_indexes_bytes_to_activate;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INDEX_NOT_USED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_PARTITIONS;
    extern const int NO_SUCH_DATA_PART;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_TABLE;
}

static bool checkAllPartsOnRemoteFS(const RangesInDataParts & parts)
{
    for (const auto & part : parts)
    {
        if (!part.data_part->isStoredOnRemoteDisk())
            return false;
    }
    return true;
}

static bool checkAnyPartOnRemoteFS(const RangesInDataParts & parts)
{
    for (const auto & part : parts)
    {
        if (part.data_part->isStoredOnRemoteDisk())
            return true;
    }
    return false;
}

/// build sort description for output stream
static SortDescription getSortDescriptionForOutputHeader(
    const SharedHeader & output_header,
    const Names & sorting_key_columns,
    const std::vector<bool> & reverse_flags,
    const int sort_direction,
    InputOrderInfoPtr input_order_info,
    const FilterDAGInfoPtr & row_level_filter,
    const PrewhereInfoPtr & prewhere_info,
    bool enable_vertical_final)
{
    /// Updating sort description can be done after PREWHERE actions are applied to the header.
    /// After PREWHERE actions are applied, column names in header can differ from storage column names due to aliases
    /// To mitigate it, we're trying to build original header and use it to deduce sorting description
    /// TODO: this approach is fragile, it'd be more robust to update sorting description for the whole plan during plan optimization
    Block original_header = output_header->cloneEmpty();
    if (prewhere_info)
    {
        {
            FindOriginalNodeForOutputName original_column_finder(prewhere_info->prewhere_actions);
            for (auto & column : original_header)
            {
                const auto * original_node = original_column_finder.find(column.name);
                if (original_node)
                    column.name = original_node->result_name;
            }
        }
    }

    if (row_level_filter)
    {
        FindOriginalNodeForOutputName original_column_finder(row_level_filter->actions);
        for (auto & column : original_header)
        {
            const auto * original_node = original_column_finder.find(column.name);
            if (original_node)
                column.name = original_node->result_name;
        }
    }

    SortDescription sort_description;
    const Block & header = *output_header;
    size_t sort_columns_size = sorting_key_columns.size();
    sort_description.reserve(sort_columns_size);
    for (size_t i = 0; i < sort_columns_size; ++i)
    {
        const auto & sorting_key = sorting_key_columns[i];
        const auto it = std::find_if(
            original_header.begin(), original_header.end(), [&sorting_key](const auto & column) { return column.name == sorting_key; });
        if (it == original_header.end())
            break;

        const size_t column_pos = std::distance(original_header.begin(), it);
        if (!reverse_flags.empty() && reverse_flags[i])
            sort_description.emplace_back((header.begin() + column_pos)->name, sort_direction * -1);
        else
            sort_description.emplace_back((header.begin() + column_pos)->name, sort_direction);
    }

    if (input_order_info && !enable_vertical_final)
    {
        // output_stream.sort_scope = DataStream::SortScope::Stream;
        const size_t used_prefix_of_sorting_key_size = input_order_info->used_prefix_of_sorting_key_size;
        if (sort_description.size() > used_prefix_of_sorting_key_size)
            sort_description.resize(used_prefix_of_sorting_key_size);

        return sort_description;
    }

    return {};
}

std::shared_ptr<QueryIdHolder> ReadFromMergeTree::AnalysisResult::checkLimits(
    const Context & context_, const MergeTreeData & data_, const MergeTreeSettings & data_settings_) const
{
    const Settings & settings = context_.getSettingsRef();
    auto max_partitions_to_read = settings[Setting::max_partitions_to_read].changed
        ? settings[Setting::max_partitions_to_read].value
        : data_settings_[MergeTreeSetting::max_partitions_to_read].value;
    if (max_partitions_to_read > 0)
    {
        std::set<String> partitions;
        for (const auto & part_with_ranges : parts_with_ranges)
            partitions.insert(part_with_ranges.data_part->info.getPartitionId());
        if (partitions.size() > static_cast<size_t>(max_partitions_to_read))
        {
            throw Exception(
                ErrorCodes::TOO_MANY_PARTITIONS,
                "Too many partitions to read. Current {}, max {}",
                partitions.size(),
                max_partitions_to_read);
        }
    }

    if (data_settings_[MergeTreeSetting::max_concurrent_queries] > 0
        && data_settings_[MergeTreeSetting::min_marks_to_honor_max_concurrent_queries] > 0
        && selected_marks >= data_settings_[MergeTreeSetting::min_marks_to_honor_max_concurrent_queries])
    {
        auto query_id = context_.getCurrentQueryId();
        if (!query_id.empty())
            return data_.getQueryIdHolder(query_id, data_settings_[MergeTreeSetting::max_concurrent_queries]);
    }

    return nullptr;
}

ReadFromMergeTree::ReadFromMergeTree(
    RangesInDataPartsPtr parts_,
    MergeTreeData::MutationsSnapshotPtr mutations_,
    Names all_column_names_,
    const MergeTreeData & data_,
    MergeTreeSettingsPtr data_settings_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    size_t max_block_size_,
    size_t num_streams_,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
    LoggerPtr log_,
    AnalysisResultPtr analyzed_result_ptr_,
    bool enable_parallel_reading_,
    std::optional<MergeTreeAllRangesCallback> all_ranges_callback_,
    std::optional<MergeTreeReadTaskCallback> read_task_callback_,
    std::optional<size_t> number_of_current_replica_)
    : SourceStepWithFilter(std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot_->getSampleBlockForColumns(all_column_names_),
        query_info_.row_level_filter,
        query_info_.prewhere_info)), all_column_names_, query_info_, storage_snapshot_, context_)
    , data_settings(std::move(data_settings_))
    , reader_settings(MergeTreeReaderSettings::createForQuery(context_, *data_settings, query_info_))
    , prepared_parts(std::move(parts_))
    , mutations_snapshot(std::move(mutations_))
    , all_column_names(std::move(all_column_names_))
    , data(data_)
    , actions_settings(ExpressionActionsSettings(context_))
    , block_size{
        .max_block_size_rows = max_block_size_,
        .preferred_block_size_bytes = context->getSettingsRef()[Setting::preferred_block_size_bytes],
        .preferred_max_column_in_block_size_bytes = context->getSettingsRef()[Setting::preferred_max_column_in_block_size_bytes]}
    , requested_num_streams(num_streams_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , log(std::move(log_))
    , analyzed_result_ptr(analyzed_result_ptr_)
    , is_parallel_reading_from_replicas(enable_parallel_reading_)
    , number_of_current_replica(number_of_current_replica_)
{
    if (is_parallel_reading_from_replicas)
    {
        /// Taken exactly as given: a read marked by `enableParallelReadingFromReplicasForSerialization`
        /// is coordinated but carries no callbacks, because it is executed on the replicas, not here.
        all_ranges_callback = std::move(all_ranges_callback_);
        read_task_callback = std::move(read_task_callback_);
    }

    const auto & settings = context->getSettingsRef();
    /// The `max_streams_for_merge_tree_reading` setting is bounded by `doSettingsSanityCheckClamp`.
    if (const UInt64 max_streams_for_merge_tree_reading = settings[Setting::max_streams_for_merge_tree_reading])
    {
        if (settings[Setting::allow_asynchronous_read_from_io_pool_for_merge_tree])
        {
            /// When async reading is enabled, allow to read using more streams.
            /// Will add resize to output_streams_limit to reduce memory usage.
            output_streams_limit = std::min<size_t>(requested_num_streams, max_streams_for_merge_tree_reading);
            /// We intentionally set `max_streams` to 1 in InterpreterSelectQuery in case of small limit.
            /// Changing it here to `max_streams_for_merge_tree_reading` proven itself as a threat for performance.
            if (requested_num_streams != 1)
                requested_num_streams = std::max<size_t>(requested_num_streams, max_streams_for_merge_tree_reading);
        }
        else
            /// Just limit requested_num_streams otherwise.
            requested_num_streams = std::min<size_t>(requested_num_streams, max_streams_for_merge_tree_reading);
    }
    /// `requested_num_streams` drives pipes.reserve()/resize() downstream, which throws
    /// std::length_error when unbounded. It can be amplified past any setting clamp via
    /// `max_streams_to_max_threads_ratio`, so bound the effective value here too.
    requested_num_streams = std::min<size_t>(requested_num_streams, 256 * getNumberOfCPUCoresToUse());

    /// Add explicit description.
    std::string description = data.getStorageID().getFullNameNotQuoted();
    setStepDescription(description, context->getSettingsRef()[Setting::query_plan_max_step_description_length]);
    enable_vertical_final = query_info.isFinal() && context->getSettingsRef()[Setting::enable_vertical_final]
        && data.merging_params.mode == MergeTreeData::MergingParams::Replacing;
}

std::unique_ptr<ReadFromMergeTree> ReadFromMergeTree::createLocalParallelReplicasReadingStep(
    ContextPtr & context_,
    AnalysisResultPtr analyzed_result_ptr_,
    MergeTreeAllRangesCallback all_ranges_callback_,
    MergeTreeReadTaskCallback read_task_callback_,
    size_t replica_number)
{
    const bool enable_parallel_reading = true;
    auto parallel_replicas_step = std::make_unique<ReadFromMergeTree>(
        /// Optimized version of getParts() to avoid extra copy
        analyzed_result_ptr ? std::make_shared<RangesInDataParts>(analyzed_result_ptr->parts_with_ranges) : prepared_parts,
        mutations_snapshot,
        all_column_names,
        data,
        data_settings,
        getQueryInfo(),
        getStorageSnapshot(),
        context_,
        block_size.max_block_size_rows,
        requested_num_streams,
        max_block_numbers_to_read,
        log,
        std::move(analyzed_result_ptr_),
        enable_parallel_reading,
        all_ranges_callback_,
        read_task_callback_,
        replica_number);
    /// This replaces the read step in place, so it must carry over the same state as `clone`: a step
    /// that was already stamped by `tryOptimizeTopK` would otherwise look like a plain read here and
    /// consult or populate the query condition cache under the unsalted condition hash. As in `clone`,
    /// copy `top_k_filter_info` by value - the part-set salt is already folded into its `condition_hash`
    /// by `setTopKColumn` - and copy `allow_query_condition_cache`, which is what actually gates both
    /// index analysis and the reader.
    parallel_replicas_step->allow_query_condition_cache = allow_query_condition_cache;
    parallel_replicas_step->top_k_filter_info = top_k_filter_info;
    /// Same for the text-index read tasks: `createLocalPlanForParallelReplicas` runs the full plan
    /// optimization, so the replaced step can already have a predicate rewritten to `__text_index_*`
    /// virtual columns that only this task map materializes.
    parallel_replicas_step->index_read_tasks = index_read_tasks;
    return parallel_replicas_step;
}

/// Returns nullptr when no index is applied at data-read time or the feature is disabled.
static MergeTreeReadRangesRefinerPtr createIndexReadRangesRefiner(
    const MergeTreeIndexBuildContextPtr & index_build_context,
    const StorageMetadataPtr & metadata_snapshot,
    const Settings & settings)
{
    if (!index_build_context)
        return nullptr;

    if (!settings[Setting::use_indexes_refiner_in_read_pools])
        return nullptr;

    /// Refining at task-cut time could snapshot JOIN runtime filters before they are published.
    /// Keep the build at reader initialization instead.
    if (index_build_context->index_reader_pool->hasRuntimeFilters())
        return nullptr;

    return std::make_shared<IndexReadRangesRefiner>(index_build_context, metadata_snapshot);
}

Pipe ReadFromMergeTree::readFromPoolParallelReplicas(
    RangesInDataParts parts_with_range,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    Names required_columns,
    PoolSettings pool_settings)
{
    const auto & client_info = context->getClientInfo();

    auto extension = ParallelReadingExtension{
        all_ranges_callback.value(),
        read_task_callback.value(),
        number_of_current_replica.value_or(client_info.number_of_current_replica),
        context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
        data.getStorageID().getFullTableName()};

    auto pool = std::make_shared<MergeTreeReadPoolParallelReplicas>(
        extension,
        std::move(parts_with_range),
        mutations_snapshot,
        shared_virtual_fields,
        index_read_tasks,
        storage_snapshot,
        query_info.row_level_filter,
        query_info.prewhere_info,
        actions_settings,
        reader_settings,
        required_columns,
        pool_settings,
        block_size,
        context);

    pool->setReadRangesRefiner(createIndexReadRangesRefiner(index_build_context, storage_snapshot->metadata, context->getSettingsRef()));

    /// Default pool ignores the announcement response. The latter is relevant only to InOrder
    /// reading where we split the table into multiple streams.
    std::ignore = extension.sendInitialRequest(
        CoordinationMode::Default,
        pool->buildAnnouncementDescriptions(),
        pool->getMarkSegmentSize(),
        pool->getMinMarksPerRequest());

    Pipes pipes;

    for (size_t i = 0; i < pool_settings.threads; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool,
            std::move(algorithm),
            query_info.row_level_filter,
            query_info.prewhere_info,
            index_read_tasks,
            actions_settings,
            reader_settings,
            index_build_context,
            lazy_materializing_rows,
            &storage_snapshot->metadata->getColumns());

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), data.getLogName());
        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}


Pipe ReadFromMergeTree::readFromPool(
    RangesInDataParts parts_with_range,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    Names required_columns,
    PoolSettings pool_settings)
{
    size_t total_rows = parts_with_range.getRowsCountAllParts();

    if (query_info.trivial_limit > 0 && query_info.trivial_limit < total_rows)
        total_rows = query_info.trivial_limit;

    const auto & settings = context->getSettingsRef();

    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (block_size.max_block_size_rows && !data.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = (*data_settings)[MergeTreeSetting::index_granularity];
        pool_settings.min_marks_for_concurrent_read
            = (pool_settings.min_marks_for_concurrent_read * fixed_index_granularity + block_size.max_block_size_rows - 1)
            / block_size.max_block_size_rows * block_size.max_block_size_rows / fixed_index_granularity;
    }

    bool all_parts_are_remote = true;
    bool all_parts_are_local = true;
    for (const auto & part : parts_with_range)
    {
        const bool is_remote = part.data_part->isStoredOnRemoteDisk();
        all_parts_are_local &= !is_remote;
        all_parts_are_remote &= is_remote;
    }

    MergeTreeReadPoolPtr pool;

    bool allow_prefetched_remote = all_parts_are_remote && settings[Setting::allow_prefetched_read_pool_for_remote_filesystem]
        && MergeTreePrefetchedReadPool::checkReadMethodAllowed(reader_settings.read_settings.remote_fs_settings.method);

    bool allow_prefetched_local = all_parts_are_local && settings[Setting::allow_prefetched_read_pool_for_local_filesystem]
        && MergeTreePrefetchedReadPool::checkReadMethodAllowed(reader_settings.read_settings.local_fs_settings.method);

    /** Do not use prefetched read pool if query is trivial limit query.
      * Because time spend during filling per thread tasks can be greater than whole query
      * execution for big tables with small limit.
      */
    bool use_prefetched_read_pool = query_info.trivial_limit == 0 && !query_info.small_limit_above_array_join
        && (allow_prefetched_remote || allow_prefetched_local);

    if (use_prefetched_read_pool)
    {
        auto prefetched_pool = std::make_shared<MergeTreePrefetchedReadPool>(
            std::move(parts_with_range),
            mutations_snapshot,
            shared_virtual_fields,
            index_read_tasks,
            storage_snapshot,
            query_info.row_level_filter,
            query_info.prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            block_size,
            context,
            dataflow_cache_updater);

        prefetched_pool->setReadRangesRefiner(createIndexReadRangesRefiner(index_build_context, storage_snapshot->metadata, settings));
        pool = std::move(prefetched_pool);
    }
    else
    {
        auto read_pool = std::make_shared<MergeTreeReadPool>(
            std::move(parts_with_range),
            mutations_snapshot,
            shared_virtual_fields,
            index_read_tasks,
            storage_snapshot,
            query_info.row_level_filter,
            query_info.prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            block_size,
            context,
            dataflow_cache_updater);

        read_pool->setReadRangesRefiner(createIndexReadRangesRefiner(index_build_context, storage_snapshot->metadata, settings));
        pool = std::move(read_pool);
    }

    LOG_DEBUG(log, "Reading approx. {} rows with {} streams", total_rows, pool_settings.threads);

    Pipes pipes;
    for (size_t i = 0; i < pool_settings.threads; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool,
            std::move(algorithm),
            query_info.row_level_filter,
            query_info.prewhere_info,
            index_read_tasks,
            actions_settings,
            reader_settings,
            index_build_context,
            lazy_materializing_rows,
            &storage_snapshot->metadata->getColumns());

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), data.getLogName());

        if (i == 0)
            source->addTotalRowsApprox(total_rows);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (output_streams_limit && output_streams_limit < pipe.numOutputPorts())
        pipe.resize(output_streams_limit);
    return pipe;
}

Pipe ReadFromMergeTree::readInOrder(
    RangesInDataParts parts_with_ranges,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    Names required_columns,
    PoolSettings pool_settings,
    ReadType read_type,
    UInt64 read_limit,
    std::optional<size_t> split_index,
    const MergeTreeReadTask::BlockSizeParams * block_size_override)
{
    const auto & read_block_size = block_size_override ? *block_size_override : block_size;

    /// For reading in order it makes sense to read only
    /// one range per task to reduce number of read rows.
    const bool has_hard_limit_below_one_block = read_type != ReadType::Default && read_limit && read_limit < read_block_size.max_block_size_rows;
    const bool has_soft_limit_below_one_block = read_type != ReadType::Default && query_task_size_limit && query_task_size_limit < read_block_size.max_block_size_rows;

    const bool use_virtual_row = virtual_row_conversion && (read_type == ReadType::InOrder || read_type == ReadType::InReverseOrder);
    const bool use_virtual_row_per_block = use_virtual_row && context->getSettingsRef()[Setting::read_in_order_use_virtual_row_per_block];

    if (use_virtual_row_per_block && read_type == ReadType::InReverseOrder)
        reader_settings.force_read_complete_granules = true;

    MergeTreeReadPoolPtr pool;

    /// Used when reading multiple table splits with parallel replicas. The initiator node owns
    /// the decision of which parts are assigned to which split (in particular, because it is
    /// the only node that actually does index analysis by default). It communicates its decision
    /// in response to the announcement request and followers should use that to filter out parts
    /// that don't belong to the given split. This is only relevant for InOrder reading,
    /// because the Default reading mode doesn't split the table into multiple streams.
    std::optional<std::set<std::pair<MergeTreePartInfo, String>>> initiator_selected_parts;

    if (is_parallel_reading_from_replicas)
    {
        const auto & client_info = context->getClientInfo();
        /// Each split gets its own stream_id so the coordinator maintains an independent
        /// ImplInterface instance per split. When splitting, suffix every split with `#split_{i}`.
        /// When the whole table is read by a single pool, keep the bare table name.
        String stream_id = data.getStorageID().getFullTableName();
        if (split_index)
            stream_id += fmt::format("#split_{}", *split_index);

        ParallelReadingExtension extension{
            all_ranges_callback.value(),
            read_task_callback.value(),
            number_of_current_replica.value_or(client_info.number_of_current_replica),
            context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
            std::move(stream_id)};

        CoordinationMode mode = read_type == ReadType::InOrder
            ? CoordinationMode::WithOrder
            : CoordinationMode::ReverseOrder;

        auto in_order_pool = std::make_shared<MergeTreeReadPoolParallelReplicasInOrder>(
            extension,
            mode,
            parts_with_ranges,
            mutations_snapshot,
            shared_virtual_fields,
            index_read_tasks,
            has_hard_limit_below_one_block,
            has_soft_limit_below_one_block,
            storage_snapshot,
            query_info.row_level_filter,
            query_info.prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            read_block_size,
            context);

        in_order_pool->setReadRangesRefiner(
            createIndexReadRangesRefiner(index_build_context, storage_snapshot->metadata, context->getSettingsRef()));

        /// The response tells us exactly which parts this stream owns: phantom parts are skipped
        /// during source construction below, so the pool never sees `getTask` for them.
        auto response = extension.sendInitialRequest(
            mode,
            in_order_pool->buildAnnouncementDescriptions(),
            /*mark_segment_size=*/0,
            in_order_pool->getMinMarksPerRequest());

        if (response)
        {
            initiator_selected_parts.emplace();
            for (const auto & part : response->parts)
                initiator_selected_parts->emplace(part.info, part.projection_name);
        }

        pool = std::move(in_order_pool);
    }
    else
    {
        auto in_order_pool = std::make_shared<MergeTreeReadPoolInOrder>(
            has_hard_limit_below_one_block,
            has_soft_limit_below_one_block,
            read_type,
            parts_with_ranges,
            mutations_snapshot,
            shared_virtual_fields,
            index_read_tasks,
            storage_snapshot,
            query_info.row_level_filter,
            query_info.prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            read_block_size,
            context,
            dataflow_cache_updater);

        in_order_pool->setReadRangesRefiner(
            createIndexReadRangesRefiner(index_build_context, storage_snapshot->metadata, context->getSettingsRef()));
        pool = std::move(in_order_pool);
    }

    /// If parallel replicas enabled, set total rows in progress here only on initiator with local plan
    /// Otherwise rows will counted multiple times
    const UInt64 in_order_limit = query_info.input_order_info ? query_info.input_order_info->limit : 0;
    const bool set_total_rows_approx = !is_parallel_reading_from_replicas || isParallelReplicasLocalPlanForInitiator();

    Pipes pipes;
    /// Every processor of the pool runs the same PREWHERE: build it once.
    const MergeTreeSelectProcessor * first_processor = nullptr;
    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        const auto & part_with_ranges = parts_with_ranges[i];

        /// On followers, skip constructing source processors for parts the initiator's stream
        /// doesn't own. Projection parts are keyed by parent part info + projection name. If the
        /// initiator didn't send a response (older protocol), `initiator_selected_parts` is
        /// nullopt and we build sources for every part (legacy behavior).
        if (initiator_selected_parts)
        {
            const bool is_projection = part_with_ranges.data_part->isProjectionPart();
            const auto & part_info_for_check = is_projection ? part_with_ranges.parent_part->info : part_with_ranges.data_part->info;
            const String & projection_name_for_check = is_projection ? part_with_ranges.data_part->name : "";
            if (!initiator_selected_parts->contains({part_info_for_check, projection_name_for_check}))
                continue;
        }

        UInt64 total_rows = part_with_ranges.getRowsCount();
        if (query_info.trivial_limit > 0 && query_info.trivial_limit < total_rows)
            total_rows = query_info.trivial_limit;
        else if (in_order_limit > 0 && in_order_limit < total_rows)
            total_rows = in_order_limit;

        /// The split path reads thousands of entries and logs a summary instead.
        if (!block_size_override)
            LOG_TRACE(log, "Reading {} ranges in{}order from part {}, approx. {} rows starting from {}",
            part_with_ranges.ranges.size(),
            read_type == ReadType::InReverseOrder ? " reverse " : " ",
            part_with_ranges.data_part->name, total_rows,
            part_with_ranges.data_part->index_granularity->getMarkStartingRow(part_with_ranges.ranges.front().begin));

        MergeTreeSelectAlgorithmPtr algorithm;
        if (read_type == ReadType::InReverseOrder)
            algorithm = std::make_unique<MergeTreeInReverseOrderSelectAlgorithm>(i);
        else
            algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool,
            std::move(algorithm),
            query_info.row_level_filter,
            query_info.prewhere_info,
            index_read_tasks,
            actions_settings,
            reader_settings,
            index_build_context,
            lazy_materializing_rows,
            &storage_snapshot->metadata->getColumns(),
            first_processor);
        if (!first_processor)
            first_processor = processor.get();
        /// Splitting by key prefix opens a reader per value; most are done long before the merge asks for more.
        if (block_size_override)
            processor->setReleaseFinishedTask();

        processor->addPartLevelToChunk(isQueryWithFinal());

        Block pk_header;
        if (use_virtual_row)
        {
            const auto & primary_key = storage_snapshot->metadata->primary_key;
            size_t num_pk_columns_required = virtual_row_conversion->getRequiredColumnsWithTypes().size();

            ColumnsWithTypeAndName pk_header_columns;
            pk_header_columns.reserve(num_pk_columns_required);
            for (size_t j = 0; j < num_pk_columns_required; ++j)
                pk_header_columns.push_back(
                    {primary_key.data_types[j]->createColumn(), primary_key.data_types[j], primary_key.column_names[j]});

            pk_header = Block(std::move(pk_header_columns));

            if (use_virtual_row_per_block)
                processor->setVirtualRowConversions(virtual_row_conversion, pk_header, read_type == ReadType::InReverseOrder);
        }

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), data.getLogName());
        if (set_total_rows_approx)
            source->addTotalRowsApprox(total_rows);

        Pipe pipe(source);

        if (use_virtual_row)
        {
            const auto & index = part_with_ranges.data_part->getIndex();

            bool has_final_mark = part_with_ranges.data_part->index_granularity->hasFinalMark();
            bool read_in_direct_order = read_type == ReadType::InOrder;
            size_t mark_range_pos = read_in_direct_order ? part_with_ranges.ranges.front().begin : part_with_ranges.ranges.back().end;
            bool has_pk_value = (read_in_direct_order || has_final_mark) && std::ranges::all_of(*index, [&](const auto & col) { return col->size() > mark_range_pos; });

            /// The index may have fewer columns than the primary key if suffix columns were
            /// removed by optimizeIndexColumns (controlled by primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns).
            /// In that case, we cannot apply virtual row optimization because we don't have all required columns.
            auto pk_columns = pk_header.cloneEmptyColumns();
            if (index->size() >= pk_columns.size() && has_pk_value)
            {
                for (size_t j = 0; j < pk_columns.size(); ++j)
                    pk_columns[j]->insert((*(*index)[j])[mark_range_pos]);

                Block pk_block = pk_header.cloneWithColumns(std::move(pk_columns));
                pipe.addSimpleTransform([&](const SharedHeader & header)
                {
                    return std::make_shared<VirtualRowTransform>(header, pk_block, virtual_row_conversion);
                });
            }
        }

        pipes.emplace_back(std::move(pipe));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    /// Empty pipe — return as-is; the caller in `spreadMarkRangesAmongStreamsWithOrder` filters out
    /// empty pipes, and `initializePipeline` substitutes a `NullSource` for an empty top-level pipe.
    if (pipe.empty())
        return pipe;

    if (read_type == ReadType::InReverseOrder)
    {
        pipe.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<ReverseTransform>(header);
        });
    }

    return pipe;
}

Pipe ReadFromMergeTree::read(
    RangesInDataParts parts_with_range,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    Names required_columns,
    ReadType read_type,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache)
{
    const auto & settings = context->getSettingsRef();
    size_t sum_marks = parts_with_range.getMarksCountAllParts();

    const size_t total_query_nodes = is_parallel_reading_from_replicas
        ? std::min<size_t>(
              context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
              context->getSettingsRef()[Setting::max_parallel_replicas])
        : 1;

    PoolSettings pool_settings{
        .threads = max_streams,
        .sum_marks = sum_marks,
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = use_uncompressed_cache,
        .use_const_size_tasks_for_remote_reading = settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
        .total_query_nodes = total_query_nodes,
    };

    if (read_type == ReadType::ParallelReplicas)
        return readFromPoolParallelReplicas(
            std::move(parts_with_range), index_build_context, std::move(required_columns), std::move(pool_settings));

    /// Reading from default thread pool is beneficial for remote storage because of new prefetches.
    if (read_type == ReadType::Default && (max_streams > 1 || checkAllPartsOnRemoteFS(parts_with_range)))
        return readFromPool(
            std::move(parts_with_range), index_build_context, std::move(required_columns), std::move(pool_settings));

    auto pipe = readInOrder(parts_with_range, index_build_context, required_columns, pool_settings, read_type, /*limit=*/0);

    /// Use ConcatProcessor to concat sources together.
    /// It is needed to read in parts order (and so in PK order) if single thread is used.
    if (read_type == ReadType::Default && pipe.numOutputPorts() > 1)
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getSharedHeader(), pipe.numOutputPorts()));

    return pipe;
}

namespace
{

struct PartRangesReadInfo
{
    std::vector<size_t> sum_marks_in_parts;

    size_t sum_marks = 0;
    size_t total_rows = 0;
    size_t adaptive_parts = 0;
    size_t index_granularity_bytes = 0;
    size_t max_marks_to_use_cache = 0;
    size_t min_marks_for_concurrent_read = 0;
    bool use_uncompressed_cache = false;

    PartRangesReadInfo(
        const RangesInDataParts & parts,
        const Settings & settings,
        const MergeTreeSettings & data_settings)
    {
        /// Count marks for each part.
        sum_marks_in_parts.resize(parts.size());

        for (size_t i = 0; i < parts.size(); ++i)
        {
            total_rows += parts[i].getRowsCount();
            sum_marks_in_parts[i] = parts[i].getMarksCount();
            sum_marks += sum_marks_in_parts[i];

            if (parts[i].data_part->index_granularity_info.mark_type.adaptive)
                ++adaptive_parts;
        }

        if (adaptive_parts > parts.size() / 2)
            index_granularity_bytes = data_settings[MergeTreeSetting::index_granularity_bytes];

        max_marks_to_use_cache = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
            settings[Setting::merge_tree_max_rows_to_use_cache],
            settings[Setting::merge_tree_max_bytes_to_use_cache],
            data_settings[MergeTreeSetting::index_granularity],
            index_granularity_bytes);

        auto all_parts_on_remote_disk = checkAllPartsOnRemoteFS(parts);

        size_t min_rows_for_concurrent_read = 0;
        size_t min_bytes_for_concurrent_read = 0;
        if (all_parts_on_remote_disk)
        {
            min_rows_for_concurrent_read = settings[Setting::merge_tree_min_rows_for_concurrent_read_for_remote_filesystem];
            min_bytes_for_concurrent_read = settings[Setting::merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem];
        }
        else
        {
            min_rows_for_concurrent_read = settings[Setting::merge_tree_min_rows_for_concurrent_read];
            min_bytes_for_concurrent_read = settings[Setting::merge_tree_min_bytes_for_concurrent_read];
        }

        min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            min_rows_for_concurrent_read, min_bytes_for_concurrent_read,
            data_settings[MergeTreeSetting::index_granularity], index_granularity_bytes, settings[Setting::merge_tree_min_read_task_size], sum_marks);

        use_uncompressed_cache = settings[Setting::use_uncompressed_cache];
        if (sum_marks > max_marks_to_use_cache)
            use_uncompressed_cache = false;
    }
};

}

Pipe ReadFromMergeTree::readByLayers(
    const RangesInDataParts & parts_with_ranges,
    SplitPartsByRanges split_parts,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    const Names & column_names,
    const InputOrderInfoPtr & input_order_info)
{
    const auto & settings = context->getSettingsRef();

    LOG_TRACE(log, "Spreading mark ranges among streams (reading by layers)");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);
    if (0 == info.sum_marks)
        return {};

    ReadingInOrderStepGetter reading_step_getter;
    Names in_order_column_names_to_read;
    SortDescription sort_description;

    if (reader_settings.read_in_order)
    {
        /// `PREWHERE` runs before the sorting expression added below and may have removed an input
        /// column that the sorting key needs. Prohibit removing those inputs; the sorting expression
        /// keeps them, and they are dropped when the pipe header is converted to the step header.
        /// Same reasoning as in `spreadMarkRangesAmongStreamsWithOrder`.
        if (query_info.prewhere_info || query_info.row_level_filter)
        {
            NameSet sorting_key_columns;
            for (const auto & column : storage_snapshot->metadata->getSortingKey().expression->getRequiredColumnsWithTypes())
                sorting_key_columns.insert(column.name);

            restorePrewhereInputs(query_info.row_level_filter.get(), query_info.prewhere_info.get(), sorting_key_columns);
        }

        NameSet column_names_set(column_names.begin(), column_names.end());
        in_order_column_names_to_read = column_names;

        /// Add columns needed to calculate the sorting expression
        for (const auto & column_name : storage_snapshot->metadata->getColumnsRequiredForSortingKey())
        {
            if (column_names_set.contains(column_name))
                continue;

            in_order_column_names_to_read.push_back(column_name);
            column_names_set.insert(column_name);
        }
        auto sorting_expr = storage_snapshot->metadata->getSortingKey().expression;
        const auto & sorting_columns = storage_snapshot->metadata->getSortingKey().column_names;
        std::vector<bool> reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();

        sort_description.compile_sort_description = settings[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

        sort_description.reserve(input_order_info->used_prefix_of_sorting_key_size);
        for (size_t i = 0; i < input_order_info->used_prefix_of_sorting_key_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction * -1);
            else
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction);
        }

        ReadType in_order_read_type = input_order_info->direction > 0 ? ReadType::InOrder : ReadType::InReverseOrder;

        reading_step_getter
            = [this, &index_build_context, &in_order_column_names_to_read, &info, sorting_expr, &sort_description, in_order_read_type](auto parts)
        {
            auto pipe = this->read(
                std::move(parts),
                index_build_context,
                in_order_column_names_to_read,
                in_order_read_type,
                1 /* num_streams */,
                0 /* min_marks_for_concurrent_read */,
                info.use_uncompressed_cache);

            if (pipe.empty())
            {
                auto header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
                    storage_snapshot->getSampleBlockForColumns(in_order_column_names_to_read),
                    query_info.row_level_filter,
                    query_info.prewhere_info));
                pipe = Pipe(std::make_shared<NullSource>(header));
            }

            pipe.addSimpleTransform([sorting_expr](const SharedHeader & header)
            {
                return std::make_shared<ExpressionTransform>(header, sorting_expr);
            });

            if (pipe.numOutputPorts() != 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getSharedHeader(),
                    pipe.numOutputPorts(),
                    sort_description,
                    block_size.max_block_size_rows,
                    /*max_block_size_bytes=*/ 0,
                    /*max_dynamic_subcolumns*/ std::nullopt,
                    SortingQueueStrategy::Batch,
                    /*limit=*/ 0,
                    /*always_read_till_end=*/ false,
                    /*out_row_sources_buf=*/ nullptr,
                    /*filter_column_name=*/ std::nullopt,
                    /*use_average_block_sizes=*/ false,
                    /*apply_virtual_row_conversions=*/ false);

                pipe.addTransform(std::move(transform));
            }

            return pipe;
        };
    }
    else
    {
        reading_step_getter = [this, &index_build_context, &column_names, &info](auto parts)
        {
            return this->read(
                std::move(parts),
                index_build_context,
                column_names,
                ReadType::Default,
                1 /* num_streams */,
                info.min_marks_for_concurrent_read,
                info.use_uncompressed_cache);
        };
    }

    auto pipes = ::readByLayers(
        std::move(split_parts),
        storage_snapshot->metadata->getPrimaryKey(),
        std::move(reading_step_getter),
        context);
    return Pipe::unitePipes(std::move(pipes));
}

/// Whether the whole-part size of a column can be scaled by the fraction of selected rows.
///
/// A variable-width type cannot: large values may be concentrated entirely in the selected range.
/// Neither can `LowCardinality`, whose dictionary is written per part rather than per row, so
/// reading a handful of rows still pulls in the dictionary that serves them.
/// `DataTypeLowCardinality::haveMaximumSizeOfValue` delegates to the dictionary type, so a
/// fixed-size dictionary would otherwise pass the width check.
static bool canScaleSizeBySelectedRows(const IDataType & type)
{
    if (!type.haveMaximumSizeOfValue())
        return false;

    bool has_low_cardinality = type.lowCardinality();
    /// `LowCardinality` may sit below `Array`, `Nullable`, `Tuple` and friends.
    type.forEachChild([&](const IDataType & child)
    {
        has_low_cardinality |= child.lowCardinality();
    });

    return !has_low_cardinality;
}

/// Mirrors `injectRequiredColumnsRecursively`: a column that is absent from a part is filled from its
/// default expression, and evaluating that expression may require reading other physical columns of
/// the part. Returns true when at least one physical column has to be read for `column_name`.
///
/// A default that expands to no physical identifiers - an explicit `ALTER TABLE ... ADD COLUMN c UInt8
/// DEFAULT 0`, or the implicit type default of a newly added column - reads nothing, so such a column
/// costs no bytes and does not have to disable the estimate.
template <typename TryGetColumnInPart>
static bool missingColumnReadsPhysicalColumns(
    const String & column_name,
    const StorageSnapshotPtr & storage_snapshot,
    const GetColumnsOptions & storage_options,
    const TryGetColumnInPart & try_get_column_in_part,
    NameSet & visited)
{
    /// Defaults can be cyclic, and the expression can be arbitrarily deep.
    checkStackSize();

    if (!visited.emplace(column_name).second)
        return false;

    if (try_get_column_in_part(column_name))
        return true;

    const auto col_in_storage = storage_snapshot->tryGetColumn(storage_options, column_name);

    /// The part may predate a metadata-only `ALTER MODIFY COLUMN`, so it stores the parent column but
    /// not the requested subcolumn of the new type. The reader then reads the parent and extracts the
    /// subcolumn from it.
    if (col_in_storage && col_in_storage->isSubcolumn() && try_get_column_in_part(col_in_storage->getNameInStorage()))
        return true;

    auto column_default = storage_snapshot->getDefault(column_name);
    /// A subcolumn has no default expression of its own: it is extracted from the evaluated default of
    /// the column in storage (see `IMergeTreeReader::evaluateMissingDefaults`).
    if (!column_default && col_in_storage && col_in_storage->isSubcolumn())
        column_default = storage_snapshot->getDefault(col_in_storage->getNameInStorage());

    if (!column_default || !column_default->expression)
        return false;

    IdentifierNameSet identifiers;
    column_default->expression->collectIdentifierNames(identifiers);

    for (const auto & identifier : identifiers)
    {
        if (missingColumnReadsPhysicalColumns(identifier, storage_snapshot, storage_options, try_get_column_in_part, visited))
            return true;
    }

    return false;
}

/// Which of the two sizes a part records for a column the estimate below should sum up.
enum class ReadBytesKind : uint8_t
{
    /// The bytes the values occupy once decoded, i.e. the amount of work the pipeline does.
    Uncompressed,
    /// The bytes the values occupy on disk, i.e. the amount of data the read pulls in.
    Compressed,
};

/// Estimate the size of `column_names` over the mark ranges actually selected in `parts_with_ranges`.
///
/// Returns nullopt if the estimate cannot be made conservatively, in which case the caller must not
/// rely on it.
///
/// `kind` picks which size to sum, and the choice belongs to the caller's question. Stream capping
/// wants `Uncompressed`: the per-stream overhead it trades against is proportional to the work done
/// per stream, which scales with the number of values processed, not with how well they compress. A
/// highly compressible column (e.g. a constant `UInt64` under `ZSTD(9)`, ~1400x) is tiny on disk yet
/// still feeds every row through PREWHERE, expressions and aggregation. Sizing a read against a
/// byte threshold wants `Compressed`, which is what the read actually pulls off disk.
static std::optional<size_t> estimateReadBytes(
    const RangesInDataParts & parts_with_ranges,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    const ContextPtr & context,
    const Settings & settings,
    ReadBytesKind kind)
{
    const auto size_of = [kind](const ColumnSize & column_size)
    { return kind == ReadBytesKind::Compressed ? column_size.data_compressed : column_size.data_uncompressed; };

    const bool use_subcolumn_sizes = settings[Setting::allow_calculating_subcolumns_sizes_for_merge_tree_reading];
    const auto & virtuals = storage_snapshot->metadata->virtuals;

    /// A metadata-only `ALTER TABLE ... RENAME COLUMN` does not rewrite the part: until the mutation
    /// is applied the part still holds the old name, and the reader resolves the new name through
    /// `AlterConversions`. Resolve it the same way here, otherwise the whole scan would be treated
    /// as unknown. Other mutation kinds do not rename anything, so skip the work when there are none.
    const bool resolve_renames = mutations_snapshot && mutations_snapshot->hasMetadataMutations();
    const auto storage_options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();

    size_t total_bytes = 0;

    for (const auto & part : parts_with_ranges)
    {
        const auto & data_part = *part.data_part;

        const size_t part_marks = data_part.getMarksCount();
        if (part_marks == 0)
            continue;

        AlterConversionsPtr alter_conversions;
        /// A projection part carries a fake data version, which makes every mutation look pending,
        /// so the conversions computed for it would not describe its data. `injectRequiredColumns`
        /// skips them for the same reason.
        if (resolve_renames && !data_part.isProjectionPart())
            alter_conversions = MergeTreeData::getAlterConversionsForPart(part.data_part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
                , context->getAccess()->getEnabledMaskingPolicies()
#endif
            );

        const bool share_nested = (*data_part.storage.getSettings())[MergeTreeSetting::share_nested_offsets];

        /// The name of a requested column as it is stored in this particular part.
        auto try_get_column_in_part = [&](const String & col_name) -> std::optional<NameAndTypePair>
        {
            auto col = data_part.tryGetColumn(col_name);

            if (!alter_conversions)
                return col;

            const auto col_in_storage = storage_snapshot->tryGetColumn(storage_options, col_name);
            if (!col_in_storage)
                return col;

            auto name_in_part = col_in_storage->getNameInStorage();
            if (!col && alter_conversions->isColumnRenamed(name_in_part))
            {
                name_in_part = alter_conversions->getColumnOldName(name_in_part);
                col = data_part.tryGetColumn(col_in_storage->isSubcolumn()
                    ? Nested::concatenateName(name_in_part, col_in_storage->getSubcolumnName())
                    : name_in_part);
            }

            /// A pending `DROP COLUMN` leaves the data in the part, but it is stale: the reader treats
            /// such a column as missing and fills it from the default expression, which may read wide
            /// physical columns this estimate knows nothing about. That happens when a column is
            /// dropped and re-added under the same name, so the name alone does not tell them apart.
            if (col && alter_conversions->isColumnDropped(name_in_part, share_nested))
                return {};

            return col;
        };

        /// Only a fraction of the part may survive primary key / partition pruning. Scaling by it
        /// keeps the cap meaningful for selective queries, which would otherwise be sized as if the
        /// whole part were read.
        const size_t selected_rows = std::min(part.getRowsCount(), data_part.rows_count);
        if (selected_rows == 0)
            continue;

        /// Several requested subcolumns can share streams. Group them by their physical column so
        /// multiple subcolumns are never charged more than the complete physical column.
        std::unordered_map<String, NameSet> requested_names_by_column;
        for (const auto & col_name : column_names)
        {
            const auto col = try_get_column_in_part(col_name);
            if (!col)
            {
                if (isTextIndexVirtualColumn(col_name))
                    return std::nullopt;
                if (virtuals.tryGet(col_name, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::Reader))
                    continue;

                /// A column missing from the part is filled from its default. Defaults and mutation
                /// steps can make the reader fetch other physical columns, whose bytes are unknown
                /// here because the dependency set is built later for each read task, so do not cap
                /// in that case. A default that reads nothing costs nothing, and the estimate over
                /// the remaining columns stays valid.
                NameSet visited;
                if (missingColumnReadsPhysicalColumns(col_name, storage_snapshot, storage_options, try_get_column_in_part, visited))
                    return std::nullopt;
                continue;
            }

            /// Per-column sizes are stored for the whole part, so they can only be scaled down for a
            /// partial read when the size really is proportional to the number of rows.
            if (selected_rows < data_part.rows_count && !canScaleSizeBySelectedRows(*col->type))
                return std::nullopt;

            /// `col->name` rather than `col_name`: the per-column sizes below are keyed by the name
            /// the part was written with, which differs for a not-yet-applied rename.
            requested_names_by_column[col->getNameInStorage()].emplace(col->name);
        }

        /// Virtual-only reads inject the smallest physical column to determine the number of rows.
        if (requested_names_by_column.empty())
        {
            auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
                .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);
            NamesAndTypesList available_columns;
            for (const auto & column : data_part.getColumns())
                if (storage_snapshot->tryGetColumn(options, column.name))
                    available_columns.push_back(column);

            if (available_columns.empty())
                available_columns = data_part.getColumns();

            const auto physical_name = data_part.getColumnNameWithMinimumCompressedSize(available_columns);

            /// The injected column is charged like a requested one, so it has to pass the same
            /// scaling check. The smallest column of a part can well be a `String` or a
            /// `LowCardinality`, whose whole-part size is not proportional to the selected rows.
            const auto injected_col = data_part.tryGetColumn(physical_name);
            if (selected_rows < data_part.rows_count
                && (!injected_col || !canScaleSizeBySelectedRows(*injected_col->type)))
                return std::nullopt;

            requested_names_by_column[physical_name].emplace(physical_name);
        }

        size_t part_bytes = 0;
        bool part_bytes_known = true;

        for (const auto & [physical_name, requested_names] : requested_names_by_column)
        {
            size_t col_bytes = 0;
            if (requested_names.size() == 1)
            {
                const auto & requested_name = *requested_names.begin();
                const auto col = data_part.tryGetColumn(requested_name);
                if (col && col->isSubcolumn() && use_subcolumn_sizes)
                    col_bytes = size_of(data_part.getSubcolumnSize(requested_name));
            }

            /// Multiple subcolumns may overlap in streams. The complete physical column is a safe
            /// upper bound that counts every shared stream exactly once.
            if (col_bytes == 0)
            {
                /// If subcolumn pricing is unavailable, the fallback below charges the complete
                /// physical column. Its type, rather than the requested subcolumn type, determines
                /// whether the whole-part size can be scaled by the selected rows.
                const auto physical_col = data_part.tryGetColumn(physical_name);
                if (selected_rows < data_part.rows_count
                    && (!physical_col || !canScaleSizeBySelectedRows(*physical_col->type)))
                    return std::nullopt;

                col_bytes = size_of(data_part.getColumnSize(physical_name));
            }

            if (col_bytes == 0)
            {
                /// Compact parts do not track per-column sizes, so `getColumnSize` yields 0 there.
                /// Fall back to the whole part rather than silently under-counting this column.
                part_bytes_known = false;
                break;
            }

            if (__builtin_add_overflow(part_bytes, col_bytes, &part_bytes))
            {
                part_bytes = std::numeric_limits<size_t>::max();
                break;
            }
        }

        if (!part_bytes_known)
        {
            /// Compact parts only expose the size of the shared data file. Scaling that size by rows
            /// is not conservative when variable-size data is distributed unevenly between granules.
            if (selected_rows < data_part.rows_count)
                return std::nullopt;

            part_bytes = size_of(data_part.getTotalColumnsSize());
        }

        const auto selected_bytes_wide
            = (static_cast<UInt128>(part_bytes) * selected_rows + data_part.rows_count - 1) / data_part.rows_count;
        const size_t selected_bytes = selected_bytes_wide > std::numeric_limits<size_t>::max()
            ? std::numeric_limits<size_t>::max()
            : static_cast<size_t>(selected_bytes_wide);

        if (__builtin_add_overflow(total_bytes, selected_bytes, &total_bytes))
            return std::numeric_limits<size_t>::max();
    }

    return total_bytes;
}

/// Cap the number of read streams based on the estimated size of the data being read.
///
/// The mark-based stream reduction uses index_granularity_bytes (e.g. 10 MB) as a proxy for bytes
/// per mark. For narrow columns (e.g. UInt16 where each mark is ~16 KB uncompressed), this
/// overestimates by hundreds of times and creates far too many streams. Each stream spawns a full
/// pipeline processor chain
/// whose overhead (thread scheduling, graph traversal, aggregate state init/merge) grows superlinearly
/// with stream count.
///
/// Cost model: T(N) = W/N + F + V·N, where W = useful work, F = fixed overhead, V = per-stream
/// variable cost. Optimal N = sqrt(W/V). Expressing in bytes: W = total_bytes / throughput, and
/// C = throughput · V is the byte-equivalent per-stream overhead cost, giving:
///     optimal_N = sqrt(total_bytes / C)
/// The setting merge_tree_min_bytes_per_read_stream provides C (default 64 KB, 0 disables).
///
/// Never reduce below this many streams: the per-stream overhead only dominates once the pipeline is
/// wide, so capping a narrow pipeline changes its shape without winning anything.
static constexpr size_t MIN_STREAMS_TO_CAP_BY_READ_BYTES = 16;

static void capStreamsByEstimatedReadBytes(
    size_t & num_streams,
    size_t total_bytes,
    const Settings & settings,
    LoggerPtr log)
{
    const size_t overhead_cost_bytes = settings[Setting::merge_tree_min_bytes_per_read_stream];
    if (overhead_cost_bytes == 0)
        return;

    if (total_bytes == 0)
        return;

    /// Round up: truncating sqrt(3.99) to 1 would halve the streams for a rounding artefact.
    const size_t max_streams_by_volume = std::max<size_t>(
        1, static_cast<size_t>(std::ceil(std::sqrt(static_cast<double>(total_bytes) / static_cast<double>(overhead_cost_bytes)))));

    /// The stream count also sets the width of everything downstream of the read - PREWHERE,
    /// expression evaluation and aggregation - but the estimate above only measures how many bytes
    /// are read, not how much CPU work each of those bytes costs. A CPU-bound query over a small
    /// column (several hash functions per row, or a high-cardinality `GROUP BY`) would lose most of
    /// its parallelism to a cap derived purely from data volume. Two lower bounds keep that bounded:
    ///
    /// - a fixed fraction of the requested streams, so the worst case for such a query stays
    ///   bounded no matter how wide the pipeline is;
    /// - an absolute floor, because the per-stream overhead this cap removes only dominates once the
    ///   pipeline is wide. Below the floor there is nothing to win, while narrowing the read to a
    ///   single stream would change the shape of the whole pipeline (a one-stream read is planned as
    ///   a concatenation read in order rather than a thread pool), which is a large behaviour change
    ///   to make for no gain.
    const size_t min_streams = std::max<size_t>(MIN_STREAMS_TO_CAP_BY_READ_BYTES, num_streams / 4);
    const size_t capped_num_streams = std::max(max_streams_by_volume, min_streams);

    if (capped_num_streams < num_streams)
    {
        LOG_DEBUG(log, "Reducing num_streams from {} to {} (estimated_read_bytes={}, overhead_cost={}, by_volume={})",
            num_streams, capped_num_streams, total_bytes, overhead_cost_bytes, max_streams_by_volume);
        num_streams = capped_num_streams;
    }
}

static void capStreamsByReadBytes(
    size_t & num_streams,
    const RangesInDataParts & parts_with_ranges,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    const ContextPtr & context,
    const Settings & settings,
    LoggerPtr log)
{
    const auto estimated_read_bytes = estimateReadBytes(
        parts_with_ranges, column_names, storage_snapshot, mutations_snapshot, context, settings, ReadBytesKind::Uncompressed);
    if (!estimated_read_bytes)
        return;

    capStreamsByEstimatedReadBytes(
        num_streams,
        *estimated_read_bytes,
        settings,
        log);
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreams(
    RangesInDataParts && parts_with_ranges,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    size_t num_streams,
    const Names & column_names)
{
    const auto & settings = context->getSettingsRef();

    LOG_TRACE(log, "Spreading mark ranges among streams (default reading)");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);
    Names tmp_column_names(column_names.begin(), column_names.end());

    if (0 == info.sum_marks)
        return {};

    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        /// The comparison is done with a division rather than with `num_streams * info.min_marks_for_concurrent_read`,
        /// because `num_streams` is derived from `max_streams_to_max_threads_ratio`, which is a `Float64` setting,
        /// and the product can overflow. `info.min_marks_for_concurrent_read` is always at least one.
        if (info.sum_marks / info.min_marks_for_concurrent_read < num_streams && parts_with_ranges.size() < num_streams)
        {
            /*
            If the data is fragmented, then allocate the size of parts to num_streams. If the data is not fragmented, besides the sum_marks and
            min_marks_for_concurrent_read, involve the system cores to get the num_streams. Increase the num_streams and decrease the min_marks_for_concurrent_read
            if the data is small but system has plentiful cores. It helps to improve the parallel performance of `MergeTreeRead` significantly.
            Make sure the new num_streams `num_streams * increase_num_streams_ratio` will not exceed the previous calculated prev_num_streams.
            The new info.min_marks_for_concurrent_read `info.min_marks_for_concurrent_read / increase_num_streams_ratio` should be larger than 8.
            https://github.com/ClickHouse/ClickHouse/pull/53867
            */
            if ((info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read > parts_with_ranges.size())
            {
                const size_t prev_num_streams = num_streams;
                num_streams = (info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read;
                const size_t increase_num_streams_ratio = std::min(prev_num_streams / num_streams, info.min_marks_for_concurrent_read / 8);
                if (increase_num_streams_ratio > 1)
                {
                    num_streams = num_streams * increase_num_streams_ratio;
                    info.min_marks_for_concurrent_read = (info.sum_marks + num_streams - 1) / num_streams;
                }
            }
            else
                num_streams = parts_with_ranges.size();
        }

        /// The per-stream cost the cap trades against was measured for local reads. Remote reads
        /// have their own latency and prefetch tradeoffs, and they already get separate
        /// `..._for_remote_filesystem` concurrency thresholds above, so leave them alone.
        if (!is_parallel_reading_from_replicas
            && !isQueryWithFinal()
            && !checkAnyPartOnRemoteFS(parts_with_ranges)
            && settings[Setting::merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability] == 0)
            capStreamsByReadBytes(
                num_streams, parts_with_ranges, column_names, storage_snapshot, mutations_snapshot, context, settings, log);
    }

    auto read_type = is_parallel_reading_from_replicas ? ReadType::ParallelReplicas : ReadType::Default;

    double read_split_ranges_into_intersecting_and_non_intersecting_injection_probability
        = static_cast<double>(settings[Setting::merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability]);
    std::bernoulli_distribution fault(read_split_ranges_into_intersecting_and_non_intersecting_injection_probability);

    if (read_type != ReadType::ParallelReplicas &&
        num_streams > 1 &&
        read_split_ranges_into_intersecting_and_non_intersecting_injection_probability > 0.0 &&
        fault(thread_local_rng) &&
        !isQueryWithFinal() &&
        data.merging_params.is_deleted_column.empty() &&
        !query_info.row_level_filter &&
        !query_info.prewhere_info &&
        !reader_settings.use_query_condition_cache && /// the query condition cache produces incorrect results with intersecting ranges
        !isVectorColumnReplaced()) /// Vector search optimization needs ranges & offsets to be stable
    {
        NameSet column_names_set(column_names.begin(), column_names.end());
        Names in_order_column_names_to_read(column_names);

        /// Add columns needed to calculate the sorting expression
        for (const auto & column_name : storage_snapshot->metadata->getColumnsRequiredForSortingKey())
        {
            if (column_names_set.contains(column_name))
                continue;

            in_order_column_names_to_read.push_back(column_name);
            column_names_set.insert(column_name);
        }

        auto in_order_reading_step_getter = [this, &index_build_context, &in_order_column_names_to_read, &info](auto parts)
        {
            return this->read(
                std::move(parts),
                index_build_context,
                in_order_column_names_to_read,
                ReadType::InOrder,
                1 /* num_streams */,
                0 /* min_marks_for_concurrent_read */,
                info.use_uncompressed_cache);
        };

        auto sorting_expr = storage_snapshot->metadata->getSortingKey().expression;

        SplitPartsWithRangesByPrimaryKeyResult split_ranges_result = splitPartsWithRangesByPrimaryKey(
            storage_snapshot->metadata->getPrimaryKey(),
            storage_snapshot->metadata->getSortingKey(),
            std::move(sorting_expr),
            std::move(parts_with_ranges),
            num_streams,
            context,
            std::move(in_order_reading_step_getter),
            true /*split_parts_ranges_into_intersecting_and_non_intersecting_final*/,
            true /*split_intersecting_parts_ranges_into_layers*/);

        auto merging_pipes = std::move(split_ranges_result.merging_pipes);
        auto non_intersecting_parts_ranges_read_pipe = read(
            std::move(split_ranges_result.non_intersecting_parts_ranges),
            index_build_context,
            tmp_column_names,
            read_type,
            num_streams,
            info.min_marks_for_concurrent_read,
            info.use_uncompressed_cache);

        if (merging_pipes.empty())
            return non_intersecting_parts_ranges_read_pipe;

        Pipes pipes;
        pipes.resize(2);
        pipes[0] = Pipe::unitePipes(std::move(merging_pipes));
        pipes[1] = std::move(non_intersecting_parts_ranges_read_pipe);

        auto conversion_action = ActionsDAG::makeConvertingActions(
            pipes[0].getHeader().getColumnsWithTypeAndName(),
            pipes[1].getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);
        auto converting_expr = std::make_shared<ExpressionActions>(std::move(conversion_action));
        pipes[0].addSimpleTransform(
            [converting_expr](const SharedHeader & header)
            {
                return std::make_shared<ExpressionTransform>(header, converting_expr);
            });
        return Pipe::unitePipes(std::move(pipes));
    }

    return read(std::move(parts_with_ranges),
        index_build_context,
        tmp_column_names,
        read_type,
        num_streams,
        info.min_marks_for_concurrent_read,
        info.use_uncompressed_cache);
}

static ActionsDAG createProjection(const Block & header)
{
    return ActionsDAG(header.getNamesAndTypesList());
}

/// Split ranges into smaller ones to avoid reading much data with the first blocks.
/// In the direct reading order only the first few ranges are split (the reading may stop early because of LIMIT).
/// In the reverse reading order all ranges are split, because a whole range has to be kept in memory to reverse it.
static MarkRanges splitRangesToAvoidLargeReads(const MarkRanges & ranges, int direction, size_t rows_granularity, size_t max_block_size)
{
    MarkRanges new_ranges;
    const size_t max_marks_in_range = (max_block_size + rows_granularity - 1) / rows_granularity;
    size_t marks_in_range = 1;

    if (direction == 1)
    {
        /// Split first few ranges to avoid reading much data.
        bool split = false;
        for (auto range : ranges)
        {
            while (!split && range.begin + marks_in_range < range.end)
            {
                new_ranges.emplace_back(range.begin, range.begin + marks_in_range);
                range.begin += marks_in_range;
                marks_in_range *= 2;

                if (marks_in_range > max_marks_in_range)
                    split = true;
            }
            new_ranges.emplace_back(range.begin, range.end);
        }
    }
    else
    {
        /// Split all ranges to avoid reading much data, because we have to
        ///  store whole range in memory to reverse it.
        for (auto it = ranges.rbegin(); it != ranges.rend(); ++it)
        {
            auto range = *it;
            while (range.begin + marks_in_range < range.end)
            {
                new_ranges.emplace_front(range.end - marks_in_range, range.end);
                range.end -= marks_in_range;
                marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
            }
            new_ranges.emplace_front(range.begin, range.end);
        }
    }

    return new_ranges;
}

namespace
{

/// Keeps the rows whose `position` column equals `value`. The input is sorted by that column.
class KeyPrefixValueFilterTransform final : public ISimpleTransform
{
public:
    KeyPrefixValueFilterTransform(SharedHeader header_, size_t position_, ColumnPtr value_)
        : ISimpleTransform(header_, header_, true), position(position_), value(std::move(value_))
    {
    }

    String getName() const override { return "KeyPrefixValueFilterTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        const size_t rows = chunk.getNumRows();
        if (rows == 0)
            return;

        const auto column = chunk.getColumns()[position]->convertToFullIfWrapped();
        if (column->compareAt(0, 0, *value, 1) == 0 && column->compareAt(rows - 1, 0, *value, 1) == 0)
            return;

        IColumn::Filter filter(rows);
        size_t kept = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            filter[i] = column->compareAt(i, 0, *value, 1) == 0;
            kept += filter[i];
        }

        /// The chunk no longer holds every row of its granules that passed the filters so far,
        /// so a downstream filter must not record these granules in the query condition cache.
        chunk.getChunkInfos().extract<MarkRangesInfo>();

        auto columns = chunk.detachColumns();
        for (auto & col : columns)
            col = col->filter(filter, kept);
        chunk.setColumns(std::move(columns), kept);
    }

private:
    const size_t position;
    const ColumnPtr value;
};

/// Where a merge key goes in the packed key of a row: `bytes` wide, at bit `shift` of word `word`.
struct SplitMergeKey
{
    size_t position;
    bool is_signed;
    size_t bytes;
    size_t word = 0;
    size_t shift = 0;
};

/// Packs the keys, most significant first, into 64-bit words so that comparing the words as one big unsigned
/// number compares the keys. The low 32 bits of the last word are left for the rank of the input. Returns the
/// number of words.
size_t layoutSplitMergeKeys(std::vector<SplitMergeKey> & keys)
{
    size_t word = 0;
    size_t used = 0;
    for (auto & key : keys)
    {
        const size_t bits = key.bytes * 8;
        if (used + bits > 64)
        {
            ++word;
            used = 0;
        }
        key.word = word;
        key.shift = 64 - used - bits;
        used += bits;
    }
    if (used > 32)
        ++word;
    return word + 1;
}

template <typename T>
void packSplitMergeKey(const char * data, size_t rows, size_t words, const SplitMergeKey & key, bool descending, UInt64 * out)
{
    constexpr size_t bits = sizeof(T) * 8;
    const T sign = key.is_signed ? static_cast<T>(T(1) << (bits - 1)) : T(0);
    const T flip = descending ? static_cast<T>(~T(0)) : T(0);
    const T mask = sign ^ flip;
    out += key.word;
    for (size_t row = 0; row < rows; ++row)
        out[row * words] |= static_cast<UInt64>(static_cast<T>(unalignedLoad<T>(data + row * sizeof(T)) ^ mask)) << key.shift;
}

/// The packed merge keys of the rows of a chunk, `words` per row (see `layoutSplitMergeKeys`).
struct SplitMergeKeysInfo final : public ChunkInfoCloneable<SplitMergeKeysInfo>
{
    std::vector<UInt64> keys;
};

/// Computes the merge keys of the chunks of one input. It runs on the query threads, in parallel for
/// all inputs, so the merge only compares precomputed keys. Then keeps only the columns of `output_header_`, which
/// leaves out key columns computed only for the merge.
class SplitMergeKeysTransform final : public ISimpleTransform
{
public:
    SplitMergeKeysTransform(SharedHeader header_, SharedHeader output_header_, std::vector<SplitMergeKey> keys_, size_t words_, bool descending_)
        : ISimpleTransform(header_, output_header_, true)
        , keys(std::move(keys_))
        , words(words_)
        , descending(descending_)
    {
        for (const auto & column : *output_header_)
            kept.push_back(header_->getPositionByName(column.name));
    }

    String getName() const override { return "SplitMergeKeysTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        const size_t rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->convertToFullIfWrapped();

        auto info = std::make_shared<SplitMergeKeysInfo>();
        info->keys.resize(rows * words);
        for (const auto & key : keys)
        {
            const auto & column = *columns[key.position];
            if (column.sizeOfValueIfFixed() != key.bytes)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge key {} has {} bytes, expected {}", key.position, column.sizeOfValueIfFixed(), key.bytes);
            const char * data = column.getRawData().data();
            switch (key.bytes)
            {
                case 1: packSplitMergeKey<UInt8>(data, rows, words, key, descending, info->keys.data()); break;
                case 2: packSplitMergeKey<UInt16>(data, rows, words, key, descending, info->keys.data()); break;
                case 4: packSplitMergeKey<UInt32>(data, rows, words, key, descending, info->keys.data()); break;
                case 8: packSplitMergeKey<UInt64>(data, rows, words, key, descending, info->keys.data()); break;
                default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported merge key size {}", key.bytes);
            }
        }
        Columns output;
        output.reserve(kept.size());
        for (size_t position : kept)
            output.push_back(std::move(columns[position]));
        chunk.setColumns(std::move(output), rows);
        chunk.getChunkInfos().add(std::move(info));
    }

private:
    const std::vector<SplitMergeKey> keys;
    std::vector<size_t> kept;
    const size_t words;
    const bool descending;
};

/// A chunk of one input held by the merge.
struct SplitMergeBuffer
{
    Columns columns;
    /// Data of fixed-size columns, and of the nested column of Nullable ones over fixed-size values; nullptr for the others.
    std::vector<const char *> raw;
    std::vector<const UInt8 *> null_map; /// Null map of those Nullable columns, nullptr for the others.
    std::vector<UInt64> keys;
    size_t rows = 0;
};

/// The buffer of a chunk that carries its merge keys (`SplitMergeKeysInfo`).
std::shared_ptr<SplitMergeBuffer> makeSplitMergeBuffer(Chunk & chunk, size_t words)
{
    auto info = chunk.getChunkInfos().extract<SplitMergeKeysInfo>();
    if (!info || info->keys.size() != chunk.getNumRows() * words)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A chunk to merge has no merge keys for its rows");

    auto buffer = std::make_shared<SplitMergeBuffer>();
    buffer->rows = chunk.getNumRows();
    buffer->columns = chunk.detachColumns();
    buffer->keys = std::move(info->keys);
    buffer->raw.resize(buffer->columns.size());
    buffer->null_map.resize(buffer->columns.size());
    for (size_t c = 0; c < buffer->columns.size(); ++c)
    {
        const IColumn & column = *buffer->columns[c];
        if (column.isFixedAndContiguous())
            buffer->raw[c] = column.getRawData().data();
        else if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column);
                 nullable && nullable->getNestedColumn().isFixedAndContiguous())
        {
            buffer->raw[c] = nullable->getNestedColumn().getRawData().data();
            buffer->null_map[c] = nullable->getNullMapData().data();
        }
    }
    return buffer;
}

/// A merged block before its columns are copied: runs of `rows` rows of buffer `slots[slot]` from row `row`, in output order.
struct SplitMergedRowsInfo final : public ChunkInfoCloneable<SplitMergedRowsInfo>
{
    struct Record
    {
        UInt32 slot;
        UInt32 row;
        UInt32 rows;
    };

    std::vector<std::shared_ptr<const SplitMergeBuffer>> slots;
    std::vector<Record> records;
    size_t rows = 0;
    std::vector<UInt64> keys; /// Packed keys of the rows, without ranks, if the merge emits them.
};

/// An input of `SplitMergingTransform`: the rows of one split key value in a port's stream, or the whole stream.
struct SplitMergeInput
{
    ColumnPtr value; /// One row of the split key; nullptr if the port's stream is this one input.
    UInt64 rank = 0; /// Order among the inputs of the merge for equal keys, below 2^32.
    bool has_bound = false;
    std::array<UInt64, 5> bound{}; /// Packed key at or below the input's first row, if `has_bound`.
};

/// Merges inputs that are each sorted by the merge keys (see `SplitMergeKeysTransform`) into one sorted
/// stream, the way a client merges one streaming query per market:
/// - a port carries one input, or the rows of several split key values in key order (neighbouring values whose
///   rows share granules are read once); the merge cuts each chunk into the values' row ranges, and a value's
///   input ends where the stream passes it;
/// - every port reads ahead (and decompresses) up to `read_ahead_rows` rows, and more while one of its inputs
///   has no rows, so the readers run in parallel on the query threads while the merge runs;
/// - the merge is a tree of losers over an array holding only the current packed key of each input, so a row
///   costs log2(inputs) branchless multi-word compares within a few KiB;
/// - the input rank breaks ties, in the low bits of the key, so the split key is not compared;
/// - the output holds only which rows to take (`SplitMergedRowsInfo`); `SplitGatherTransform` copies the
///   columns on another thread while the next block is merged.
class SplitMergingTransform final : public IProcessor
{
public:
    static constexpr size_t MAX_WORDS = 5;
    static constexpr size_t NO_PORT = std::numeric_limits<size_t>::max();
    /// Inputs merged by one first-level merge when there are more (see `readInOrderSplitByKeyPrefix`).
    static constexpr size_t GROUP_SIZE = 128;

    /// `split_position_`: the split key column, by which ports with several inputs are cut; nullopt if every
    /// port is one input. With `emit_keys_` the output also carries the packed keys of the merged rows (without
    /// the ranks), so the gathered stream can be merged again.
    /// `port_slices_`, if not empty: for each port, whether it reads its whole stream ahead from the start, and
    /// the port that starts to when this port's input is used up (`NO_PORT` for none). See `readInOrderSplitByKeyPrefix`.
    SplitMergingTransform(
        SharedHeader header_,
        std::vector<std::vector<SplitMergeInput>> port_inputs,
        std::optional<size_t> split_position_,
        size_t words_,
        bool descending_,
        size_t read_ahead_rows_,
        size_t max_block_size_,
        bool emit_keys_,
        std::vector<std::pair<bool, size_t>> port_slices_ = {},
        size_t lazy_ahead_ = 64)
        : IProcessor(InputPorts(port_inputs.size(), header_), {std::make_shared<const Block>()})
        , words(words_)
        , split_position(split_position_)
        , descending(descending_)
        , emit_keys(emit_keys_)
        , read_ahead_rows(std::max<size_t>(read_ahead_rows_, 1))
        , max_block_size(std::max<size_t>(max_block_size_, 1))
        , lazy_ahead(std::max<size_t>(lazy_ahead_, 1))
        , ports(port_inputs.size())
    {
        if (words == 0 || words > MAX_WORDS)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitMergingTransform supports 1 to {} key words, got {}", MAX_WORDS, words);
        size_t num_inputs = 0;
        for (const auto & inputs_of_port : port_inputs)
        {
            if (inputs_of_port.empty() || (!split_position && inputs_of_port.size() != 1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitMergingTransform: a port has {} inputs", inputs_of_port.size());
            num_inputs += inputs_of_port.size();
        }
        states.resize(num_inputs);
        cursors.resize(num_inputs);
        keys.resize(num_inputs * words);
        tree.resize(num_inputs);
        size_t i = 0;
        size_t p = 0;
        for (auto & input : inputs)
        {
            auto & port = ports[p];
            port.input = &input;
            port_index.emplace(&input, p);
            if (!port_slices_.empty())
                std::tie(port.eager, port.eager_next) = port_slices_.at(p);
            /// The stream holds the values in key order, reversed when reading in reverse.
            auto & inputs_of_port = port_inputs[p];
            if (descending)
                std::ranges::reverse(inputs_of_port);
            port.lazy = !port.eager;
            for (auto & split_input : inputs_of_port)
            {
                if (split_input.rank >> 32)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitMergingTransform: rank {} does not fit 32 bits", split_input.rank);
                auto & state = states[i];
                state.port = p;
                state.value = std::move(split_input.value);
                cursors[i].rank = split_input.rank;
                port.inputs.push_back(i);
                port.lazy = port.lazy && split_input.has_bound;
                if (split_input.has_bound)
                {
                    /// Until it wins, the input stands in the tree by its bound and is not waited for.
                    state.placeholder = true;
                    for (size_t w = 0; w < words; ++w)
                        keys[i * words + w] = split_input.bound[w];
                    keys[i * words + words - 1] |= cursors[i].rank;
                }
                else
                {
                    state.in_waiting = true;
                    ++waiting;
                }
                ++i;
            }
            ++p;
        }

        /// Lazy ports (all inputs bounded) start reading in the order of their least bound.
        for (size_t q = 0; q < ports.size(); ++q)
            if (ports[q].lazy)
                lazy_order.push_back(q);
        auto port_bound = [&](size_t q)
        {
            const UInt64 * least = nullptr;
            for (size_t j : ports[q].inputs)
                if (!least || lessWords(keys.data() + j * words, least))
                    least = keys.data() + j * words;
            return least;
        };
        std::vector<const UInt64 *> bounds(ports.size());
        for (size_t q : lazy_order)
            bounds[q] = port_bound(q);
        std::ranges::stable_sort(lazy_order, [&](size_t a, size_t b) { return lessWords(bounds[a], bounds[b]); });
    }

    String getName() const override { return "SplitMergingTransform"; }

    ~SplitMergingTransform() override
    {
        LOG_TRACE(getLogger("SplitMergingTransform"), "ports {}, inputs {}, rows {}, max buffered rows {}, output blocks {}, stopped on an empty input {}, work calls {}, ms: add chunks {}, merge {} (cpu {})",
            ports.size(), states.size(), stat_rows, stat_max_buffered, stat_blocks, stat_stalls, stat_work, stat_add_ns / 1000000, stat_merge_ns / 1000000,
            stat_merge_cpu_ns / 1000000);
    }

    Status prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts &) override
    {
        auto & output = outputs.front();
        if (output.isFinished())
        {
            for (auto & port : ports)
                port.input->close();
            return Status::Finished;
        }

        if (!started)
        {
            started = true;
            for (auto & port : ports)
                if (!port.lazy)
                    port.input->setNeeded();
            requestLazy();
        }
        for (auto * input : updated_inputs)
            handleInput(port_index.at(input));
        for (size_t p : to_request)
        {
            ports[p].request_queued = false;
            handleInput(p);
        }
        to_request.clear();

        if (output_chunk)
        {
            if (!output.canPush())
                return Status::PortFull;
            output.push(std::move(*output_chunk));
            output_chunk.reset();
        }

        if (!pending.empty())
            return Status::Ready;
        if (done)
        {
            output.finish();
            return Status::Finished;
        }
        if (waiting == 0)
            return output.canPush() ? Status::Ready : Status::PortFull;
        return Status::NeedData;
    }

    void work() override
    {
        ++stat_work;
        Stopwatch watch;
        for (auto & [p, chunk] : pending)
            addChunk(p, std::move(chunk));
        pending.clear();
        stat_add_ns += watch.elapsedNanoseconds();

        if (waiting == 0 && !done)
        {
            switch (words)
            {
                case 1: merge<1>(); break;
                case 2: merge<2>(); break;
                case 3: merge<3>(); break;
                case 4: merge<4>(); break;
                case 5: merge<5>(); break;
                default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported number of key words {}", words);
            }
        }
    }

private:
    /// Rows `[begin, end)` of a chunk of a port that belong to one input.
    struct Slice
    {
        std::shared_ptr<SplitMergeBuffer> buffer;
        UInt32 begin = 0;
        UInt32 end = 0;
    };

    struct InputState
    {
        std::deque<Slice> queue;
        size_t port = 0;
        ColumnPtr value;
        bool finished = false; /// No more rows will come.
        bool placeholder = false; /// Its key in the tree is its bound; its rows are loaded when it wins.
        bool in_waiting = false; /// Counted in `waiting`.
    };

    struct PortState
    {
        InputPort * input = nullptr;
        std::vector<size_t> inputs; /// In stream order.
        size_t current = 0; /// The first of `inputs` the stream has not passed.
        size_t buffered_rows = 0; /// Rows pulled and not yet merged (rows of no input are dropped when added).
        size_t pending_chunks = 0;
        bool finished = false;
        bool request_queued = false;
        /// Reads its whole stream ahead (a slice of a busy value near the one being merged).
        bool eager = false;
        bool lazy = false; /// Every input has a bound: starts reading when its turn in `lazy_order` comes.
        bool lazy_requested = false;
        bool lazy_started = false; /// One of its inputs won.
        size_t eager_next = NO_PORT; /// Becomes eager when this port's input is used up.
    };

    /// Where an input is in its front slice. The current key itself is in `keys`.
    struct Cursor
    {
        const UInt64 * next = nullptr;
        const UInt64 * end = nullptr;
        UInt64 rank = 0; /// In the low bits of the last key word.
        UInt32 row = 0;
        UInt32 slot = 0;
        UInt32 slot_epoch = 0;
    };

    /// An input blocks the merge while it has no rows and more may come.
    static bool blocking(const InputState & state) { return state.queue.empty() && !state.finished; }

    /// Whether the port must read on regardless of its read-ahead: one of its inputs has no rows yet.
    bool portBlocking(const PortState & port) const
    {
        return port.current + 1 < port.inputs.size() || (port.current < port.inputs.size() && blocking(states[port.inputs[port.current]]));
    }

    void finishInput(size_t i)
    {
        auto & state = states[i];
        if (state.finished)
            return;
        state.finished = true;
        if (state.in_waiting)
        {
            state.in_waiting = false;
            --waiting;
        }
    }

    void finishPort(PortState & port)
    {
        for (; port.current < port.inputs.size(); ++port.current)
            finishInput(port.inputs[port.current]);
    }

    void handleInput(size_t p)
    {
        auto & port = ports[p];
        if (port.finished || (port.lazy && !port.lazy_requested))
            return;
        auto & input = *port.input;
        if (input.hasData())
        {
            Chunk chunk = input.pull(true);
            if (chunk.getNumRows())
            {
                port.buffered_rows += chunk.getNumRows();
                stat_buffered += chunk.getNumRows();
                stat_max_buffered = std::max(stat_max_buffered, stat_buffered);
                ++port.pending_chunks;
                pending.emplace_back(p, std::move(chunk));
            }
        }
        if (input.isFinished())
        {
            port.finished = true;
            if (port.pending_chunks == 0)
                finishPort(port);
            return;
        }
        if (port.buffered_rows < read_ahead_rows || port.eager || portBlocking(port))
            input.setNeeded();
        else
            input.setNotNeeded();
    }

    /// The first row in `[from, to)` at or after (`upper`: after) `value` in stream order.
    size_t boundary(const IColumn & column, size_t from, size_t to, const IColumn & value, bool upper) const
    {
        while (from < to)
        {
            const size_t mid = from + (to - from) / 2;
            int cmp = column.compareAt(mid, 0, value, 1);
            if (descending)
                cmp = -cmp;
            if (upper ? cmp <= 0 : cmp < 0)
                from = mid + 1;
            else
                to = mid;
        }
        return from;
    }

    void pushSlice(size_t i, Slice slice)
    {
        auto & state = states[i];
        state.queue.push_back(std::move(slice));
        if (state.queue.size() == 1 && !state.placeholder)
            loadCursor(i);
        if (state.in_waiting)
        {
            state.in_waiting = false;
            --waiting;
        }
    }

    void addChunk(size_t p, Chunk chunk)
    {
        auto buffer = makeSplitMergeBuffer(chunk, words);
        const size_t rows = buffer->rows;

        auto & port = ports[p];
        --port.pending_chunks;
        if (!split_position)
        {
            pushSlice(port.inputs.front(), {buffer, 0, static_cast<UInt32>(rows)});
        }
        else
        {
            const IColumn & column = *buffer->columns[*split_position];
            size_t kept = 0;
            size_t row = 0;
            while (port.current < port.inputs.size())
            {
                const size_t i = port.inputs[port.current];
                const IColumn & value = *states[i].value;
                const size_t begin = boundary(column, row, rows, value, false);
                const size_t end = boundary(column, begin, rows, value, true);
                if (end > begin)
                {
                    pushSlice(i, {buffer, static_cast<UInt32>(begin), static_cast<UInt32>(end)});
                    kept += end - begin;
                }
                if (end == rows)
                    break; /// The value may go on in the next chunk.
                finishInput(i);
                ++port.current;
                row = end;
            }
            port.buffered_rows -= rows - kept;
            stat_buffered -= rows - kept;
        }
        if (port.finished && port.pending_chunks == 0)
            finishPort(port);
    }

    /// Points the cursor of input `i` at the first row of its front slice.
    void loadCursor(size_t i)
    {
        const auto & slice = states[i].queue.front();
        auto & cursor = cursors[i];
        const UInt64 * first = slice.buffer->keys.data() + slice.begin * words;
        for (size_t w = 0; w < words; ++w)
            keys[i * words + w] = first[w];
        keys[i * words + words - 1] |= cursor.rank;
        cursor.next = first + words;
        cursor.end = slice.buffer->keys.data() + slice.end * words;
        cursor.row = slice.begin;
        cursor.slot_epoch = 0;
    }

    /// An exhausted input: all words are the maximum, above any row since row ranks are smaller. The next slice
    /// of its value in line starts reading ahead.
    void setExhausted(size_t i)
    {
        std::fill_n(keys.data() + i * words, words, std::numeric_limits<UInt64>::max());
        const size_t next = ports[states[i].port].eager_next;
        if (next != NO_PORT && !ports[next].eager)
        {
            ports[next].eager = true;
            if (!ports[next].finished && !ports[next].request_queued)
            {
                ports[next].request_queued = true;
                to_request.push_back(next);
            }
        }
    }

    /// Called when input `i` used up its front slice. Returns false if the merge must wait for its next rows.
    bool nextBuffer(size_t i)
    {
        auto & state = states[i];
        auto & port = ports[state.port];
        port.buffered_rows -= state.queue.front().end - state.queue.front().begin;
        stat_buffered -= state.queue.front().end - state.queue.front().begin;
        state.queue.pop_front();
        if (!port.finished && !port.request_queued && (port.buffered_rows < read_ahead_rows || portBlocking(port)))
        {
            port.request_queued = true;
            to_request.push_back(state.port);
        }
        if (!state.queue.empty())
        {
            loadCursor(i);
            return true;
        }
        if (blocking(state))
        {
            state.in_waiting = true;
            ++waiting;
            return false;
        }
        setExhausted(i);
        return true;
    }

    /// Keeps up to `lazy_ahead` lazy ports reading ahead of the ones whose inputs won.
    void requestLazy()
    {
        while (lazy_window < lazy_ahead && lazy_next < lazy_order.size())
        {
            const size_t p = lazy_order[lazy_next++];
            auto & port = ports[p];
            if (port.lazy_requested)
                continue;
            port.lazy_requested = true;
            ++lazy_window;
            if (!port.request_queued)
            {
                port.request_queued = true;
                to_request.push_back(p);
            }
        }
    }

    void startPort(size_t p)
    {
        auto & port = ports[p];
        if (!port.lazy || port.lazy_started)
            return;
        port.lazy_started = true;
        if (port.lazy_requested)
            --lazy_window;
        else
        {
            port.lazy_requested = true;
            if (!port.request_queued)
            {
                port.request_queued = true;
                to_request.push_back(p);
            }
        }
        requestLazy();
    }

    /// The winner `i` is a placeholder: loads its first row, or returns false if the merge must wait for it.
    bool startInput(size_t i)
    {
        auto & state = states[i];
        startPort(state.port);
        if (!state.queue.empty())
        {
            state.placeholder = false;
            loadCursor(i);
            return true;
        }
        if (state.finished)
        {
            state.placeholder = false;
            setExhausted(i);
            return true;
        }
        state.in_waiting = true;
        ++waiting;
        return false;
    }

    bool lessWords(const UInt64 * a, const UInt64 * b) const
    {
        for (size_t w = 0; w < words; ++w)
            if (a[w] != b[w])
                return a[w] < b[w];
        return false;
    }

    /// Whether `a` < `b` as big unsigned numbers of W words, most significant first: the borrow of `a` - `b`.
    template <size_t W>
    static ALWAYS_INLINE bool less(const UInt64 * a, const UInt64 * b)
    {
        unsigned long long borrow = 0;
        for (size_t w = W; w-- > 0;)
            __builtin_subcll(a[w], b[w], borrow, &borrow);
        return borrow;
    }

    /// Plays input `winner`, whose key changed, from its leaf to the root. The winner's key is kept in registers,
    /// so the loads of the nodes on the path do not wait for the compares below them.
    template <size_t W>
    ALWAYS_INLINE void replay(UInt32 winner)
    {
        const size_t n = tree.size();
        UInt32 * nodes = tree.data();
        const UInt64 * all_keys = keys.data();
        UInt64 key[W];
        for (size_t w = 0; w < W; ++w)
            key[w] = all_keys[winner * W + w];
        for (size_t node = (winner + n) >> 1; node > 0; node >>= 1)
        {
            const UInt32 other = nodes[node];
            const UInt64 * other_key = all_keys + other * W;
            const bool swap = less<W>(other_key, key);
            nodes[node] = swap ? winner : other;
            winner = swap ? other : winner;
            for (size_t w = 0; w < W; ++w)
                key[w] = swap ? other_key[w] : key[w];
        }
        nodes[0] = winner;
    }

    /// The least key among the losers on the path of `winner`, the key it must stay below to keep winning.
    template <size_t W>
    const UInt64 * runnerUp(UInt32 winner) const
    {
        static constexpr UInt64 none[MAX_WORDS] = {
            std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(),
            std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max()};
        const size_t n = tree.size();
        const UInt64 * least = none;
        for (size_t node = (winner + n) >> 1; node > 0; node >>= 1)
        {
            const UInt64 * other = keys.data() + tree[node] * W;
            if (less<W>(other, least))
                least = other;
        }
        return least;
    }

    /// Node `node` > 0 holds the loser of its two subtrees; leaf of input `i` is node `n + i`.
    template <size_t W>
    void build()
    {
        const size_t n = tree.size();
        for (size_t i = 0; i < n; ++i)
            if (states[i].queue.empty() && !states[i].placeholder)
                setExhausted(i);
        std::vector<UInt32> winners(n);
        auto winner_of = [&](size_t node) { return node >= n ? static_cast<UInt32>(node - n) : winners[node]; };
        for (size_t node = n - 1; node > 0; --node)
        {
            const UInt32 left = winner_of(2 * node);
            const UInt32 right = winner_of(2 * node + 1);
            const bool right_wins = less<W>(keys.data() + right * W, keys.data() + left * W);
            winners[node] = right_wins ? right : left;
            tree[node] = right_wins ? left : right;
        }
        tree[0] = winner_of(1);
    }

    template <size_t W>
    void merge()
    {
        Stopwatch watch;
        Stopwatch cpu_watch(CLOCK_THREAD_CPUTIME_ID);
        if (!built)
        {
            build<W>();
            built = true;
        }
        else if (resume)
        {
            /// The winner ran out of rows and was waiting for more; a placeholder is started in the loop.
            const UInt32 winner = tree[0];
            if (!states[winner].placeholder)
            {
                if (states[winner].queue.empty())
                    setExhausted(winner);
                replay<W>(winner);
            }
        }
        resume = false;

        auto info = std::make_shared<SplitMergedRowsInfo>();
        auto & records = info->records;
        auto & slots = info->slots;
        auto & merged_keys = info->keys;
        size_t & rows_out = info->rows;
        if (emit_keys)
            merged_keys.reserve(max_block_size * W);
        ++epoch;

        /// Takes `count` rows of the winner from its current one and loads its next key. Returns false if the merge
        /// must wait for its rows.
        auto take = [&](UInt32 winner, UInt64 * key, size_t count)
        {
            auto & cursor = cursors[winner];
            if (cursor.slot_epoch != epoch)
            {
                cursor.slot_epoch = epoch;
                cursor.slot = static_cast<UInt32>(slots.size());
                slots.push_back(states[winner].queue.front().buffer);
            }
            if (!records.empty() && records.back().slot == cursor.slot && records.back().row + records.back().rows == cursor.row)
                records.back().rows += static_cast<UInt32>(count);
            else
                records.push_back({cursor.slot, cursor.row, static_cast<UInt32>(count)});
            rows_out += count;
            if (emit_keys)
            {
                /// Keys in the buffer have no rank.
                const UInt64 * first = cursor.next - W;
                merged_keys.insert(merged_keys.end(), first, first + count * W);
            }

            cursor.next += (count - 1) * W;
            cursor.row += static_cast<UInt32>(count - 1);
            if (cursor.next != cursor.end)
            {
                /// With hundreds of inputs the hardware prefetcher does not follow each key stream.
                __builtin_prefetch(cursor.next + 16);
                for (size_t w = 0; w < W; ++w)
                    key[w] = cursor.next[w];
                key[W - 1] |= cursor.rank;
                cursor.next += W;
                ++cursor.row;
                return true;
            }
            return nextBuffer(winner);
        };

        /// How many rows of the winner from its current one, whose key is below `bound`, are below it: galloping
        /// then binary search over its sorted keys, up to `limit`.
        auto below = [&](UInt32 winner, const UInt64 * bound, size_t limit)
        {
            const auto & cursor = cursors[winner];
            const UInt64 * first = cursor.next - W;
            const size_t available = std::min<size_t>((cursor.end - first) / W, limit);
            auto row_below = [&](size_t r)
            {
                UInt64 key[W];
                for (size_t w = 0; w < W; ++w)
                    key[w] = first[r * W + w];
                key[W - 1] |= cursor.rank;
                return less<W>(key, bound);
            };
            /// Interleaved inputs win a few rows at a time: try those one by one first.
            size_t lo = 1; /// Rows [0, lo) are below.
            while (lo < std::min<size_t>(available, 8))
            {
                if (!row_below(lo))
                    return lo;
                ++lo;
            }
            size_t step = 1;
            while (lo + step - 1 < available && row_below(lo + step - 1))
            {
                lo += step;
                step *= 2;
            }
            size_t hi = std::min(available, lo + step - 1); /// Row hi and above are not below, or out of range.
            while (lo < hi)
            {
                const size_t mid = lo + (hi - lo) / 2;
                if (row_below(mid))
                    lo = mid + 1;
                else
                    hi = mid;
            }
            return lo;
        };

        while (rows_out < max_block_size)
        {
            const UInt32 winner = tree[0];
            UInt64 * key = keys.data() + winner * W;
            if (key[W - 1] == std::numeric_limits<UInt64>::max())
            {
                done = true;
                break;
            }

            if (states[winner].placeholder)
            {
                if (!startInput(winner))
                {
                    resume = true;
                    break;
                }
                replay<W>(winner);
                continue;
            }

            if (!take(winner, key, 1))
            {
                resume = true;
                break;
            }
            replay<W>(winner);

            /// Won again: its rows below the runner-up (the least loser on its path) need no replay. Inputs that
            /// do not interleave (slices of a value) go a buffer at a time; interleaved ones rarely get here.
            if (tree[0] == winner)
            {
                const UInt64 * runner_up = runnerUp<W>(winner);
                bool stalled = false;
                while (rows_out < max_block_size && less<W>(key, runner_up))
                {
                    if (!take(winner, key, below(winner, runner_up, max_block_size - rows_out)))
                    {
                        stalled = true;
                        break;
                    }
                }
                if (stalled)
                {
                    resume = true;
                    break;
                }
                replay<W>(winner);
            }
        }

        stat_stalls += resume;
        stat_rows += rows_out;
        stat_merge_ns += watch.elapsedNanoseconds();
        stat_merge_cpu_ns += cpu_watch.elapsedNanoseconds();
        if (!records.empty())
        {
            ++stat_blocks;
            output_chunk.emplace(Columns{}, rows_out);
            output_chunk->getChunkInfos().add(std::move(info));
        }
    }

    const size_t words; /// Words of a packed key.
    const std::optional<size_t> split_position;
    const bool descending;
    const bool emit_keys;
    const size_t read_ahead_rows;
    const size_t max_block_size;

    const size_t lazy_ahead;

    std::vector<PortState> ports;
    std::unordered_map<const InputPort *, size_t> port_index;
    std::vector<size_t> lazy_order; /// Lazy ports by least bound.
    size_t lazy_next = 0;
    size_t lazy_window = 0; /// Requested lazy ports none of whose inputs won yet.
    std::vector<InputState> states;
    std::vector<Cursor> cursors;
    std::vector<UInt64> keys; /// Current packed key of each input, with its rank.
    std::vector<UInt32> tree;
    size_t waiting = 0;
    bool started = false;
    bool built = false;
    bool resume = false;
    bool done = false;

    std::vector<std::pair<size_t, Chunk>> pending;
    std::vector<size_t> to_request;
    UInt32 epoch = 0;
    size_t stat_rows = 0;
    size_t stat_blocks = 0;
    size_t stat_buffered = 0;
    size_t stat_max_buffered = 0;
    size_t stat_stalls = 0;
    size_t stat_work = 0;
    UInt64 stat_add_ns = 0;
    UInt64 stat_merge_ns = 0;
    UInt64 stat_merge_cpu_ns = 0;
    std::optional<Chunk> output_chunk;
};

/// Rows for a worker of the parallel merge: of each input, rows of its buffers in stream order. All rows of a task
/// come after those of earlier tasks and before those of later ones.
struct SplitMergeTaskInfo final : public ChunkInfoCloneable<SplitMergeTaskInfo>
{
    struct Part
    {
        std::shared_ptr<const SplitMergeBuffer> buffer;
        UInt32 begin;
        UInt32 end;
    };

    struct Input
    {
        UInt64 rank; /// In the low bits of the last key word.
        std::vector<Part> parts;
    };

    std::vector<Input> inputs;
    size_t rows = 0;
};

/// Whether `a` < `b` as big unsigned numbers of W words, most significant first.
template <size_t W>
ALWAYS_INLINE bool splitKeyLess(const UInt64 * a, const UInt64 * b)
{
    unsigned long long borrow = 0;
    for (size_t w = W; w-- > 0;)
        __builtin_subcll(a[w], b[w], borrow, &borrow);
    return borrow;
}

/// Key of row `row` of `buffer` with the input's rank.
template <size_t W>
ALWAYS_INLINE void splitRankedKey(const SplitMergeBuffer & buffer, size_t row, UInt64 rank, UInt64 * key)
{
    for (size_t w = 0; w < W; ++w)
        key[w] = buffer.keys[row * W + w];
    key[W - 1] |= rank;
}

/// Cuts the sorted streams of its inputs (each the output of a merge, with merge keys) into tasks for
/// `SplitTaskMergeTransform`s working in parallel. Rows below the frontier, the least last key of the inputs that
/// may still get rows, go to tasks of about `task_rows` rows split at sampled keys, in key order.
class SplitBatchingTransform final : public IProcessor
{
public:
    SplitBatchingTransform(
        SharedHeader header, std::vector<UInt64> ranks_, size_t words_, size_t read_ahead_rows_, size_t task_rows_, size_t max_ready_tasks_)
        : IProcessor(InputPorts(ranks_.size(), InputPort(header)), OutputPorts{OutputPort(std::make_shared<const Block>())})
        , words(words_)
        , read_ahead_rows(read_ahead_rows_)
        , task_rows(task_rows_)
        , max_ready_tasks(max_ready_tasks_)
    {
        for (auto & port : inputs)
        {
            states.emplace_back();
            states.back().port = &port;
            states.back().rank = ranks_[states.size() - 1];
        }
    }

    String getName() const override { return "SplitBatchingTransform"; }

    ~SplitBatchingTransform() override
    {
        LOG_TRACE(getLogger("SplitBatchingTransform"), "inputs {}, rows {}, tasks {}, cuts {}, ms: cut {}", states.size(), stat_rows, stat_tasks, stat_cuts, stat_cut_ns / 1000000);
    }

    Status prepare() override
    {
        auto & output = outputs.front();
        if (output.isFinished())
        {
            for (auto & port : inputs)
                port.close();
            return Status::Finished;
        }

        if (!ready.empty() && output.canPush())
        {
            output.push(std::move(ready.front()));
            ready.pop_front();
        }

        for (size_t i = 0; i < states.size(); ++i)
        {
            auto & state = states[i];
            if (state.port_finished)
                continue;
            auto & port = *state.port;
            if (port.hasData())
            {
                Chunk chunk = port.pull(true);
                if (chunk.getNumRows())
                {
                    state.buffered_rows += chunk.getNumRows();
                    pending.emplace_back(i, std::move(chunk));
                }
            }
            if (port.isFinished())
            {
                state.port_finished = true;
                cut_again = true; /// The frontier may move.
                continue;
            }
            /// Tasks waiting for the workers hold rows too: no more reading until they go.
            if (ready.size() < max_ready_tasks && (state.buffered_rows < read_ahead_rows || state.buffered_rows == 0 || i == frontier_input))
                port.setNeeded();
            else
                port.setNotNeeded();
        }

        if (!pending.empty())
            return Status::Ready;
        if (ready.empty())
        {
            bool all_done = true;
            for (const auto & state : states)
                all_done = all_done && state.port_finished && state.queue.empty();
            if (all_done)
            {
                output.finish();
                return Status::Finished;
            }
            if (cut_again)
                return Status::Ready;
            return Status::NeedData;
        }
        if (cut_again && ready.size() < max_ready_tasks)
            return Status::Ready;
        return Status::PortFull;
    }

    void work() override
    {
        /// The frontier moves only when its input gets rows (or an input finishes, see `prepare`).
        for (auto & [i, chunk] : pending)
        {
            auto buffer = makeSplitMergeBuffer(chunk, words);
            const auto rows = static_cast<UInt32>(buffer->rows);
            auto & state = states[i];
            state.queue.push_back({std::move(buffer), 0, rows, state.pushed_rows});
            state.pushed_rows += rows;
            cut_again = cut_again || i == frontier_input || frontier_input == NO_INPUT;
        }
        pending.clear();
        if (!cut_again || ready.size() >= max_ready_tasks)
            return;
        Stopwatch watch;
        switch (words)
        {
            case 1: cut<1>(); break;
            case 2: cut<2>(); break;
            case 3: cut<3>(); break;
            case 4: cut<4>(); break;
            case 5: cut<5>(); break;
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported number of key words {}", words);
        }
        stat_cut_ns += watch.elapsedNanoseconds();
    }

private:
    struct Slice
    {
        std::shared_ptr<const SplitMergeBuffer> buffer;
        UInt32 begin;
        UInt32 end;
        size_t offset; /// Rows of the input before `begin`.
    };

    struct State
    {
        InputPort * port = nullptr;
        UInt64 rank = 0;
        std::deque<Slice> queue;
        size_t pushed_rows = 0;
        size_t buffered_rows = 0;
        bool port_finished = false; /// Finished, and its chunks are in `pending` or `queue`.
    };

    /// Where the rows of input `i` reach `bound` (ranked, exclusive): the slice and row of its first row at or
    /// above it, and the number of rows below it.
    template <size_t W>
    std::tuple<size_t, UInt32, size_t> position(size_t i, const UInt64 * bound) const
    {
        const auto & state = states[i];
        if (state.queue.empty())
            return {0, 0, 0};
        UInt64 key[W];
        /// The first slice whose last row is not below.
        size_t lo_slice = 0;
        size_t hi_slice = state.queue.size();
        while (lo_slice < hi_slice)
        {
            const size_t mid = lo_slice + (hi_slice - lo_slice) / 2;
            const auto & slice = state.queue[mid];
            splitRankedKey<W>(*slice.buffer, slice.end - 1, state.rank, key);
            if (splitKeyLess<W>(key, bound))
                lo_slice = mid + 1;
            else
                hi_slice = mid;
        }
        const size_t first = state.queue.front().offset;
        if (lo_slice == state.queue.size())
            return {lo_slice, 0, state.pushed_rows - first};
        const auto & slice = state.queue[lo_slice];
        UInt32 lo = slice.begin;
        UInt32 hi = slice.end - 1; /// Known not below.
        while (lo < hi)
        {
            const UInt32 mid = lo + (hi - lo) / 2;
            splitRankedKey<W>(*slice.buffer, mid, state.rank, key);
            if (splitKeyLess<W>(key, bound))
                lo = mid + 1;
            else
                hi = mid;
        }
        return {lo_slice, lo, slice.offset + (lo - slice.begin) - first};
    }

    template <size_t W>
    void cut()
    {
        cut_again = false;
        /// The frontier: rows at or below it will not be preceded by rows still to come.
        std::array<UInt64, W> frontier;
        frontier.fill(std::numeric_limits<UInt64>::max());
        frontier_input = NO_INPUT;
        bool all_finished = true;
        for (size_t i = 0; i < states.size(); ++i)
        {
            const auto & state = states[i];
            if (state.port_finished)
                continue;
            all_finished = false;
            if (state.queue.empty())
            {
                frontier_input = i;
                return;
            }
            std::array<UInt64, W> last;
            splitRankedKey<W>(*state.queue.back().buffer, state.queue.back().end - 1, state.rank, last.data());
            if (splitKeyLess<W>(last.data(), frontier.data()))
            {
                frontier = last;
                frontier_input = i;
            }
        }
        /// Exclusive bound: one above the frontier (its last word holds a rank, below the maximum).
        std::array<UInt64, W> bound = frontier;
        if (!all_finished)
            ++bound[W - 1];

        /// Rows below the bound, and a sample of their keys every `STRIDE` rows.
        static constexpr size_t STRIDE = 32;
        std::vector<size_t> available(states.size());
        size_t total = 0;
        for (size_t i = 0; i < states.size(); ++i)
        {
            available[i] = std::get<2>(position<W>(i, bound.data()));
            total += available[i];
        }
        /// All rows below the bound go, in tasks of at least `task_rows` rows but the last ones.
        const size_t tasks = all_finished ? (total + task_rows - 1) / task_rows : total / task_rows;
        if (tasks == 0)
            return;

        std::vector<std::array<UInt64, W>> samples;
        samples.reserve(total / STRIDE + states.size());
        for (size_t i = 0; i < states.size(); ++i)
        {
            const auto & state = states[i];
            size_t seen = 0;
            for (const auto & slice : state.queue)
            {
                if (seen >= available[i])
                    break;
                const size_t length = std::min<size_t>(slice.end - slice.begin, available[i] - seen);
                for (size_t offset = (STRIDE - seen % STRIDE) % STRIDE; offset < length; offset += STRIDE)
                {
                    samples.emplace_back();
                    splitRankedKey<W>(*slice.buffer, slice.begin + offset, state.rank, samples.back().data());
                }
                seen += length;
            }
        }
        ++stat_cuts;
        std::sort(samples.begin(), samples.end());

        /// Task `t` takes rows in [bounds[t], bounds[t + 1]).
        std::vector<std::array<UInt64, W>> bounds(tasks + 1);
        bounds[0].fill(0);
        for (size_t t = 1; t < tasks; ++t)
            bounds[t] = samples[std::min(samples.size() - 1, t * samples.size() / tasks)];
        bounds[tasks] = bound;

        std::vector<std::tuple<size_t, UInt32, size_t>> from(states.size(), {0, 0, 0});
        for (size_t i = 0; i < states.size(); ++i)
            from[i] = {0, states[i].queue.empty() ? 0 : states[i].queue.front().begin, 0};
        for (size_t t = 1; t <= tasks; ++t)
        {
            auto info = std::make_shared<SplitMergeTaskInfo>();
            for (size_t i = 0; i < states.size(); ++i)
            {
                const auto & state = states[i];
                const auto to = position<W>(i, bounds[t].data());
                const auto [from_slice, from_row, from_below] = from[i];
                const auto [to_slice, to_row, to_below] = to;
                if (to_below > from_below)
                {
                    auto & input = info->inputs.emplace_back();
                    input.rank = state.rank;
                    for (size_t s = from_slice; s <= to_slice && s < state.queue.size(); ++s)
                    {
                        const auto & slice = state.queue[s];
                        const UInt32 begin = s == from_slice ? from_row : slice.begin;
                        const UInt32 end = s == to_slice ? to_row : slice.end;
                        if (begin < end)
                            input.parts.push_back({slice.buffer, begin, end});
                    }
                    info->rows += to_below - from_below;
                }
                from[i] = to;
            }
            if (info->rows == 0)
                continue;
            stat_rows += info->rows;
            ++stat_tasks;
            Chunk chunk(Columns{}, info->rows);
            chunk.getChunkInfos().add(std::move(info));
            ready.push_back(std::move(chunk));
        }

        /// Drop the rows given to tasks.
        for (size_t i = 0; i < states.size(); ++i)
        {
            auto & state = states[i];
            const auto [slice, row, below] = from[i];
            for (size_t s = 0; s < slice; ++s)
                state.queue.pop_front();
            if (!state.queue.empty())
            {
                state.queue.front().offset += row - state.queue.front().begin;
                state.queue.front().begin = row;
            }
            state.buffered_rows -= below;
        }
        cut_again = all_finished;
    }

    static constexpr size_t NO_INPUT = std::numeric_limits<size_t>::max();

    const size_t words;
    const size_t read_ahead_rows;
    const size_t task_rows;
    const size_t max_ready_tasks;
    std::vector<State> states;
    std::vector<std::pair<size_t, Chunk>> pending;
    std::deque<Chunk> ready;
    size_t frontier_input = NO_INPUT;
    bool cut_again = false;
    size_t stat_rows = 0;
    size_t stat_tasks = 0;
    size_t stat_cuts = 0;
    UInt64 stat_cut_ns = 0;
};

/// Merges the rows of a task of `SplitBatchingTransform`, for `SplitGatherTransform` to copy.
class SplitTaskMergeTransform final : public ISimpleTransform
{
public:
    SplitTaskMergeTransform(SharedHeader header, size_t words_)
        : ISimpleTransform(header, header, false)
        , words(words_)
    {
    }

    String getName() const override { return "SplitTaskMergeTransform"; }

    ~SplitTaskMergeTransform() override
    {
        LOG_TRACE(getLogger("SplitTaskMergeTransform"), "rows {}, ms: merge {}", stat_rows, stat_ns / 1000000);
    }

protected:
    void transform(Chunk & chunk) override
    {
        Stopwatch watch;
        auto task = chunk.getChunkInfos().extract<SplitMergeTaskInfo>();
        if (!task)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitTaskMergeTransform expects a task");
        auto info = std::make_shared<SplitMergedRowsInfo>();
        switch (words)
        {
            case 1: merge<1>(*task, *info); break;
            case 2: merge<2>(*task, *info); break;
            case 3: merge<3>(*task, *info); break;
            case 4: merge<4>(*task, *info); break;
            case 5: merge<5>(*task, *info); break;
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported number of key words {}", words);
        }
        if (info->rows != task->rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merged {} rows of a task of {}", info->rows, task->rows);
        stat_rows += info->rows;
        chunk.setColumns(Columns{}, info->rows);
        chunk.getChunkInfos().add(std::move(info));
        stat_ns += watch.elapsedNanoseconds();
    }

private:
    template <size_t W>
    void merge(const SplitMergeTaskInfo & task, SplitMergedRowsInfo & out)
    {
        const size_t n = task.inputs.size();
        /// Each part is a slot of the output.
        std::vector<UInt32> first_slot(n);
        for (size_t i = 0; i < n; ++i)
        {
            first_slot[i] = static_cast<UInt32>(out.slots.size());
            for (const auto & part : task.inputs[i].parts)
                out.slots.push_back(part.buffer);
        }

        struct Cursor
        {
            UInt32 part = 0;
            UInt32 row = 0;
        };
        std::vector<Cursor> cursors(n);
        std::vector<UInt64> keys(n * W);
        auto load = [&](size_t i)
        {
            const auto & input = task.inputs[i];
            const auto & cursor = cursors[i];
            if (cursor.part == input.parts.size())
                std::fill_n(keys.data() + i * W, W, std::numeric_limits<UInt64>::max());
            else
                splitRankedKey<W>(*input.parts[cursor.part].buffer, cursor.row, input.rank, keys.data() + i * W);
        };
        for (size_t i = 0; i < n; ++i)
        {
            cursors[i].row = task.inputs[i].parts.front().begin;
            load(i);
        }

        /// Loser tree: node > 0 holds the loser of its subtrees, leaf of input `i` is node `n + i`.
        std::vector<UInt32> tree(std::max<size_t>(n, 1));
        {
            std::vector<UInt32> winners(n);
            auto winner_of = [&](size_t node) { return node >= n ? static_cast<UInt32>(node - n) : winners[node]; };
            for (size_t node = n - 1; node > 0; --node)
            {
                const UInt32 left = winner_of(2 * node);
                const UInt32 right = winner_of(2 * node + 1);
                const bool right_wins = splitKeyLess<W>(keys.data() + right * W, keys.data() + left * W);
                winners[node] = right_wins ? right : left;
                tree[node] = right_wins ? left : right;
            }
            tree[0] = winner_of(1);
        }

        static constexpr UInt64 none[SplitMergingTransform::MAX_WORDS] = {
            std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(),
            std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max()};

        auto & records = out.records;
        while (true)
        {
            const UInt32 winner = tree[0];
            const UInt64 * key = keys.data() + winner * W;
            if (key[W - 1] == std::numeric_limits<UInt64>::max())
                break;

            /// Its rows below the least loser on its path go at once.
            const UInt64 * runner_up = none;
            for (size_t node = (winner + n) >> 1; node > 0; node >>= 1)
            {
                const UInt64 * other = keys.data() + tree[node] * W;
                if (splitKeyLess<W>(other, runner_up))
                    runner_up = other;
            }
            const auto & input = task.inputs[winner];
            auto & cursor = cursors[winner];
            const auto & part = input.parts[cursor.part];
            auto row_below = [&](size_t row)
            {
                UInt64 row_key[W];
                splitRankedKey<W>(*part.buffer, row, input.rank, row_key);
                return splitKeyLess<W>(row_key, runner_up);
            };
            size_t lo = cursor.row + 1; /// Rows [cursor.row, lo) are below.
            while (lo < std::min<size_t>(part.end, cursor.row + 8) && row_below(lo))
                ++lo;
            if (lo == cursor.row + 8)
            {
                size_t step = 1;
                while (lo + step - 1 < part.end && row_below(lo + step - 1))
                {
                    lo += step;
                    step *= 2;
                }
                size_t hi = std::min<size_t>(part.end, lo + step - 1);
                while (lo < hi)
                {
                    const size_t mid = lo + (hi - lo) / 2;
                    if (row_below(mid))
                        lo = mid + 1;
                    else
                        hi = mid;
                }
            }
            const UInt32 slot = first_slot[winner] + cursor.part;
            const auto count = static_cast<UInt32>(lo - cursor.row);
            if (!records.empty() && records.back().slot == slot && records.back().row + records.back().rows == cursor.row)
                records.back().rows += count;
            else
                records.push_back({slot, cursor.row, count});
            out.rows += count;
            cursor.row = static_cast<UInt32>(lo);
            if (cursor.row == part.end)
            {
                ++cursor.part;
                if (cursor.part < input.parts.size())
                    cursor.row = input.parts[cursor.part].begin;
            }
            load(winner);

            /// Replay the winner from its leaf.
            UInt32 current = winner;
            for (size_t node = (winner + n) >> 1; node > 0; node >>= 1)
            {
                const UInt32 other = tree[node];
                if (splitKeyLess<W>(keys.data() + other * W, keys.data() + current * W))
                {
                    tree[node] = current;
                    current = other;
                }
            }
            tree[0] = current;
        }
    }

    const size_t words;
    size_t stat_rows = 0;
    UInt64 stat_ns = 0;
};

/// Deals the chunks of its input to its outputs in turn, so that they are processed in parallel and
/// `SplitCollectTransform` can restore their order.
class SplitDealTransform final : public IProcessor
{
public:
    SplitDealTransform(SharedHeader header, size_t num_outputs)
        : IProcessor(InputPorts{InputPort(header)}, OutputPorts(num_outputs, OutputPort(header)))
    {
        for (auto & output : outputs)
            output_ports.push_back(&output);
    }

    String getName() const override { return "SplitDealTransform"; }

    Status prepare() override
    {
        auto & input = inputs.front();
        while (true)
        {
            auto & output = *output_ports[next];
            if (output.isFinished())
            {
                input.close();
                for (auto * port : output_ports)
                    port->finish();
                return Status::Finished;
            }
            if (!output.canPush())
            {
                input.setNotNeeded();
                return Status::PortFull;
            }
            if (input.isFinished())
            {
                for (auto * port : output_ports)
                    port->finish();
                return Status::Finished;
            }
            input.setNeeded();
            if (!input.hasData())
                return Status::NeedData;
            output.push(input.pull());
            next = (next + 1) % output_ports.size();
        }
    }

private:
    std::vector<OutputPort *> output_ports;
    size_t next = 0;
};

/// Takes a chunk from each input in turn: the order `SplitDealTransform` dealt them in.
class SplitCollectTransform final : public IProcessor
{
public:
    SplitCollectTransform(SharedHeader header, size_t num_inputs)
        : IProcessor(InputPorts(num_inputs, InputPort(header)), OutputPorts{OutputPort(header)})
    {
        for (auto & input : inputs)
            input_ports.push_back(&input);
    }

    String getName() const override { return "SplitCollectTransform"; }

    Status prepare() override
    {
        auto & output = outputs.front();
        if (output.isFinished())
        {
            for (auto * port : input_ports)
                port->close();
            return Status::Finished;
        }
        if (!output.canPush())
            return Status::PortFull;
        /// Every input is needed, so that each one's transform works ahead.
        for (auto * port : input_ports)
            port->setNeeded();
        auto & input = *input_ports[next];
        if (input.isFinished())
        {
            /// The dealer finished after the previous chunk.
            for (auto * port : input_ports)
                port->close();
            output.finish();
            return Status::Finished;
        }
        if (!input.hasData())
            return Status::NeedData;
        output.push(input.pull(true));
        next = (next + 1) % input_ports.size();
        return Status::PortFull;
    }

private:
    std::vector<InputPort *> input_ports;
    size_t next = 0;
};

/// Copies the rows chosen by `SplitMergingTransform` into columns.
class SplitGatherTransform final : public ISimpleTransform
{
public:
    SplitGatherTransform(SharedHeader input_header_, SharedHeader output_header_)
        : ISimpleTransform(input_header_, output_header_, false)
    {
    }

    String getName() const override { return "SplitGatherTransform"; }

    ~SplitGatherTransform() override
    {
        LOG_TRACE(getLogger("SplitGatherTransform"), "ms: gather {} (cpu {}), rows {}, runs {}, run-copied rows {}", stat_ns / 1000000, stat_cpu_ns / 1000000, stat_rows, stat_runs, stat_run_rows);
    }

protected:
    void transform(Chunk & chunk) override
    {
        Stopwatch watch;
        Stopwatch cpu_watch(CLOCK_THREAD_CPUTIME_ID);
        auto info = chunk.getChunkInfos().extract<SplitMergedRowsInfo>();
        if (!info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitGatherTransform expects merged rows");
        const auto & records = info->records;
        const auto & slots = info->slots;
        const size_t rows = info->rows;
        /// Long runs (inputs that do not interleave) are copied a run at a time, short ones a row at a time.
        const bool copy_runs = records.size() * 4 <= rows;
        stat_rows += rows;
        stat_runs += records.size();
        stat_run_rows += copy_runs ? rows : 0;

        MutableColumns columns = getOutputPort().getHeader().cloneEmptyColumns();
        for (size_t c = 0; c < columns.size(); ++c)
        {
            auto & dst = *columns[c];
            auto * nullable = typeid_cast<ColumnNullable *>(&dst);
            IColumn & values = nullable ? nullable->getNestedColumn() : dst;
            slot_raw.resize(slots.size());
            slot_null_map.resize(slots.size());
            bool fixed = values.isFixedAndContiguous();
            for (size_t slot = 0; slot < slots.size(); ++slot)
            {
                slot_raw[slot] = slots[slot]->raw[c];
                slot_null_map[slot] = reinterpret_cast<const char *>(slots[slot]->null_map[c]);
                fixed = fixed && slot_raw[slot] != nullptr && (nullable != nullptr) == (slot_null_map[slot] != nullptr);
            }
            const size_t size = fixed ? values.sizeOfValueIfFixed() : 0;

            if (size == 1 || size == 2 || size == 4 || size == 8 || size == 16)
            {
                char * data = resizeRaw(values, size, rows);
                if (copy_runs)
                    copyRuns(records, slot_raw, size, data);
                else
                {
                    switch (size)
                    {
                        case 1: copyFixed<UInt8>(records, slot_raw, data); break;
                        case 2: copyFixed<UInt16>(records, slot_raw, data); break;
                        case 4: copyFixed<UInt32>(records, slot_raw, data); break;
                        case 8: copyFixed<UInt64>(records, slot_raw, data); break;
                        case 16: copyFixed<UInt128>(records, slot_raw, data); break;
                        default: break;
                    }
                }
                if (nullable)
                {
                    auto & null_map = nullable->getNullMapData();
                    null_map.resize(rows);
                    if (copy_runs)
                        copyRuns(records, slot_null_map, 1, reinterpret_cast<char *>(null_map.data()));
                    else
                        copyFixed<UInt8>(records, slot_null_map, reinterpret_cast<char *>(null_map.data()));
                }
                continue;
            }

            dst.reserve(rows);
            for (const auto & record : records)
            {
                const IColumn & src = *slots[record.slot]->columns[c];
                if (record.rows == 1)
                    dst.insertFrom(src, record.row);
                else
                    dst.insertRangeFrom(src, record.row, record.rows);
            }
        }
        chunk.setColumns(std::move(columns), rows);
        if (!info->keys.empty())
        {
            auto keys_info = std::make_shared<SplitMergeKeysInfo>();
            keys_info->keys = std::move(info->keys);
            chunk.getChunkInfos().add(std::move(keys_info));
        }
        stat_ns += watch.elapsedNanoseconds();
        stat_cpu_ns += cpu_watch.elapsedNanoseconds();
    }

private:
    /// Resizes a fixed-size column to `rows` values without initializing them.
    static char * resizeRaw(IColumn & column, size_t size, size_t rows)
    {
        switch (size)
        {
            case 1: return resizeAs<ColumnVector<UInt8>>(column, rows);
            case 2: return resizeAs<ColumnVector<UInt16>>(column, rows);
            case 4: return resizeAs<ColumnVector<UInt32>, ColumnVector<Int32>, ColumnVector<Float32>, ColumnDecimal<Decimal32>>(column, rows);
            case 8:
                return resizeAs<ColumnVector<UInt64>, ColumnVector<Int64>, ColumnVector<Float64>, ColumnDecimal<DateTime64>, ColumnDecimal<Decimal64>>(
                    column, rows);
            default: return resizeAs<ColumnVector<UInt128>, ColumnVector<Int128>, ColumnDecimal<Decimal128>>(column, rows);
        }
    }

    /// The common column types are resized without the zeroing `insertManyDefaults` does.
    template <typename... Columns>
    static char * resizeAs(IColumn & column, size_t rows)
    {
        char * data = nullptr;
        auto resize = [&]<typename Column>(Column * typed)
        {
            if (!typed)
                return false;
            typed->getData().resize(rows);
            data = reinterpret_cast<char *>(typed->getData().data());
            return true;
        };
        if ((resize(typeid_cast<Columns *>(&column)) || ...))
            return data;
        column.insertManyDefaults(rows);
        return const_cast<char *>(column.getRawData().data());
    }

    static void copyRuns(const std::vector<SplitMergedRowsInfo::Record> & records, const std::vector<const char *> & src, size_t size, char * dst)
    {
        for (const auto & run : records)
        {
            memcpy(dst, src[run.slot] + run.row * size, run.rows * size);
            dst += run.rows * size;
        }
    }

    template <typename T>
    static void copyFixed(const std::vector<SplitMergedRowsInfo::Record> & records, const std::vector<const char *> & src, char * dst)
    {
        T * out = reinterpret_cast<T *>(dst);
        for (const auto & record : records)
        {
            const char * from = src[record.slot] + record.row * sizeof(T);
            for (size_t r = 0; r < record.rows; ++r)
                *out++ = unalignedLoad<T>(from + r * sizeof(T));
        }
    }

    std::vector<const char *> slot_raw; /// Data of one column of each slot, while gathering.
    std::vector<const char *> slot_null_map; /// Null map of one Nullable column of each slot.
    UInt64 stat_rows = 0;
    UInt64 stat_runs = 0;
    UInt64 stat_run_rows = 0;
    UInt64 stat_ns = 0;
    UInt64 stat_cpu_ns = 0;
};

/// Whether a key column can be merged by `SplitMergingTransform`, and if its values are signed.
std::optional<bool> splitMergeKeySigned(const DataTypePtr & type)
{
    WhichDataType which(type);
    if (which.isNativeUInt() || which.isDate() || which.isDateTime())
        return false;
    if (which.isNativeInt() || which.isDate32() || which.isDateTime64() || which.isDecimal32() || which.isDecimal64() || which.isEnum8() || which.isEnum16())
        return true;
    return {};
}

}

Pipe ReadFromMergeTree::readInOrderSplitByKeyPrefix(
    RangesInDataParts && parts_with_ranges,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    const Names & required_columns,
    const PoolSettings & pool_settings,
    ReadType read_type,
    UInt64 read_limit,
    std::optional<ActionsDAG> & out_projection)
{
    if (is_parallel_reading_from_replicas)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading split by key prefix values is not supported with parallel replicas");

    SetPtr set = split_by_key_prefix_set->get();
    if (!set || !set->hasExplicitSetElements())
        set = split_by_key_prefix_set->buildOrderedSetInplace(context);
    if (!set || !set->hasExplicitSetElements())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The set for reading split by key prefix values is not built");

    const Columns elements = set->getSetElements();
    if (elements.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a single column set, got {} columns", elements.size());

    const auto & sorting_key = storage_snapshot->metadata->getSortingKey();
    const String & key_column = sorting_key.column_names.at(0);
    const DataTypePtr & key_type = sorting_key.data_types.at(0);

    std::vector<Field> values;
    values.reserve(elements[0]->size());
    for (size_t i = 0; i < elements[0]->size(); ++i)
    {
        Field value = convertFieldToType((*elements[0])[i], *key_type);
        if (!value.isNull())
            values.push_back(std::move(value));
    }
    std::ranges::sort(values);
    values.erase(std::unique(values.begin(), values.end()), values.end());

    /// Granule `i` holds keys in [index[i], index[i + 1]], so the granules of value `v` are
    /// [lower_bound(v) - 1, upper_bound(v)), intersected with the ranges selected by the filter.
    std::vector<std::vector<MarkRanges>> value_ranges(values.size(), std::vector<MarkRanges>(parts_with_ranges.size()));
    for (size_t p = 0; p < parts_with_ranges.size(); ++p)
    {
        const auto & part = parts_with_ranges[p];
        const auto index = part.data_part->getIndex();
        if (!index || index->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has no primary index", part.data_part->name);
        const IColumn & key_index = *index->at(0);
        const size_t granules = part.data_part->index_granularity->getMarksCountWithoutFinal();
        if (key_index.size() < granules)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has {} index marks for {} granules", part.data_part->name, key_index.size(), granules);

        std::vector<Field> marks(granules);
        for (size_t i = 0; i < granules; ++i)
            key_index.get(i, marks[i]);

        for (size_t v = 0; v < values.size(); ++v)
        {
            const size_t lower = std::lower_bound(marks.begin(), marks.end(), values[v]) - marks.begin();
            const size_t upper = std::upper_bound(marks.begin(), marks.end(), values[v]) - marks.begin();
            const size_t begin = lower == 0 ? 0 : lower - 1;
            if (upper == 0)
                continue;

            /// The ranges are sorted and disjoint: start at the first that ends after `begin`.
            auto range = std::partition_point(part.ranges.begin(), part.ranges.end(), [&](const MarkRange & r) { return r.end <= begin; });
            for (; range != part.ranges.end() && range->begin < upper; ++range)
            {
                const size_t range_begin = std::max(range->begin, begin);
                const size_t range_end = std::min(range->end, upper);
                if (range_begin < range_end)
                    value_ranges[v][p].emplace_back(range_begin, range_end);
            }
        }
    }

    const size_t read_ahead_rows = context->getSettingsRef()[Setting::read_in_order_split_by_key_prefix_in_read_ahead_rows];
    auto value_block_size = block_size;
    /// Blocks of half the read-ahead, so one block is merged while the next is read.
    value_block_size.max_block_size_rows = std::min<UInt64>(value_block_size.max_block_size_rows, std::max<size_t>(read_ahead_rows / 2, 1));
    const bool descending = query_info.input_order_info->direction < 0;

    /// The merge keys, if they can be merged in the reading step: the inputs are in the order of the split key
    /// values, so when the split key is the last merge key, the merge breaks ties by input instead of comparing it.
    std::vector<SplitMergeKey> keys;
    size_t words = 0;
    bool merge_here = !split_merge_key_positions.empty();
    std::span<const size_t> merge_keys(split_merge_key_positions);
    if (merge_here && merge_keys.back() == 0)
        merge_keys = merge_keys.first(merge_keys.size() - 1);
    for (size_t key : merge_keys)
    {
        if (!merge_here)
            break;
        const auto & type = sorting_key.data_types.at(key);
        auto key_signed = splitMergeKeySigned(type);
        if (!key_signed)
        {
            /// The merge above the reading step merges the streams instead.
            LOG_DEBUG(log, "Key column {} of type {} is not merged in the reading step", sorting_key.column_names.at(key), type->getName());
            merge_here = false;
            break;
        }
        keys.push_back({0, *key_signed, type->getSizeOfValueInMemory()});
    }
    if (merge_here)
    {
        words = layoutSplitMergeKeys(keys);
        if (words > SplitMergingTransform::MAX_WORDS)
        {
            LOG_DEBUG(log, "{} merge keys do not fit the merge in the reading step", keys.size());
            merge_here = false;
        }
    }

    /// Values are merged in merge groups of consecutive values with about `GROUP_SIZE` inputs (see below). Inside
    /// a merge group, in each part, consecutive values whose granules in the part fit `GROUP_MARKS` are read by one
    /// reader, so a granule shared by neighbouring values is read and filtered once; the merge cuts the stream by
    /// value. A value with more granules in the part is read alone. Without the merge here, every (value, part) is
    /// read alone and filtered, so each stream is sorted by the merge keys.
    static constexpr size_t GROUP_MARKS = 32;
    static constexpr size_t GROUP_VALUES = 64;

    /// A busy value read by one reader decompresses on one thread. When few values are busy, each busy value's
    /// ranges in a part are cut into slices, one input each, in order (their rows do not interleave). A slice
    /// reads all its rows ahead while it is within `slices_ahead` of the slice being merged, so the next slices
    /// decompress in parallel and memory stays bounded. A slice starting inside a compressed block decompresses
    /// that block again (a block of a column of small values spans up to ~100 granules), so slices are as few as
    /// the threads allow: about `threads` per part, `SLICE_MARKS` to `MAX_SLICE_MARKS` granules each.
    static constexpr size_t SLICE_MARKS = 16;
    static constexpr size_t MAX_SLICE_MARKS = 256;
    std::vector<size_t> value_marks(values.size(), 0);
    for (size_t v = 0; v < values.size(); ++v)
        for (const auto & ranges : value_ranges[v])
            value_marks[v] += ranges.getNumberOfMarks();
    std::vector<char> sliced(values.size(), 0);
    size_t slices_ahead = 0;
    if (merge_here)
    {
        const size_t busy = std::ranges::count_if(value_marks, [](size_t marks) { return marks >= 2 * SLICE_MARKS; });
        const size_t threads = std::max<size_t>(pool_settings.threads, 1);
        if (busy > 0 && busy <= threads)
        {
            slices_ahead = std::max<size_t>(2, threads / busy);
            for (size_t v = 0; v < values.size(); ++v)
                sliced[v] = value_marks[v] >= 2 * SLICE_MARKS;
        }
    }
    auto slices_of = [&](size_t v, size_t p)
    {
        const size_t threads = std::max<size_t>(pool_settings.threads, 1);
        const size_t slice_marks = std::clamp<size_t>(
            (value_ranges[v][p].getNumberOfMarks() + threads - 1) / threads, SLICE_MARKS, MAX_SLICE_MARKS);
        std::vector<MarkRanges> slices;
        size_t in_slice = slice_marks;
        for (auto range : value_ranges[v][p])
        {
            while (range.begin < range.end)
            {
                if (in_slice == slice_marks)
                {
                    slices.emplace_back();
                    in_slice = 0;
                }
                const size_t take = std::min(range.end - range.begin, slice_marks - in_slice);
                slices.back().emplace_back(range.begin, range.begin + take);
                range.begin += take;
                in_slice += take;
            }
        }
        return slices;
    };

    /// An input is (value, part, slice); its index orders inputs value-major.
    std::vector<std::vector<size_t>> input_index(values.size(), std::vector<size_t>(parts_with_ranges.size(), 0));
    std::vector<std::pair<size_t, size_t>> input_start; /// (part, first granule) of each input.
    std::vector<size_t> merge_group_value_ends;
    size_t num_inputs = 0;
    {
        size_t group_inputs = 0;
        for (size_t v = 0; v < values.size(); ++v)
        {
            for (size_t p = 0; p < parts_with_ranges.size(); ++p)
            {
                if (value_ranges[v][p].empty())
                    continue;
                input_index[v][p] = num_inputs;
                if (sliced[v])
                    for (const auto & slice : slices_of(v, p))
                        input_start.emplace_back(p, slice.front().begin);
                else
                    input_start.emplace_back(p, value_ranges[v][p].front().begin);
                const size_t inputs = input_start.size() - num_inputs;
                num_inputs += inputs;
                group_inputs += inputs;
            }
            if (merge_here && group_inputs >= SplitMergingTransform::GROUP_SIZE)
            {
                merge_group_value_ends.push_back(v + 1);
                group_inputs = 0;
            }
        }
        if (merge_group_value_ends.empty() || merge_group_value_ends.back() != values.size())
            merge_group_value_ends.push_back(values.size());
    }

    /// One reader per (merge group, part, reading group), or per slice; entries of a merge group are consecutive.
    RangesInDataParts entries;
    std::vector<std::vector<std::pair<size_t, size_t>>> entry_inputs; /// (value, input index) of each entry.
    std::vector<std::pair<bool, size_t>> entry_slices; /// (reads ahead from the start, entry that starts when it is used up)
    std::vector<size_t> merge_group_ends; /// End entry of each merge group.
    {
        size_t value_begin = 0;
        for (size_t value_end : merge_group_value_ends)
        {
            for (size_t p = 0; p < parts_with_ranges.size(); ++p)
            {
                const auto & part = parts_with_ranges[p];
                auto add_entry = [&](MarkRanges ranges, std::vector<std::pair<size_t, size_t>> inputs, bool eager, size_t next)
                {
                    entries.emplace_back(part.data_part, part.parent_part, part.part_index_in_query, part.part_starting_offset_in_query, std::move(ranges), part.read_hints);
                    entry_inputs.push_back(std::move(inputs));
                    entry_slices.emplace_back(eager, next);
                };

                MarkRanges group_ranges;
                std::vector<std::pair<size_t, size_t>> group_inputs;
                size_t group_marks = 0;
                auto flush = [&]
                {
                    if (group_inputs.empty())
                        return;
                    add_entry(std::move(group_ranges), std::move(group_inputs), false, SplitMergingTransform::NO_PORT);
                    group_ranges = {};
                    group_inputs = {};
                    group_marks = 0;
                };

                for (size_t v = value_begin; v < value_end; ++v)
                {
                    const auto & ranges = value_ranges[v][p];
                    if (ranges.empty())
                        continue;
                    const size_t marks = ranges.getNumberOfMarks();
                    if (sliced[v])
                    {
                        flush();
                        const auto slices = slices_of(v, p);
                        const size_t first_entry = entries.size();
                        for (size_t k = 0; k < slices.size(); ++k)
                        {
                            /// Reading in reverse merges the last slice first.
                            const bool eager = descending ? k + slices_ahead >= slices.size() : k < slices_ahead;
                            size_t next = SplitMergingTransform::NO_PORT;
                            if (descending && k >= slices_ahead)
                                next = first_entry + k - slices_ahead;
                            else if (!descending && k + slices_ahead < slices.size())
                                next = first_entry + k + slices_ahead;
                            add_entry(slices[k], {{v, input_index[v][p] + k}}, eager, next);
                        }
                        continue;
                    }
                    if (!merge_here || marks > GROUP_MARKS)
                    {
                        flush();
                        add_entry(ranges, {{v, input_index[v][p]}}, false, SplitMergingTransform::NO_PORT);
                        continue;
                    }
                    if (group_marks + marks > GROUP_MARKS || group_inputs.size() >= GROUP_VALUES)
                        flush();
                    for (const auto & range : ranges)
                    {
                        if (!group_ranges.empty() && group_ranges.back().end >= range.begin)
                            group_ranges.back().end = std::max(group_ranges.back().end, range.end);
                        else
                            group_ranges.push_back(range);
                    }
                    group_inputs.emplace_back(v, input_index[v][p]);
                    group_marks += marks;
                }
                flush();
            }
            if (merge_group_ends.empty() ? !entries.empty() : merge_group_ends.back() != entries.size())
                merge_group_ends.push_back(entries.size());
            value_begin = value_end;
        }
    }

    LOG_DEBUG(log, "Reading in order split by {} values of {}: {} inputs, {} streams", values.size(), key_column, num_inputs, entries.size());
    if (entries.empty())
        return {};

    Pipe pipe = readInOrder(
        std::move(entries), index_build_context, required_columns, pool_settings, read_type, read_limit,
        /*split_index=*/std::nullopt, &value_block_size);
    if (pipe.empty())
        return {};
    if (pipe.numOutputPorts() != entry_inputs.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} streams, got {}", entry_inputs.size(), pipe.numOutputPorts());

    const size_t split_position = pipe.getHeader().getPositionByName(key_column);
    const DataTypePtr split_type = pipe.getHeader().getByPosition(split_position).type;
    ColumnPtr all_values;
    {
        auto column = split_type->createColumn();
        column->reserve(values.size());
        for (const auto & value : values)
            column->insert(value);
        all_values = std::move(column);
    }
    auto value_column = [&](size_t v) { return all_values->cut(v, 1); };

    /// One input needs no merge: its stream is filtered to its value.
    if (num_inputs == 1)
        merge_here = false;

    if (!merge_here)
    {
        size_t port = 0;
        pipe.addSimpleTransform([&](const SharedHeader & stream_header, Pipe::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != Pipe::StreamType::Main)
                return nullptr;
            const auto & inputs_of_entry = entry_inputs.at(port++);
            if (inputs_of_entry.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A stream read without the merge holds {} values", inputs_of_entry.size());
            return std::make_shared<KeyPrefixValueFilterTransform>(stream_header, split_position, value_column(inputs_of_entry.front().first));
        });
        if (!split_merge_key_positions.empty())
            out_projection = createProjection(pipe.getHeader());
        return pipe;
    }

    /// Compute the sorting key columns the merge needs (e.g. an expression in the key), and drop them afterwards.
    const Block header_before_keys = pipe.getHeader();
    const size_t prefix_size = *std::ranges::max_element(split_merge_key_positions) + 1;
    bool keys_read = true;
    for (size_t key = 0; key < prefix_size; ++key)
        keys_read = keys_read && header_before_keys.has(sorting_key.column_names.at(key));
    if (!keys_read)
    {
        auto order_key_prefix_ast = sorting_key.expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);
        auto syntax_result = TreeRewriter(context).analyze(
            order_key_prefix_ast, storage_snapshot->metadata->getColumns().get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
        auto sorting_key_expr = std::make_shared<ExpressionActions>(ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false));
        pipe.addSimpleTransform([sorting_key_expr](const SharedHeader & stream_header)
            { return std::make_shared<ExpressionTransform>(stream_header, sorting_key_expr); });
    }

    const auto & merge_header = pipe.getHeader();
    for (size_t k = 0; k < keys.size(); ++k)
    {
        const auto & column = merge_header.getByName(sorting_key.column_names.at(merge_keys[k]));
        if (column.type->getSizeOfValueInMemory() != keys[k].bytes)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge key {} has type {}, expected {} bytes", column.name, column.type->getName(), keys[k].bytes);
        keys[k].position = merge_header.getPositionByName(column.name);
    }
    /// The merge buffers rows until their time comes, so it keeps only the columns of the step's output: not key
    /// columns computed for the merge, nor columns read only to compute others (e.g. an ALIAS in PREWHERE).
    Block kept_columns;
    for (const auto & column : header_before_keys)
        if (getOutputHeader()->has(column.name) || column.name == key_column)
            kept_columns.insert(column);
    const auto header_after_keys = std::make_shared<const Block>(std::move(kept_columns));
    pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        { return std::make_shared<SplitMergeKeysTransform>(stream_header, header_after_keys, keys, words, descending); });
    auto merged_header = pipe.getSharedHeader();
    const size_t merged_split_position = merged_header->getPositionByName(key_column);

    /// Lazy start: an input whose first granule begins with its own value has that granule's index mark as a
    /// bound of its first key, so the merge does not wait for (or start reading) it before the merge reaches the
    /// bound. A granule that begins with an earlier value says nothing about the value's first row. Reading in
    /// reverse would need the bound of the last row; it has none.
    size_t bounded_inputs = 0;
    auto setBound = [&](size_t v, size_t input, SplitMergeInput & split_input)
    {
        if (descending)
            return;
        const auto [p, granule] = input_start.at(input);
        const auto index = parts_with_ranges[p].data_part->getIndex();
        if (granule >= index->at(0)->size())
            return;
        Field first;
        index->at(0)->get(granule, first);
        if (first != values[v])
            return;
        for (size_t k = 0; k < keys.size(); ++k)
        {
            /// Keys missing from the index (trimmed suffix) stay 0, the least packed value.
            if (merge_keys[k] >= index->size())
                break;
            const auto mark_value = index->at(merge_keys[k])->getDataAt(granule);
            if (mark_value.size() != keys[k].bytes)
                return;
            switch (keys[k].bytes)
            {
                case 1: packSplitMergeKey<UInt8>(mark_value.data(), 1, words, keys[k], false, split_input.bound.data()); break;
                case 2: packSplitMergeKey<UInt16>(mark_value.data(), 1, words, keys[k], false, split_input.bound.data()); break;
                case 4: packSplitMergeKey<UInt32>(mark_value.data(), 1, words, keys[k], false, split_input.bound.data()); break;
                case 8: packSplitMergeKey<UInt64>(mark_value.data(), 1, words, keys[k], false, split_input.bound.data()); break;
                default: return;
            }
        }
        split_input.has_bound = true;
    };

    /// The inputs of streams [begin, end), ranked from `base_input` up (down when descending).
    auto inputs_of_streams = [&](size_t begin, size_t end)
    {
        size_t base = std::numeric_limits<size_t>::max();
        size_t last = 0;
        for (size_t e = begin; e < end; ++e)
            for (const auto & [v, input] : entry_inputs[e])
            {
                base = std::min(base, input);
                last = std::max(last, input);
            }
        std::vector<std::vector<SplitMergeInput>> port_inputs(end - begin);
        for (size_t e = begin; e < end; ++e)
            for (const auto & [v, input] : entry_inputs[e])
            {
                auto & split_input = port_inputs[e - begin].emplace_back();
                split_input.value = value_column(v);
                split_input.rank = descending ? last - input : input - base;
                setBound(v, input, split_input);
                bounded_inputs += split_input.has_bound;
            }
        return port_inputs;
    };
    auto slices_of_streams = [&](size_t begin, size_t end)
    {
        std::vector<std::pair<bool, size_t>> port_slices(end - begin, {false, SplitMergingTransform::NO_PORT});
        for (size_t e = begin; e < end; ++e)
        {
            const auto [eager, next] = entry_slices[e];
            if (next == SplitMergingTransform::NO_PORT && !eager)
                continue;
            if (next != SplitMergingTransform::NO_PORT && (next < begin || next >= end))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A slice and its next one are in different merges");
            port_slices[e - begin] = {eager, next == SplitMergingTransform::NO_PORT ? next : next - begin};
        }
        return port_slices;
    };

    /// With many inputs, one merge touches a few cache lines of every input per output row, which misses the
    /// caches and the TLB on nearly every row. So the merge groups (above) are first merged in parallel, each a
    /// small tree whose inputs' current rows stay in cache; then the groups' sorted streams are merged. A merge
    /// group holds consecutive inputs, so its rank and then the input's rank inside it give the same order as
    /// the input's rank among all inputs.
    /// Lazy ports read ahead of the merge, `LAZY_AHEAD` over all merges.
    static constexpr size_t LAZY_AHEAD = 64;
    const size_t lazy_ahead = std::max<size_t>(1, LAZY_AHEAD / merge_group_ends.size());

    if (merge_group_ends.size() > 1)
    {
        const size_t group_block_rows = std::max<size_t>(read_ahead_rows / 2, 1);
        pipe.transform([&](OutputPortRawPtrs ports)
        {
            Processors processors;
            size_t begin = 0;
            for (size_t end : merge_group_ends)
            {
                auto merging = std::make_shared<SplitMergingTransform>(
                    merged_header, inputs_of_streams(begin, end), merged_split_position, words, descending, read_ahead_rows, group_block_rows,
                    /*emit_keys_=*/true, slices_of_streams(begin, end), lazy_ahead);
                auto input = merging->getInputs().begin();
                for (size_t i = begin; i < end; ++i, ++input)
                    connect(*ports[i], *input);
                auto gather = std::make_shared<SplitGatherTransform>(merging->getOutputs().front().getSharedHeader(), merged_header);
                connect(merging->getOutputs().front(), gather->getInputPort());
                processors.push_back(std::move(merging));
                processors.push_back(std::move(gather));
                begin = end;
            }
            return processors;
        });

        /// The groups' streams are merged in parallel: cut into tasks of rows between sampled keys, merged and
        /// gathered by several workers, and put back in order.
        const size_t num_groups = merge_group_ends.size();
        std::vector<UInt64> group_ranks(num_groups);
        for (size_t g = 0; g < num_groups; ++g)
            group_ranks[g] = descending ? num_groups - 1 - g : g;
        static constexpr size_t MERGE_WORKERS = 8;
        const size_t workers = std::clamp<size_t>(pool_settings.threads / 2, 1, MERGE_WORKERS);
        pipe.transform([&](OutputPortRawPtrs ports)
        {
            Processors processors;
            auto batching = std::make_shared<SplitBatchingTransform>(
                ports.front()->getSharedHeader(), group_ranks, words, read_ahead_rows, block_size.max_block_size_rows, 4 * workers);
            auto batching_input = batching->getInputs().begin();
            for (auto * port : ports)
                connect(*port, *batching_input++);
            auto deal = std::make_shared<SplitDealTransform>(batching->getOutputs().front().getSharedHeader(), workers);
            connect(batching->getOutputs().front(), deal->getInputs().front());
            auto collect = std::make_shared<SplitCollectTransform>(merged_header, workers);
            auto collect_input = collect->getInputs().begin();
            for (auto & deal_output : deal->getOutputs())
            {
                auto merging = std::make_shared<SplitTaskMergeTransform>(deal_output.getSharedHeader(), words);
                connect(deal_output, merging->getInputPort());
                auto gather = std::make_shared<SplitGatherTransform>(merging->getOutputPort().getSharedHeader(), merged_header);
                connect(merging->getOutputPort(), gather->getInputPort());
                connect(gather->getOutputPort(), *collect_input++);
                processors.push_back(std::move(merging));
                processors.push_back(std::move(gather));
            }
            processors.push_back(std::move(batching));
            processors.push_back(std::move(deal));
            processors.push_back(std::move(collect));
            return processors;
        });
        LOG_DEBUG(log, "Merge in reading: {} merge groups, merged by {} workers, {} of {} inputs start lazily", merge_group_ends.size(), workers, bounded_inputs, num_inputs);
        out_projection = createProjection(*header_after_keys);
        return pipe;
    }

    pipe.addTransform(std::make_shared<SplitMergingTransform>(
        merged_header, inputs_of_streams(0, pipe.numOutputPorts()), merged_split_position, words, descending, read_ahead_rows,
        block_size.max_block_size_rows, /*emit_keys_=*/false, slices_of_streams(0, pipe.numOutputPorts())));
    /// The gather is mostly copying rows that other threads read, so a few gather in parallel.
    static constexpr size_t GATHERS = 4;
    const size_t gathers = std::min(GATHERS, std::max<size_t>(pool_settings.threads, 1));
    if (gathers > 1)
    {
        pipe.transform([&](OutputPortRawPtrs ports)
        {
            Processors processors;
            auto deal = std::make_shared<SplitDealTransform>(ports.front()->getSharedHeader(), gathers);
            connect(*ports.front(), deal->getInputs().front());
            auto collect = std::make_shared<SplitCollectTransform>(merged_header, gathers);
            auto collect_input = collect->getInputs().begin();
            for (auto & deal_output : deal->getOutputs())
            {
                auto gather = std::make_shared<SplitGatherTransform>(deal_output.getSharedHeader(), merged_header);
                connect(deal_output, gather->getInputPort());
                connect(gather->getOutputPort(), *collect_input++);
                processors.push_back(std::move(gather));
            }
            processors.push_back(std::move(deal));
            processors.push_back(std::move(collect));
            return processors;
        });
    }
    else
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
            { return std::make_shared<SplitGatherTransform>(stream_header, merged_header); });
    LOG_DEBUG(log, "Merge in reading: {} merge groups, {} of {} inputs start lazily", merge_group_ends.size(), bounded_inputs, num_inputs);
    out_projection = createProjection(*header_after_keys);
    return pipe;
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts_with_ranges,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    size_t num_streams,
    const Names & column_names,
    std::optional<ActionsDAG> & out_projection,
    const InputOrderInfoPtr & input_order_info)
{
    const auto & settings = context->getSettingsRef();

    LOG_TRACE(log, "Spreading ranges among streams with order");

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    Pipes res;

    if (info.sum_marks == 0)
        return {};

    /// PREWHERE actions can remove some input columns (which are needed only for prewhere condition).
    /// In case of read-in-order, PREWHERE is executed before sorting. But removed columns could be needed for sorting key.
    /// To fix this, we prohibit removing any input in prewhere actions. Instead, projection actions will be added after sorting.
    /// See 02354_read_in_order_prewhere.sql as an example.
    bool have_input_columns_removed_after_prewhere = false;
    if (query_info.prewhere_info || query_info.row_level_filter)
    {
        NameSet sorting_columns;
        for (const auto & column : storage_snapshot->metadata->getSortingKey().expression->getRequiredColumnsWithTypes())
            sorting_columns.insert(column.name);

        have_input_columns_removed_after_prewhere = restorePrewhereInputs(query_info.row_level_filter.get(), query_info.prewhere_info.get(), sorting_columns);
    }

    if (num_streams > 1)
    {
        /// Reduce num_streams if requested value is unnecessarily large.
        ///
        /// Additional increase of streams number in case of skewed parts, like it's
        /// done in `spreadMarkRangesAmongStreams` won't affect overall performance
        /// due to the single downstream `MergingSortedTransform`.
        /// As in `spreadMarkRangesAmongStreams`, the comparison avoids the overflow of
        /// `num_streams * info.min_marks_for_concurrent_read`.
        if (info.sum_marks / info.min_marks_for_concurrent_read < num_streams && parts_with_ranges.size() < num_streams)
        {
            num_streams = std::max(
                (info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read, parts_with_ranges.size());
        }
    }

    bool need_preliminary_merge = (parts_with_ranges.size() > settings[Setting::read_in_order_two_level_merge_threshold]);

    /// Preliminary MergingSortedTransform consumes virtual row, so it won't reach downstream sorting and optimization won't work.
    if (settings[Setting::read_in_order_use_virtual_row_per_block] && virtual_row_conversion)
        need_preliminary_merge = false;

    const auto read_type = input_order_info->direction == 1 ? ReadType::InOrder : ReadType::InReverseOrder;

    const size_t total_query_nodes = is_parallel_reading_from_replicas
        ? std::min<size_t>(
              context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
              context->getSettingsRef()[Setting::max_parallel_replicas])
        : 1;

    PoolSettings pool_settings{
        .threads = num_streams,
        .sum_marks = parts_with_ranges.getMarksCountAllParts(),
        .min_marks_for_concurrent_read = info.min_marks_for_concurrent_read,
        .preferred_block_size_bytes = settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = info.use_uncompressed_cache,
        .total_query_nodes = total_query_nodes,
    };

    if (split_by_key_prefix_set)
    {
        Pipe pipe = readInOrderSplitByKeyPrefix(
            std::move(parts_with_ranges), index_build_context, column_names, pool_settings, read_type, input_order_info->limit, out_projection);
        if (!pipe.empty() && !out_projection && have_input_columns_removed_after_prewhere)
            out_projection = createProjection(pipe.getHeader());
        return pipe;
    }

    const bool is_local_plan_initiator = isParallelReplicasLocalPlanForInitiator();
    /// Split-stream topology requires both sides to speak the announcement-response protocol so
    /// each `#split_i` pool can ask the initiator "which parts does this stream own?". An older
    /// initiator (parallel-replicas protocol < `DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_ANNOUNCEMENT_RESPONSE`)
    /// has no concept of `#split_i` streams — it either errors out on the extra announcements
    /// (e.g. 25.x raises "more initial requests than there are replicas") or silently registers
    /// each split as its own full-table stream and the follower amplifies reads `~num_streams`×.
    /// When the upstream can't speak the response protocol, fall through to the legacy
    /// single-pool branch below — every parallel-replicas-aware server understands that shape.
    const bool upstream_supports_split_topology
        = context->getClientInfo().connection_parallel_replicas_protocol_version
        >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_ANNOUNCEMENT_RESPONSE;
    const bool is_local_plan_follower = isParallelReplicasLocalPlanForFollower() && upstream_supports_split_topology;
    /// Genuine range splitting runs only for the initiator and for purely-local reads.
    /// Followers use all parts for every split and only need `num_streams` as the split count,
    /// since the initiator is the authority on split topology.
    const bool need_split = is_local_plan_initiator || !is_parallel_reading_from_replicas;

    /// Only the local-plan follower path needs all parts replicated across per-split pools
    /// (each split reads from a copy and filters down to its assigned subset). The legacy
    /// single-pool path (`parallel_replicas_local_plan=0`) consumes `parts_with_ranges` exactly
    /// once with a `std::move`, so a separate copy would just be wasted work on the legacy
    /// in-order parallel-replica hot path.
    RangesInDataParts all_parts_for_replicas;
    if (is_local_plan_follower)
        all_parts_for_replicas = parts_with_ranges;

    std::vector<RangesInDataParts> split_parts_and_ranges;
    if (need_split)
    {
        const size_t min_marks_per_stream = (info.sum_marks - 1) / num_streams + 1;
        split_parts_and_ranges.reserve(num_streams);

        for (size_t i = 0; i < num_streams && !parts_with_ranges.empty(); ++i)
        {
            size_t need_marks = min_marks_per_stream;
            RangesInDataParts new_parts;

            /// Loop over parts.
            /// We will iteratively take part or some subrange of a part from the back
            ///  and assign a stream to read from it.
            while (need_marks > 0 && !parts_with_ranges.empty())
            {
                RangesInDataPart part = parts_with_ranges.back();
                parts_with_ranges.pop_back();
                size_t & marks_in_part = info.sum_marks_in_parts.back();

                /// We will not take too few rows from a part.
                if (marks_in_part >= info.min_marks_for_concurrent_read && need_marks < info.min_marks_for_concurrent_read)
                    need_marks = info.min_marks_for_concurrent_read;

                /// Do not leave too few rows in the part.
                if (marks_in_part > need_marks && marks_in_part - need_marks < info.min_marks_for_concurrent_read)
                    need_marks = marks_in_part;

                MarkRanges ranges_to_get_from_part;

                /// We take full part if it contains enough marks or
                /// if we know limit and part contains less than 'limit' rows.
                bool take_full_part = marks_in_part <= need_marks || (input_order_info->limit && input_order_info->limit < part.getRowsCount());

                /// We take the whole part if it is small enough.
                if (take_full_part)
                {
                    ranges_to_get_from_part = part.ranges;

                    need_marks -= marks_in_part;
                    info.sum_marks_in_parts.pop_back();
                }
                else
                {
                    /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                    while (need_marks > 0)
                    {
                        if (part.ranges.empty())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected end of ranges while spreading marks among streams");

                        MarkRange & range = part.ranges.front();

                        const size_t marks_in_range = range.end - range.begin;
                        const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                        ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                        range.begin += marks_to_get_from_range;
                        marks_in_part -= marks_to_get_from_range;
                        need_marks -= marks_to_get_from_range;
                        if (range.begin == range.end)
                            part.ranges.pop_front();
                    }
                    parts_with_ranges.emplace_back(part);
                }

                ranges_to_get_from_part = splitRangesToAvoidLargeReads(
                    ranges_to_get_from_part,
                    input_order_info->direction,
                    (*data_settings)[MergeTreeSetting::index_granularity],
                    block_size.max_block_size_rows);
                new_parts.emplace_back(
                    part.data_part,
                    part.parent_part,
                    part.part_index_in_query,
                    part.part_starting_offset_in_query,
                    std::move(ranges_to_get_from_part),
                    part.read_hints);
            }

            split_parts_and_ranges.emplace_back(std::move(new_parts));
        }
    }

    Pipes pipes;
    /// Each split runs as an independent pool. If we pass the top-level `.threads = num_streams`
    /// to every pool, `min_marks_per_request = min_marks_per_task * threads` is inflated by
    /// num_splits-fold across all pools. Divide threads evenly across splits (rounded up).
    auto make_per_split_pool_settings = [&](size_t num_splits)
    {
        PoolSettings per_split = pool_settings;
        const size_t divisor = std::max<size_t>(num_splits, 1);
        per_split.threads = (pool_settings.threads + divisor - 1) / divisor;
        return per_split;
    };

    if (is_local_plan_initiator)
    {
        /// Initiator with local plan: each split gets its own subset of parts (genuine splitting).
        const size_t num_splits = split_parts_and_ranges.size();
        const PoolSettings per_split_pool_settings = make_per_split_pool_settings(num_splits);
        for (size_t i = 0; i < num_splits; ++i)
        {
            pipes.emplace_back(readInOrder(
                std::move(split_parts_and_ranges[i]), index_build_context, column_names, per_split_pool_settings, read_type,
                input_order_info->limit, /*split_index=*/i));
        }
    }
    else if (is_local_plan_follower)
    {
        /// Non-initiator with local_plan=1: create `num_streams` pools, each over ALL local parts.
        /// The follower can't compute the initiator's authoritative split assignment, so it
        /// optimistically launches `num_streams` streams; the per-stream announcement response
        /// tells each pool which parts actually belong to its split (the rest are filtered out
        /// during source construction in `readInOrder`). Streams that own no parts on this
        /// follower produce empty pipes and are dropped by the `erase_if` below.
        const size_t num_splits = num_streams;
        const PoolSettings per_split_pool_settings = make_per_split_pool_settings(num_splits);
        for (size_t i = 0; i < num_splits; ++i)
        {
            pipes.emplace_back(readInOrder(
                RangesInDataParts(all_parts_for_replicas), index_build_context, column_names, per_split_pool_settings, read_type,
                input_order_info->limit, /*split_index=*/i));
        }
    }
    else if (is_parallel_reading_from_replicas)
    {
        /// parallel_replicas_local_plan=0: old behavior, single pool with all parts. We never
        /// took the local-plan-follower branch above, so `parts_with_ranges` is still intact —
        /// move it directly into the only pool that will consume it (no copy needed).
        pipes.emplace_back(readInOrder(
            std::move(parts_with_ranges), index_build_context, column_names, pool_settings, read_type,
            input_order_info->limit));
    }
    else /* local reading case */
    {
        /// Preserve master behaviour: every split gets the unmodified `pool_settings` (with
        /// `.threads = num_streams`). The per-split divider only exists to keep the new
        /// parallel-replicas split topology from inflating `min_marks_per_request` across the
        /// per-split pools — local reads have no such concern.
        for (auto && item : split_parts_and_ranges)
        {
            pipes.emplace_back(readInOrder(
                std::move(item), index_build_context, column_names, pool_settings, read_type, input_order_info->limit));
        }
    }

    std::erase_if(pipes, [](const Pipe & p) { return p.empty(); });

    Block pipe_header;
    if (!pipes.empty())
        pipe_header = pipes.front().getHeader();

    if (need_preliminary_merge || output_each_partition_through_separate_port)
    {
        size_t prefix_size = input_order_info->used_prefix_of_sorting_key_size;
        auto order_key_prefix_ast = storage_snapshot->metadata->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, storage_snapshot->metadata->getColumns().get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()));
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);
        const auto & sorting_columns = storage_snapshot->metadata->getSortingKey().column_names;
        std::vector<bool> reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();

        SortDescription sort_description;
        sort_description.compile_sort_description = settings[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

        sort_description.reserve(prefix_size);
        for (size_t i = 0; i < prefix_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction * -1);
            else
                sort_description.emplace_back(sorting_columns[i], input_order_info->direction);
        }

        auto sorting_key_expr = std::make_shared<ExpressionActions>(std::move(sorting_key_prefix_expr));

        auto merge_streams = [&](Pipe & pipe)
        {
            pipe.addSimpleTransform([sorting_key_expr](const SharedHeader & header)
                                    { return std::make_shared<ExpressionTransform>(header, sorting_key_expr); });

            if (pipe.numOutputPorts() > 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getSharedHeader(),
                    pipe.numOutputPorts(),
                    sort_description,
                    block_size.max_block_size_rows,
                    /*max_block_size_bytes=*/0,
                    /*max_dynamic_subcolumns=*/ std::nullopt,
                    SortingQueueStrategy::Batch,
                    /*limit=*/ 0,
                    /*always_read_till_end=*/ false,
                    /*out_row_sources_buf=*/ nullptr,
                    /*filter_column_name=*/ std::nullopt,
                    /*use_average_block_sizes=*/ false,
                    /*apply_virtual_row_conversions*/ false);

                pipe.addTransform(std::move(transform));
            }
        };

        if (!pipes.empty() && output_each_partition_through_separate_port)
        {
            /// In contrast with usual aggregation in order that allocates separate AggregatingTransform for each data part,
            /// aggregation of partitioned data uses the same AggregatingTransform for all parts of the same partition.
            /// Thus we need to merge all partition parts into a single sorted stream.
            Pipe pipe = Pipe::unitePipes(std::move(pipes));
            merge_streams(pipe);
            return pipe;
        }

        for (auto & pipe : pipes)
            merge_streams(pipe);
    }

    if (!pipes.empty() && (need_preliminary_merge || have_input_columns_removed_after_prewhere))
        /// Drop temporary columns, added by 'sorting_key_prefix_expr'
        out_projection = createProjection(pipe_header);

    return Pipe::unitePipes(std::move(pipes));
}

/// Returns the list of column names required for the transforms in addMergingFinal
static NameSet getColumnsRequiredForMergingFinal(
    const SortDescription & sort_description, const StorageMetadataPtr & metadata_snapshot, MergeTreeData::MergingParams merging_params)
{
    NameSet required_columns = sort_description | std::views::transform([](const SortColumnDescription & desc) { return desc.column_name; })
        | std::ranges::to<NameSet>();
    /// The merge always orders by the physical sorting key, so those columns must be read even when
    /// they are not in the query output (e.g. a sorting-key column moved to PREWHERE and pruned from
    /// the output header would otherwise be dropped, leaving the merge without its key column).
    for (const auto & column : metadata_snapshot->getColumnsRequiredForFinal())
        required_columns.insert(column);
    switch (merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            [[fallthrough]];
        case MergeTreeData::MergingParams::Aggregating:
            [[fallthrough]];
        case MergeTreeData::MergingParams::Coalescing:
            [[fallthrough]];
        case MergeTreeData::MergingParams::Summing:
            break;
        case MergeTreeData::MergingParams::VersionedCollapsing:
            [[fallthrough]];
        case MergeTreeData::MergingParams::Collapsing: {
            required_columns.insert(merging_params.sign_column);
            break;
        }
        case MergeTreeData::MergingParams::Replacing: {
            required_columns.insert(merging_params.is_deleted_column);
            required_columns.insert(merging_params.version_column);
            break;
        }
        case MergeTreeData::MergingParams::Graphite:
            required_columns.insert(merging_params.graphite_params.path_column_name);
            required_columns.insert(merging_params.graphite_params.time_column_name);
            required_columns.insert(merging_params.graphite_params.version_column_name);
            required_columns.insert(merging_params.graphite_params.value_column_name);
            break;
    }
    required_columns.erase(""); // remove empty column names
    return required_columns;
}

bool ReadFromMergeTree::doNotMergePartsAcrossPartitionsFinal() const
{
    const auto & settings = context->getSettingsRef();

    /// If setting do_not_merge_across_partitions_select_final is set always prefer it
    if (settings[Setting::do_not_merge_across_partitions_select_final])
        return true;
    /// If automatic decision is disabled, should return false straight away
    else if (!settings[Setting::enable_automatic_decision_for_merging_across_partitions_for_final])
        return false;

    if (!storage_snapshot->metadata->hasPrimaryKey() || !storage_snapshot->metadata->hasPartitionKey())
        return false;

    /** To avoid merging parts across partitions we want result of partition key expression for
      * rows with same primary key to be the same.
      *
      * If partition key expression is deterministic, and contains only columns that are included
      * in primary key, then for same primary key column values, result of partition key expression
      * will be the same.
      */
    const auto & partition_key_expression = storage_snapshot->metadata->getPartitionKey().expression;
    if (partition_key_expression->getActionsDAG().hasNonDeterministic())
        return false;

    const auto & primary_key_columns = storage_snapshot->metadata->getPrimaryKey().column_names;
    NameSet primary_key_columns_set(primary_key_columns.begin(), primary_key_columns.end());

    const auto & partition_key_required_columns = partition_key_expression->getRequiredColumns();
    for (const auto & partition_key_required_column : partition_key_required_columns)
        if (!primary_key_columns_set.contains(partition_key_required_column))
            return false;

    return true;
}

std::optional<FilterDAGInfo> ReadFromMergeTree::getSamplingFilter() const
{
    const auto & sampling = getAnalysisResult().sampling;
    if (!sampling.use_sampling)
        return {};
    return FilterDAGInfo{sampling.filter_expression->clone(), sampling.filter_function->getColumnName(), /*do_remove_column=*/false};
}

Pipe ReadFromMergeTree::readNonIntersectingWithEngineFilter(
    RangesInDataParts && parts,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    size_t num_streams,
    const Names & origin_column_names)
{
    return readNonIntersectingFinalWithEngineFilter(
        data.merging_params, origin_column_names, context,
        [&](const Names & columns)
        { return spreadMarkRangesAmongStreams(std::move(parts), index_build_context, num_streams, columns); });
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsFinal(
    RangesInDataParts && parts_with_ranges,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    size_t num_streams,
    const Names & origin_column_names,
    const Names & column_names,
    std::optional<ActionsDAG> & out_projection)
{
    const size_t total_marks_to_read = parts_with_ranges.getMarksCountAllParts();
    if (total_marks_to_read == 0)
        return {};

    const auto & settings = context->getSettingsRef();
    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    chassert(num_streams == requested_num_streams);
    num_streams = std::min<size_t>(num_streams, settings[Setting::max_final_threads]);

    /// The read-in-order optimization may request reading in the reverse order (see `requestReadingInOrder`).
    const bool read_in_reverse = reader_settings.read_in_order && query_info.input_order_info && query_info.input_order_info->direction < 0;
    const auto read_type = read_in_reverse ? ReadType::InReverseOrder : ReadType::InOrder;

    /// A whole range has to be kept in memory to read it in the reverse order, so split ranges into smaller ones.
    auto split_ranges_for_reverse_read = [this](RangesInDataParts parts)
    {
        for (auto & part : parts)
            part.ranges = splitRangesToAvoidLargeReads(
                part.ranges, -1, (*data_settings)[MergeTreeSetting::index_granularity], block_size.max_block_size_rows);
        return parts;
    };

    /// If do_not_merge_across_partitions_select_final is true than we won't merge parts from different partitions.
    /// We have all parts in parts vector, where parts with same partition are nearby.
    /// So we will store iterators pointed to the beginning of each partition range (and parts.end()),
    /// then we will create a pipe for each partition that will run selecting processor and merging processor
    /// for the parts with this partition. In the end we will unite all the pipes.
    std::vector<RangesInDataParts::iterator> parts_to_merge_ranges;
    auto it = parts_with_ranges.begin();
    parts_to_merge_ranges.push_back(it);

    bool do_not_merge_across_partitions_select_final = doNotMergePartsAcrossPartitionsFinal();
    if (do_not_merge_across_partitions_select_final)
    {
        while (it != parts_with_ranges.end())
        {
            it = std::find_if(
                it, parts_with_ranges.end(), [&it](auto & part) { return it->data_part->info.getPartitionId() != part.data_part->info.getPartitionId(); });
            parts_to_merge_ranges.push_back(it);
        }
    }
    else
    {
        /// If do_not_merge_across_partitions_select_final is false we just merge all the parts.
        parts_to_merge_ranges.push_back(parts_with_ranges.end());
    }

    Pipes merging_pipes;
    Pipes no_merging_pipes;

    /// If do_not_merge_across_partitions_select_final is true and num_streams > 1
    /// we will store lonely parts with level > 0 to use parallel select on them.
    RangesInDataParts non_intersecting_parts_by_primary_key;

    auto sorting_expr = storage_snapshot->metadata->getSortingKey().expression;

    if (query_info.prewhere_info || query_info.row_level_filter)
    {
        NameSet columns_to_restore;
        for (const auto & column : storage_snapshot->metadata->getSortingKey().expression->getRequiredColumnsWithTypes())
            columns_to_restore.insert(column.name);
        if (!data.merging_params.version_column.empty())
            columns_to_restore.insert(data.merging_params.version_column);
        if (!data.merging_params.sign_column.empty())
            columns_to_restore.insert(data.merging_params.sign_column);
        if (!data.merging_params.is_deleted_column.empty())
            columns_to_restore.insert(data.merging_params.is_deleted_column);
        restorePrewhereInputs(query_info.row_level_filter.get(), query_info.prewhere_info.get(), columns_to_restore);
    }

    if (distributed_read_bucket_count > 0)
    {
        /// Distributed parallel FINAL: resolve each lane's coordinator-selected marks to local parts, then build
        /// the per-lane merge pipeline (parallel across lanes, as single-node FINAL) via `buildDistributedFinalPipe`.
        std::unordered_map<String, RangesInDataPart> parts_by_name;
        for (const auto & part : parts_with_ranges)
            parts_by_name.emplace(part.data_part->info.getPartNameV1(), part);

        auto resolve_lane_parts = [&](const RangesInDataPartsDescription & marks)
        {
            RangesInDataParts lane_parts;
            lane_parts.reserve(marks.size());
            for (const auto & part_desc : marks)
            {
                auto found_part = parts_by_name.find(part_desc.info.getPartNameV1());
                if (found_part == parts_by_name.end())
                    throw Exception(ErrorCodes::NO_SUCH_DATA_PART,
                        "Distributed read: part {} selected by the coordinator is not available on this replica "
                        "(diverged by merge or replication lag); retry the query", part_desc.info.getPartNameV1());
                RangesInDataPart lane_part = found_part->second;
                lane_part.ranges = part_desc.ranges;
                lane_parts.push_back(std::move(lane_part));
            }
            return lane_parts;
        };

        auto read_lane_in_order = [&](const RangesInDataPartsDescription & marks)
        {
            return read(resolve_lane_parts(marks), index_build_context, column_names, ReadType::InOrder, 1, 0, info.use_uncompressed_cache);
        };
        auto read_non_intersecting = [&](const RangesInDataPartsDescription & marks)
        {
            return readNonIntersectingWithEngineFilter(resolve_lane_parts(marks), index_build_context, num_streams, origin_column_names);
        };

        return buildDistributedFinalPipe(
            distributed_read_task_buckets, storage_snapshot->metadata, data.merging_params,
            block_size.max_block_size_rows, enable_vertical_final, context, out_projection,
            read_lane_in_order, read_non_intersecting);
    }

    for (size_t range_index = 0; range_index < parts_to_merge_ranges.size() - 1; ++range_index)
    {
        /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
        /// with level > 0 then we won't post-process this part, and if num_streams > 1 we
        /// can use parallel select on such parts.
        bool no_merging_final = do_not_merge_across_partitions_select_final &&
            std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
            parts_to_merge_ranges[range_index]->data_part->info.level > 0 &&
            !reader_settings.read_in_order;

        if (no_merging_final)
        {
            non_intersecting_parts_by_primary_key.push_back(std::move(*parts_to_merge_ranges[range_index]));
            continue;
        }

        Pipes pipes;
        {
            RangesInDataParts new_parts;
            size_t current_ranges_marks = 0;

            for (auto part_it = parts_to_merge_ranges[range_index]; part_it != parts_to_merge_ranges[range_index + 1]; ++part_it)
            {
                new_parts.emplace_back(
                    part_it->data_part,
                    part_it->parent_part,
                    part_it->part_index_in_query,
                    part_it->part_starting_offset_in_query,
                    part_it->ranges,
                    part_it->read_hints);
                current_ranges_marks += part_it->getMarksCount();
            }

            if (new_parts.empty())
                continue;

            /// Maximal number of streams could be very small compared to the number of parts. It gets even worse when we split those parts further.
            /// To not produce too many layers, i.e., to wide pipeline, let's limit the number of streams proportionally to the total number of marks in parts.
            const size_t max_layers = std::max<size_t>((num_streams * current_ranges_marks) / total_marks_to_read, 1);

            if (storage_snapshot->metadata->hasPrimaryKey())
            {
                // Let's split parts into non intersecting parts ranges and layers to ensure data parallelism of FINAL.
                auto in_order_reading_step_getter
                    = [this, &index_build_context, &column_names, &info, read_type, read_in_reverse, &split_ranges_for_reverse_read](auto parts)
                {
                    if (read_in_reverse)
                        parts = split_ranges_for_reverse_read(std::move(parts));

                    return this->read(
                        std::move(parts),
                        index_build_context,
                        column_names,
                        read_type,
                        1 /* num_streams */,
                        0 /* min_marks_for_concurrent_read */,
                        info.use_uncompressed_cache);
                };

                /// Parts of non-zero level still may contain duplicate PK values to merge on FINAL if there's is_deleted column.
                /// Non-intersecting ranges will just go through extra filter added by createExpressionForIsDeleted() to filter
                /// deleted rows.
                bool split_parts_ranges_into_intersecting_and_non_intersecting_final
                    = settings[Setting::split_parts_ranges_into_intersecting_and_non_intersecting_final] &&
                          !reader_settings.read_in_order;

                bool split_intersecting_parts_ranges_into_layers = settings[Setting::split_intersecting_parts_ranges_into_layers_final];

                /// In case of read-in-order, all layer streams are consumed by a single downstream merging transform,
                /// which cannot start until every layer produces its first block. With a small query limit the result
                /// is expected to come from the first layer only, so splitting into layers would only add work-ahead
                /// that is thrown away once the limit is reached, and would delay the first block. Keep a single
                /// lazy merging stream instead.
                /// Use the hard read limit, which is set only when nothing filters the stream above the reading.
                /// With a filter the query consumes limit / selectivity rows, and the selectivity is unknown here:
                /// for a rare-value filter the single merging stream would process most of the table sequentially,
                /// many times slower than the parallel layered merge.
                const UInt64 hard_limit = query_info.input_order_info ? query_info.input_order_info->limit : 0;
                if (split_intersecting_parts_ranges_into_layers && reader_settings.read_in_order && hard_limit)
                {
                    /// The limit counts rows after the FINAL collapse, while source rows may contain multiple
                    /// versions of the same key. A merge collapses them, so a part of non-zero level contains a
                    /// key at most once (at most twice for Collapsing engines), while an unmerged part may
                    /// contain arbitrarily many versions of a key (e.g. inserted with optimize_on_insert = 0)
                    /// and gives no lower bound on the collapsed size. Count only rows of merged parts and
                    /// divide by the number of parts to estimate a layer's output; keep layers unless the
                    /// limit fits into one layer even under this worst-case duplication.
                    /// The estimate deliberately counts rows that FINAL may still drop (is_deleted tombstones,
                    /// Collapsing rows cancelling across parts, rows masked by lightweight deletes): they are
                    /// assumed to be a small fraction of the data.
                    size_t merged_parts_rows = 0;
                    for (const auto & part : new_parts)
                        if (part.data_part->info.level > 0)
                            merged_parts_rows += part.getRowsCount();

                    const size_t max_collapsed_rows_per_key_in_merged_part
                        = (data.merging_params.mode == MergeTreeData::MergingParams::Collapsing
                           || data.merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
                        ? 2
                        : 1;

                    const size_t rows_per_layer = std::max<size_t>(
                        merged_parts_rows / max_layers / new_parts.size() / max_collapsed_rows_per_key_in_merged_part, 1);
                    if (hard_limit < rows_per_layer)
                    {
                        LOG_TRACE(
                            log,
                            "Skipping split of intersecting ranges into layers for FINAL: reading in order with limit {} "
                            "is expected to consume less than one layer of at least {} rows",
                            hard_limit,
                            rows_per_layer);
                        split_intersecting_parts_ranges_into_layers = false;
                    }
                }

                SplitPartsWithRangesByPrimaryKeyResult split_ranges_result = splitPartsWithRangesByPrimaryKey(
                    storage_snapshot->metadata->getPrimaryKey(),
                    storage_snapshot->metadata->getSortingKey(),
                    sorting_expr,
                    std::move(new_parts),
                    max_layers,
                    context,
                    std::move(in_order_reading_step_getter),
                    split_parts_ranges_into_intersecting_and_non_intersecting_final,
                    split_intersecting_parts_ranges_into_layers);

                for (auto && non_intersecting_parts_range : split_ranges_result.non_intersecting_parts_ranges)
                    non_intersecting_parts_by_primary_key.push_back(std::move(non_intersecting_parts_range));

                /// A layer may produce an empty pipe (the in-order getter creates one source per part,
                /// and a layer may end up with no parts). An empty pipe has no header, so it must not
                /// reach `createProjection` or `addMergingFinal` below. Dropping it is safe here:
                /// unlike the join-by-shards path, the per-layer pipes are simply united, so their
                /// positions carry no meaning.
                for (auto && merging_pipe : split_ranges_result.merging_pipes)
                {
                    if (!merging_pipe.empty())
                        pipes.push_back(std::move(merging_pipe));
                }
            }
            else
            {
                if (read_in_reverse)
                    new_parts = split_ranges_for_reverse_read(std::move(new_parts));

                pipes.emplace_back(read(
                    std::move(new_parts),
                    index_build_context,
                    column_names,
                    read_type,
                    max_layers,
                    0,
                    info.use_uncompressed_cache));

                pipes.back().addSimpleTransform([sorting_expr](const SharedHeader & header)
                                                { return std::make_shared<ExpressionTransform>(header, sorting_expr); });
            }

            /// Drop temporary columns, added by 'sorting_key_expr'
            if (!out_projection && !pipes.empty())
                out_projection = createProjection(pipes.front().getHeader());
        }

        if (pipes.empty())
            continue;

        Names sort_columns = storage_snapshot->metadata->getSortingKeyColumns();
        std::vector<bool> reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();
        SortDescription sort_description;
        sort_description.compile_sort_description = settings[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        /// When reading in the reverse order, the direction of each sorting key column is flipped.
        const int direction_multiplier = read_in_reverse ? -1 : 1;
        for (size_t i = 0; i < sort_columns_size; ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sort_columns[i], -1 * direction_multiplier);
            else
                sort_description.emplace_back(sort_columns[i], 1 * direction_multiplier);
        }

        for (auto & pipe : pipes)
            addMergingFinal(
                pipe,
                sort_description,
                data.merging_params,
                storage_snapshot->metadata,
                block_size.max_block_size_rows,
                enable_vertical_final,
                read_in_reverse);

        merging_pipes.emplace_back(Pipe::unitePipes(std::move(pipes)));
    }

    if (!non_intersecting_parts_by_primary_key.empty())
        no_merging_pipes.emplace_back(readNonIntersectingWithEngineFilter(
            std::move(non_intersecting_parts_by_primary_key), index_build_context, num_streams, origin_column_names));

    if (!merging_pipes.empty() && !no_merging_pipes.empty())
    {
        out_projection = {}; /// We do projection here
        Pipes pipes;
        pipes.resize(2);
        pipes[0] = Pipe::unitePipes(std::move(merging_pipes));
        pipes[1] = Pipe::unitePipes(std::move(no_merging_pipes));
        auto conversion_action = ActionsDAG::makeConvertingActions(
            pipes[0].getHeader().getColumnsWithTypeAndName(),
            pipes[1].getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);
        auto converting_expr = std::make_shared<ExpressionActions>(std::move(conversion_action));
        pipes[0].addSimpleTransform(
            [converting_expr](const SharedHeader & header)
            {
                return std::make_shared<ExpressionTransform>(header, converting_expr);
            });
        return Pipe::unitePipes(std::move(pipes));
    }
    return merging_pipes.empty() ? Pipe::unitePipes(std::move(no_merging_pipes)) : Pipe::unitePipes(std::move(merging_pipes));
}

ReadFromMergeTree::AnalysisResultPtr ReadFromMergeTree::selectRangesToRead(bool find_exact_ranges) const
{
    analyzed_result_ptr = selectRangesToRead(
        getParts(),
        mutations_snapshot,
        vector_search_parameters,
        top_k_filter_info,
        storage_snapshot->metadata,
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        data_settings,
        all_column_names,
        log,
        indexes,
        find_exact_ranges,
        is_parallel_reading_from_replicas,
        allow_query_condition_cache,
        supportsSkipIndexesOnDataRead(),
        /*check_row_limits=*/true);

    return analyzed_result_ptr;
}

ReadFromMergeTree::AnalysisResultPtr ReadFromMergeTree::estimateRangesToReadWithoutQueryConditionCache() const
{
    /// Deliberately not stored in `analyzed_result_ptr`: the result must not become the analysis of the
    /// executed read, which has to re-analyze once its final shape (and with it the TopK gate) is known.
    return selectRangesToRead(
        getParts(),
        mutations_snapshot,
        vector_search_parameters,
        top_k_filter_info,
        storage_snapshot->metadata,
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        data_settings,
        all_column_names,
        log,
        indexes,
        /*find_exact_ranges=*/false,
        is_parallel_reading_from_replicas,
        /*allow_query_condition_cache_=*/false,
        supportsSkipIndexesOnDataRead(),
        /*check_row_limits=*/true);
}

ReadFromMergeTree::AnalysisResultPtr ReadFromMergeTree::selectRangesToReadForEstimation() const
{
    return selectRangesToRead(
        getParts(),
        mutations_snapshot,
        vector_search_parameters,
        top_k_filter_info,
        storage_snapshot->metadata,
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        data_settings,
        all_column_names,
        log,
        indexes,
        /*find_exact_ranges=*/false,
        is_parallel_reading_from_replicas,
        allow_query_condition_cache,
        supportsSkipIndexesOnDataRead(),
        /*check_row_limits=*/false);
}

std::optional<size_t> ReadFromMergeTree::estimateCompressedBytesToRead() const
{
    const auto analysis = analyzed_result_ptr ? analyzed_result_ptr : selectRangesToRead();
    if (!analysis)
        return {};

    /// Reads that are priced per part below, but whose column set is only known once a read task is
    /// built for a concrete part: `getReadTaskColumns` gives every index read task and on-fly mutation
    /// step its own input columns. Patch parts are the same case twice over - `MergeTreeReadPoolBase`
    /// picks them per part and `addPatchPartsColumns` then adds the patch key columns to the main
    /// read, and the patch parts themselves are read in full but are not among `parts_with_ranges`.
    /// They are counted separately from data mutations, so a lightweight update leaves
    /// `hasDataMutations` false. Rather than under-count any of this, decline to answer - the caller
    /// reads that as "could be any size".
    if (!index_read_tasks.empty()
        || (mutations_snapshot && (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasPatchParts())))
        return {};

    /// With the range-splitting fault injection enabled, `spreadMarkRangesAmongStreams` may turn an
    /// ordinary read into an in-order one and append the whole sorting key to the columns it reads,
    /// which the estimate below does not account for because `reader_settings.read_in_order` is not
    /// set. The decision is a per-execution coin flip made when the pipeline is built, long after
    /// this runs, so it cannot be predicted here - decline to answer whenever the injection is armed,
    /// as `capStreamsByReadBytes` already does with the same estimate. The setting is only ever set
    /// by tests, so this costs nothing in production.
    if (context->getSettingsRef()[Setting::merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability] > 0)
        return {};

    Names column_names = analysis->column_names_to_read.empty() ? all_column_names : analysis->column_names_to_read;
    {
        NameSet present(column_names.begin(), column_names.end());
        const auto add_columns = [&](const Names & names_to_add)
        {
            for (const auto & column_name : names_to_add)
                if (present.emplace(column_name).second)
                    column_names.push_back(column_name);
        };

        if (query_info.prewhere_info)
            add_columns(query_info.prewhere_info->prewhere_actions.getRequiredColumnsNames());
        if (query_info.row_level_filter)
            add_columns(query_info.row_level_filter->actions.getRequiredColumnsNames());
        if (analysis->sampling.use_sampling && analysis->sampling.filter_expression)
            add_columns(analysis->sampling.filter_expression->getRequiredColumns().getNames());

        if (reader_settings.read_in_order)
            add_columns(storage_snapshot->metadata->getColumnsRequiredForSortingKey());

        if (reader_settings.apply_deleted_mask)
            add_columns({RowExistsColumn::name});
    }

    if (const auto estimate = estimateReadBytes(
            analysis->parts_with_ranges,
            column_names,
            storage_snapshot,
            mutations_snapshot,
            context,
            context->getSettingsRef(),
            ReadBytesKind::Compressed))
        return estimate;

    /// No per-column estimate is available. Charge every selected part in full rather than giving up:
    /// the caller needs a number it can act on, and over-estimating only makes it act less often.
    size_t total_bytes = 0;
    for (const auto & part : analysis->parts_with_ranges)
    {
        if (__builtin_add_overflow(total_bytes, part.data_part->getTotalColumnsSize().data_compressed, &total_bytes))
            return std::numeric_limits<size_t>::max();
    }
    return total_bytes;
}

namespace
{

/// Check if all columns of all useful skip indexes are also part of the primary key.
/// When true, skip indexes cannot cause incorrect FINAL results (since PK-based filtering cannot drop parts with overlapping key ranges),
/// so the `findPKRangesForFinalAfterSkipIndex` recovery pass can be skipped.
bool areAllSkipIndexColumnsInPrimaryKey(const Names & primary_key_columns, const UsefulSkipIndexes & skip_indexes)
{
    NameSet primary_key_columns_set(primary_key_columns.begin(), primary_key_columns.end());

    for (const auto & skip_index : skip_indexes.useful_indices)
    {
        for (const auto & column : skip_index.index->index.column_names)
        {
            if (!primary_key_columns_set.contains(column))
                return false;
        }
    }

    return true;
}

}

void ReadFromMergeTree::addJoinRuntimeFilterIndexAnalysisOnDataRead(const String & filter_id, const String & column_name, const DataTypePtr & column_type)
{
    /// Prunable only if in the primary key or has a minmax/set/bloom_filter skip index.
    const auto & metadata = *storage_snapshot->metadata;
    const auto & primary_key_columns = metadata.getPrimaryKey().column_names;
    const bool is_primary_key_column
        = std::find(primary_key_columns.begin(), primary_key_columns.end(), column_name) != primary_key_columns.end();

    bool has_applicable_skip_index = false;
    for (const auto & index : metadata.getSecondaryIndices())
    {
        if (index.type != "minmax" && index.type != "set" && index.type != "bloom_filter")
            continue;
        if (std::find(index.column_names.begin(), index.column_names.end(), column_name) != index.column_names.end())
        {
            has_applicable_skip_index = true;
            break;
        }
    }

    if (!is_primary_key_column && !has_applicable_skip_index)
        return;

    join_runtime_filters_for_index_analysis.push_back({filter_id, column_name, column_type});
    LOG_DEBUG(log, "Registered join runtime filter {} on column {} (primary_key={}, skip_index={})",
        filter_id, column_name, is_primary_key_column, has_applicable_skip_index);
}

void ReadFromMergeTree::buildPartitionPruningIndexes(
    Indexes & indexes,
    const std::shared_ptr<ActionsDAGWithInversionPushDown> & filter_dag_ptr,
    const MergeTreeData & data,
    const ContextPtr & query_context,
    const StorageMetadataPtr & metadata_snapshot,
    bool skip_partition_pruning_,
    bool require_ready_sets)
{
    const auto & settings = query_context->getSettingsRef();
    const bool skip_constant_folding = skip_partition_pruning_ || !settings[Setting::use_constant_folding_in_index_analysis];
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    const auto data_settings = data.getSettings();

    if (auto minmax_columns = MergeTreeData::getMinMaxColumns(partition_key, data_settings); !minmax_columns.empty())
    {
        auto key_condition_factory = [query_context, metadata_snapshot, skip_partition_pruning_, minmax_columns, data_settings, require_ready_sets](const ActionsDAG *, const ActionsDAG::Node * predicate)
        {
            auto minmax_expression_actions = MergeTreeData::getMinMaxExpr(metadata_snapshot->getPartitionKey(), data_settings, ExpressionActionsSettings(query_context));
            ActionsDAGWithInversionPushDown wrapped(predicate, query_context, /* boolean_context */ false);
            return KeyCondition{
                wrapped, query_context, minmax_columns.getNames(), minmax_expression_actions,
                /* single_point_ = */ false,
                /* skip_analysis_ = */ skip_partition_pruning_ || !query_context->getSettingsRef()[Setting::use_partition_pruning] || !query_context->getSettingsRef()[Setting::use_skip_indexes],
                require_ready_sets};
        };
        indexes.minmax_idx_condition = std::make_shared<ConditionTemplate<KeyCondition>>(filter_dag_ptr, std::move(key_condition_factory), metadata_snapshot, query_context, skip_constant_folding);
    }

    if (metadata_snapshot->hasPartitionKey())
    {
        indexes.partition_pruner.emplace(
            metadata_snapshot,
            *filter_dag_ptr,
            query_context,
            /*strict=*/false,
            /*skip_analysis=*/skip_partition_pruning_ || !settings[Setting::use_partition_pruning],
            require_ready_sets);
    }
}

RangesInDataParts ReadFromMergeTree::filterPartsForStatistics(
    const RangesInDataParts & parts,
    const ActionsDAG::Node * predicate,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & query_context,
    bool skip_partition_pruning_)
{
    auto filter_dag = std::make_shared<ActionsDAGWithInversionPushDown>(predicate, query_context, /* boolean_context */ true);
    Indexes partition_indexes(nullptr);
    buildPartitionPruningIndexes(partition_indexes, filter_dag, data, query_context, metadata_snapshot, skip_partition_pruning_, /* require_ready_sets */ true);
    IndexStats unused_stats;
    /// Execution checks forced index usage after it builds subquery sets.
    return MergeTreeDataSelectExecutor::filterPartsByPartition(
        parts, partition_indexes.partition_pruner, partition_indexes.minmax_idx_condition,
        std::nullopt, metadata_snapshot, data, query_context, nullptr, getLogger("ReadFromMergeTree"), unused_stats,
        /* check_index_usage */ false);
}

void ReadFromMergeTree::buildIndexes(
    std::optional<ReadFromMergeTree::Indexes> & indexes,
    const ActionsDAG * filter_actions_dag_,
    const MergeTreeData & data,
    const RangesInDataParts & parts,
    [[maybe_unused]] const std::optional<VectorSearchParameters> & vector_search_parameters,
    [[maybe_unused]] const std::optional<TopKFilterInfo> top_k_filter_info,
    const ContextPtr & query_context,
    const SelectQueryInfo & query_info_,
    const StorageMetadataPtr & metadata_snapshot,
    bool skip_partition_pruning_)
{
    indexes.reset();

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;

    const auto & settings = query_context->getSettingsRef();
    const bool skip_constant_folding = skip_partition_pruning_ || !settings[Setting::use_constant_folding_in_index_analysis];

    auto filter_dag_ptr = std::make_shared<ActionsDAGWithInversionPushDown>(filter_actions_dag_ ? filter_actions_dag_->getOutputs().front() : nullptr, query_context, /* boolean_context */ true);
    const auto & filter_dag = *filter_dag_ptr;

    {
        auto key_condition_factory = [query_context, metadata_snapshot](const ActionsDAG *, const ActionsDAG::Node * predicate)
        {
            ActionsDAGWithInversionPushDown wrapped(predicate, query_context, /* boolean_context */ false);
            return KeyCondition{wrapped, query_context, metadata_snapshot->getPrimaryKey(), /* single_point_ = */ false, !query_context->getSettingsRef()[Setting::use_primary_key]};
        };
        auto key_condition_template = std::make_shared<ConditionTemplate<KeyCondition>>(filter_dag_ptr, std::move(key_condition_factory), metadata_snapshot, query_context, skip_constant_folding);
        indexes.emplace(std::move(key_condition_template));
    }

    {
        auto key_condition_factory = [query_context](const ActionsDAG *, const ActionsDAG::Node * predicate)
        {
            ActionsDAGWithInversionPushDown wrapped(predicate, query_context, /* boolean_context */ false);
            return KeyCondition{wrapped, query_context, {}, std::make_shared<ExpressionActions>(ActionsDAG(NamesAndTypesList{}))};
        };
        indexes->key_condition_rpn_template = std::make_shared<ConditionTemplate<KeyCondition>>(filter_dag_ptr, std::move(key_condition_factory), metadata_snapshot, query_context, skip_constant_folding);
    }

    buildPartitionPruningIndexes(*indexes, filter_dag_ptr, data, query_context, metadata_snapshot, skip_partition_pruning_);

    indexes->part_values
        = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(metadata_snapshot, data, parts, filter_dag.predicate, query_context);

    /// Perform virtual column key analysis only when no corresponding physical columns exist.
    const auto & columns = metadata_snapshot->getColumns();
    if (!columns.has("_part_offset") && !columns.has("_part"))
        indexes->part_offset_condition = MergeTreeDataSelectExecutor::buildKeyConditionFromPartOffset(filter_dag_ptr, metadata_snapshot, skip_constant_folding, query_context);
    if (!columns.has("_part_offset") && !columns.has("_part_starting_offset"))
        indexes->total_offset_condition = MergeTreeDataSelectExecutor::buildKeyConditionFromTotalOffset(filter_dag_ptr, metadata_snapshot, skip_constant_folding, query_context);

    indexes->use_skip_indexes = settings[Setting::use_skip_indexes];
    if (query_info_.isFinal() && !settings[Setting::use_skip_indexes_if_final])
        indexes->use_skip_indexes = false;

    if (!indexes->use_skip_indexes)
        return;

    const auto & all_indexes = metadata_snapshot->getSecondaryIndices();

    if (all_indexes.empty())
        return;

    std::unordered_set<std::string> ignored_index_names;

    if (settings[Setting::ignore_data_skipping_indices].changed)
    {
        const auto & indices = settings[Setting::ignore_data_skipping_indices].toString();
        ignored_index_names = parseIdentifiersOrStringLiteralsToSet(indices, settings);
    }

    UsefulSkipIndexes skip_indexes;

    for (const auto & index : all_indexes)
    {
        if (ignored_index_names.contains(index.name))
            continue;

        auto index_helper = MergeTreeIndexFactory::instance().get(metadata_snapshot, index, *data.getSettings());

        /// Inert indices (a removed index type kept only for attach compatibility) hold no data and
        /// cannot answer queries. Skip them so a filtered query does not throw building the condition.
        if (index_helper->isInert())
            continue;

        ConditionTemplate<MergeTreeIndexConditionPtr>::Factory factory;
        if (index_helper->isVectorSimilarityIndex())
        {
#if USE_USEARCH
            const auto * vector_similarity_index = typeid_cast<const MergeTreeIndexVectorSimilarity *>(index_helper.get());
            chassert(vector_similarity_index);

            factory = [vector_similarity_index, query_context, vector_search_parameters](const ActionsDAG *, const ActionsDAG::Node * predicate)
            {
                return vector_similarity_index->createIndexCondition(predicate, query_context, vector_search_parameters);
            };
#endif
        }
        else
        {
            factory = [index_helper, query_context](const ActionsDAG *, const ActionsDAG::Node * predicate) -> MergeTreeIndexConditionPtr
            {
                if (!predicate)
                    return nullptr;
                return index_helper->createIndexCondition(predicate, query_context);
            };
        }

        auto condition_template = std::make_shared<ConditionTemplate<MergeTreeIndexConditionPtr>>(filter_dag_ptr, std::move(factory), metadata_snapshot, query_context, skip_constant_folding);

        const auto & unsubstituted = condition_template->generateUnsubstituted();
        if (unsubstituted && !unsubstituted->alwaysUnknownOrTrue())
            skip_indexes.useful_indices.emplace_back(index_helper, std::move(condition_template));

        auto can_skip_index_be_used_for_top_k_filtering = [top_k_filter_info](const MergeTreeIndexPtr & skip_index)
        {
                if (!top_k_filter_info || !skip_index->index.isSimpleSingleColumnIndex()
                    || skip_index->index.type != "minmax"
                    || top_k_filter_info->column_name != skip_index->index.column_names[0])
                    return false;

                /// The skip-index top-k path ranks granules via raw Field comparison
                /// (MinMaxGranuleItem::operator<) which does not respect nulls_direction
                /// or collation. Only allow types where raw Field ordering matches
                /// the ORDER BY semantics.
                /// TODO: generalize MinMaxGranuleItem comparison and getTopKMarks to use
                /// nulls_direction/collator so this restriction can be lifted.
                if (top_k_filter_info->data_type->isNullable()
                    || !top_k_filter_info->data_type->isValueRepresentedByNumber())
                    return false;

                if (top_k_filter_info->threshold_tracker
                    && top_k_filter_info->threshold_tracker->getCollator())
                    return false;

                return true;
        };

        if (settings[Setting::use_skip_indexes_for_top_k] && can_skip_index_be_used_for_top_k_filtering(index_helper))
        {
            skip_indexes.skip_index_for_top_k_filtering = index_helper;
            LOG_TRACE(getLogger("MergeTreeSkipIndexReader"), "Selected index {} on column {} for top-K optimization, k = {}, direction = {}, sort columns = {}",
                        index_helper->index.name, top_k_filter_info->column_name, top_k_filter_info->limit_n, top_k_filter_info->direction, top_k_filter_info->num_sort_columns);
            if (settings[Setting::use_skip_indexes_on_data_read])
                skip_indexes.threshold_tracker = top_k_filter_info->threshold_tracker;
        }
    }

    indexes->use_skip_indexes_for_disjunctions = settings[Setting::use_skip_indexes_for_disjunctions]
                                                    && skip_indexes.useful_indices.size() > 1
                                                    && !indexes->key_condition_rpn_template->generateUnsubstituted().hasOnlyConjunctions()
                                                    && indexes->key_condition_rpn_template->generateUnsubstituted().getRPN().size() <= MergeTreeDataSelectExecutor::MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT;

    indexes->use_skip_indexes_if_final_exact_mode = indexes->use_skip_indexes && !skip_indexes.empty()
                                                        && query_info_.isFinal()
                                                        && settings[Setting::use_skip_indexes_if_final_exact_mode]
                                                        && !areAllSkipIndexColumnsInPrimaryKey(primary_key_column_names, skip_indexes);

    indexes->skip_indexes = std::move(skip_indexes);
}

String SkipIndexOrderCache::makeKey(const IMergeTreeDataPart & part)
{
    if (const auto * parent_part = part.getParentPart())
        return parent_part->name + "/" + part.name;
    return part.name;
}

bool ReadFromMergeTree::isRowPolicyDeferredAfterFinal() const
{
    if (!isQueryWithFinal() || !query_info.row_level_filter)
        return false;

    if (!context->getSettingsRef()[Setting::apply_row_policy_after_final])
        return false;

    const auto & sorting_key_columns = storage_snapshot->metadata->getSortingKeyColumns();
    NameSet sorting_key_set(sorting_key_columns.begin(), sorting_key_columns.end());

    const auto * filter_output = &query_info.row_level_filter->actions.findInOutputs(
        query_info.row_level_filter->column_name);

    /// Safe to apply before FINAL only if the policy is Sorting-Key-only (verdict
    /// is the same for every row of a dedup group) and deterministic
    /// (no `rand`/`now` flipping the winner)
    return !(isNodeOverSortingKey(filter_output, sorting_key_set) && isNodeDeterministic(filter_output));
}

bool ReadFromMergeTree::isPrewhereDeferredAfterFinal() const
{
    if (!isQueryWithFinal())
        return false;

    /// PREWHERE must run after the row policy, so deferred row policy defers PREWHERE as well
    return context->getSettingsRef()[Setting::apply_prewhere_after_final] || isRowPolicyDeferredAfterFinal();
}

void ReadFromMergeTree::deferFiltersAfterFinalIfNeeded()
{
    if (!isQueryWithFinal())
        return;

    const auto & settings = context->getSettingsRef();

    if (isRowPolicyDeferredAfterFinal())
        deferred_row_level_filter = query_info.row_level_filter;
    if (query_info.prewhere_info && isPrewhereDeferredAfterFinal())
        deferred_prewhere_info = query_info.prewhere_info;

    /// Don't prune partitions unless the partition key is determined by the sorting key:
    /// when FINAL merges across partitions, rows with the same primary key in different
    /// partitions must all participate in deduplication, so partition pruning would drop
    /// rows that affect the FINAL result.
    ///
    /// Users whose data structure guarantees same-PK rows cannot span partitions (e.g. event-log
    /// tables whose partition column is set at insert time and never changes) can opt out via
    /// `defer_partition_pruning_after_final = 0` to restore pre-26.3 performance.
    if (settings[Setting::defer_partition_pruning_after_final]
        && !doNotMergePartsAcrossPartitionsFinal()
        && storage_snapshot->metadata->hasPartitionKey())
    {
        const auto & partition_key = storage_snapshot->metadata->getPartitionKey();
        const auto & sorting_key_columns = storage_snapshot->metadata->getSortingKeyColumns();
        NameSet sorting_key_set(sorting_key_columns.begin(), sorting_key_columns.end());

        const auto & partition_expr_names = partition_key.column_names;
        bool exprs_match = std::all_of(
            partition_expr_names.begin(), partition_expr_names.end(),
            [&](const auto & expr_name) { return sorting_key_set.contains(expr_name); });

        const auto & partition_required_columns = partition_key.expression->getRequiredColumnsWithTypes();
        bool columns_match = std::all_of(
            partition_required_columns.begin(), partition_required_columns.end(),
            [&](const auto & col) { return sorting_key_set.contains(col.name); });

        /// "Determined by the sorting key" holds for value identity, not for comparator equality, and
        /// the two differ for floating-point columns: `-0.0` compares equal to `0.0`, and every `NaN`
        /// bit pattern compares equal to every other. A partition expression can tell exactly those
        /// values apart - `toString(f)` maps `-0.0` and `0.0` to `'-0'` and `'0'`,
        /// `reinterpretAsUInt64(f)` separates `NaN` payloads - so two rows of one deduplication group
        /// can sit in different partitions. Pruning then drops the partition holding the group's
        /// winner and the superseded row survives the FINAL merge.
        bool reads_float_column = std::any_of(
            partition_required_columns.begin(), partition_required_columns.end(),
            [](const auto & col)
            {
                if (isFloat(removeLowCardinalityAndNullable(col.type)))
                    return true;

                bool has_float = false;
                col.type->forEachChild([&](const IDataType & child)
                {
                    if (!has_float && WhichDataType(child).isFloat())
                        has_float = true;
                });
                return has_float;
            });

        skip_partition_pruning = (!exprs_match && !columns_match) || reads_float_column;
    }
}

void ReadFromMergeTree::applyFilters(ActionDAGNodes added_filter_nodes)
{
    /// Streaming queries do index analysis in MergeTreeCommitOrderSource.
    if (query_info.isStream())
        return;

    if (!indexes)
    {
        auto node_name_to_input = query_info.buildNodeNameToInputNodeColumn();
        auto dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, node_name_to_input);
        filter_actions_dag = dag ? std::make_shared<const ActionsDAG>(std::move(*dag)) : nullptr;

        /// NOTE: Currently we store two DAGs for analysis:
        /// (1) SourceStepWithFilter::filter_nodes, (2) query_info.filter_actions_dag. Make sure they are consistent.
        /// TODO: Get rid of filter_actions_dag in query_info after we move analysis of
        /// parallel replicas and unused shards into optimization, similar to projection analysis.
        if (filter_actions_dag)
            query_info.filter_actions_dag = filter_actions_dag;

        /// don't let deferred filters participate in index analysis
        /// otherwise partition pruning / skip indexes could drop data that FINAL still needs
        const ActionsDAG * index_filter_dag = query_info.filter_actions_dag.get();
        std::shared_ptr<const ActionsDAG> index_filter_dag_without_deferred;

        deferFiltersAfterFinalIfNeeded();
        if (deferred_row_level_filter || deferred_prewhere_info)
        {
            /// exclude deferred filters from index analysis, but keep sorting-key AND atoms
            NameSet deferred_column_names;
            if (deferred_row_level_filter)
                deferred_column_names.insert(deferred_row_level_filter->column_name);
            if (deferred_prewhere_info)
                deferred_column_names.insert(deferred_prewhere_info->prewhere_column_name);

            const auto & sorting_key_columns = storage_snapshot->metadata->getSortingKeyColumns();
            NameSet sorting_key_set(sorting_key_columns.begin(), sorting_key_columns.end());

            std::vector<const ActionsDAG::Node *> index_nodes;

            /// collect sorting-key-only atoms from a (possibly nested) AND tree
            std::function<void(const ActionsDAG::Node *)> collect_sorting_key_atoms =
                [&](const ActionsDAG::Node * n)
            {
                if (isNodeOverSortingKey(n, sorting_key_set))
                {
                    index_nodes.push_back(n);
                    return;
                }
                if (n->type == ActionsDAG::ActionType::FUNCTION
                    && n->function_base && n->function_base->getName() == "and")
                {
                    for (const auto * child : n->children)
                        collect_sorting_key_atoms(child);
                }
            };

            for (const auto * node : added_filter_nodes.nodes)
            {
                if (!deferred_column_names.contains(node->result_name))
                    index_nodes.push_back(node);
                else
                    collect_sorting_key_atoms(node);
            }

            auto idx_dag = ActionsDAG::buildFilterActionsDAG(index_nodes, node_name_to_input);
            if (idx_dag)
                index_filter_dag_without_deferred = std::make_shared<const ActionsDAG>(std::move(*idx_dag));
            /// nullptr is fine here: all filters are deferred, nothing left for indexes
            index_filter_dag = index_filter_dag_without_deferred.get();

            LOG_DEBUG(
                log,
                "Excluding deferred filters from index analysis: row_policy={}, prewhere={}",
                deferred_row_level_filter != nullptr,
                deferred_prewhere_info != nullptr);
        }

        /// Build indexes before PREWHERE sets. KeyCondition (inside buildIndexes) calls
        /// buildOrderedSetInplace only for IN sets whose left argument maps to key columns,
        /// so ordered sets are built only when actually needed for primary key analysis.
        /// Building indexes first is important because the set is shared between the PREWHERE
        /// DAG and the index filter DAG via ColumnSet: if we built non-ordered sets for
        /// PREWHERE first, the set would be created without elements and KeyCondition would
        /// not be able to use it for index analysis.
        buildIndexes(
            indexes,
            index_filter_dag,
            data,
            getParts(),
            vector_search_parameters,
            top_k_filter_info,
            context,
            query_info,
            storage_snapshot->metadata,
            skip_partition_pruning);

        /// Build sets for PREWHERE and row_level_filter synchronously during applyFilters.
        /// PREWHERE is evaluated at the storage level during data reading, before the
        /// pipeline-level CreatingSetsStep has a chance to execute. Although CreatingSetsStep
        /// uses DelayedPortsProcessor to ensure sets are built before the main query starts,
        /// there is a race condition: if a downstream processor (e.g. JoiningTransform with
        /// an empty right side) closes its inputs early, DelayedPortsProcessor may terminate
        /// the set-building pipeline before the set is ready.
        /// Building sets synchronously here eliminates this race condition entirely.
        if (query_info.prewhere_info)
            VirtualColumnUtils::buildSetsForDAG(query_info.prewhere_info->prewhere_actions, context);
        if (query_info.row_level_filter)
            VirtualColumnUtils::buildSetsForDAG(query_info.row_level_filter->actions, context);
    }
}

bool ReadFromMergeTree::filterDependsOnNonDeterministicVirtuals(const VirtualColumnsDescription & virtuals, const SelectQueryInfo & query_info_)
{
    auto dag_has_input = [&](const ActionsDAG & dag)
    {
        for (const auto * input : dag.getInputs())
        {
            const auto * column = virtuals.tryGetDescription(input->result_name, VirtualsKind::All, VirtualsMaterializationPlace::All);
            if (column && !column->deterministic)
                return true;
        }
        return false;
    };

    if (query_info_.filter_actions_dag && dag_has_input(*query_info_.filter_actions_dag))
        return true;
    if (query_info_.prewhere_info && dag_has_input(query_info_.prewhere_info->prewhere_actions))
        return true;
    return false;
}

using PartsRangesMap = std::unordered_map<std::string, const RangesInDataPart *>;
/// Same as filterPartsByPrimaryKeyAndSkipIndexes(), but accept part names and parts map to transform parts names to parts
/// Used for distributed index analysis
static IndexAnalysisPartsRanges filterPartsNamesByPrimaryKeyAndSkipIndexes(MergeTreeDataSelectExecutor::IndexAnalysisContext & filter_context, PartsRangesMap & parts_ranges_map, const std::vector<std::string_view> & parts_to_analyze)
{
    /// Resolve part names to RangesInDataParts
    RangesInDataParts parts_ranges_to_analyze;
    for (const auto & part : parts_to_analyze)
        parts_ranges_to_analyze.push_back(*parts_ranges_map.at(std::string(part)));

    ReadFromMergeTree::IndexStats ignore_stats;
    auto parts_ranges_res = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(filter_context, parts_ranges_to_analyze, ignore_stats);

    std::unordered_set<std::string_view> processed_parts;

    /// Convert RangesInDataParts to IndexAnalysisPartsRanges
    IndexAnalysisPartsRanges res;
    for (const auto & part_ranges : parts_ranges_res)
    {
        const auto & part_name = part_ranges.data_part->name;
        res[part_name].insert(res[part_name].end(), part_ranges.ranges.begin(), part_ranges.ranges.end());
    }

    /// Add empty parts back, to take it into account in "Parts send"
    for (const auto & part_name : parts_to_analyze)
    {
        if (processed_parts.contains(part_name))
            continue;
        res.emplace(part_name, MarkRanges{});
    }

    return res;
}

ReadFromMergeTree::AnalysisResultPtr ReadFromMergeTree::selectRangesToRead(
    const RangesInDataParts & parts,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    const std::optional<VectorSearchParameters> & vector_search_parameters,
    const std::optional<TopKFilterInfo> & top_k_filter_info,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info_,
    ContextPtr context_,
    size_t num_streams,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read,
    const MergeTreeData & data,
    const MergeTreeSettingsPtr & data_settings_,
    const Names & all_column_names,
    LoggerPtr log,
    std::optional<Indexes> & indexes,
    bool find_exact_ranges,
    bool is_parallel_reading_from_replicas_,
    bool allow_query_condition_cache_,
    bool supports_skip_indexes_on_data_read,
    bool check_row_limits)
{
    ProfileEvents::increment(ProfileEvents::IndexAnalysisRounds);

    AnalysisResult result;
    RangesInDataParts res_parts;
    const auto & settings = context_->getSettingsRef();

    size_t total_parts = parts.size();

    result.column_names_to_read = all_column_names;

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (result.column_names_to_read.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        result.column_names_to_read.push_back(ExpressionActions::getSmallestColumn(available_real_columns).name);
    }

    /// Streaming queries do index analysis in MergeTreeCommitOrderSource
    /// and return here, bypassing the UNIQUE KEY snapshot/pin + delete-bitmap
    /// filter below. Fail closed rather than serve logically-deleted rows.
    ///
    /// TODO(unique-key): wire the delete-bitmap filter into the streaming source.
    if (query_info_.isStream())
    {
        if (metadata_snapshot->hasUniqueKey())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Streaming reads (FROM ... STREAM) are not supported on tables with UNIQUE KEY.");
        return std::make_shared<AnalysisResult>(std::move(result));
    }

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;

    if (!indexes)
        buildIndexes(
            indexes,
            query_info_.filter_actions_dag.get(),
            data,
            parts,
            vector_search_parameters,
            top_k_filter_info,
            context_,
            query_info_,
            metadata_snapshot);

    NameSet indexes_column_names;
    /// We need not only PK columns, but source columns for this PK calculation as well
    if (auto required_columns = primary_key.expression->getRequiredColumns(); !required_columns.empty())
        indexes_column_names.insert(required_columns.begin(), required_columns.end());
    for (const auto & skip_index : indexes->skip_indexes.useful_indices)
    {
        const auto & skip_index_required_columns = skip_index.index->getColumnsRequiredForIndexCalc();
        indexes_column_names.insert(skip_index_required_columns.begin(), skip_index_required_columns.end());
    }

    indexes->use_skip_indexes_on_data_read = supports_skip_indexes_on_data_read;
    if (indexes->part_values && indexes->part_values->empty())
    {
        result.has_exact_ranges = true;
        return std::make_shared<AnalysisResult>(std::move(result));
    }

    if (indexes->key_condition->generateUnsubstituted().alwaysUnknownOrTrue())
    {
        if (settings[Setting::force_primary_key])
        {
            throw Exception(ErrorCodes::INDEX_NOT_USED,
                "Primary key ({}) is not used and setting 'force_primary_key' is set",
                fmt::join(primary_key_column_names, ", "));
        }
    } else
    {
        ProfileEvents::increment(ProfileEvents::SelectQueriesWithPrimaryKeyUsage);
    }

    LOG_DEBUG(log, "Key condition: {}", indexes->key_condition->generateUnsubstituted().toString());

    if (indexes->part_offset_condition)
        LOG_DEBUG(log, "Part offset condition: {}", indexes->part_offset_condition->generateUnsubstituted().toString());

    if (indexes->total_offset_condition)
        LOG_DEBUG(log, "Total offset condition: {}", indexes->total_offset_condition->generateUnsubstituted().toString());

    if (indexes->key_condition->generateUnsubstituted().alwaysFalse())
    {
        result.has_exact_ranges = true;
        return std::make_shared<AnalysisResult>(std::move(result));
    }

    size_t total_marks_pk = 0;
    size_t parts_before_pk = 0;
    bool add_index_stat_row_for_pk_expand = false;

    res_parts = MergeTreeDataSelectExecutor::filterParts(
        parts,
        *indexes,
        metadata_snapshot,
        data,
        query_info_,
        mutations_snapshot,
        context_,
        max_block_numbers_to_read.get(),
        log,
        result.index_stats);

    result.sampling = MergeTreeDataSelectExecutor::getSampling(
        query_info_,
        metadata_snapshot->getColumns().getAllPhysical(),
        res_parts,
        indexes->key_condition,
        data,
        metadata_snapshot,
        context_,
        log);

    if (result.sampling.read_nothing)
    {
        result.has_exact_ranges = true;
        return std::make_shared<AnalysisResult>(std::move(result));
    }

    for (const auto & part : res_parts)
        total_marks_pk += part.data_part->index_granularity->getMarksCountWithoutFinal();
    parts_before_pk = res_parts.size();


    /// Check if we have projections or exact-range analysis, as that can determine whether we fail
    /// during reading parts or analyze projection / exact-count candidates to serve the query more
    /// efficiently.  When find_exact_ranges is true the caller (optimizeUseAggregateProjection) can
    /// compute exact counts from the primary key without reading data, so the max_rows_to_read limit
    /// on the full table scan should not cause an immediate failure.
    bool projection_parts_exist = std::any_of(res_parts.begin(), res_parts.end(), [](const auto & part) { return part.data_part->isProjectionPart(); });
    bool has_projections = metadata_snapshot->hasProjections() || projection_parts_exist || find_exact_ranges;
    bool support_projection_optimization = settings[Setting::parallel_replicas_support_projection] && (has_projections || find_exact_ranges);

    auto reader_settings = MergeTreeReaderSettings::createForQuery(context_, *data_settings_, query_info_);
    if (!allow_query_condition_cache_)
        reader_settings.use_query_condition_cache = false;

    /// TODO(unique-key): unique-key multi-versioned bitmap is conflicted with the single-versioned query-condition cache
    const bool table_has_unique_key = metadata_snapshot->hasUniqueKey();
    if (table_has_unique_key)
        reader_settings.use_query_condition_cache = false;

    const bool filter_depends_on_non_deterministic_virtuals = filterDependsOnNonDeterministicVirtuals(metadata_snapshot->virtuals, query_info_);
    if (filter_depends_on_non_deterministic_virtuals)
        reader_settings.use_query_condition_cache = false;

    MergeTreeDataSelectExecutor::IndexAnalysisContext filter_context
    {
        .metadata_snapshot = metadata_snapshot,
        .mutations_snapshot = mutations_snapshot,
        .query_info = query_info_,
        .context = context_,
        .indexes = *indexes,
        .top_k_filter_info = top_k_filter_info,
        .reader_settings = reader_settings,
        .log = log,
        .num_streams = num_streams,
        .find_exact_ranges = find_exact_ranges,
        .is_parallel_reading_from_replicas = is_parallel_reading_from_replicas_,
        .has_projections = has_projections,
        .check_row_limits = check_row_limits,
        .result = result,
    };

    if (context_->canUseParallelReplicasOnFollower() && settings[Setting::parallel_replicas_local_plan]
        && settings[Setting::parallel_replicas_index_analysis_only_on_coordinator]
        /// If parallel replicas support projection optimization, selected_marks will be used to determine the optimal projection.
        && !support_projection_optimization)
    {
        // Skip index analysis and return parts with all marks
        // The coordinator will choose ranges to read for workers based on index analysis on its side
        result.parts_with_ranges = std::move(res_parts);
    }
    else
    {
        if (!table_has_unique_key && !filter_depends_on_non_deterministic_virtuals && allow_query_condition_cache_)
            MergeTreeDataSelectExecutor::filterPartsByQueryConditionCache(res_parts, query_info_, vector_search_parameters, top_k_filter_info, mutations_snapshot, *indexes, context_, log);

        auto get_indexes_size = [&]() -> size_t
        {
            size_t res = 0;
            for (const auto & part : res_parts)
            {
                res += part.data_part->getTotalSecondaryIndicesSize().data_uncompressed;
                res += part.data_part->getIndexSizeFromFile().data_uncompressed;
            }
            return res;
        };

        /// Note, use_skip_indexes_if_final_exact_mode requires complete PK, so we cannot apply distributed_index_analysis with it
        bool final_second_pass = indexes->use_skip_indexes_if_final_exact_mode;
        UInt64 distributed_index_analysis_min_parts_to_activate = (*data_settings_)[MergeTreeSetting::distributed_index_analysis_min_parts_to_activate];
        UInt64 distributed_index_analysis_min_indexes_bytes_to_activate = (*data_settings_)[MergeTreeSetting::distributed_index_analysis_min_indexes_bytes_to_activate];
        bool is_initial_query = context_->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY;

        bool distributed_index_analysis_enabled = !final_second_pass
            /// Projection parts are identified only by the projection name, which is identical in every
            /// parent part, so per-part analysis results cannot be attributed back, and remote replicas
            /// resolve part names against the parent table. Analyze projection parts locally.
            && !projection_parts_exist
            && settings[Setting::distributed_index_analysis]
            && (settings[Setting::distributed_index_analysis_for_non_shared_merge_tree] || data.isSharedStorage())
            && (total_parts >= distributed_index_analysis_min_parts_to_activate)
            && (!distributed_index_analysis_min_indexes_bytes_to_activate || get_indexes_size() >= distributed_index_analysis_min_indexes_bytes_to_activate)
            /// When `distributed_index_analysis_only_on_coordinator` is set, restrict distributed index analysis to the coordinator (initial query).
            /// Otherwise, subqueries in the predicate (e.g. `IN (SELECT ...)`) on follower replicas would each independently trigger distributed index analysis, causing O(N^2) queries.
            && (is_initial_query || !settings[Setting::distributed_index_analysis_only_on_coordinator]);

        if (!distributed_index_analysis_enabled)
        {
            result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(filter_context, res_parts, result.index_stats);

            if (final_second_pass)
            {
                result.parts_with_ranges
                    = findPKRangesForFinalAfterSkipIndex(primary_key, metadata_snapshot->getSortingKey(), result.parts_with_ranges, log);
                add_index_stat_row_for_pk_expand = true;
            }
        }
        else
        {
            std::unordered_map<std::string, const RangesInDataPart *> parts_ranges_map;
            for (const auto & part_ranges : res_parts)
                parts_ranges_map[part_ranges.data_part->name] = &part_ranges;

            LocalIndexAnalysisCallback local_index_analysis_callback = [&filter_context, &parts_ranges_map](const std::vector<std::string_view> & parts_to_analyze) -> IndexAnalysisPartsRanges
            {
                return filterPartsNamesByPrimaryKeyAndSkipIndexes(filter_context, parts_ranges_map, parts_to_analyze);
            };

            DistributedIndexAnalysisPartsRanges distributed_index_analysis = distributedIndexAnalysisOnReplicas(data.getStorageID(),
                query_info_.filter_actions_dag.get(),
                result.sampling.filter_function,
                indexes_column_names,
                res_parts,
                vector_search_parameters,
                local_index_analysis_callback,
                context_);

            IndexAnalysisPartsRanges analyzed_parts_ranges;

            /// Index stats
            {
                std::vector<DistributedIndexStat> distributed_index_stats;

                size_t received_granules = 0;
                size_t received_parts = 0;
                for (auto & [replica_address, parts_on_replica] : distributed_index_analysis)
                {
                    size_t replica_granules_received = 0;
                    for (const auto & [_, marks] : parts_on_replica)
                        replica_granules_received += marks.getNumberOfMarks();

                    size_t replica_granules_send = 0;
                    for (const auto & [part, _] : parts_on_replica)
                        replica_granules_send += parts_ranges_map.at(std::string(part))->getMarksCount();

                    size_t num_parts_send = parts_on_replica.size();
                    std::erase_if(parts_on_replica, [&](const auto & ranges) { return ranges.second.empty(); });

                    distributed_index_stats.emplace_back(DistributedIndexStat{
                        .address = replica_address,
                        .num_parts_send = num_parts_send,
                        .num_parts_received = parts_on_replica.size(),
                        .num_granules_send = replica_granules_send,
                        .num_granules_received = replica_granules_received,
                    });

                    received_granules += replica_granules_received;
                    received_parts += parts_on_replica.size();

                    analyzed_parts_ranges.insert_range(std::move(parts_on_replica));
                }

                auto index_description = indexes->key_condition->generateUnsubstituted().getDescription();
                result.index_stats.emplace_back(IndexStat{
                    .type = IndexType::PrimaryKey,
                    .condition = index_description.condition,
                    .used_keys = index_description.used_keys,
                    .num_parts_after = received_parts,
                    .num_granules_after = received_granules,
                    .distributed = std::move(distributed_index_stats),
                });
            }

            LOG_DEBUG(log, "Received parts ranges for {} parts via distributed index analysis", analyzed_parts_ranges.size());

            RangesInDataParts result_parts_ranges;
            for (const auto & [part_name, ranges] : analyzed_parts_ranges)
            {
                auto part_range_info = *parts_ranges_map.at(part_name);
                /// Note: part_range_info.ranges may have been split by Query Condition Cache,
                /// so we cannot assert ranges.size() == 1 here.
                chassert(part_range_info.exact_ranges.empty());

                part_range_info.ranges = ranges;
                result_parts_ranges.push_back(part_range_info);
            }

            /// Parts should be sorted by part_index_in_query for Query Condition Cache
            std::sort(result_parts_ranges.begin(), result_parts_ranges.end(),
                [](const auto & a, const auto & b) { return a.part_index_in_query < b.part_index_in_query; });

            result.parts_with_ranges = std::move(result_parts_ranges);
        }

        std::optional<size_t> condition_hash;
        if (reader_settings.use_query_condition_cache && query_info_.filter_actions_dag && !query_info_.isFinal()
                && !vector_search_parameters.has_value() /// Vector search filters through the ORDER BY, so excluded ranges are not described by the WHERE DAG hash alone.
                && !result.sampling.use_sampling)        /// SAMPLE-ing narrows the marks too, but the query condition cache cache key encodes only the WHERE predicate.
                                                         /// Avoid that SAMPLE-narrowed entries poison the cache (later non-SAMPLE-ing queries would return wrong results).
        {
            const auto & outputs = query_info_.filter_actions_dag->getOutputs();
            /// The query condition cache for `ORDER BY ... LIMIT N` (TopK) reads is gated behind the
            /// `use_query_condition_cache_for_top_k` setting (enabled by default). When it is off, do
            /// not record index-analysis exclusions for TopK reads: their excluded ranges include marks
            /// dropped by the running `__topKFilter` threshold, which is not sound to store in the
            /// (threshold-oblivious) QCC. When it is on, salt the key with the TopK plan parameters so
            /// only the same plan reuses them (mirrors the write path in `updateQueryConditionCache`).
            /// For a non-TopK read `top_k_filter_info` is empty and `isDeterministicAllowingTopKFilter`
            /// is equivalent to `VirtualColumnUtils::isDeterministic` (no `__topKFilter` can appear).
            const bool skip_top_k = top_k_filter_info && !settings[Setting::use_query_condition_cache_for_top_k];
            if (outputs.size() == 1 && !skip_top_k && isDeterministicAllowingTopKFilter(outputs.front()))
            {
                size_t hash = queryConditionCacheHash(outputs.front()->getHash(), reader_settings.query_condition_cache_settings_salt);
                if (top_k_filter_info)
                    boost::hash_combine(hash, top_k_filter_info->condition_hash);
                condition_hash = hash;
            }
        }

        /// Fill query condition cache with ranges excluded by index analysis.
        if (condition_hash)
        {
            RangesInDataParts remaining;

            auto it_parts = res_parts.begin();
            auto it_result = result.parts_with_ranges.begin();

            while (it_parts != res_parts.end())
            {
                if (it_result != result.parts_with_ranges.end() && it_parts->part_index_in_query == it_result->part_index_in_query)
                {
                    auto & full_ranges = it_parts->ranges;
                    const auto & kept_ranges = it_result->ranges;

                    MarkRanges diff_ranges;

                    auto * it_full = full_ranges.begin();
                    const auto * it_kept = kept_ranges.begin();

                    while (it_full != full_ranges.end())
                    {
                        if (it_kept == kept_ranges.end() || it_full->end <= it_kept->begin)
                        {
                            /// full range is completely before kept range, keep it
                            diff_ranges.push_back(*it_full);
                            ++it_full;
                        }
                        else if (it_full->begin >= it_kept->end)
                        {
                            /// full range is completely after kept range, move to next kept
                            ++it_kept;
                        }
                        else
                        {
                            /// overlap, need to slice
                            if (it_full->begin < it_kept->begin)
                                diff_ranges.push_back({it_full->begin, it_kept->begin});

                            if (it_full->end > it_kept->end)
                            {
                                /// adjust full range and check next kept range
                                *it_full = {it_kept->end, it_full->end};
                                ++it_kept;
                            }
                            else
                            {
                                /// fully covered or trimmed
                                ++it_full;
                            }
                        }
                    }

                    if (!diff_ranges.empty())
                    {
                        remaining.emplace_back(
                            it_parts->data_part,
                            it_parts->parent_part,
                            it_parts->part_index_in_query,
                            it_parts->part_starting_offset_in_query,
                            std::move(diff_ranges),
                            it_parts->read_hints);
                    }

                    ++it_parts;
                    ++it_result;
                }
                else
                {
                    /// part was erased entirely, keep it whole
                    remaining.push_back(*it_parts);
                    ++it_parts;
                }
            }

            auto query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
            const auto * output = query_info_.filter_actions_dag->getOutputs().front();
            /// These exclusions come from skip-index (and primary-key) analysis, which can diverge
            /// from the row-level predicate (e.g. a text index with a preprocessor). Store them
            /// under a key salted with the effective skip-index profile so that only a query that
            /// ran the same set of indexes consults them; a query that disabled skip indexes (or
            /// ignored an index) reads its own profile's key and is not poisoned. See issue #108519.
            const UInt64 profiled_condition_hash = MergeTreeDataSelectExecutor::getSkipIndexProfiledConditionHash(*condition_hash, *indexes);
            for (const auto & remaining_ranges : remaining)
            {
                const auto & data_part = remaining_ranges.data_part;
                String part_name = data_part->isProjectionPart() ? fmt::format("{}:{}", data_part->getParentPartName(), data_part->name)
                                                                 : data_part->name;
                query_condition_cache->write(
                    data_part->storage.getStorageID().uuid,
                    part_name,
                    profiled_condition_hash,
                    output->result_name,
                    remaining_ranges.ranges,
                    data_part->index_granularity->getMarksCount(),
                    data_part->index_granularity->hasFinalMark());
            }
        }
    }

    size_t sum_marks_pk = total_marks_pk;
    for (const auto & stat : result.index_stats)
        if (stat.type == IndexType::PrimaryKey)
            sum_marks_pk = stat.num_granules_after;

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;

    for (const auto & part : result.parts_with_ranges)
    {
        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
        sum_rows += part.getRowsCount();
    }

    if (add_index_stat_row_for_pk_expand)
    {
        result.index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::PrimaryKeyExpand,
            .description = "Selects all granules that intersect by PK values with the previous skip indexes selection",
            .num_parts_after = result.parts_with_ranges.size(),
            .num_granules_after = sum_marks});
    }

    result.total_parts = total_parts;
    result.parts_before_pk = parts_before_pk;
    result.selected_parts = result.parts_with_ranges.size();
    result.selected_ranges = sum_ranges;
    result.selected_marks = sum_marks;
    result.selected_marks_pk = sum_marks_pk;
    result.total_marks_pk = total_marks_pk;
    result.selected_rows = sum_rows;
    result.has_exact_ranges = result.selected_parts == 0 || find_exact_ranges;

    if (query_info_.input_order_info)
        result.read_type = (query_info_.input_order_info->direction > 0)
            ? ReadType::InOrder
            : ReadType::InReverseOrder;

    return std::make_shared<AnalysisResult>(std::move(result));
}

int ReadFromMergeTree::getSortDirection() const
{
    if (query_info.input_order_info)
        return query_info.input_order_info->direction;

    return 1;
}

void ReadFromMergeTree::updateSortDescription()
{
    result_sort_description = getSortDescriptionForOutputHeader(
        output_header,
        storage_snapshot->metadata->getSortingKeyColumns(),
        storage_snapshot->metadata->getSortingKeyReverseFlags(),
        getSortDirection(),
        query_info.input_order_info,
        query_info.row_level_filter,
        query_info.prewhere_info,
        enable_vertical_final);
}

bool ReadFromMergeTree::isParallelReplicasLocalPlanForInitiator() const
{
    return is_parallel_reading_from_replicas
        && ClusterProxy::canUseLocalPlanForParallelReplicas(context)
        && context->canUseParallelReplicasOnInitiator();
}

bool ReadFromMergeTree::isParallelReplicasLocalPlanForFollower() const
{
    return is_parallel_reading_from_replicas
        && ClusterProxy::canUseLocalPlanForParallelReplicas(context)
        && context->canUseParallelReplicasOnFollower();
}

bool ReadFromMergeTree::requestReadingInOrder(size_t prefix_size, int direction, size_t read_limit, size_t query_limit)
{
    /// if direction is not set, use current one
    if (!direction)
        direction = getSortDirection();

    /// Reading in reverse order flips which row of a group with equal keys FINAL selects. Only the
    /// Replacing algorithm compensates for that (see `ReplacingSortedAlgorithm`).
    if (direction != 1 && query_info.isFinal()
        && (data.merging_params.mode != MergeTreeData::MergingParams::Replacing
            || !context->getSettingsRef()[Setting::optimize_read_in_reverse_order_final]))
        return false;

    /// The prefix indexes this snapshot's sorting key, and a clone of its expression list is resized
    /// to `prefix_size`, which appends null `ASTPtr` children when the prefix is longer than the key.
    if (prefix_size > storage_snapshot->metadata->getSortingKey().column_names.size())
        return false;

    /// Only a later request that WIDENS an already-established prefix (distinct/aggregation-in-order
    /// re-entering after ORDER BY) can strand a too-narrow virtual row conversion. The first,
    /// conversion-building request may legitimately be narrower than prefix_size (fixed middle key,
    /// e.g. WHERE b = 1 ORDER BY a, c on key (a, b, c)); dropping it there would defeat the
    /// optimization and, behind a join, re-enable the path the !uses_virtual_row guard blocks.
    const bool widened_over_previous_request
        = query_info.input_order_info && query_info.input_order_info->used_prefix_of_sorting_key_size < prefix_size;

    query_info.input_order_info = std::make_shared<InputOrderInfo>(SortDescription{}, prefix_size, direction, read_limit);
    query_task_size_limit = query_limit ? query_limit : read_limit;
    reader_settings.read_in_order = true;

    /// The conversion only produces its own leading sort columns; the extra merge columns of a
    /// widened re-request are default-filled by setVirtualRow, so the announced boundary is wrong.
    /// Drop the virtual row here: the merge then falls back to normal cross-part comparison.
    /// Coverage is the number of primary key columns the conversion reads, not the number of
    /// columns it outputs: constant ORDER BY columns are outputs backed by no key column.
    if (widened_over_previous_request && virtual_row_conversion
        && virtual_row_conversion->getRequiredColumnsWithTypes().size() < prefix_size)
        resetVirtualRowConversions();

    /// In case of read-in-order, don't create too many reading streams.
    /// Almost always we are reading from a single stream at a time because of merge sort.
    if (output_streams_limit)
        requested_num_streams = output_streams_limit;

    /// All *InOrder optimization rely on an assumption that output stream is sorted, but vertical FINAL breaks this rule
    /// Let prefer in-order optimization over vertical FINAL for now
    enable_vertical_final = false;

    updateSortDescription();

    /// Set correct read_type
    if (analyzed_result_ptr)
    {
        analyzed_result_ptr->read_type = (query_info.input_order_info->direction > 0)
            ? ReadType::InOrder
            : ReadType::InReverseOrder;
    }

    return true;
}

bool ReadFromMergeTree::setVirtualRowConversions(ActionsDAG virtual_row_conversion_)
{
    /// Disable virtual row for FINAL.
    if (isQueryWithFinal() || !context->getSettingsRef()[Setting::read_in_order_use_virtual_row])
        return false;

    virtual_row_conversion = std::make_shared<ExpressionActions>(std::move(virtual_row_conversion_));
    return true;
}


bool ReadFromMergeTree::readsInOrder() const
{
    return reader_settings.read_in_order;
}

void ReadFromMergeTree::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    query_info.prewhere_info = prewhere_info_value;

    /// when PREWHERE is deferred after FINAL, a later rewrite must apply to the filter that actually runs
    if (isPrewhereDeferredAfterFinal())
        deferred_prewhere_info = prewhere_info_value;

    /// Build sets for the new PREWHERE synchronously. PREWHERE is evaluated at the
    /// storage level during data reading, before the pipeline-level CreatingSetsStep
    /// has a chance to execute. If a condition with IN (subquery) was moved to PREWHERE
    /// by optimizePrewhere after applyFilters already ran, the set would remain unbuilt
    /// and cause a "Not-ready Set" error.
    /// We must skip sets used in GLOBAL IN functions because ReadFromRemote needs to
    /// attach external tables to those sets before they are built. Building them here
    /// would cause "Trying to attach external table to a ready set" errors.
    /// Only build sets when applyFilters has already been called for this step (indicated by
    /// `indexes` being populated). The plan built by `considerEnablingParallelReplicas` for
    /// statistics collection runs `optimizePrewhere` without `optimizePrimaryKeyConditionAndLimit`,
    /// so `applyFilters` is skipped there and sets must not be built — the original plan's
    /// `CreatingSetsStep` (added later via `addStepsToBuildSets`) handles them. Building here
    /// would re-execute the IN-subquery and double-count its rows against `max_rows_to_read`.
    if (query_info.prewhere_info && indexes.has_value())
        VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn(query_info.prewhere_info->prewhere_actions, context);

    output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        query_info.row_level_filter,
        prewhere_info_value));

    updateSortDescription();
}

void ReadFromMergeTree::replaceVectorColumnWithDistanceColumn(const String & vector_column)
{
    if (isVectorColumnReplaced())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "The `_distance` column is an internal virtual column of vector search and cannot be referenced directly in queries. "
            "Use the distance function (e.g. `L2Distance`, `cosineDistance`) in ORDER BY instead");
    std::erase(all_column_names, vector_column);
    all_column_names.emplace_back("_distance");
    output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        query_info.row_level_filter,
        query_info.prewhere_info));

    /// if analysis has already been done (like in optimization for projections),
    /// then update columns to read in analysis result
    if (analyzed_result_ptr)
        analyzed_result_ptr->column_names_to_read = all_column_names;
}

bool ReadFromMergeTree::isVectorColumnReplaced() const
{
    return std::ranges::find(all_column_names, "_distance") != all_column_names.end();
}

bool ReadFromMergeTree::isPartitionIndependentProcessingProfitable(ProcessorKind kind) const
{
    const auto & settings = context->getSettingsRef();

    std::string_view operation;
    std::string_view force_setting;
    std::string_view max_partitions_setting;
    UInt64 max_partitions = 0;
    switch (kind)
    {
        case ProcessorKind::Aggregation:
            operation = "aggregation";
            force_setting = "force_aggregate_partitions_independently";
            max_partitions_setting = "max_number_of_partitions_for_independent_aggregation";
            max_partitions = settings[Setting::max_number_of_partitions_for_independent_aggregation];
            break;
        case ProcessorKind::Distinct:
            operation = "DISTINCT";
            force_setting = "force_distinct_partitions_independently";
            max_partitions_setting = "max_number_of_partitions_for_independent_distinct";
            max_partitions = settings[Setting::max_number_of_partitions_for_independent_distinct];
            break;
        case ProcessorKind::Window:
            operation = "window functions";
            force_setting = "force_window_partitions_independently";
            max_partitions_setting = "max_number_of_partitions_for_independent_window";
            max_partitions = settings[Setting::max_number_of_partitions_for_independent_window];
            break;
    }

    const auto partitions_cnt = countPartitions(getParts());

    if (partitions_cnt == 1 || partitions_cnt < settings[Setting::max_threads] / 2)
    {
        LOG_TRACE(
            log,
            "Independent {} by partitions won't be used because there are too few of them: {}. You can set {} to suppress this check",
            operation,
            partitions_cnt,
            force_setting);
        return false;
    }

    if (partitions_cnt > max_partitions)
    {
        LOG_TRACE(
            log,
            "Independent {} by partitions won't be used because there are too many of them: {}. You can increase {} "
            "(current value is {}) or set {} to suppress this check",
            operation,
            partitions_cnt,
            max_partitions_setting,
            max_partitions,
            force_setting);
        return false;
    }

    std::unordered_map<String, size_t> partition_rows;
    for (const auto & part : getParts())
        partition_rows[part.data_part->info.getPartitionId()] += part.data_part->rows_count;
    size_t sum_rows = 0;
    size_t max_rows = 0;
    for (const auto & [_, rows] : partition_rows)
    {
        sum_rows += rows;
        max_rows = std::max(max_rows, rows);
    }

    /// Merging shouldn't take more time than the per-stream pre-step in normal cases, and exec time is
    /// proportional to the amount of data. We assume exec time of independent processing is proportional
    /// to the maximum partition size, and of ordinary processing to (sum / threads) * 2 (pre-step + merge).
    const size_t avg_rows_in_partition = sum_rows / settings[Setting::max_threads];
    if (max_rows > avg_rows_in_partition * 2)
    {
        LOG_TRACE(
            log,
            "Independent {} by partitions won't be used because of too big skew in the number of rows between partitions. "
            "You can set {} to suppress this check",
            operation,
            force_setting);
        return false;
    }

    return true;
}

void ReadFromMergeTree::addReadColumn(const String & column)
{
    if (std::ranges::find(all_column_names, column) != all_column_names.end())
        return;

    all_column_names.emplace_back(column);

    /// A PREWHERE / row-level-filter ActionsDAG only outputs the columns it was built with, and `transformHeader` below
    /// runs the read header through it. A column added here after analysis (e.g. the `Quantize` companion subcolumns
    /// pulled in by the quantized-vector-search rewrite) is neither an input nor an output of those DAGs, so it would be
    /// dropped after PREWHERE and the rewrite would then fail to find it. Pass it through, mirroring
    /// `restorePrewhereInputs` but also for a column that is not yet an input of the DAG.
    const auto column_type = storage_snapshot->getSampleBlockForColumns({column}).getByName(column).type;
    auto pass_through_filter = [&](ActionsDAG & dag)
    {
        if (dag.tryFindInOutputs(column))
            return;
        for (const auto * input : dag.getInputs())
        {
            if (input->result_name == column)
            {
                dag.getOutputs().push_back(input);
                return;
            }
        }
        dag.getOutputs().push_back(&dag.addInput(column, column_type));
    };
    if (query_info.row_level_filter)
        pass_through_filter(query_info.row_level_filter->actions);
    if (query_info.prewhere_info)
        pass_through_filter(query_info.prewhere_info->prewhere_actions);

    output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        query_info.row_level_filter,
        query_info.prewhere_info));

    /// If analysis has already been done (like in optimization for projections),
    /// then update columns to read in analysis result.
    if (analyzed_result_ptr)
        analyzed_result_ptr->column_names_to_read = all_column_names;
}

bool ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePortForAggregation()
{
    if (isQueryWithFinal())
        return false;

    /// With parallel replicas we have to have only a single instance of `MergeTreeReadPoolParallelReplicas` per replica.
    /// With aggregation-by-partitions optimisation we might create a separate pool for each partition.
    if (is_parallel_reading_from_replicas)
        return false;

    if (!context->getSettingsRef()[Setting::force_aggregate_partitions_independently]
        && !isPartitionIndependentProcessingProfitable(ProcessorKind::Aggregation))
        return false;

    return output_each_partition_through_separate_port = true;
}

/// The LIMIT BY version is much more lenient than the GROUP BY / DISTINCT alternatives. The reason is
/// that ordinary LIMIT BY merges all incoming streams into one and the transform happens in a single
/// stream. Only simple cases are optimized, such as SELECT * FROM table [WHERE ...] LIMIT .. BY key; for
/// such cases the main cost is in LIMIT BY, so any parallelism at all in LIMIT BY is a win.
bool ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePortForLimitBy()
{
    if (isQueryWithFinal())
        return false;

    /// With parallel replicas we have to have only a single instance of `MergeTreeReadPoolParallelReplicas` per replica.
    /// With limit-by by partitions optimisation we might create a separate pool for each partition.
    if (is_parallel_reading_from_replicas)
        return false;

    /// This becomes no different from ordinary LIMIT BY which is single stream anyway.
    if (countPartitions(getParts()) == 1)
        return false;

    return output_each_partition_through_separate_port = true;
}

/// Like LIMIT BY, set building for `IN (subquery)` is lenient: the ordinary set fill merges all incoming
/// streams into one and hashes every row in a single `CreatingSetsTransform`. With per-partition streams
/// each partition is pre-deduplicated in parallel, so the single filling transform only sees unique rows;
/// any parallelism at all in the reduction is a win over the fully serial baseline. The one layout that
/// loses is heavy partition skew, checked below.
bool ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePortForCreatingSet()
{
    if (isQueryWithFinal())
        return false;

    /// With parallel replicas we have to have only a single instance of `MergeTreeReadPoolParallelReplicas` per replica.
    /// With creating-set by partitions optimisation we might create a separate pool for each partition.
    if (is_parallel_reading_from_replicas)
        return false;

    /// This becomes no different from the ordinary set fill which is single stream anyway.
    if (countPartitions(getParts()) == 1)
        return false;

    if (!context->getSettingsRef()[Setting::force_creating_set_partitions_independently])
    {
        /// A dominant partition is deduplicated through a single stream and its single-threaded pass
        /// dominates the whole build, while the ordinary fill at least reads in parallel. What matters is
        /// skew, not the partition count: a balanced layout wins at any count (even two partitions halve
        /// the serial reduction), so unlike the aggregation/DISTINCT heuristic there is no lower bound on
        /// the number of partitions, and the threshold compares against the average partition rather than
        /// a `max_threads`-based share (the baseline is a serial fill whose cost does not scale with
        /// threads).
        std::unordered_map<String, size_t> partition_rows;
        for (const auto & part : getParts())
            partition_rows[part.data_part->info.getPartitionId()] += part.data_part->rows_count;
        size_t sum_rows = 0;
        size_t max_rows = 0;
        for (const auto & [_, rows] : partition_rows)
        {
            sum_rows += rows;
            max_rows = std::max(max_rows, rows);
        }

        if (max_rows * partition_rows.size() > 2 * sum_rows)
        {
            LOG_TRACE(
                log,
                "Independent set creation by partitions won't be used because the largest partition holds more than twice "
                "the rows of the average partition. You can set force_creating_set_partitions_independently to suppress this check");
            return false;
        }
    }

    return output_each_partition_through_separate_port = true;
}

/// DISTINCT uses the same cost heuristic as GROUP BY. Similar to GROUP BY, the ordinary DISTINCT plan has
/// a parallel preliminary `DistinctTransform` per stream.
void ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePortForDistinct()
{
    if (isQueryWithFinal())
        return;

    /// With parallel replicas we have to have only a single instance of `MergeTreeReadPoolParallelReplicas` per replica.
    /// With distinct-by-partitions optimisation we might create a separate pool for each partition.
    if (is_parallel_reading_from_replicas)
        return;

    if (!context->getSettingsRef()[Setting::force_distinct_partitions_independently]
        && !isPartitionIndependentProcessingProfitable(ProcessorKind::Distinct))
        return;

    output_each_partition_through_separate_port = true;
}

/// Window functions use the same cost heuristic as GROUP BY / DISTINCT: the ordinary plan is already
/// parallel (the input is scattered by the hash of the window `PARTITION BY` columns and every stream is
/// sorted and processed by its own window transform), so per-partition processing must provide comparable
/// parallelism (enough partitions, no dominant partition) to make skipping the scatter worthwhile.
void ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePortForWindow()
{
    if (isQueryWithFinal())
        return;

    /// With parallel replicas we have to have only a single instance of `MergeTreeReadPoolParallelReplicas` per replica.
    /// With window-by-partitions optimisation we might create a separate pool for each partition.
    if (is_parallel_reading_from_replicas)
        return;

    if (!context->getSettingsRef()[Setting::force_window_partitions_independently]
        && !isPartitionIndependentProcessingProfitable(ProcessorKind::Window))
        return;

    output_each_partition_through_separate_port = true;
}

ReadFromMergeTree::AnalysisResult & ReadFromMergeTree::getAnalysisResultImpl() const
{
    if (!analyzed_result_ptr)
        analyzed_result_ptr = selectRangesToRead();

    return *analyzed_result_ptr;
}

bool ReadFromMergeTree::isQueryWithSampling() const
{
    if (context->getSettingsRef()[Setting::parallel_replicas_count] > 1 && data.supportsSampling())
        return true;

    if (query_info.table_expression_modifiers)
        return query_info.table_expression_modifiers->getSampleSizeRatio() != std::nullopt;

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    return select.sampleSize() != nullptr;
}

Pipe ReadFromMergeTree::spreadMarkRanges(
    RangesInDataParts && parts_with_ranges,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    size_t num_streams,
    AnalysisResult & result,
    std::optional<ActionsDAG> & result_projection)
{
    const bool final = isQueryWithFinal();
    Names column_names_to_read = result.column_names_to_read;
    NameSet names(column_names_to_read.begin(), column_names_to_read.end());

    if (result.sampling.use_sampling)
    {
        NameSet sampling_columns;

        /// Add columns needed for `sample_by_ast` to `column_names_to_read`.
        for (const auto & column : result.sampling.filter_expression->getRequiredColumns().getNames())
        {
            if (names.emplace(column).second)
                column_names_to_read.push_back(column);

            sampling_columns.insert(column);
        }

        if (query_info.prewhere_info || query_info.row_level_filter)
            restorePrewhereInputs(query_info.row_level_filter.get(), query_info.prewhere_info.get(), sampling_columns);
    }

    if (final)
    {
        chassert(!is_parallel_reading_from_replicas);

        if (output_each_partition_through_separate_port)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Optimization isn't supposed to be used for queries with final");

        auto original_column_names = column_names_to_read;

        /// Add columns needed to calculate the sorting expression and the sign.
        for (const auto & column : storage_snapshot->metadata->getColumnsRequiredForSortingKey())
        {
            if (names.emplace(column).second)
                column_names_to_read.push_back(column);
        }

        if (!data.merging_params.is_deleted_column.empty() && names.emplace(data.merging_params.is_deleted_column).second)
            column_names_to_read.push_back(data.merging_params.is_deleted_column);
        if (!data.merging_params.sign_column.empty() && names.emplace(data.merging_params.sign_column).second)
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty() && names.emplace(data.merging_params.version_column).second)
            column_names_to_read.push_back(data.merging_params.version_column);

        return spreadMarkRangesAmongStreamsFinal(
            std::move(parts_with_ranges),
            index_build_context,
            num_streams,
            original_column_names,
            column_names_to_read,
            result_projection);
    }

    /// `split_parts` is a static pre-split of the parts into primary-key layers made by
    /// `optimizeJoinByShards` for the single-node plan, where the join steps above consume one
    /// output port per layer. It must be ignored for a read coordinated across parallel replicas:
    /// automatic parallel replicas (`considerEnablingParallelReplicas`) reuses the single-node
    /// analysis for the replicas plan, whose fresh join does not expect layered output, and each
    /// layer would create its own reading pool announcing to the coordinator, which rejects the
    /// second announcement from the same replica with a "Duplicate announcement received" exception.
    if (!result.split_parts.layers.empty() && !is_parallel_reading_from_replicas)
        return readByLayers(
            result.parts_with_ranges,
            std::move(result.split_parts),
            index_build_context,
            column_names_to_read,
            query_info.input_order_info);

    if (query_info.input_order_info)
    {
        return spreadMarkRangesAmongStreamsWithOrder(
            std::move(parts_with_ranges),
            index_build_context,
            num_streams,
            column_names_to_read,
            result_projection,
            query_info.input_order_info);
    }

    return spreadMarkRangesAmongStreams(std::move(parts_with_ranges), index_build_context, num_streams, column_names_to_read);
}

Pipe ReadFromMergeTree::groupPartitionsByStreams(AnalysisResult &)
{
#if !defined(OS_LINUX) && !defined(OS_DARWIN)
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Streaming queries are supported only on Linux and macOS");
#else
    const size_t num_streams = std::max<size_t>(1, requested_num_streams);
    const SharedHeader header = getOutputHeader();

    Pipes pipes;
    pipes.reserve(num_streams);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto subscription = std::make_shared<MergeTreeBoundsSubscription>(num_streams, i);
        data.subscription_manager.registerSubscription(subscription);
        pipes.emplace_back(std::make_shared<MergeTreeCommitOrderSource>(
            header,
            data,
            query_info,
            context,
            all_column_names,
            num_streams,
            block_size.max_block_size_rows,
            std::move(subscription)));
    }

    data.triggerStreamingSubscriptionEnrichment();
    return Pipe::unitePipes(std::move(pipes));
#endif
}

Pipe ReadFromMergeTree::groupStreamsByPartition(
    AnalysisResult & result,
    const MergeTreeIndexBuildContextPtr & index_build_context,
    std::optional<ActionsDAG> & result_projection)
{
    auto && parts_with_ranges = std::move(result.parts_with_ranges);

    if (parts_with_ranges.empty())
        return {};

    const size_t partitions_cnt = std::max<size_t>(countPartitions(parts_with_ranges), 1);
    const size_t partitions_per_stream = std::max<size_t>(1, partitions_cnt / requested_num_streams);
    const size_t num_streams = std::max<size_t>(1, requested_num_streams / partitions_cnt);

    Pipes pipes;
    for (auto begin = parts_with_ranges.begin(), end = begin; end != parts_with_ranges.end(); begin = end)
    {
        for (size_t i = 0; i < partitions_per_stream; ++i)
            end = std::find_if(
                end,
                parts_with_ranges.end(),
                [&end](const auto & part) { return end->data_part->info.getPartitionId() != part.data_part->info.getPartitionId(); });

        RangesInDataParts partition_parts{std::make_move_iterator(begin), std::make_move_iterator(end)};

        pipes.emplace_back(
            spreadMarkRanges(std::move(partition_parts), index_build_context, num_streams, result, result_projection));
        if (!pipes.back().empty())
            pipes.back().resize(1);
    }

    return Pipe::unitePipes(std::move(pipes));
}

QueryPlanStepPtr ReadFromMergeTree::clone() const
{
    AnalysisResultPtr analysis_result_copy;
    if (analyzed_result_ptr)
        analysis_result_copy = std::make_shared<AnalysisResult>(*analyzed_result_ptr);

    /// Filter DAGs can be modified by optimizations.
    SelectQueryInfo query_info_copy = query_info;
    if (query_info_copy.prewhere_info)
        query_info_copy.prewhere_info = std::make_shared<PrewhereInfo>(query_info_copy.prewhere_info->clone());
    if (query_info_copy.row_level_filter)
    {
        auto row_level_filter_copy = std::make_shared<FilterDAGInfo>();
        row_level_filter_copy->actions = query_info_copy.row_level_filter->actions.clone();
        row_level_filter_copy->column_name = query_info_copy.row_level_filter->column_name;
        row_level_filter_copy->do_remove_column = query_info_copy.row_level_filter->do_remove_column;
        query_info_copy.row_level_filter = std::move(row_level_filter_copy);
    }

    auto cloned_step = std::make_unique<ReadFromMergeTree>(
        prepared_parts,
        mutations_snapshot,
        all_column_names,
        data,
        data_settings,
        query_info_copy,
        storage_snapshot,
        context,
        block_size.max_block_size_rows,
        requested_num_streams,
        max_block_numbers_to_read,
        log,
        std::move(analysis_result_copy),
        is_parallel_reading_from_replicas,
        all_ranges_callback,
        read_task_callback,
        number_of_current_replica);
    cloned_step->allow_query_condition_cache = allow_query_condition_cache;
    cloned_step->distributed_read_bucket_count = distributed_read_bucket_count;
    /// The coordinator-computed mark buckets and their per-task grouping; without them the
    /// fan-out has nothing to ship in the per-read bucket task parameters, and a FINAL read
    /// with several lanes per task would serialize misaligned lanes and lose rows.
    cloned_step->distributed_read_buckets = distributed_read_buckets;
    cloned_step->distributed_read_lanes_per_task = distributed_read_lanes_per_task;
    cloned_step->distributed_read_param_name = distributed_read_param_name;
    /// Filters deferred until after FINAL merging: losing them would apply the filter
    /// before deduplication and return rows a newer version should have replaced.
    cloned_step->deferred_row_level_filter = deferred_row_level_filter;
    cloned_step->deferred_prewhere_info = deferred_prewhere_info;
    /// Carry over the TopK marker. `tryOptimizeTopK` runs in the first optimization pass, so a clone
    /// made later (`materializeQueryPlanReferences` for a common subplan reference, `cloneSubtree` for a
    /// parallel-replicas plan fragment) clones a subtree whose filter still contains `__topKFilter` and
    /// whose sorting step still shares the threshold tracker. Losing `top_k_filter_info` here would turn
    /// the clone into an apparently plain read: it would consult and populate the query condition cache
    /// under the unsalted condition hash even though its granule-skip decisions depend on the running
    /// TopK threshold. `condition_hash` already has the part-set salt folded in by `setTopKColumn`, so
    /// copy the value instead of calling `setTopKColumn` again (which would fold it in twice).
    cloned_step->top_k_filter_info = top_k_filter_info;
    /// Carry over the text-index read tasks for the same reason. `processAndOptimizeTextIndexFunctions`
    /// runs in the second optimization pass before `materializeQueryPlanReferences`, so a clone can
    /// already have a predicate rewritten to `__text_index_*` virtual columns; those columns are
    /// materialized only by this task map, and losing it makes the clone evaluate the rewritten filter
    /// without the index readers (`optimizeLazyFinal` copies the same map onto its synthetic reads).
    cloned_step->index_read_tasks = index_read_tasks;
    cloned_step->setStepDescription(*this);
    return cloned_step;
}

std::unique_ptr<LazilyReadFromMergeTree> ReadFromMergeTree::keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_outputs)
{
    if (output_header == nullptr)
        return {};

    NameSet columns_to_keep;

    for (const auto & column_name : required_outputs)
        columns_to_keep.insert(column_name);

    if (query_info.row_level_filter)
    {
        for (const auto * input : query_info.row_level_filter->actions.getInputs())
            columns_to_keep.insert(input->result_name);
    }


    if (query_info.prewhere_info)
    {
        for (const auto * input : query_info.prewhere_info->prewhere_actions.getInputs())
            columns_to_keep.insert(input->result_name);
    }

    const auto & virtuals = getStorageMetadata()->virtuals;

    Names new_column_names;
    Names columns_to_remove;
    for (const auto & column_name : all_column_names)
    {
        if (columns_to_keep.contains(column_name) || virtuals.has(column_name))
            new_column_names.push_back(column_name);
        else
            columns_to_remove.push_back(column_name);
    }

    if (columns_to_remove.empty())
        return {};

    auto lazy_reading_header = std::make_shared<const Block>(
        MergeTreeSelectProcessor::transformHeader(
            storage_snapshot->getSampleBlockForColumns(columns_to_remove),
            nullptr, //query_info.row_level_filter,
            nullptr) //query_info.prewhere_info)
    );

    PartRangesReadInfo info(getParts(), context->getSettingsRef(), *data.getSettings());

    auto new_reading = std::make_unique<LazilyReadFromMergeTree>(
        std::move(lazy_reading_header),
        block_size.max_block_size_rows,
        info.min_marks_for_concurrent_read,
        reader_settings,
        mutations_snapshot,
        storage_snapshot,
        context,
        data.getLogName());

    all_column_names = std::move(new_column_names);

    output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        query_info.row_level_filter,
        query_info.prewhere_info));

    /// Update analysis result if it exists
    if (analyzed_result_ptr)
        analyzed_result_ptr->column_names_to_read = all_column_names;

    required_source_columns = all_column_names;

    return new_reading;
}

void ReadFromMergeTree::addStartingPartOffsetAndPartOffset(bool & added_part_starting_offset, bool & added_part_offset)
{
    /// A read column consumed by a filter is exposed again by adding it back to the filter outputs,
    /// the same way `PREWHERE` keeps pass-through columns. When the column is the filter column itself
    /// (e.g. `PREWHERE _part_offset`), it is already among the outputs, so the remove-filter flag is
    /// cleared instead: after filtering it keeps its original values for the surviving rows.
    /// Both are required for the data-read pipeline to actually emit it, not only for the plan header.
    auto reexpose_in_filter = [](ActionsDAG & filter_actions, const String & filter_column_name, bool & remove_filter_column, const String & column_name) -> bool
    {
        auto & dag_outputs = filter_actions.getOutputs();
        for (const auto * input : filter_actions.getInputs())
        {
            if (input->result_name != column_name)
                continue;

            if (std::ranges::find(dag_outputs, input) == dag_outputs.end())
            {
                dag_outputs.push_back(input);
                return true;
            }

            if (filter_column_name == column_name && remove_filter_column)
            {
                remove_filter_column = false;
                return true;
            }
        }
        return false;
    };

    auto expose_in_output = [&](const String & column_name) -> bool
    {
        if (output_header && output_header->has(column_name))
            return false;

        bool reexposed = false;
        if (query_info.row_level_filter)
            reexposed |= reexpose_in_filter(
                query_info.row_level_filter->actions,
                query_info.row_level_filter->column_name,
                query_info.row_level_filter->do_remove_column,
                column_name);
        if (query_info.prewhere_info)
            reexposed |= reexpose_in_filter(
                query_info.prewhere_info->prewhere_actions,
                query_info.prewhere_info->prewhere_column_name,
                query_info.prewhere_info->remove_prewhere_column,
                column_name);

        if (!reexposed && std::ranges::find(all_column_names, column_name) == all_column_names.end())
            all_column_names.insert(all_column_names.begin(), column_name);

        return true;
    };

    added_part_starting_offset = expose_in_output("_part_starting_offset");
    added_part_offset = expose_in_output("_part_offset");

    if (!added_part_starting_offset && !added_part_offset)
        return;

    output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        query_info.row_level_filter,
        query_info.prewhere_info));

    /// Update analysis result if it exists
    if (analyzed_result_ptr)
        analyzed_result_ptr->column_names_to_read = all_column_names;

    required_source_columns = all_column_names;
}

bool ReadFromMergeTree::supportsSkipIndexesOnDataRead() const
{
    if (!indexes || !indexes->use_skip_indexes || indexes->skip_indexes.empty())
        return false;

    /// When a vector similarity index is present, disable the use_skip_indexes_on_data_read path entirely and apply
    /// all skip indexes during index analysis instead - the vector index runs first (it is the most selective) and the
    /// remaining skip indexes run after it.
    const bool has_vector_similarity_index = std::ranges::any_of(indexes->skip_indexes.useful_indices, [](const auto & idx)
    {
        return idx.index->isVectorSimilarityIndex();
    });
    if (has_vector_similarity_index)
        return false;

    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::use_skip_indexes_on_data_read])
        return false;

    /// Remove this after statistics based cardinality estimation is enabled.
    if (query_info.query_tree)
    {
        const QueryTreeNodePtr & join_tree_node = query_info.query_tree->as<QueryNode &>().getJoinTreeNode();

        if (join_tree_node && (join_tree_node->getNodeType() == QueryTreeNodeType::JOIN || join_tree_node->getNodeType() == QueryTreeNodeType::CROSS_JOIN))
            return false;
    }

    if (query_info.isFinal() && settings[Setting::use_skip_indexes_if_final_exact_mode])
        return false;

    /// Settings `read_overflow_mode = 'throw'` with `max_rows_to_read` (and the symmetric
    /// `read_overflow_mode_leaf` with `max_rows_to_read_leaf`) are evaluated early during execution,
    /// during initialization of the pipeline based on estimated row counts. Estimation doesn't work properly
    /// if the skip index is evaluated during data read (scan).
    if (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
        return false;
    if (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf])
        return false;

    /// Pending ALTER mutations (e.g. `MODIFY COLUMN`) can change the type of an indexed column,
    /// making the existing on-disk index data incompatible with the current column type.
    /// In the data-read phase the skip index is applied without the per-part `can_use_index` check
    /// that `filterPartsByPrimaryKeyAndSkipIndexes` performs, so disable the feature entirely when
    /// any data/alter mutations or patches are pending.
    if (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasAlterMutations() || mutations_snapshot->hasPatchParts())
        return false;

    return true;
}


static const char * indexTypeToString(ReadFromMergeTree::IndexType type);

void ReadFromMergeTree::logPredicateStatistics(const AnalysisResult & result) const
{
    UInt64 sample_rate = context->getSettingsRef()[Setting::predicate_statistics_sample_rate];
    if (sample_rate == 0)
        return;

    if (sample_rate > 1)
    {
        auto qid = CurrentThread::getQueryId();
        if (CityHash_v1_0_2::CityHash64(qid.data(), qid.size()) % sample_rate != 0)
            return;
    }

    auto predicate_stats_log = context->getPredicateStatisticsLog();
    if (!predicate_stats_log)
        return;

    if (result.index_stats.empty())
        return;

    auto storage_id = data.getStorageID();
    if (storage_id.database_name.empty())
        return;

    bool has_index_stats = false;
    for (const auto & stat : result.index_stats)
    {
        if (stat.type != IndexType::None && stat.part_name.empty())
        {
            has_index_stats = true;
            break;
        }
    }
    if (!has_index_stats)
        return;

    auto now = time(nullptr);
    predicate_stats_log->add([&](PredicateStatisticsLogElement & element)
    {
        element.event_date = static_cast<UInt16>(DateLUT::instance().toDayNum(now));
        element.event_time = now;
        element.database = storage_id.database_name;
        element.table = storage_id.table_name;
        element.query_id = String(CurrentThread::getQueryId());

        UInt64 prev_granules = 0;
        for (const auto & stat : result.index_stats)
        {
            if (stat.type == IndexType::None)
            {
                prev_granules = stat.num_granules_after;
                continue;
            }

            if (!stat.part_name.empty())
                continue;

            UInt64 total = prev_granules > 0 ? prev_granules : stat.num_granules_after;
            UInt64 after = stat.num_granules_after;

            element.index_names.push_back(stat.name.empty() ? indexTypeToString(stat.type) : stat.name);
            element.index_types.push_back(indexTypeToString(stat.type));
            element.total_granules.push_back(total);
            element.granules_after.push_back(after);
            element.index_selectivities.push_back(total > 0 ? static_cast<Float64>(after) / static_cast<Float64>(total) : 1.0);

            prev_granules = after;
        }
    });
}

/// Splits the analyzed marks across `bucket_count` distributed-read buckets as contiguous, mark-balanced
/// slices: bucket b gets global mark offsets [b*M/bucket_count, (b+1)*M/bucket_count) of the parts' marks
/// flattened in analyzed order (M = total marks). Computed on the coordinator so a worker never re-derives
/// ranges. Consecutive ranges of one part are coalesced; a bucket with no marks is left empty.
static std::vector<RangesInDataPartsDescription> sliceMarksAcrossBuckets(const RangesInDataParts & parts, size_t bucket_count)
{
    std::vector<RangesInDataPartsDescription> result(bucket_count);
    const size_t total_marks = parts.getMarksCountAllParts();
    if (total_marks == 0 || bucket_count == 0)
        return result;

    auto bucket_end_global = [&](size_t bucket) -> size_t
    {
        return bucket + 1 >= bucket_count ? total_marks : ((bucket + 1) * total_marks) / bucket_count;
    };

    size_t current_bucket = 0;
    size_t global_offset = 0;
    for (const auto & part : parts)
    {
        const auto & info = part.data_part->info;
        for (const auto & range : part.ranges)
        {
            const size_t length = range.end - range.begin;
            size_t covered = 0;
            while (covered < length)
            {
                const size_t global_mark = global_offset + covered;
                while (current_bucket + 1 < bucket_count && global_mark >= bucket_end_global(current_bucket))
                    ++current_bucket;
                const size_t take = std::min(length - covered, bucket_end_global(current_bucket) - global_mark);
                const MarkRange sub_range{range.begin + covered, range.begin + covered + take};
                auto & bucket = result[current_bucket];
                if (!bucket.empty() && bucket.back().info == info)
                    bucket.back().ranges.push_back(sub_range);
                else
                {
                    RangesInDataPartDescription desc;
                    desc.info = info;
                    desc.ranges = MarkRanges{sub_range};
                    bucket.push_back(std::move(desc));
                }
                covered += take;
            }
            global_offset += length;
        }
    }
    return result;
}

Pipe ReadFromMergeTree::createEmptyPipe(size_t num_streams) const
{
    Pipes pipes;
    pipes.reserve(num_streams);
    for (size_t i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<NullSource>(getOutputHeader()));

    return Pipe::unitePipes(std::move(pipes));
}

size_t ReadFromMergeTree::getNumStreamsWhenNothingToRead(const AnalysisResult & result) const
{
    /// The layers are a static pre-split of the parts made by `optimizeJoinByShards`, and the number of
    /// output ports is a part of that plan: the JOIN above consumes exactly one port per layer and pairs
    /// the ports of its two sides positionally. The layers are built from the parts of all the sources at
    /// once, so both sides always get the same number of them, even when a source contributes no parts to
    /// a layer. That is why an empty layer still occupies its port instead of being dropped, and why a
    /// source that reads nothing at all must produce one port per layer as well: collapsing it to a single
    /// port makes the two sides disagree on the number of shards. The same reasoning does not apply to a
    /// read coordinated across parallel replicas, which ignores the layers (see `spreadMarkRanges`).
    if (result.split_parts.layers.empty() || is_parallel_reading_from_replicas)
        return 1;

    return result.split_parts.layers.size();
}

void ReadFromMergeTree::initializePipeline(QueryPipelineBuilder & pipeline, [[maybe_unused]] const BuildQueryPipelineSettings & settings)
{
    auto & result = getAnalysisResult();

    /// `spreadMarkRanges` consumes `result.split_parts`, so remember the number of ports the plan expects
    /// before it is moved from.
    const size_t num_streams_when_nothing_to_read = getNumStreamsWhenNothingToRead(result);

    logPredicateStatistics(result);

    /// A distributed worker reads exactly the bucket described by its per-read bucket task parameter: its
    /// marks, whether it needs a FINAL merge, and (for a merge layer) the borders + index. Match the marks
    /// to local parts by name; a missing part is a retryable error (the replica diverged by merge or lag).
    if (distributed_read_bucket_count > 0 && settings.parameter_lookup)
    {
        /// Read this task's lanes from this read's own bucket parameter, in the layout
        /// `serializeDistributedReadBuckets` wrote.
        String blob = settings.parameter_lookup->getParameter(distributed_read_param_name).safeGet<String>();
        ReadBufferFromString buf(blob);
        const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
        DB::FormatSettings format_settings;
        size_t num_lanes = 0;
        readVarUInt(num_lanes, buf);
        distributed_read_task_buckets.clear();
        distributed_read_task_buckets.reserve(num_lanes);
        for (size_t lane = 0; lane < num_lanes; ++lane)
        {
            DistributedReadBucket bucket;
            bucket.marks.deserialize(buf, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);
            readBinary(bucket.needs_merge, buf);
            if (bucket.needs_merge)
            {
                size_t border_arity = 0;
                readVarUInt(border_arity, buf);
                size_t num_borders = 0;
                readVarUInt(num_borders, buf);
                bucket.borders.assign(num_borders, std::vector<Field>(border_arity));
                for (auto & border : bucket.borders)
                    for (size_t i = 0; i < border_arity; ++i)
                        primary_key.data_types[i]->getDefaultSerialization()->deserializeBinary(border[i], buf, format_settings);
                readVarUInt(bucket.index, buf);
            }
            distributed_read_task_buckets.push_back(std::move(bucket));
        }

        /// A FINAL worker keeps all local parts and resolves each lane's marks against them in
        /// `spreadMarkRangesAmongStreamsFinal`. A non-FINAL read has one bucket: pin its marks here so the
        /// plain read path reads exactly them.
        if (!isQueryWithFinal())
        {
            const auto & bucket_marks = distributed_read_task_buckets.front().marks;
            std::unordered_map<String, RangesInDataPart> parts_by_name;
            for (auto & part : result.parts_with_ranges)
                parts_by_name.emplace(part.data_part->info.getPartNameV1(), std::move(part));
            RangesInDataParts bucket_parts;
            bucket_parts.reserve(bucket_marks.size());
            for (const auto & part_desc : bucket_marks)
            {
                auto it = parts_by_name.find(part_desc.info.getPartNameV1());
                if (it == parts_by_name.end())
                    throw Exception(ErrorCodes::NO_SUCH_DATA_PART,
                        "Distributed read: part {} selected by the coordinator is not available on this replica "
                        "(diverged by merge or replication lag); retry the query", part_desc.info.getPartNameV1());
                RangesInDataPart part = std::move(it->second);
                part.ranges = part_desc.ranges;
                bucket_parts.push_back(std::move(part));
            }
            result.parts_with_ranges = std::move(bucket_parts);
        }

        /// Cannot cache PREWHERE results when ranges are pinned per bucket.
        reader_settings.use_query_condition_cache = false;
    }

    /// Do not keep data parts in snapshot.
    {
        auto stripped_snapshot_data = std::make_unique<MergeTreeData::SnapshotData>();
        if (const auto * snapshot_data = dynamic_cast<const MergeTreeData::SnapshotData *>(storage_snapshot->data.get()))
        {
            stripped_snapshot_data->storage = snapshot_data->storage;
            stripped_snapshot_data->mutations_snapshot = snapshot_data->mutations_snapshot;
        }

        /// The snapshot object may be shared with other query plan steps.
        /// So replace our own pointer with a stripped clone instead of mutating the shared object in place.
        storage_snapshot = storage_snapshot->clone(std::move(stripped_snapshot_data));
    }

    /// Check if we should apply row policy and prewhere after FINAL instead of during reading
    /// (for correct behavior with ReplacingMergeTree where row policy should not affect which row "wins" during deduplication)
    /// also PREWHERE must always be executed after row policy, so if row policy is deferred, prewhere must be too
    if (deferred_row_level_filter || deferred_prewhere_info)
    {
        if (deferred_row_level_filter)
            query_info.row_level_filter = nullptr;
        if (deferred_prewhere_info)
            query_info.prewhere_info = nullptr;


        /// Ensure columns required by deferred filters are included in the columns to read
        /// Without this, SELECT x would fail if row policy uses column y
        NameSet columns_to_read_set(result.column_names_to_read.begin(), result.column_names_to_read.end());
        NameSet all_columns_set(all_column_names.begin(), all_column_names.end());

        auto add_required_columns = [&](const Names & required_columns)
        {
            for (const auto & col : required_columns)
            {
                if (!columns_to_read_set.contains(col))
                {
                    result.column_names_to_read.push_back(col);
                    columns_to_read_set.insert(col);
                }
                if (!all_columns_set.contains(col))
                {
                    all_column_names.push_back(col);
                    all_columns_set.insert(col);
                }
            }
        };

        if (deferred_row_level_filter)
            add_required_columns(deferred_row_level_filter->actions.getRequiredColumnsNames());

        if (deferred_prewhere_info)
            add_required_columns(deferred_prewhere_info->prewhere_actions.getRequiredColumnsNames());

        /// The declared output header must not change here: parent steps, and under
        /// `make_distributed_plan` an already serialized `ShuffleReceiveStep`, are built from it.
        /// The deferred filters run as pipeline transforms and the converting actions below restore it.

        LOG_DEBUG(
            log,
            "Deferring filters to after FINAL: row_policy={}, prewhere={}. columns_to_read={}",
            deferred_row_level_filter != nullptr,
            deferred_prewhere_info != nullptr,
            fmt::join(result.column_names_to_read, ","));
    }

    shared_virtual_fields.emplace("_sample_factor", result.sampling.used_sample_factor);
    shared_virtual_fields.emplace("_table", data.getStorageID().getTableName());
    shared_virtual_fields.emplace("_database", data.getStorageID().getDatabaseName());

    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);

    // Adding partition info to QueryAccessInfo.
    if (context->hasQueryContext() && !query_info.is_internal)
    {
        Names partition_names;
        for (const auto & part : result.parts_with_ranges)
        {
            partition_names.emplace_back(
                fmt::format("{}.{}", data.getStorageID().getFullNameNotQuoted(), part.data_part->info.getPartitionId()));
        }
        context->getQueryContext()->addQueryAccessInfo(partition_names);
    }

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedPartsTotal, result.total_parts);
    ProfileEvents::increment(ProfileEvents::SelectedMarksTotal, result.total_marks_pk);
    /// SelectedRanges / SelectedMarks are accounted below, after we know whether a read-time
    /// skip-index reader will be installed: such a reader re-counts them post-pruning, so
    /// incrementing here as well would double-count.

    auto query_id_holder = result.checkLimits(*context, data, *data_settings);

    /// If we have neither a WHERE nor a PREWHERE condition, the query condition cache doesn't save anything --> disable it.
    bool has_where_or_prewhere = query_info.prewhere_info || query_info.filter_actions_dag;
    if (!allow_query_condition_cache || !has_where_or_prewhere)
        reader_settings.use_query_condition_cache = false;

    /// TODO(unique-key): unique-key multi-versioned bitmap is conflicted with the single-versioned query-condition cache
    if (storage_snapshot->metadata->hasUniqueKey())
        reader_settings.use_query_condition_cache = false;

    /// SAMPLE-ing narrows the marks too, but the query condition cache cache key encodes only the WHERE predicate.
    /// Avoid that SAMPLE-narrowed entries poison the cache (later non-SAMPLE-ing queries would return wrong results).
    if (result.sampling.use_sampling)
        reader_settings.use_query_condition_cache = false;

    if (filterDependsOnNonDeterministicVirtuals(storage_snapshot->metadata->virtuals, query_info))
        reader_settings.use_query_condition_cache = false;

    /// Initializing parallel replicas coordinator with empty ranges to read in case of
    /// local plan for initiator to prevent coordinator initialization by other replicas
    /// (which may skip index analysis).
    if (result.parts_with_ranges.empty())
        announceEmptyReadRangesToCoordinatorIfInitiator();

    if (result.parts_with_ranges.empty() && !query_info.isStream())
    {
        pipeline.init(createEmptyPipe(num_streams_when_nothing_to_read));
        return;
    }

    selected_marks = result.selected_marks;
    selected_rows = result.selected_rows;
    selected_parts = result.selected_parts;
    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    std::optional<ActionsDAG> result_projection;

    /// Optionally initializes index build context to filter on data reading. This context is shared across multiple
    /// MergeTreeSelectProcessor instances, and is used to construct and apply index filters in a thread-safe manner.
    MergeTreeIndexBuildContextPtr index_build_context;
    MergeTreeSkipIndexReaderPtr skip_index_reader;
    MergeTreeProjectionIndexReaderPtr projection_index_reader;

    /// Now check if we have to use primary-key or skip indexes for join pruning
    bool runtime_prune_primary_key = false;
    const bool pending_mutations = mutations_snapshot->hasDataMutations() || mutations_snapshot->hasAlterMutations() || mutations_snapshot->hasPatchParts();
    MergeTreeIndices runtime_skip_indexes;
    if (context->getSettingsRef()[Setting::use_skip_indexes_on_data_read]
        && !query_info.isFinal()
        && !join_runtime_filters_for_index_analysis.empty()
        && !pending_mutations
        /// Not supported under parallel replicas: the descriptor is not carried to remote replica
        /// reads, so pruning would only cover the local replica's share. Skip it entirely there.
        && !isParallelReadingFromReplicas()
        && indexes.has_value())
    {
        /// The PK path only needs the data-read safety checks above; only the secondary skip-index
        /// part is gated by use_skip_indexes (buildIndexes builds key_condition_rpn_template regardless).
        const bool collect_skip_indexes = context->getSettingsRef()[Setting::use_skip_indexes];

        /// Need to check ignore_data_skipping_indices
        std::unordered_set<String> ignored_index_names;
        if (context->getSettingsRef()[Setting::ignore_data_skipping_indices].changed)
            ignored_index_names = parseIdentifiersOrStringLiteralsToSet(
                context->getSettingsRef()[Setting::ignore_data_skipping_indices].toString(),
                context->getSettingsRef());

        const auto & metadata = *storage_snapshot->metadata;
        const auto & pk_columns = metadata.getPrimaryKey().column_names;
        std::unordered_set<String> seen_index_names;
        for (const auto & descr : join_runtime_filters_for_index_analysis)
        {
            if (std::find(pk_columns.begin(), pk_columns.end(), descr.key_column_name) != pk_columns.end())
            {
                runtime_prune_primary_key = true;
                continue;
            }
            if (!collect_skip_indexes)
                continue;
            for (const auto & index : metadata.getSecondaryIndices())
            {
                if (ignored_index_names.contains(index.name))
                    continue;
                if (index.type != "minmax" && index.type != "set" && index.type != "bloom_filter")
                    continue;
                if (std::find(index.column_names.begin(), index.column_names.end(), descr.key_column_name) == index.column_names.end())
                    continue;
                if (seen_index_names.insert(index.name).second)
                    runtime_skip_indexes.push_back(MergeTreeIndexFactory::instance().get(storage_snapshot->metadata, index, *data_settings));
            }
        }
    }

    /// Use a callback to isolate MergeTreeReader from JoinRuntimeFilter
    MergeTreeSkipIndexReader::DynamicPredicateBuilder dynamic_predicate_builder;
    MergeTreeSkipIndexReader::DynamicSkipIndexFilter dynamic_skip_index_filter;
    if (!join_runtime_filters_for_index_analysis.empty())
    {
        dynamic_predicate_builder =
            [lookup = context->getRuntimeFilterLookup(), descriptors = join_runtime_filters_for_index_analysis, ctx = context]
            (ActionsDAG & dag) -> const ActionsDAG::Node *
            {
                return buildRuntimeRangePredicate(*lookup, descriptors, dag, ctx);
            };

        const UInt64 bloom_filter_in_cap = context->getSettingsRef()[Setting::join_runtime_filter_exact_values_limit] / 100;
        dynamic_skip_index_filter =
            [lookup = context->getRuntimeFilterLookup(), descriptors = join_runtime_filters_for_index_analysis, bloom_filter_in_cap]
            (const IMergeTreeIndex & index) -> bool
            {
                if (index.index.type != "bloom_filter")
                    return true;
                for (const auto & descr : descriptors)
                {
                    if (std::find(index.index.column_names.begin(), index.index.column_names.end(), descr.key_column_name) == index.index.column_names.end())
                        continue;
                    auto filter = lookup->find(descr.filter_id);
                    if (!filter)
                        return false;
                    auto values = filter->getRecordedKeyValues();
                    return values && values->size() <= bloom_filter_in_cap;
                }
                return false;
            };
    }

    if (supportsSkipIndexesOnDataRead())
    {
        UsefulSkipIndexes applicable_skip_indexes = indexes->skip_indexes;

        std::erase_if(
            applicable_skip_indexes.useful_indices,
            [](const auto & idx)
            {
                /// Vector similarity indexes are not applicable on data reads.
                return idx.index->isVectorSimilarityIndex();
            });

        if (!applicable_skip_indexes.empty())
        {
            skip_index_reader = std::make_shared<MergeTreeSkipIndexReader>(
                applicable_skip_indexes,
                indexes->key_condition_rpn_template,
                indexes->use_skip_indexes_for_disjunctions,
                context->getIndexMarkCache(),
                context->getIndexUncompressedCache(),
                context->getVectorSimilarityIndexCache(),
                reader_settings,
                dynamic_predicate_builder,
                runtime_prune_primary_key,
                runtime_skip_indexes,
                dynamic_skip_index_filter,
                context,
                getLogger("MergeTreeSkipIndexReader"));
        }
    }

    /// Need a reader if the join runtime filter selected something to prune.
    /// Both flags stay unset when the guard above bails (setting off or FINAL).
    if (!skip_index_reader && (runtime_prune_primary_key || !runtime_skip_indexes.empty()))
    {
        skip_index_reader = std::make_shared<MergeTreeSkipIndexReader>(
            UsefulSkipIndexes{},
            indexes->key_condition_rpn_template,
            /*use_for_disjunctions=*/false,
            context->getIndexMarkCache(),
            context->getIndexUncompressedCache(),
            context->getVectorSimilarityIndexCache(),
            reader_settings,
            dynamic_predicate_builder,
            runtime_prune_primary_key,
            runtime_skip_indexes,
            dynamic_skip_index_filter,
            context,
            getLogger("MergeTreeSkipIndexReader"));
    }

    /// Account SelectedRanges / SelectedMarks here, once both reader-creation paths above have
    /// run. When a read-time skip-index reader is installed (either the use_skip_indexes_on_data_read
    /// path or the join runtime-filter fallback), it increments these ProfileEvents itself after
    /// read-time pruning, so we must not increment the pre-pruning AnalysisResult counts here too.
    if (!skip_index_reader)
    {
        ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
        ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);
    }

    if (!projection_index_read_desc.read_ranges.empty())
    {
        auto empty_mutations_snapshot = mutations_snapshot->cloneEmpty();
        const auto & query_settings = context->getSettingsRef();
        PartRangesReadInfo info(result.parts_with_ranges, query_settings, *data_settings);
        PoolSettings pool_settings{
            .threads = 1,
            .sum_marks = info.sum_marks,
            .min_marks_for_concurrent_read = info.min_marks_for_concurrent_read,
            .preferred_block_size_bytes = query_settings[Setting::preferred_block_size_bytes],
            .use_uncompressed_cache = info.use_uncompressed_cache,
            .use_const_size_tasks_for_remote_reading = query_settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
            .total_query_nodes = 1,
        };

        ProjectionIndexReaderByName readers;

        /// Create a reader for each projection index based on its metadata and prewhere info.
        for (const auto & read_info : projection_index_read_desc.read_infos)
        {
            readers.emplace(
                read_info.projection->name,
                SingleProjectionIndexReader(
                    std::make_shared<MergeTreeReadPoolProjectionIndex>(
                        empty_mutations_snapshot,
                        std::make_shared<StorageSnapshot>(storage_snapshot->storage, read_info.projection->metadata),
                        read_info.prewhere_info,
                        actions_settings,
                        reader_settings,
                        read_info.prewhere_info->prewhere_actions.getRequiredColumnsNames(),
                        pool_settings,
                        block_size,
                        context),
                    read_info.prewhere_info,
                    actions_settings,
                    reader_settings));
        }

        projection_index_reader = std::make_shared<MergeTreeProjectionIndexReader>(std::move(readers));
    }

    if (skip_index_reader || projection_index_reader)
    {
        MergeTreeIndexReadResultPoolPtr index_read_result_pool
            = std::make_shared<MergeTreeIndexReadResultPool>(std::move(skip_index_reader), std::move(projection_index_reader));

        RangesByIndex read_ranges;
        PartRemainingMarks part_remaining_marks;

        for (const auto & ranges : result.parts_with_ranges)
        {
            read_ranges.emplace(
                ranges.part_index_in_query,
                SkipIndexReadInput{ranges.ranges, ranges.read_hints, ranges.part_starting_offset_in_query});
            part_remaining_marks.emplace(ranges.part_index_in_query, ranges.getMarksCount());
        }

        index_build_context = std::make_shared<MergeTreeIndexBuildContext>(
            std::move(read_ranges),
            std::move(projection_index_read_desc.read_ranges),
            std::move(index_read_result_pool),
            std::move(part_remaining_marks));
    }

    Pipe pipe;
    if (query_info.isStream())
        pipe = groupPartitionsByStreams(result);
    else if (output_each_partition_through_separate_port)
        pipe = groupStreamsByPartition(result, index_build_context, result_projection);
    else
        pipe = spreadMarkRanges(std::move(result.parts_with_ranges), index_build_context, requested_num_streams, result, result_projection);

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);

    if (pipe.empty())
    {
        pipeline.init(createEmptyPipe(num_streams_when_nothing_to_read));
        return;
    }

    if (result.sampling.use_sampling)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression->clone());
        pipe.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<FilterTransform>(
                header,
                sampling_actions,
                result.sampling.filter_function->getColumnName(),
                false);
        });
    }

    /// apply row policy after FINAL if needed (must be applied before prewhere)
    auto add_deferred_filter = [&pipe](ActionsDAG filter_dag, const String & column_name, bool remove_column)
    {
        NameSet input_names;
        for (const auto * input : filter_dag.getInputs())
            input_names.insert(input->result_name);
        restoreDAGInputs(filter_dag, input_names);

        /// The filter column can be a source column, which the stream must keep for the rest of the query.
        if (input_names.contains(column_name))
            remove_column = false;

        auto actions = std::make_shared<ExpressionActions>(std::move(filter_dag));
        pipe.addSimpleTransform([&, actions, remove_column](const SharedHeader & header)
        {
            return std::make_shared<FilterTransform>(header, actions, column_name, remove_column);
        });
    };

    if (deferred_row_level_filter)
        add_deferred_filter(
            deferred_row_level_filter->actions.clone(),
            deferred_row_level_filter->column_name,
            deferred_row_level_filter->do_remove_column);

    /// apply deferred PREWHERE after row policy
    if (deferred_prewhere_info)
        add_deferred_filter(
            deferred_prewhere_info->prewhere_actions.clone(),
            deferred_prewhere_info->prewhere_column_name,
            deferred_prewhere_info->remove_prewhere_column);

    Block cur_header = pipe.getHeader();

    auto append_actions = [&result_projection](ActionsDAG actions)
    {
        if (!result_projection)
            result_projection = std::move(actions);
        else
            result_projection = ActionsDAG::merge(std::move(*result_projection), std::move(actions));
    };

    if (result_projection)
        cur_header = result_projection->updateHeader(cur_header);

    /// Extra columns may be returned (for example, if sampling is used).
    /// Convert pipe to step header structure.
    if (!isCompatibleHeader(cur_header, *getOutputHeader()))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            cur_header.getColumnsWithTypeAndName(),
            getOutputHeader()->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);

        append_actions(std::move(converting));
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(std::move(*result_projection));
        pipe.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
        });
    }

    /// Some extra columns could be added by sample/final/in-order/etc
    /// Remove them from header if not needed.
    if (!blocksHaveEqualStructure(pipe.getHeader(), *getOutputHeader()))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            getOutputHeader()->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context,
            true);

        auto converting_dag_expr = std::make_shared<ExpressionActions>(std::move(convert_actions_dag));

        pipe.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<ExpressionTransform>(header, converting_dag_expr);
        });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    /// If the actual number of streams is less than what was originally requested,
    /// the read step deliberately reduced streams (e.g. because data is small).
    /// Downstream steps like AggregatingStep use this to avoid expanding the pipeline
    /// back to max_threads, which would create overhead from mostly-empty streams.
    /// Don't set this flag for read-in-order: the stream count there is determined
    /// by the number of parts and ordering requirements, not by data size.
    /// After merge-sort, the pipeline will have 1 stream, and AggregatingStep
    /// should still expand it to max_threads.
    if (pipeline.getNumStreams() < requested_num_streams && !reader_settings.read_in_order)
        pipeline.setReadStreamCountWasReduced(true);

    pipeline.addContext(context);
    // Attach QueryIdHolder if needed
    if (query_id_holder)
        pipeline.setQueryIdHolder(std::move(query_id_holder));
}

static const char * indexTypeToString(ReadFromMergeTree::IndexType type)
{
    switch (type)
    {
        case ReadFromMergeTree::IndexType::None:
            return "None";
        case ReadFromMergeTree::IndexType::MinMax:
            return "Min-Max";
        case ReadFromMergeTree::IndexType::Partition:
            return "Partition";
        case ReadFromMergeTree::IndexType::Statistics:
            return "Statistics";
        case ReadFromMergeTree::IndexType::PrimaryKey:
            return "PrimaryKey";
        case ReadFromMergeTree::IndexType::Skip:
            return "Skip";
        case ReadFromMergeTree::IndexType::PrimaryKeyExpand:
            return "PrimaryKeyExpand";
        case ReadFromMergeTree::IndexType::NonIntersectingSplit:
            return "NonIntersectingSplit";
    }
}

static const char * readTypeToString(ReadFromMergeTree::ReadType type)
{
    switch (type)
    {
        case ReadFromMergeTree::ReadType::Default:
            return "Default";
        case ReadFromMergeTree::ReadType::InOrder:
            return "InOrder";
        case ReadFromMergeTree::ReadType::InReverseOrder:
            return "InReverseOrder";
        case ReadFromMergeTree::ReadType::ParallelReplicas:
            return "Parallel";
    }
}

void ReadFromMergeTree::describeActions(FormatSettings & format_settings) const
{
    const auto & result = getAnalysisResult();
    std::string prefix = format_settings.detail_prefix;
    std::string_view read_type_label = format_settings.pretty ? "Read type: " : "ReadType: ";
    format_settings.out << prefix << read_type_label << readTypeToString(result.read_type) << '\n';

    if (isQueryWithFinal())
        format_settings.out << prefix << "FINAL: 1\n";

    if (!result.index_stats.empty())
    {
        std::string_view delimiter = format_settings.pretty ? " | " : "\n";
        format_settings.out << prefix << "Parts: " << result.index_stats.back().num_parts_after << delimiter;
        format_settings.out << (format_settings.pretty ? "" : prefix) << "Granules: " << result.index_stats.back().num_granules_after << '\n';
    }

    if (output_each_partition_through_separate_port)
        format_settings.out << prefix << "Read each partition through separate port: 1\n";

    if (format_settings.pretty)
        QueryPlanFormat::formatOutputColumns(format_settings.pretty_names, format_settings.out, *this, prefix);

    if (query_info.prewhere_info || query_info.row_level_filter)
    {
        if (!format_settings.pretty)
        {
            format_settings.out << prefix << "Prewhere info" << '\n';
            if (query_info.prewhere_info)
                format_settings.out << prefix << "Need filter: " << query_info.prewhere_info->need_filter << '\n';

            prefix.push_back(format_settings.indent_char);
            prefix.push_back(format_settings.indent_char);
        }
    }

    if (query_info.prewhere_info)
    {
        const auto pretty_expression = format_settings.pretty
            ? QueryPlanFormat::formatColumnPretty(query_info.prewhere_info->prewhere_column_name, format_settings.pretty_names)
            : String{};

        if (!format_settings.pretty || !pretty_expression.empty())
        {
            format_settings.out << prefix << "Prewhere filter" << '\n';
            format_settings.out << prefix << "Prewhere filter column: "
                                << (format_settings.pretty ? pretty_expression : query_info.prewhere_info->prewhere_column_name);
            if (!format_settings.pretty && query_info.prewhere_info->remove_prewhere_column)
                format_settings.out << " (removed)";
            format_settings.out << '\n';
        }

        if (format_settings.pretty)
        {
            const auto annotation = QueryPlanFormat::getColumnAnnotation(query_info.prewhere_info->prewhere_column_name, format_settings);
            if (!annotation.empty())
                format_settings.out << prefix << annotation << '\n';
        }

        if (!format_settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions.clone());
            expression->describeActions(format_settings.out, prefix);
        }
    }

    if (query_info.row_level_filter)
    {
        const auto pretty_expression = format_settings.pretty
            ? QueryPlanFormat::formatColumnPretty(query_info.row_level_filter->column_name, format_settings.pretty_names)
            : String{};

        if (!format_settings.pretty || !pretty_expression.empty())
        {
            format_settings.out << prefix << "Row level filter" << '\n';
            format_settings.out << prefix << "Row level filter column: "
                                << (format_settings.pretty ? pretty_expression : query_info.row_level_filter->column_name);
            if (!format_settings.pretty && query_info.row_level_filter->do_remove_column)
                format_settings.out << " (removed)";
            format_settings.out << '\n';
        }

        if (format_settings.pretty)
        {
            const auto annotation = QueryPlanFormat::getColumnAnnotation(query_info.row_level_filter->column_name, format_settings);
            if (!annotation.empty())
                format_settings.out << prefix << annotation << '\n';
        }

        if (!format_settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(query_info.row_level_filter->actions.clone());
            expression->describeActions(format_settings.out, prefix);
        }
    }

    if (deferred_prewhere_info || deferred_row_level_filter)
    {
        format_settings.out << prefix << "Deferred filters (applied after FINAL)" << '\n';
        if (deferred_row_level_filter)
        {
            format_settings.out << prefix << "  Deferred row level filter column: "
                                << QueryPlanFormat::formatColumnPretty(deferred_row_level_filter->column_name, format_settings.pretty_names)
                                << '\n';
            const auto annotation = QueryPlanFormat::getColumnAnnotation(deferred_row_level_filter->column_name, format_settings);
            if (!annotation.empty())
                format_settings.out << prefix << "  " << annotation << '\n';
        }
        if (deferred_prewhere_info)
        {
            format_settings.out << prefix << "  Deferred prewhere filter column: "
                                << QueryPlanFormat::formatColumnPretty(
                                       deferred_prewhere_info->prewhere_column_name, format_settings.pretty_names)
                                << '\n';
            const auto annotation = QueryPlanFormat::getColumnAnnotation(deferred_prewhere_info->prewhere_column_name, format_settings);
            if (!annotation.empty())
                format_settings.out << prefix << "  " << annotation << '\n';
        }
    }

    if (virtual_row_conversion)
    {
        format_settings.out << prefix << "Virtual row conversions" << '\n';
        if (!format_settings.compact)
            virtual_row_conversion->describeActions(format_settings.out, prefix);
    }
}

void ReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    const auto & result = getAnalysisResult();
    map.add("Read Type", readTypeToString(result.read_type));
    if (isQueryWithFinal())
        map.add("FINAL", true);
    if (!result.index_stats.empty())
    {
        map.add("Parts", result.index_stats.back().num_parts_after);
        map.add("Granules", result.index_stats.back().num_granules_after);
    }

    if (output_each_partition_through_separate_port)
        map.add("Read each partition through separate port", true);

    std::unique_ptr<JSONBuilder::JSONMap> prewhere_info_map;
    if (query_info.prewhere_info || query_info.row_level_filter)
    {
        prewhere_info_map = std::make_unique<JSONBuilder::JSONMap>();
        if (query_info.prewhere_info)
            prewhere_info_map->add("Need filter", query_info.prewhere_info->need_filter);
    }

    if (query_info.prewhere_info)
    {
        std::unique_ptr<JSONBuilder::JSONMap> prewhere_filter_map = std::make_unique<JSONBuilder::JSONMap>();
        prewhere_filter_map->add("Prewhere filter column", query_info.prewhere_info->prewhere_column_name);
        prewhere_filter_map->add("Prewhere filter remove filter column", query_info.prewhere_info->remove_prewhere_column);
        auto expression = std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions.clone());
        prewhere_filter_map->add("Prewhere filter expression", expression->toTree());

        prewhere_info_map->add("Prewhere filter", std::move(prewhere_filter_map));
    }

    if (query_info.row_level_filter)
    {
        std::unique_ptr<JSONBuilder::JSONMap> row_level_filter_map = std::make_unique<JSONBuilder::JSONMap>();
        row_level_filter_map->add("Row level filter column", query_info.row_level_filter->column_name);
        auto expression = std::make_shared<ExpressionActions>(query_info.row_level_filter->actions.clone());
        row_level_filter_map->add("Row level filter expression", expression->toTree());

        prewhere_info_map->add("Row level filter", std::move(row_level_filter_map));
    }

    if (prewhere_info_map)
        map.add("Prewhere info", std::move(prewhere_info_map));

    if (deferred_prewhere_info || deferred_row_level_filter)
    {
        auto deferred_map = std::make_unique<JSONBuilder::JSONMap>();
        if (deferred_row_level_filter)
            deferred_map->add("Deferred row level filter column", deferred_row_level_filter->column_name);
        if (deferred_prewhere_info)
            deferred_map->add("Deferred prewhere filter column", deferred_prewhere_info->prewhere_column_name);
        map.add("Deferred filters (applied after FINAL)", std::move(deferred_map));
    }

    if (virtual_row_conversion)
        map.add("Virtual row conversions", virtual_row_conversion->toTree());
}

namespace
{
    std::string_view searchAlgorithmToString(const MarkRanges::SearchAlgorithm search_algorithm)
    {
        switch (search_algorithm)
        {
        case MarkRanges::SearchAlgorithm::BinarySearch:
            return "binary search";
        case MarkRanges::SearchAlgorithm::GenericExclusionSearch:
            return "generic exclusion search";
        default:
            return "";
        }
    };
}

void ReadFromMergeTree::describeIndexes(FormatSettings & format_settings) const
{
    const auto & result = getAnalysisResult();
    const auto & index_stats = result.index_stats;

    const std::string & prefix = format_settings.detail_prefix;
    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        std::string indent(format_settings.base_indent, format_settings.indent_char);
        format_settings.out << prefix << "Indexes:\n";

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            format_settings.out << prefix << indent << indexTypeToString(stat.type) << '\n';

            if (!stat.name.empty())
                format_settings.out << prefix << indent << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.used_keys.empty())
            {
                format_settings.out << prefix << indent << indent << "Keys:" << '\n';
                for (const auto & used_key : stat.used_keys)
                    format_settings.out << prefix << indent << indent << indent << used_key << '\n';
            }

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            format_settings.out << prefix << indent << indent << "Parts: " << stat.num_parts_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_parts_after;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Granules: " << stat.num_granules_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_granules_after;
            format_settings.out << '\n';

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                format_settings.out << prefix << indent << indent << "Search Algorithm: " << search_algorithm << "\n";

            if (!stat.distributed.empty())
            {
                format_settings.out << prefix << indent << indent << "Distributed:" << '\n';
                for (const auto & node_stat : stat.distributed)
                {
                    format_settings.out << prefix << indent << indent << indent << "Address: " << node_stat.address << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Parts send: " << node_stat.num_parts_send << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Parts received: " << node_stat.num_parts_received << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Granules send: " << node_stat.num_granules_send << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Granules received: " << node_stat.num_granules_received << '\n';
                }
            }
        }

        format_settings.out << prefix << indent << "Ranges: " << result.selected_ranges << '\n';
    }
}

void ReadFromMergeTree::describeIndexes(JSONBuilder::JSONMap & map) const
{
    const auto & result = getAnalysisResult();
    const auto & index_stats = result.index_stats;

    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            auto index_map = std::make_unique<JSONBuilder::JSONMap>();

            index_map->add("Type", indexTypeToString(stat.type));

            if (!stat.name.empty())
                index_map->add("Name", stat.name);

            if (!stat.description.empty())
                index_map->add("Description", stat.description);

            if (!stat.used_keys.empty())
            {
                auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & used_key : stat.used_keys)
                    keys_array->add(used_key);

                index_map->add("Keys", std::move(keys_array));
            }

            if (!stat.condition.empty())
                index_map->add("Condition", stat.condition);

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                index_map->add("Search Algorithm", search_algorithm);

            if (i)
                index_map->add("Initial Parts", index_stats[i - 1].num_parts_after);
            index_map->add("Selected Parts", stat.num_parts_after);

            if (i)
                index_map->add("Initial Granules", index_stats[i - 1].num_granules_after);
            index_map->add("Selected Granules", stat.num_granules_after);

            if (!stat.distributed.empty())
            {
                auto distributed_index_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & node_stat : stat.distributed)
                {
                    auto node_stat_map = std::make_unique<JSONBuilder::JSONMap>();
                    node_stat_map->add("Address", node_stat.address);
                    node_stat_map->add("Parts send", node_stat.num_parts_send);
                    node_stat_map->add("Parts received", node_stat.num_parts_received);
                    node_stat_map->add("Granules send", node_stat.num_granules_send);
                    node_stat_map->add("Granules received", node_stat.num_granules_received);
                    distributed_index_array->add(std::move(node_stat_map));
                }

                index_map->add("Distributed", std::move(distributed_index_array));
            }

            indexes_array->add(std::move(index_map));
        }

        map.add("Indexes", std::move(indexes_array));
    }
}

void ReadFromMergeTree::describeProjections(FormatSettings & format_settings) const
{
    const auto & result = getAnalysisResult();
    const auto & projection_stats = result.projection_stats;

    const std::string & prefix = format_settings.detail_prefix;
    if (!projection_stats.empty())
    {
        std::string indent(format_settings.base_indent, format_settings.indent_char);
        format_settings.out << prefix << "Projections:\n";

        for (const auto & stat : projection_stats)
        {
            format_settings.out << prefix << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                format_settings.out << prefix << indent << indent << "Search Algorithm: " << search_algorithm << "\n";

            format_settings.out << prefix << indent << indent << "Parts: " << stat.selected_parts;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Marks: " << stat.selected_marks;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Ranges: " << stat.selected_ranges;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Rows: " << stat.selected_rows;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Filtered Parts: " << stat.filtered_parts;
            format_settings.out << '\n';
        }
    }
}

void ReadFromMergeTree::describeProjections(JSONBuilder::JSONMap & map) const
{
    const auto & result = getAnalysisResult();
    const auto & projection_stats = result.projection_stats;

    if (!projection_stats.empty())
    {
        auto projections_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & stat : projection_stats)
        {
             auto projection_map = std::make_unique<JSONBuilder::JSONMap>();
            projection_map->add("Name", stat.name);

            if (!stat.description.empty())
                projection_map->add("Description", stat.description);

            if (!stat.condition.empty())
                projection_map->add("Condition", stat.condition);

            auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
            if (!search_algorithm.empty())
                projection_map->add("Search Algorithm", search_algorithm);

            projection_map->add("Selected Parts", stat.selected_parts);
            projection_map->add("Selected Marks", stat.selected_marks);
            projection_map->add("Selected Ranges", stat.selected_ranges);
            projection_map->add("Selected Rows", stat.selected_rows);
            projection_map->add("Filtered Parts", stat.filtered_parts);

            projections_array->add(std::move(projection_map));
        }

        map.add("Projections", std::move(projections_array));
    }
}

void ReadFromMergeTree::clearParallelReadingExtension()
{
    if (!is_parallel_reading_from_replicas)
        return;

    is_parallel_reading_from_replicas = false;
    all_ranges_callback.reset();
    read_task_callback.reset();
}

std::shared_ptr<ParallelReadingExtension> ReadFromMergeTree::getParallelReadingExtension()
{
    if (!is_parallel_reading_from_replicas)
        return nullptr;

    chassert(all_ranges_callback.has_value() && read_task_callback.has_value());
    const auto & client_info = context->getClientInfo();
    return std::make_shared<ParallelReadingExtension>(
        all_ranges_callback.value(),
        read_task_callback.value(),
        number_of_current_replica.value_or(client_info.number_of_current_replica),
        context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
        data.getStorageID().getFullTableName());
}

bool ReadFromMergeTree::announceEmptyReadRangesToCoordinatorIfInitiator()
{
    if (!isParallelReplicasLocalPlanForInitiator())
        return false;

    const auto & client_info = context->getClientInfo();

    auto extension = ParallelReadingExtension{
        all_ranges_callback.value(),
        read_task_callback.value(),
        number_of_current_replica.value_or(client_info.number_of_current_replica),
        context->getClusterForParallelReplicas()->getShardsInfo().at(0).getAllNodeCount(),
        data.getStorageID().getFullTableName()};

    auto get_coordination_mode = [&]
    {
        if (!query_info.input_order_info)
            return CoordinationMode::Default;

        if (!query_info.input_order_info->direction)
            return CoordinationMode::Default;

        return query_info.input_order_info->direction > 0
            ? CoordinationMode::WithOrder
            : CoordinationMode::ReverseOrder;
    };
    // This code is executed only if there is no parts to read, so the parameter values don't really matter
    std::ignore = extension.sendInitialRequest(
        get_coordination_mode(), /*description=*/{}, /*mark_segment_size=*/1, /*min_marks_per_request=*/1);
    return true;
}

void ReadFromMergeTree::createReadTasksForTextIndex(const UsefulSkipIndexes & skip_indexes, const IndexReadColumns & added_columns, const Names & removed_columns, bool is_final)
{
    index_read_tasks.clear();

    if (added_columns.empty())
        return;

    for (const auto & column_name : removed_columns)
    {
        auto it = std::ranges::find(all_column_names, column_name);
        all_column_names.erase(it);
    }

    /// We have to recreate virtual columns and storage snapshot to add new virtual columns for reading from text index.
    auto new_metadata = StorageInMemoryMetadata::clone(storage_snapshot->metadata);

    for (const auto & [index_name, added_virtual_columns] : added_columns)
    {
        auto [task_it, inserted] = index_read_tasks.try_emplace(index_name);
        auto & index_task = task_it->second;

        if (inserted)
        {
            if (!indexes)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} not found in analyzed indexes, indexes are not initialized", index_name);

            const auto & useful_indices = indexes->skip_indexes.useful_indices;
            auto index_it = std::ranges::find_if(useful_indices, [&](const auto & index) { return index.index->index.name == index_name; });

            if (index_it == useful_indices.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} not found in analyzed indexes", index_name);

            index_task.index = *index_it;
            index_task.is_final = is_final;
        }

        for (const auto & added_virtual_column : added_virtual_columns)
        {
            auto it = std::ranges::find(all_column_names, added_virtual_column.name);
            if (it != all_column_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} already added for reading", added_virtual_column.name);

            all_column_names.push_back(added_virtual_column.name);
            new_metadata->virtuals.add(added_virtual_column);
            index_task.columns.emplace_back(added_virtual_column.name, added_virtual_column.type);
        }
    }

    for (const auto & index : skip_indexes.useful_indices)
    {
        if (dynamic_cast<const MergeTreeIndexText *>(index.index.get()))
        {
            /// Create tasks for text indexes which don't read virtual columns.
            /// It's required to always read text indexes on separate step on data read.
            if (!index_read_tasks.contains(index.index->index.name))
                index_read_tasks.emplace(index.index->index.name, IndexReadTask{.columns = {}, .index = index, .is_final = is_final});
        }
    }

    storage_snapshot = std::make_shared<StorageSnapshot>(storage_snapshot->storage, std::move(new_metadata));

    if (output_header != nullptr)
    {
        output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
            storage_snapshot->getSampleBlockForColumns(all_column_names),
            query_info.row_level_filter,
            query_info.prewhere_info));
    }

    if (analyzed_result_ptr)
    {
        analyzed_result_ptr->column_names_to_read = all_column_names;
    }

    required_source_columns = all_column_names;
}

void ReadFromMergeTree::setTopKColumn(const TopKFilterInfo & top_k_filter_info_)
{
    top_k_filter_info = top_k_filter_info_;

    /// The query condition cache for `ORDER BY ... LIMIT N` (TopK) reads is gated behind the
    /// `use_query_condition_cache_for_top_k` setting (enabled by default). When it is off, turn the
    /// cache off for this read: a TopK read can drop granules during execution depending on the running
    /// `__topKFilter` threshold, so writing threshold-oblivious QCC entries is unsound.
    /// Use `allow_query_condition_cache` rather than mutating `reader_settings` directly: it is the
    /// step's persistent "this read must not use the cache" flag, it is carried over by `clone`, and it
    /// disables the cache both for index analysis (`selectRangesToReadImpl`) and for the reader
    /// (`initializePipeline` derives `reader_settings.use_query_condition_cache` from it).
    if (!context->getSettingsRef()[Setting::use_query_condition_cache_for_top_k])
        disableQueryConditionCache();

    /// A TopK granule-skip decision recorded for one part is computed against the running
    /// `__topKFilter` threshold, which is derived from the rows of *all* parts the query reads.
    /// The query condition cache key is `(table_uuid, part_name, condition_hash)`, so an entry
    /// written for a part stays matchable as long as that part keeps its name - even after a
    /// *different* part is dropped or mutated and the threshold that made the granule skippable
    /// no longer holds. Fold a hash of the whole part-set snapshot into the salt so that any
    /// change to the set of parts read (`DROP PARTITION`, mutation, merge, new `INSERT`) yields a
    /// fresh key and the now-stale decisions of the unchanged parts are never reused.
    SipHash parts_hash;
    for (const auto & part_with_ranges : getParts())
        parts_hash.update(part_with_ranges.data_part->name);

    /// `size_t` (not `UInt64`) so `boost::hash_combine` binds its seed argument on platforms where
    /// they differ (e.g. Apple, where `size_t` is `unsigned long` but `UInt64` is `unsigned long long`).
    size_t combined_hash = top_k_filter_info->condition_hash;
    boost::hash_combine(combined_hash, parts_hash.get64());
    top_k_filter_info->condition_hash = combined_hash;
}

bool ReadFromMergeTree::isSkipIndexAvailableForTopK(const String & sort_column) const
{
    const auto & all_indexes = storage_snapshot->metadata->getSecondaryIndices();

    if (all_indexes.empty())
        return false;

    for (const auto & index : all_indexes)
    {
        if (index.isSimpleSingleColumnIndex() && index.type == "minmax" && index.column_names[0] == sort_column)
            return true;
    }
    return false;
}


RangesInDataParts ReadFromMergeTree::getPartsForPrewhere() const
{
    if (analyzed_result_ptr || !indexes)
        return getParts();

    /// Share all part filters with `selectRangesToRead`, including the snapshot boundary and statistics.
    /// Keep this snapshot temporary: `PREWHERE` optimization can still change filters,
    /// so execution must filter again with the final conditions.
    IndexStats unused_stats;
    return MergeTreeDataSelectExecutor::filterParts(
        getParts(), *indexes, getStorageMetadata(), data, query_info, mutations_snapshot, getContext(),
        max_block_numbers_to_read.get(), log, unused_stats);
}

IStorage::ColumnSizeByName ReadFromMergeTree::getColumnSizesForPrewhere(
    const Names & columns, const RangesInDataParts & parts) const
{
    const bool calculate_subcolumn_sizes
        = getContext()->getSettingsRef()[Setting::allow_calculating_subcolumns_sizes_for_merge_tree_reading];

    /// Filtering and index analysis only ever remove whole parts from the snapshot, so an equal count means
    /// the same part set. Nothing was pruned: keep the table-wide estimate the storage already caches instead
    /// of measuring every part and column again, exactly as before pruned parts were taken into account.
    const size_t parts_before_pruning = analyzed_result_ptr ? analyzed_result_ptr->total_parts : prepared_parts->size();
    if (parts.size() == parts_before_pruning)
        return data.getColumnSizes(columns, calculate_subcolumn_sizes);

    IStorage::ColumnSizeByName result;
    for (const auto & part : parts)
    {
        for (const auto & column_name : columns)
        {
            const auto column = part.data_part->tryGetColumn(column_name);
            if (!column)
                continue;

            const auto size = column->isSubcolumn() && calculate_subcolumn_sizes
                ? part.data_part->getSubcolumnSize(column_name)
                : part.data_part->getColumnSize(column->getNameInStorage());
            result[column_name].add(size);
        }
    }

    /// `Compact` parts do not publish per-column sizes, so a selection made only of them measures nothing,
    /// while the wide parts that were pruned away would still describe the relative column sizes.
    /// Keep the table-wide estimate in that case, exactly as when no parts are pruned.
    const bool nothing_measured = std::ranges::all_of(result, [](const auto & entry) { return entry.second.data_compressed == 0; });
    if (!parts.empty() && nothing_measured)
        return data.getColumnSizes(columns, calculate_subcolumn_sizes);

    return result;
}

ConditionSelectivityEstimatorPtr ReadFromMergeTree::getConditionSelectivityEstimator(
    const Names & required_columns, const RangesInDataParts & parts) const
{
    if (!getStorageMetadata()->hasStatistics() || !getContext()->getSettingsRef()[Setting::use_statistics])
        return nullptr;
    return data.getConditionSelectivityEstimator(parts, required_columns, getContext());
}

ConditionSelectivityEstimatorPtr ReadFromMergeTree::getConditionSelectivityEstimatorForPrewhere(
    const Names & required_columns, const ActionsDAG::Node * predicate) const
{
    if (analyzed_result_ptr || indexes)
        return getConditionSelectivityEstimator(required_columns);

    if (!getStorageMetadata()->hasStatistics() || !getContext()->getSettingsRef()[Setting::use_statistics])
        return nullptr;

    /// Statistics-only plans have not built indexes yet. Analyze the scalar predicate
    /// without executing `IN` subqueries, which belong to the actual execution plan.
    auto parts = filterPartsForStatistics(getParts(), predicate, data, getStorageMetadata(), getContext(), skip_partition_pruning);
    return data.getConditionSelectivityEstimator(parts, required_columns, getContext());
}

ConditionSelectivityEstimatorPtr ReadFromMergeTree::getConditionSelectivityEstimator(const Names & required_columns) const
{
    return getConditionSelectivityEstimator(required_columns, analyzed_result_ptr);
}

ConditionSelectivityEstimatorPtr ReadFromMergeTree::getConditionSelectivityEstimator(const Names & required_columns, const AnalysisResultPtr & analyzed_result) const
{
    /// Just attempting to read statistics files on disk can increase query latencies
    /// First check the in-memory metadata if statistics are present at all
    if (!getStorageMetadata()->hasStatistics() || !getContext()->getSettingsRef()[Setting::use_statistics])
        return nullptr;

    const RangesInDataParts & parts = analyzed_result ? analyzed_result->parts_with_ranges : getParts();

    /// Use the execution path's min-max-before-partition order: min-max pruning can
    /// exclude parts on which partition expressions would throw.
    if (!analyzed_result && indexes)
    {
        IndexStats unused_stats;
        auto pruned_parts = MergeTreeDataSelectExecutor::filterPartsByPartition(
            parts, indexes->partition_pruner, indexes->minmax_idx_condition,
            indexes->part_values, getStorageMetadata(), data, getContext(),
            max_block_numbers_to_read.get(), getLogger("ReadFromMergeTree"), unused_stats);
        return data.getConditionSelectivityEstimator(pruned_parts, required_columns, getContext());
    }

    return data.getConditionSelectivityEstimator(parts, required_columns, getContext());
}

bool ReadFromMergeTree::canRemoveUnusedColumns() const
{
    /// The existing logic is not correct for Graphite, e.g. reading from graphite while having PREWHERE filter on the
    /// time column results in NOT_FOUND_COLUMN_IN_BLOCK
    if (data.merging_params.mode == MergeTreeData::MergingParams::Graphite)
        return false;

    if (query_info.isFinal())
    {
        // Cannot remove columns if FINAL requires them for merging
        NameSet required_for_final
            = getColumnsRequiredForMergingFinal(result_sort_description, storage_snapshot->metadata, data.merging_params);
        const auto has_column_that_is_not_required_for_final
            = std::ranges::any_of(all_column_names, [&](const auto & column_name) { return !required_for_final.contains(column_name); });

        if (!has_column_that_is_not_required_for_final)
            return false;
    }
    return true;
}

ReadFromMergeTree::RemoveUnusedColumnsResult ReadFromMergeTree::removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool /*remove_inputs*/)
{
    if (output_header == nullptr)
        return {};

    /// Positions in the final RFMT output that must be preserved for the parent step or FINAL.
    std::set<size_t> required_final_output_positions(required_output_positions.begin(), required_output_positions.end());
    /// Positions in all_column_names that must still be read from storage.
    std::set<size_t> required_storage_column_positions;
    if (query_info.isFinal())
    {
        const auto required_for_final
            = getColumnsRequiredForMergingFinal(result_sort_description, storage_snapshot->metadata, data.merging_params);

        for (size_t pos = 0; pos < output_header->columns(); ++pos)
        {
            if (required_for_final.contains(output_header->getByPosition(pos).name))
                required_final_output_positions.insert(pos);
        }

        /// Merging columns absent from the output header are added by initializePipeline and projected back off there.
        for (size_t pos = 0; pos < all_column_names.size(); ++pos)
        {
            if (required_for_final.contains(all_column_names[pos]) && output_header->has(all_column_names[pos]))
                required_storage_column_positions.insert(pos);
        }
    }

    /// Sorted vector form of required_final_output_positions, used as the initial backward-pruning frontier.
    std::vector<size_t> final_output_positions(
        required_final_output_positions.begin(),
        required_final_output_positions.end());

    Block storage_header = storage_snapshot->getSampleBlockForColumns(all_column_names);
    Block row_level_output_header = storage_header;
    if (query_info.row_level_filter)
        row_level_output_header = SourceStepWithFilter::applyPrewhereActions(std::move(row_level_output_header), query_info.row_level_filter, nullptr);

    /// Positions in the row-policy output header, which is the input header for PREWHERE.
    std::vector<size_t> required_row_level_output_positions;
    /// Positions from the old final RFMT output that remain after pruning.
    std::vector<size_t> kept_output_positions = final_output_positions;
    bool removed_output_from_prewhere = false;
    if (query_info.prewhere_info)
    {
        auto prewhere_pruning = pruneFilterDAGOutputsByPosition(
            query_info.prewhere_info->prewhere_actions,
            query_info.prewhere_info->prewhere_column_name,
            query_info.prewhere_info->remove_prewhere_column,
            row_level_output_header,
            final_output_positions,
            true);
        removed_output_from_prewhere = prewhere_pruning.changed;
        required_row_level_output_positions = std::move(prewhere_pruning.required_input_positions);
    }
    else
    {
        required_row_level_output_positions = final_output_positions;
    }

    bool removed_output_from_row_level_filter = false;
    /// Positions in the storage header required by row policy and PREWHERE filters.
    std::vector<size_t> required_storage_positions_from_filters;
    if (query_info.row_level_filter)
    {
        auto row_level_pruning = pruneFilterDAGOutputsByPosition(
            query_info.row_level_filter->actions,
            query_info.row_level_filter->column_name,
            query_info.row_level_filter->do_remove_column,
            storage_header,
            required_row_level_output_positions,
            true);
        removed_output_from_row_level_filter = row_level_pruning.changed;
        required_storage_positions_from_filters = std::move(row_level_pruning.required_input_positions);
    }
    else
    {
        required_storage_positions_from_filters = required_row_level_output_positions;
    }

    required_storage_column_positions.insert(required_storage_positions_from_filters.begin(), required_storage_positions_from_filters.end());

    Names new_column_names;
    for (size_t pos = 0; pos < all_column_names.size(); ++pos)
    {
        if (required_storage_column_positions.contains(pos))
            new_column_names.push_back(all_column_names[pos]);
    }

    if (!removed_output_from_prewhere && !removed_output_from_row_level_filter && new_column_names.size() == all_column_names.size())
        return {};

    all_column_names = std::move(new_column_names);

    output_header = std::make_shared<const Block>(MergeTreeSelectProcessor::transformHeader(
        storage_snapshot->getSampleBlockForColumns(all_column_names),
        query_info.row_level_filter,
        query_info.prewhere_info));

    if (kept_output_positions.size() != output_header->columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected number of kept output positions after removing unused columns from ReadFromMergeTree: expected {}, got {}",
            output_header->columns(),
            kept_output_positions.size());

    /// Update analysis result if it exists
    if (analyzed_result_ptr)
        analyzed_result_ptr->column_names_to_read = all_column_names;

    required_source_columns = all_column_names;

    return {true, {}, std::move(kept_output_positions)};
}

bool ReadFromMergeTree::canRemoveColumnsFromOutput() const
{
    if (output_header == nullptr)
        return false;

    return canRemoveUnusedColumns() && output_header->columns() > 0;
}

void ReadFromMergeTree::setDistributedRead(size_t bucket_count)
{
    distributed_read_bucket_count = bucket_count;
}

/// A process-wide counter gives each bucketed read a distinct task-parameter key. Assigned on the
/// coordinator and serialized, so the worker reads under the same key.
static String nextDistributedReadParamName()
{
    static std::atomic<UInt64> counter{0};
    return "read_bucket_" + toString(counter.fetch_add(1, std::memory_order_relaxed));
}

size_t ReadFromMergeTree::setupDistributedReadBuckets(size_t target_buckets, size_t max_total_buckets)
{
    /// A bucketed read is pinned to the coordinator's marks and cannot reproduce these features on the
    /// worker, so fall back to a serial read instead of bucketing and then failing when the fragment ships.
    if (!supportsBucketedRead())
    {
        LOG_TRACE(log, "Distributed read not bucketed: the read does not support bucketing");
        return 0;
    }

    /// A non-FINAL read needs no merge, so its marks can be split arbitrarily: cut the analyzed marks into
    /// contiguous mark-balanced slices, one bucket each. FINAL needs primary-key-range layers (handled
    /// below) so a deduplication group stays within one bucket.
    if (!isQueryWithFinal() || data.merging_params.mode == MergeTreeData::MergingParams::Ordinary)
    {
        auto analysis = getOrCreateAnalyzedResult();
        if (!analysis || analysis->parts_with_ranges.empty())
        {
            LOG_TRACE(log, "Distributed read not bucketed: nothing to read");
            return 0;
        }

        /// Keep every bucket, including empty ones, so the count stays `target_buckets` and matches the
        /// downstream exchange (dropping empties would shrink the count for tiny tables and force a reshuffle).
        std::vector<DistributedReadBucket> buckets;
        for (auto & slice : sliceMarksAcrossBuckets(analysis->parts_with_ranges, target_buckets))
            buckets.push_back({std::move(slice), /*needs_merge=*/ false, {}, 0});

        if (buckets.empty() || buckets.size() > max_total_buckets)
            return 0;

        distributed_read_lanes_per_task = 1;
        setDistributedRead(buckets.size());
        distributed_read_param_name = nextDistributedReadParamName();
        distributed_read_buckets = std::move(buckets);
        return distributed_read_buckets.size();
    }

    /// FINAL: split into primary-key-range layers, each merged independently.
    /// SAMPLE interacts with layer boundaries in undefined ways, and splitting needs a safe, uniformly
    /// ordered primary key. Read serially otherwise.
    const auto & modifiers = query_info.table_expression_modifiers;
    if (modifiers && (modifiers->hasSampleSizeRatio() || modifiers->hasSampleOffsetRatio()))
    {
        LOG_TRACE(log, "Distributed FINAL read not bucketed: SAMPLE");
        return 0;
    }

    /// `Graphite` rollup parameters are not shipped to the worker, so its FINAL cannot be range-split. Read serially.
    if (data.merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        LOG_TRACE(log, "Distributed FINAL read not bucketed: Graphite rollup");
        return 0;
    }

    const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
    if (!isSafePrimaryKey(primary_key))
    {
        LOG_TRACE(log, "Distributed FINAL read not bucketed: the primary key is not safe for range splitting");
        return 0;
    }

    auto in_reverse_order = deriveReverseOrder(primary_key, storage_snapshot->metadata->getSortingKey());
    if (!in_reverse_order)
    {
        LOG_TRACE(log, "Distributed FINAL read not bucketed: mixed sort directions");
        return 0;
    }

    auto analysis = getOrCreateAnalyzedResult();
    if (!analysis || analysis->parts_with_ranges.empty())
    {
        LOG_TRACE(log, "Distributed read not bucketed: nothing to read");
        return 0;
    }

    /// When FINAL does not merge across partitions, a layer must not span partitions (a key may
    /// repeat across partitions and must stay unmerged): make one span per partition. Otherwise
    /// all parts form a single span.
    std::vector<RangesInDataParts> spans;
    if (doNotMergePartsAcrossPartitionsFinal())
    {
        auto part_it = analysis->parts_with_ranges.begin();
        while (part_it != analysis->parts_with_ranges.end())
        {
            const auto partition_id = part_it->data_part->info.getPartitionId();
            RangesInDataParts span;
            while (part_it != analysis->parts_with_ranges.end() && part_it->data_part->info.getPartitionId() == partition_id)
                span.push_back(*part_it++);
            spans.push_back(std::move(span));
        }
    }
    else
    {
        /// Copy (not move): the analysis result is cached and reused by later callers (serialize, pipeline).
        spans.push_back(analysis->parts_with_ranges);
    }

    size_t total_marks = 0;
    for (const auto & span : spans)
        total_marks += span.getMarksCountAllParts();

    /// Target number of merge layers per task, fixed so the split does not depend on the
    /// machine that builds the plan.
    constexpr size_t target_lanes_per_task = 16;
    const size_t layer_budget = target_buckets * target_lanes_per_task;

    /// Split each span by primary key into non-intersecting ranges (owned by a single level>0 part,
    /// already deduplicated, read without a merge) and intersecting ranges (merged in PK-range layers),
    /// each getting a share of the span's budget proportional to its marks.
    /// `split_parts_ranges_into_intersecting_and_non_intersecting_final` (default on) gates the split.
    const bool split_non_intersecting
        = context->getSettingsRef()[Setting::split_parts_ranges_into_intersecting_and_non_intersecting_final];
    std::vector<DistributedReadBucket> buckets;
    for (auto & span : spans)
    {
        const size_t span_marks = span.getMarksCountAllParts();
        const size_t span_budget = total_marks == 0 ? 1 : std::max<size_t>(1, layer_budget * span_marks / total_marks);

        RangesInDataParts intersecting;
        if (split_non_intersecting)
        {
            auto ranges = splitPartsRanges(std::move(span), *in_reverse_order, log);
            intersecting = std::move(ranges.intersecting_parts_ranges);

            /// Non-intersecting ranges read without a merge (the worker applies the engine sign/is_deleted
            /// filter); slice them by marks like a plain read.
            const size_t non_intersecting_marks = ranges.non_intersecting_parts_ranges.getMarksCountAllParts();
            if (non_intersecting_marks > 0)
            {
                const size_t non_intersecting_buckets = std::max<size_t>(1, span_budget * non_intersecting_marks / span_marks);
                for (auto & slice : sliceMarksAcrossBuckets(ranges.non_intersecting_parts_ranges, non_intersecting_buckets))
                    if (!slice.empty())
                        buckets.push_back({std::move(slice), /*needs_merge=*/ false, {}, 0});
            }
        }
        else
        {
            intersecting = std::move(span);
        }

        /// Intersecting ranges become PK-range layers; each keeps its span's borders and its index among
        /// them so the worker can rebuild the trimming filter for its interval, then merge-dedup.
        const size_t intersecting_marks = intersecting.getMarksCountAllParts();
        if (intersecting_marks > 0)
        {
            const size_t intersecting_layers = std::max<size_t>(1, span_budget * intersecting_marks / span_marks);
            auto split = splitIntersectingPartsRangesIntoLayers(
                std::move(intersecting), intersecting_layers, primary_key.column_names.size(), *in_reverse_order, log);
            for (size_t i = 0; i < split.layers.size(); ++i)
                if (!split.layers[i].empty())
                    buckets.push_back({split.layers[i].getDescriptions(), /*needs_merge=*/ true, split.borders, i});
        }
    }

    /// Group the layers into `target_buckets` tasks. More layers than the target (a per-partition
    /// split makes at least one per partition) just mean more lanes per task, and each task ships
    /// only its own lanes. A single task or a task count above the ceiling reads serially.
    const size_t lanes_per_task = std::max<size_t>(1, (buckets.size() + target_buckets - 1) / target_buckets);
    const size_t tasks = (buckets.size() + lanes_per_task - 1) / lanes_per_task;
    if (tasks <= 1 || tasks > max_total_buckets)
    {
        LOG_TRACE(log, "Distributed FINAL read not bucketed: {} layers in {} lanes per task make {} tasks (target {}, limit {})",
            buckets.size(), lanes_per_task, tasks, target_buckets, max_total_buckets);
        return 0;
    }

    LOG_TRACE(log, "Distributed FINAL read bucketed: {} layers in {} lanes per task make {} tasks (target {})",
        buckets.size(), lanes_per_task, tasks, target_buckets);
    distributed_read_lanes_per_task = lanes_per_task;
    distributed_read_buckets = std::move(buckets);
    setDistributedRead(tasks);
    distributed_read_param_name = nextDistributedReadParamName();
    return tasks;
}

std::vector<String> ReadFromMergeTree::serializeDistributedReadBuckets() const
{
    std::vector<String> result;
    if (distributed_read_buckets.empty())
        return result;

    const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
    DB::FormatSettings format_settings;

    /// Each task gets `distributed_read_lanes_per_task` consecutive virtual buckets (lanes): a count, then
    /// for each lane its marks, its merge flag, and (for a merge lane) the span borders + its index among
    /// them so the worker can rebuild the trimming filter. Borders are concrete PK values (the producer
    /// gates on `isSafePrimaryKey`).
    const size_t lanes_per_task = std::max<size_t>(1, distributed_read_lanes_per_task);
    for (size_t start = 0; start < distributed_read_buckets.size(); start += lanes_per_task)
    {
        const size_t end = std::min(start + lanes_per_task, distributed_read_buckets.size());
        WriteBufferFromOwnString buf;
        writeVarUInt(end - start, buf);
        for (size_t i = start; i < end; ++i)
        {
            const auto & bucket = distributed_read_buckets[i];
            bucket.marks.serialize(buf, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);
            writeBinary(bucket.needs_merge, buf);
            if (bucket.needs_merge)
            {
                const size_t border_arity = bucket.borders.empty() ? 0 : bucket.borders.front().size();
                writeVarUInt(border_arity, buf);
                writeVarUInt(bucket.borders.size(), buf);
                for (const auto & border : bucket.borders)
                    for (size_t j = 0; j < border_arity; ++j)
                        primary_key.data_types[j]->getDefaultSerialization()->serializeBinary(border[j], buf, format_settings);
                writeVarUInt(bucket.index, buf);
            }
        }
        result.push_back(buf.str());
    }
    return result;
}

Strings ReadFromMergeTree::getShardsForDistributedRead() const
{
    Strings default_shard_list = {"0"};

    if (distributed_read_bucket_count == 0)
        return default_shard_list;

    auto analysis_result = getOrCreateAnalyzedResult();
    if (!analysis_result)
        return default_shard_list;

    /// TODO: take into account selected ranges?

    Strings list_of_shards;
    for (size_t i = 0; i < distributed_read_bucket_count; ++i)
        list_of_shards.push_back(std::to_string(i));

    return list_of_shards;
}


bool ReadFromMergeTree::supportsBucketedRead() const
{
    bool unsupported_deferred_filters = deferred_row_level_filter || deferred_prewhere_info;
#if CLICKHOUSE_CLOUD
    /// Deferred FINAL filters are reapplied after the merge only by the shared-storage stateless-worker
    /// read, which ships them explicitly. The replica path and the non-shared full-replica fallback would
    /// apply them before FINAL, so a deferred-FINAL read can be bucketed only on the shared-storage worker.
    if (data.isSharedStorage()
        && !context->getSettingsRef()[Setting::distributed_plan_prefer_replicas_over_workers])
        unsupported_deferred_filters = false;
#endif
    /// An order set before the plan was optimized (the old analyzer's executeOrderOptimized) is rejected in
    /// getReasonReadCannotBeDistributed, so it cannot reach here. Do not gate on it: the worker
    /// path asks for its order before consulting this, and refusing would route the read to a node with no catalog.
    return !unsupported_deferred_filters
        && !(analyzed_result_ptr && analyzed_result_ptr->readFromProjection())
        && index_read_tasks.empty();
}


void ReadFromMergeTree::verifyBucketedReadSupported() const
{
    /// A bucketed read is pinned to the coordinator's part list and cannot re-derive a projection or
    /// text index tasks. A non-bucket read reaches a node that re-plans it locally and re-derives them
    /// (a full replica; reads the stateless worker cannot reproduce are routed to a replica too).
    /// ReadInOrder is now supported, it's info is serialized and sent to the replicas. Deferred
    /// FINAL filters are gated in supportsBucketedRead (bucketed only for the worker path) and rejected
    /// in serialize.
    if (distributed_read_bucket_count == 0)
        return;

    if (analyzed_result_ptr && analyzed_result_ptr->readFromProjection())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan does not support a distributed read from a projection");
    if (!index_read_tasks.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan does not support a distributed read using direct text index tasks");
}


void ReadFromMergeTree::serialize(Serialization & ctx) const
{
    /// Serializing the STREAM modifier is not implemented yet, so reject it instead of silently
    /// reading a plain snapshot. (Pinned block boundaries and part-order virtual columns are rejected
    /// earlier in getReasonReadCannotBeDistributed.)
    if (query_info.isStream())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan does not support a distributed read with the STREAM modifier");

    verifyBucketedReadSupported();
    /// The replica path serializes deferred FINAL filters as ordinary read filters, which would apply them
    /// before FINAL. The coordinator only buckets a deferred-FINAL read for the stateless worker, so a
    /// bucketed deferred read must never reach this replica serializer -- reject it rather than return
    /// rows filtered before the merge.
    if (distributed_read_bucket_count > 0 && (deferred_row_level_filter || deferred_prewhere_info))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan does not support a bucketed distributed read with deferred FINAL filters on the replica path");

    StorageID table_id = data.getStorageID();
    writeStringBinary(table_id.getDatabaseName(), ctx.out);
    writeStringBinary(table_id.getTableName(), ctx.out);
    writeVarUInt(getAllColumnNames().size(), ctx.out);
    for (const auto & column : getAllColumnNames())
        writeStringBinary(column, ctx.out);

    /// TODO: not sure that these fields should be serialized, maybe they should be recalculated at target
    writeVarUInt(getMaxBlockSize(), ctx.out);
    writeVarUInt(getNumStreams(), ctx.out);

    const auto & table_expression_modifiers = query_info.table_expression_modifiers;

    UInt8 flags = 0;
    if (table_expression_modifiers && table_expression_modifiers->hasFinal())
        flags |= 1;
    if (table_expression_modifiers && table_expression_modifiers->hasSampleSizeRatio())
        flags |= 2;
    if (table_expression_modifiers && table_expression_modifiers->hasSampleOffsetRatio())
        flags |= 4;
    if (query_info.row_level_filter != nullptr)
        flags |= 8;
    if (query_info.prewhere_info != nullptr)
        flags |= 16;
    /// Parallel replicas reading: the replica rebuilds the read in parallel-reading mode and resolves the
    /// coordinator callbacks + its replica number from its own context, so neither is serialized here.
    if (is_parallel_reading_from_replicas)
        flags |= 32;

    writeIntBinary(flags, ctx.out);
    if (table_expression_modifiers && table_expression_modifiers->hasSampleSizeRatio())
        serializeRational(*table_expression_modifiers->getSampleSizeRatio(), ctx.out);

    if (table_expression_modifiers && table_expression_modifiers->hasSampleOffsetRatio())
        serializeRational(*table_expression_modifiers->getSampleOffsetRatio(), ctx.out);

    if (query_info.row_level_filter)
        query_info.row_level_filter->serialize(ctx);

    if (query_info.prewhere_info)
        query_info.prewhere_info->serialize(ctx);

    /// `join_runtime_filters_for_index_analysis` (the descriptors that drive left-side granule pruning
    /// for `enable_join_runtime_filters_index_analysis`) is intentionally not serialized: the worker
    /// rebuilds a fresh `ReadFromMergeTree` in `deserialize` without these descriptors, so the pruning is
    /// simply skipped on distributed reads. Results stay correct (the read just does no runtime pruning);
    /// only the optimization is lost. This mirrors the parallel-replicas guard in `initializePipeline`.
    /// Propagating the descriptors to worker plans is a follow-up.

    /// Bucketed reads exist only since query-plan serialization version 2. If the peer only understands
    /// version 1, throw a clear error rather than write bytes it would misread (the deserialize side checks
    /// the same).
    if (distributed_read_bucket_count > 0 && ctx.version < 2)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: a bucketed ReadFromMergeTree read requires query plan serialization "
            "version >= 2; all nodes must run the same version");

    /// Every distributed bucket's marks travel in its own per-read task parameter (set during fan-out),
    /// so the shared step carries only the bucket count and this read's parameter key.
    writeVarUInt(distributed_read_bucket_count, ctx.out);

    const bool ship_input_order_info = query_info.input_order_info != nullptr && distributed_read_bucket_count > 0;

    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_READ_IN_ORDER)
    {
        writeVarUInt(ship_input_order_info ? 1 : 0, ctx.out);
        if (ship_input_order_info)
        {
            writeVarUInt(query_info.input_order_info->used_prefix_of_sorting_key_size, ctx.out);
            writeIntBinary(static_cast<Int8>(query_info.input_order_info->direction), ctx.out);
            writeVarUInt(query_info.input_order_info->limit, ctx.out);
        }
    }
    else if (ship_input_order_info)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: a ReadInOrder distributed read requires query plan serialization "
            "version >= {}; all nodes must run the same version",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_READ_IN_ORDER);
    if (distributed_read_bucket_count > 0)
        writeStringBinary(distributed_read_param_name, ctx.out);
}

std::unique_ptr<IQueryPlanStep> ReadFromMergeTree::deserialize(Deserialization & ctx)
{
    String database_name;
    String table_name;
    readStringBinary(database_name, ctx.in);
    readStringBinary(table_name, ctx.in);

    size_t num_columns = 0;
    readVarUInt(num_columns, ctx.in);
    Names column_names;
    column_names.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        String column_name;
        readStringBinary(column_name, ctx.in);
        column_names.push_back(column_name);
    }

    UInt64 max_block_size = 0;
    readVarUInt(max_block_size, ctx.in);
    size_t num_streams = 0;
    readVarUInt(num_streams, ctx.in);

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    const bool has_final = flags & 1;
    const bool has_sample_size_ratio = flags & 2;
    const bool has_sample_offset_ratio = flags & 4;
    const bool has_row_level_filter = flags & 8;
    const bool has_prewhere_info = flags & 16;
    const bool enable_parallel_reading = flags & 32;

    std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
    std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;
    if (has_sample_size_ratio)
        sample_size_ratio = deserializeRational(ctx.in);
    if (has_sample_offset_ratio)
        sample_offset_ratio = deserializeRational(ctx.in);

    SelectQueryInfo query_info;
    query_info.table_expression_modifiers.emplace(has_final, sample_size_ratio, sample_offset_ratio);

    if (has_row_level_filter)
        query_info.row_level_filter = std::make_shared<FilterDAGInfo>(FilterDAGInfo::deserialize(ctx));
    if (has_prewhere_info)
        query_info.prewhere_info = std::make_shared<PrewhereInfo>(PrewhereInfo::deserialize(ctx));

    /// The per-bucket marks travel in a per-read task parameter, so the step carries only the count and
    /// this read's parameter key.
    size_t distributed_read_bucket_count = 0;
    readVarUInt(distributed_read_bucket_count, ctx.in);

    size_t input_order_prefix_size = 0;
    Int8 input_order_direction = 1;
    UInt64 input_order_limit = 0;
    bool has_input_order_info = false;
    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_READ_IN_ORDER)
    {
        UInt64 flag = 0;
        readVarUInt(flag, ctx.in);
        has_input_order_info = flag != 0;
        if (has_input_order_info)
        {
            readVarUInt(input_order_prefix_size, ctx.in);
            readIntBinary(input_order_direction, ctx.in);
            readVarUInt(input_order_limit, ctx.in);
        }
    }
    /// A version-1 bucketed step had a trailing part-name payload this reader would leave unconsumed; fail
    /// closed at the version boundary instead of misparsing the rest of the plan.
    if (distributed_read_bucket_count > 0 && ctx.version < 2)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: a bucketed ReadFromMergeTree read requires query plan serialization "
            "version >= 2; all nodes must run the same version");
    String distributed_read_param_name;
    if (distributed_read_bucket_count > 0)
        readStringBinary(distributed_read_param_name, ctx.in);

    /// The plan is only being drained off the buffer (TCPHandler::skipData) and will be discarded.
    /// All serialized fields have been consumed above, so return a lightweight placeholder that
    /// carries the serialized header (satisfies the header check) instead of doing a table lookup,
    /// index analysis and parallel-replicas callback wiring for a step that is never executed.
    if (ctx.skipping)
        return std::make_unique<ReadNothingStep>(ctx.output_header);

    /// The table could be dropped concurrently after the plan was serialized,
    /// so a failed lookup is a regular error, not a logical one.
    StorageID table_id(database_name, table_name);
    auto storage_ptr = DatabaseCatalog::instance().getTable(table_id, ctx.context);

    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage_ptr.get());
    if (!merge_tree)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Table {} is not a MergeTree table", table_id.getNameForLogs());

    MergeTreeData & table = *merge_tree;
    MergeTreeDataSelectExecutor executor(table);

    const auto metadata_snapshot = table.getInMemoryMetadataPtr(ctx.context, false);
    StorageSnapshotPtr storage_snapshot = table.getStorageSnapshot(metadata_snapshot, ctx.context);
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

    auto step = executor.readFromParts(
        snapshot_data.parts,
        snapshot_data.mutations_snapshot,
        column_names,
        storage_snapshot,
        query_info,
        ctx.context,
        max_block_size,
        num_streams,
        /*max_block_numbers_to_read*/ nullptr,
        /*merge_tree_select_result_ptr*/ nullptr,
        /// On a replica this rebuilds the read in parallel-reading mode. Passing no extension makes
        /// `readFromParts` take the coordinator callbacks from ctx.context (set by TCPHandler) and the
        /// replica number from client_info. This path is only reached for a plan that will actually be
        /// executed (see the ctx.skipping short-circuit above), so the callbacks are always present.
        enable_parallel_reading,
        /*extension*/ nullptr);

    if (distributed_read_bucket_count)
    {
        auto * read_from_merge_tree_step = dynamic_cast<ReadFromMergeTree *>(step.get());
        if (!read_from_merge_tree_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromMergeTree step is expected to be created by readFromParts");
        read_from_merge_tree_step->setDistributedRead(distributed_read_bucket_count);

        /// Inject the "read in order" the coordinator chose. This replica's ReadFromMergeTree step
        /// drives the ordinary in-order path, including the per-layer merge and the reverse transform.
        /// Only a bucketed read carries the contract: findReadingStep installs the order only when the
        /// exchange pair collapses, which means the read was made distributed.
        if (has_input_order_info
            && !read_from_merge_tree_step->requestReadingInOrder(
                input_order_prefix_size, static_cast<int>(input_order_direction), input_order_limit))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Coordinator asked for a read-in-order distributed read that this node refused");
        read_from_merge_tree_step->setDistributedReadParamName(std::move(distributed_read_param_name));
    }

    /// Need to keep shared pointer to MergeTree table till the end of plan execution
    ctx.storage_holders.push_back(storage_ptr);
    return step;
}

void registerReadFromMergeTreeStep(QueryPlanStepRegistry & registry);
void registerReadFromMergeTreeStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromMergeTree", ReadFromMergeTree::deserialize);
}

}
