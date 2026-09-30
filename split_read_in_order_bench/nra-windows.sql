-- NRA windows from best_bid_ask, no fees: a superset of the TypeScript filter's windows (same books).
-- NO basket over an event's n markets: pnl = Σ YES best_bid − 1 (an unquoted leg has bid 0).
-- Open while Σ best_bid ≥ 1000 (0.001 ticks; minPnl −0.0001 rounds to this without fees).
-- Params: from (a snapshot boundary, whole minute), to (UTC 'YYYY-MM-DD hh:mm:ss').
WITH
    toDateTime64({from:String}, 6, 'UTC') AS t0,
    toDateTime64({to:String}, 6, 'UTC') AS t1,
    snap AS (SELECT max(fetched_at) AS at FROM polymarket_gamma_events WHERE fetched_at <= toDateTime64({from:String}, 3, 'UTC')),
    legs AS (
        SELECT event_id, market_id, condition_id, count() OVER (PARTITION BY event_id) AS n_legs
        FROM (
            SELECT
                event_id,
                toUInt32(JSONExtractString(m, 'id')) AS market_id,
                JSONExtractString(m, 'conditionId') AS condition_id
            FROM polymarket_gamma_events
            ARRAY JOIN JSONExtractArrayRaw(payload, 'markets') AS m
            WHERE fetched_at = (SELECT at FROM snap)
              AND JSONExtractBool(payload, 'negRisk')
              AND ifNull(JSONExtract(m, 'active', 'Nullable(Bool)'), true)
              AND NOT ifNull(JSONExtract(m, 'closed', 'Nullable(Bool)'), false)
              AND JSONExtractRaw(m, 'clobTokenIds') NOT IN ('', 'null')
        )
    ),
    -- Each market's book at `from`: its recorder snapshot at that boundary (as the filter seeds it), then
    -- best_bid_ask from `from` on. A market with no snapshot counts bid 0 until its first BBO row.
    ticks AS (
        SELECT l.event_id AS event_id, s.market_id AS market_id, t0 AS received_at, 0 AS phase, toUInt32(0) AS seq, s.bid AS bid
        FROM (
            SELECT market_id, toInt32(argMax(if(empty(bid_prices), 0, arrayMax(bid_prices)), (received_at, received_sequence))) AS bid
            FROM polymarket_ws_initial_book_dump
            WHERE received_at >= t0 - INTERVAL 20 MINUTE AND received_at <= t0 + INTERVAL 1 MINUTE
              AND snapshot AND timestamp = toDateTime64({from:String}, 3, 'UTC')
              AND market_id IN (SELECT market_id FROM legs)
            GROUP BY market_id
        ) AS s
        INNER JOIN legs AS l ON s.market_id = l.market_id
        UNION ALL
        SELECT l.event_id, b.market_id, b.received_at, 1, b.received_sequence, toInt32(b.best_bid)
        FROM polymarket_ws_best_bid_ask AS b
        INNER JOIN legs AS l ON b.market_id = l.market_id
        WHERE b.received_at >= t0 AND b.received_at < t1
          AND b.market_id IN (SELECT market_id FROM legs)
    ),
    deltas AS (
        SELECT event_id, received_at, phase, seq, bid - lagInFrame(bid, 1, 0) OVER (PARTITION BY market_id ORDER BY received_at, phase, seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS d
        FROM ticks
    ),
    state AS (
        SELECT event_id, received_at, phase, seq,
               sum(d) OVER (PARTITION BY event_id ORDER BY received_at, phase, seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) >= 1000 AS open
        FROM deltas
    ),
    edges AS (
        SELECT event_id, received_at, phase, seq, open,
               open != lagInFrame(open, 1, false) OVER w AS flip,
               leadInFrame(received_at, 1, t1) OVER w AS next_at
        FROM state
        WINDOW w AS (PARTITION BY event_id ORDER BY received_at, phase, seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    ),
    runs AS (
        SELECT event_id, received_at, phase, seq, open, next_at,
               sum(flip) OVER (PARTITION BY event_id ORDER BY received_at, phase, seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run
        FROM edges
    ),
    windows AS (
        SELECT event_id, greatest(min(received_at), t0) AS opened, least(argMax(next_at, (received_at, phase, seq)), t1) AS closed
        FROM runs
        WHERE open
        GROUP BY event_id, run
        HAVING closed > t0
    )
SELECT
    l.condition_id AS market,
    l.market_id AS marketId,
    toUnixTimestamp64Micro(w.opened) / 1000 AS fromMs,
    toUnixTimestamp64Micro(w.closed) / 1000 + 1000 AS toMs,
    toString(w.event_id) AS groupId,
    (toUnixTimestamp64Micro(w.closed) - toUnixTimestamp64Micro(w.opened)) / 1000 AS openMs
FROM windows AS w
INNER JOIN legs AS l ON w.event_id = l.event_id
ORDER BY fromMs, groupId, market
FORMAT JSONEachRow
