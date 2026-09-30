// k-way merge of one streaming query per market, all multiplexed on ONE HTTP/2 connection (ClickHouse gRPC).
// Each market is its own HTTP/2 stream with its own flow-control window, so a market the merge does not
// need yet stops being read, and the server pauses that query only.
// Rows are RowBinary, 23 bytes: market_id u32, received_at i64 (us), tie u32, price u16, size u32, side u8.
// Usage: benchg <markets file>   env: TAG, STREAM_WINDOW (bytes, default 1 MiB)
pub mod pb {
    tonic::include_proto!("clickhouse.grpc");
}

use pb::click_house_client::ClickHouseClient;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tonic::transport::Endpoint;

const ROW: usize = 23;
const COLS: &str = "market_id, received_at, tie, price, size, side";

#[inline]
fn key(b: &[u8], o: usize) -> (i64, u32, u32) {
    let market = u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let ts = i64::from_le_bytes(b[o + 4..o + 12].try_into().unwrap());
    let tie = u32::from_le_bytes(b[o + 12..o + 16].try_into().unwrap());
    (ts, tie, market)
}

#[tokio::main(worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let tag = std::env::var("TAG").unwrap_or_default();
    let window: u32 = std::env::var("STREAM_WINDOW").ok().map(|x| x.parse().unwrap()).unwrap_or(1 << 20);
    let markets: Vec<u32> = std::fs::read_to_string(&args[1]).unwrap().split_whitespace().map(|x| x.parse().unwrap()).collect();
    let t0 = Instant::now();

    // One Channel = one TCP connection; every call below is a separate HTTP/2 stream on it.
    let channel = Endpoint::from_static("http://127.0.0.1:9100")
        .initial_stream_window_size(Some(window))
        .initial_connection_window_size(Some(1 << 30))
        .connect()
        .await
        .expect("connect");

    // The server accepts calls one at a time and the gRPC library cancels calls that wait too long
    // to be accepted, so cap calls still waiting for response headers. Open streams are not capped.
    let starting = Arc::new(Semaphore::new(256));
    // MODE=single: one query for all markets (the merge below then has one input);
    // MODE=kway: one query per market. TABLE picks the table, SPLIT the patched read setting.
    let mode = std::env::var("MODE").unwrap_or("kway".into());
    let table = std::env::var("TABLE").unwrap_or("pc".into());
    let split = std::env::var("SPLIT").unwrap_or("0".into());
    let queries: Vec<String> = if mode == "single" {
        let list: Vec<String> = markets.iter().map(|m| m.to_string()).collect();
        vec![format!("SELECT {COLS} FROM {table} WHERE market_id IN ({}) ORDER BY received_at, tie, market_id SETTINGS read_in_order_split_by_key_prefix_in = {split}", list.join(","))]
    } else {
        markets.iter().map(|m| format!("SELECT {COLS} FROM {table} WHERE market_id = {m} ORDER BY received_at, tie")).collect()
    };
    let mut receivers = Vec::with_capacity(queries.len());
    for (m, query) in queries.into_iter().enumerate() {
        let (tx, rx) = mpsc::channel::<Result<Vec<u8>, String>>(2);
        receivers.push(rx);
        let mut client = ClickHouseClient::new(channel.clone()).max_decoding_message_size(usize::MAX);
        let tag = tag.clone();
        let starting = starting.clone();
        tokio::spawn(async move {
            let info = pb::QueryInfo {
                query,
                output_format: "RowBinary".into(),
                user_name: "bench".into(),
                settings: HashMap::from([("log_comment".into(), tag), ("max_execution_time".into(), "0".into())]),
                ..Default::default()
            };
            let permit = starting.acquire().await.unwrap();
            let call = client.execute_query_with_stream_output(info).await;
            drop(permit);
            let mut stream = match call {
                Ok(r) => r.into_inner(),
                Err(e) => return drop(tx.send(Err(format!("query {m}: {e}"))).await),
            };
            let mut rest: Vec<u8> = Vec::new();
            loop {
                let r = match stream.message().await {
                    Ok(Some(r)) => r,
                    Ok(None) => break,
                    Err(e) => return drop(tx.send(Err(format!("query {m}: {e}"))).await),
                };
                if let Some(e) = r.exception {
                    return drop(tx.send(Err(format!("query {m}: {}", e.display_text))).await);
                }
                if r.output.is_empty() {
                    continue;
                }
                rest.extend_from_slice(&r.output);
                let whole = rest.len() - rest.len() % ROW;
                let tail = rest.split_off(whole);
                let block = std::mem::replace(&mut rest, tail);
                if tx.send(Ok(block)).await.is_err() {
                    return;
                }
            }
            assert!(rest.is_empty(), "truncated row");
        });
    }

    let result = tokio::task::spawn_blocking(move || {
        let (mut rows, mut hash, mut last, mut first_row) = (0u64, 0i32, (0i64, 0u32, 0u32), None);
        let mut cur: Vec<(Vec<u8>, usize)> = Vec::with_capacity(receivers.len());
        let mut heap = BinaryHeap::new();
        for (i, rx) in receivers.iter_mut().enumerate() {
            match rx.blocking_recv().map(|r| r.unwrap_or_else(|e| panic!("{e}"))) {
                Some(b) => {
                    heap.push(Reverse((key(&b, 0), i)));
                    cur.push((b, 0));
                }
                None => cur.push((Vec::new(), 0)),
            }
        }
        while let Some(Reverse((k, i))) = heap.pop() {
            assert!(rows == 0 || k >= last, "out of order at row {rows}");
            last = k;
            hash = hash.wrapping_mul(31).wrapping_add(k.1 as i32).wrapping_add(k.2 as i32);
            if rows == 0 {
                first_row = Some(Instant::now());
            }
            rows += 1;
            let (b, o) = &mut cur[i];
            *o += ROW;
            if *o == b.len() {
                match receivers[i].blocking_recv().map(|r| r.unwrap_or_else(|e| panic!("{e}"))) {
                    Some(next) => {
                        *b = next;
                        *o = 0;
                    }
                    None => continue,
                }
            }
            heap.push(Reverse((key(b, *o), i)));
        }
        (rows, hash, first_row)
    })
    .await
    .unwrap();

    let s = t0.elapsed().as_secs_f64();
    let first = result.2.map(|t| (t - t0).as_secs_f64() * 1000.0).unwrap_or(-1.0);
    println!(
        "{{\"mode\":\"grpc_{mode}\",\"client\":\"rust\",\"markets\":{},\"rows\":{},\"hash\":{},\"wall_s\":{:.2},\"first_row_ms\":{:.1},\"rows_per_s\":{}}}",
        markets.len(), result.0, result.1, s, first, (result.0 as f64 / s) as u64
    );
}
