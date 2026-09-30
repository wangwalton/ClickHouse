// Same benchmark as bench.mjs: `single` = one query, `kway` = one streaming query per market merged here.
// Rows are RowBinary, 23 bytes: market_id u32, received_at i64 (us), tie u32, price u16, size u32, side u8.
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{sync_channel, Receiver};
use std::time::Instant;

const ROW: usize = 23;
const COLS: &str = "market_id, received_at, tie, price, size, side";

/// HTTP/1.1 response body with chunked transfer encoding.
struct Body {
    r: BufReader<TcpStream>,
    left: usize,
    started: bool,
    done: bool,
}

impl Read for Body {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.done {
            return Ok(0);
        }
        if self.left == 0 {
            let mut line = String::new();
            if self.started {
                self.r.read_line(&mut line)?; // CRLF after the previous chunk
                line.clear();
            }
            self.started = true;
            self.r.read_line(&mut line)?;
            let size = usize::from_str_radix(line.trim().split(';').next().unwrap(), 16).expect("chunk size");
            if size == 0 {
                self.done = true;
                return Ok(0);
            }
            self.left = size;
        }
        let n = buf.len().min(self.left);
        let n = self.r.read(&mut buf[..n])?;
        assert!(n > 0, "connection closed inside a chunk");
        self.left -= n;
        Ok(n)
    }
}

fn open(sql: &str, tag: &str) -> Body {
    let mut s = TcpStream::connect("127.0.0.1:8123").expect("connect");
    let body = format!("{sql} FORMAT RowBinary");
    write!(s, "POST /?user=bench&max_execution_time=0&log_comment={tag} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    let mut r = BufReader::with_capacity(1 << 16, s);
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    let ok = line.contains(" 200 ");
    let mut chunked = false;
    loop {
        line.clear();
        r.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        chunked |= line.to_ascii_lowercase().starts_with("transfer-encoding: chunked");
    }
    if !ok {
        let mut rest = String::new();
        r.read_to_string(&mut rest).ok();
        panic!("HTTP error: {}", &rest[..rest.len().min(500)]);
    }
    assert!(chunked, "expected a chunked response");
    Body { r, left: 0, started: false, done: false }
}

/// Reads blocks of whole rows (~256 KiB) from a body.
fn blocks(mut body: Body, mut each: impl FnMut(Vec<u8>) -> bool) {
    let mut rest: Vec<u8> = Vec::new();
    loop {
        let mut buf = rest;
        let have = buf.len();
        buf.resize(have + (256 << 10), 0);
        let mut filled = have;
        while filled < buf.len() {
            let n = body.read(&mut buf[filled..]).unwrap();
            if n == 0 {
                break;
            }
            filled += n;
        }
        let whole = filled - filled % ROW;
        rest = buf[whole..filled].to_vec();
        buf.truncate(whole);
        let end = filled < have + (256 << 10);
        if !buf.is_empty() && !each(buf) {
            return;
        }
        if end {
            assert!(rest.is_empty(), "truncated row");
            return;
        }
    }
}

#[inline]
fn key(b: &[u8], o: usize) -> (i64, u32, u32) {
    let market = u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let ts = i64::from_le_bytes(b[o + 4..o + 12].try_into().unwrap());
    let tie = u32::from_le_bytes(b[o + 12..o + 16].try_into().unwrap());
    (ts, tie, market)
}

struct Consumer {
    rows: u64,
    hash: i32,
    last: (i64, u32, u32),
    first_row: Option<Instant>,
}

impl Consumer {
    #[inline]
    fn consume(&mut self, b: &[u8], o: usize) {
        let k = key(b, o);
        assert!(self.rows == 0 || k >= self.last, "out of order at row {}", self.rows);
        self.last = k;
        self.hash = self.hash.wrapping_mul(31).wrapping_add(k.1 as i32).wrapping_add(k.2 as i32);
        if self.rows == 0 {
            self.first_row = Some(Instant::now());
        }
        self.rows += 1;
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mode, file) = (args[1].as_str(), &args[2]);
    let tag = std::env::var("TAG").unwrap_or_default();
    let markets: Vec<u32> = std::fs::read_to_string(file).unwrap().split_whitespace().map(|x| x.parse().unwrap()).collect();
    let t0 = Instant::now();
    let mut c = Consumer { rows: 0, hash: 0, last: (0, 0, 0), first_row: None };

    // single: one query for all markets (the merge below then has one input); kway: one query per market.
    let table = std::env::var("TABLE").unwrap_or("pc".into());
    let split = std::env::var("SPLIT").unwrap_or("0".into());
    let queries: Vec<String> = match mode {
        "single" => {
            let list: Vec<String> = markets.iter().map(|m| m.to_string()).collect();
            vec![format!("SELECT {COLS} FROM {table} WHERE market_id IN ({}) ORDER BY received_at, tie, market_id SETTINGS read_in_order_split_by_key_prefix_in = {split}{}", list.join(","), std::env::var("EXTRA").unwrap_or_default())]
        }
        "kway" => markets.iter().map(|m| format!("SELECT {COLS} FROM {table} WHERE market_id = {m} ORDER BY received_at, tie")).collect(),
        _ => panic!("mode must be single or kway"),
    };
    {
        {
            // One thread per query; a bounded channel of 4 blocks (~1 MiB) is the backpressure.
            let receivers: Vec<Receiver<Vec<u8>>> = queries
                .into_iter()
                .map(|sql| {
                    let (tx, rx) = sync_channel::<Vec<u8>>(4);
                    let tag = tag.clone();
                    std::thread::Builder::new().stack_size(256 << 10).spawn(move || {
                        blocks(open(&sql, &tag), |b| tx.send(b).is_ok());
                    }).unwrap();
                    rx
                })
                .collect();
            let mut cur: Vec<(Vec<u8>, usize)> = Vec::with_capacity(receivers.len());
            let mut heap = BinaryHeap::new();
            for (i, rx) in receivers.iter().enumerate() {
                match rx.recv() {
                    Ok(b) => {
                        heap.push(Reverse((key(&b, 0), i)));
                        cur.push((b, 0));
                    }
                    Err(_) => cur.push((Vec::new(), 0)),
                }
            }
            while let Some(Reverse((_, i))) = heap.pop() {
                let (b, o) = &mut cur[i];
                c.consume(b, *o);
                *o += ROW;
                if *o == b.len() {
                    match receivers[i].recv() {
                        Ok(next) => {
                            *b = next;
                            *o = 0;
                        }
                        Err(_) => continue,
                    }
                }
                heap.push(Reverse((key(b, *o), i)));
            }
        }
    }
    let s = t0.elapsed().as_secs_f64();
    let first = c.first_row.map(|t| (t - t0).as_secs_f64() * 1000.0).unwrap_or(-1.0);
    println!(
        "{{\"mode\":\"{mode}\",\"client\":\"rust\",\"markets\":{},\"rows\":{},\"hash\":{},\"wall_s\":{:.2},\"first_row_ms\":{:.1},\"rows_per_s\":{}}}",
        markets.len(), c.rows, c.hash, s, first, (c.rows as f64 / s) as u64
    );
}
