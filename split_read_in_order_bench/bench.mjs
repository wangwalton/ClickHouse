// Receive-order read of N markets from table pc, two ways:
//   single: one query `WHERE market_id IN (...) ORDER BY received_at, tie, market_id` (patched server streams it)
//   kway:   one streaming query per market, merged in this process with a min-heap
// Rows are RowBinary, fixed 23 bytes: market_id u32, received_at i64 (us), tie u32, price u16, size u32, side u8.
// Usage: node bench.mjs <single|kway> <markets file> [extra settings]
import http from 'node:http'
import fs from 'node:fs'

const ROW = 23
const COLS = 'market_id, received_at, tie, price, size, side'
const [mode, marketsFile, extra = ''] = process.argv.slice(2)
const markets = fs.readFileSync(marketsFile, 'utf8').trim().split('\n').map(Number)
const agent = new http.Agent({ keepAlive: true, maxSockets: Infinity })

function query(sql, onResponse) {
  const req = http.request({ host: '127.0.0.1', port: 8123, method: 'POST', path: `/?user=bench&max_execution_time=0&log_comment=${encodeURIComponent(process.env.TAG ?? '')}`, agent }, (res) => {
    if (res.statusCode !== 200) {
      let body = ''
      res.on('data', (d) => (body += d))
      res.on('end', () => { throw new Error(`HTTP ${res.statusCode}: ${body.slice(0, 500)}`) })
      return
    }
    onResponse(res)
  })
  req.end(sql + ' FORMAT RowBinary')
}

// Per-row consumer: counts, order-sensitive hash, and order check.
let rows = 0, hash = 0, lastTs = -1, lastTie = -1, lastMarket = -1, firstRowAt = 0
const t0 = performance.now()
function consume(buf, off) {
  const market = buf.readUInt32LE(off)
  const ts = buf.readUInt32LE(off + 4) + buf.readInt32LE(off + 8) * 4294967296
  const tie = buf.readUInt32LE(off + 12)
  if (ts < lastTs || (ts === lastTs && (tie < lastTie || (tie === lastTie && market < lastMarket))))
    throw new Error(`out of order at row ${rows}`)
  lastTs = ts; lastTie = tie; lastMarket = market
  hash = (Math.imul(hash, 31) + tie + market) | 0
  if (rows++ === 0) firstRowAt = performance.now()
}

function report() {
  const s = (performance.now() - t0) / 1000
  console.log(JSON.stringify({ mode, markets: markets.length, rows, hash, wall_s: +s.toFixed(2), first_row_ms: +(firstRowAt - t0).toFixed(1), rows_per_s: Math.round(rows / s) }))
  process.exit(0)
}

// Splits a byte stream into buffers holding whole rows.
function rowChunks(res, onChunk, onEnd) {
  let rest = null
  res.on('data', (d) => {
    const buf = rest ? Buffer.concat([rest, d]) : d
    const whole = buf.length - (buf.length % ROW)
    rest = whole < buf.length ? buf.subarray(whole) : null
    if (whole > 0) onChunk(buf.subarray(0, whole))
  })
  res.on('end', () => {
    if (rest) throw new Error('truncated row')
    onEnd()
  })
}

if (mode === 'single') {
  query(`SELECT ${COLS} FROM pc WHERE market_id IN (${markets.join(',')}) ORDER BY received_at, tie, market_id SETTINGS read_in_order_split_by_key_prefix_in = 1${extra}`, (res) => {
    rowChunks(res, (buf) => { for (let off = 0; off < buf.length; off += ROW) consume(buf, off) }, report)
  })
} else if (mode === 'kway') {
  const HIGH = 1 << 20 // pause a market's response above 1 MiB buffered
  const streams = []
  const heap = [] // streams with a head row, ordered by (ts, tie, market)
  let waiting = 0 // open streams with no buffered row: nothing can be emitted until they get one
  let open = markets.length

  const key = (s) => {
    const b = s.chunks[0], o = s.off
    return [b.readUInt32LE(o + 4) + b.readInt32LE(o + 8) * 4294967296, b.readUInt32LE(o + 12), b.readUInt32LE(o)]
  }
  const less = (a, b) => a.k[0] < b.k[0] || (a.k[0] === b.k[0] && (a.k[1] < b.k[1] || (a.k[1] === b.k[1] && a.k[2] < b.k[2])))
  const push = (s) => {
    s.k = key(s)
    heap.push(s)
    for (let i = heap.length - 1; i > 0;) {
      const p = (i - 1) >> 1
      if (!less(heap[i], heap[p])) break
      ;[heap[i], heap[p]] = [heap[p], heap[i]]; i = p
    }
  }
  const pop = () => {
    const top = heap[0], last = heap.pop()
    if (heap.length > 0) {
      heap[0] = last
      for (let i = 0; ;) {
        const l = 2 * i + 1, r = l + 1
        let m = i
        if (l < heap.length && less(heap[l], heap[m])) m = l
        if (r < heap.length && less(heap[r], heap[m])) m = r
        if (m === i) break
        ;[heap[i], heap[m]] = [heap[m], heap[i]]; i = m
      }
    }
    return top
  }
  const pump = () => {
    while (waiting === 0 && heap.length > 0) {
      const s = pop()
      consume(s.chunks[0], s.off)
      s.off += ROW
      s.buffered -= ROW
      if (s.off === s.chunks[0].length) { s.chunks.shift(); s.off = 0 }
      if (s.paused && s.buffered < HIGH / 2) { s.paused = false; s.res.resume() }
      if (s.chunks.length > 0) push(s)
      else if (!s.ended) { s.waitingForData = true; waiting++ }
    }
    if (open === 0 && heap.length === 0) report()
  }
  for (const m of markets) {
    const s = { chunks: [], off: 0, buffered: 0, ended: false, paused: false, waitingForData: true, res: null }
    waiting++
    streams.push(s)
    query(`SELECT ${COLS} FROM pc WHERE market_id = ${m} ORDER BY received_at, tie${extra ? ' SETTINGS ' + extra.replace(/^, */, '') : ''}`, (res) => {
      s.res = res
      rowChunks(res, (buf) => {
        s.chunks.push(buf)
        s.buffered += buf.length
        if (s.buffered > HIGH && !s.paused) { s.paused = true; res.pause() }
        if (s.waitingForData) { s.waitingForData = false; waiting--; push(s); pump() }
      }, () => {
        s.ended = true
        open--
        if (s.waitingForData) { s.waitingForData = false; waiting--; pump() }
        else if (open === 0) pump()
      })
    })
  }
} else throw new Error('mode must be single or kway')
