import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import YahooFinance from 'yahoo-finance2';
import path from 'path';
import { fileURLToPath } from 'url';

// ───────────────────────── 설정 ─────────────────────────
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const chartLibPath = path.resolve(
  __dirname,
  'node_modules/lightweight-charts/dist/lightweight-charts.standalone.production.mjs'
);
const PORT = Number(process.env.PORT) || 3000;
// 외부에 공개하려면 HOST=0.0.0.0 으로 실행 (그 경우 인증/Origin 체크 필요)
// Render
const HOST = '0.0.0.0';
// Local
//const HOST = process.env.HOST || '127.0.0.1';

const HL_REST_URL = 'https://api.hyperliquid.xyz/info';
const HL_WS_URL = 'wss://api.hyperliquid.xyz/ws';
const INTERVALS = new Set(['1m', '5m', '15m']);
const HISTORY_TTL_MS = 45_000;

const yahooFinance = new YahooFinance();
const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 1024 });

// market: KR(한국장) / US(미국 정규장) / FX24(평일 24시간, 주말 휴장) / 24H(상시)
const SYMBOLS = [
  { id: 'KS11',  symbol: '^KS11',     name: '코스피',     market: 'KR',   source: 'yahoo' },
  { id: 'KQ11',  symbol: '^KQ11',     name: '코스닥',     market: 'KR',   source: 'yahoo' },
  { id: 'KS200', symbol: '^KS200',    name: '코스피200',  market: 'KR',   source: 'yahoo' },
  { id: 'KR200', symbol: 'xyz:KR200', name: '코스피200영선', market: '24H', source: 'hl' },
  { id: 'KRW',   symbol: 'KRW=X',     name: '달러/원',    market: 'FX24', source: 'yahoo' },
  { id: 'DXY',   symbol: 'DX-Y.NYB',  name: '달러인덱스', market: 'FX24', source: 'yahoo' },
  { id: 'BZF',   symbol: 'BZ=F',      name: '브렌트유',   market: 'FX24', source: 'yahoo' },
  { id: 'BTC',   symbol: 'BTC',       name: '비트코인',   market: '24H',  source: 'hl' },
  { id: 'VIX',   symbol: '^VIX',      name: 'VIX',        market: 'US',   source: 'yahoo' },
  { id: 'GSPC',  symbol: '^GSPC',     name: 'S&P 500',    market: 'US',   source: 'yahoo' },
  { id: 'NDX',   symbol: '^NDX',      name: '나스닥100',  market: 'US',   source: 'yahoo' },
  { id: 'AAPL',  symbol: 'AAPL',      name: '애플',       market: 'US',   source: 'yahoo' },
  { id: 'NVDA',  symbol: 'NVDA',      name: '엔비디아',   market: 'US',   source: 'yahoo' },
  { id: 'SPCX',  symbol: 'SPCX',      name: 'SPCX',       market: 'US',   source: 'yahoo' },
  { id: 'SCHD',  symbol: 'SCHD',      name: 'SCHD',       market: 'US',   source: 'yahoo' },
  { id: 'JEPI',  symbol: 'JEPI',      name: 'JEPI',       market: 'US',   source: 'yahoo' },
  { id: 'SPYM',  symbol: 'SPYM',      name: 'SPYM',       market: 'US',   source: 'yahoo' },
  { id: 'QQQM',  symbol: 'QQQM',      name: 'QQQM',       market: 'US',   source: 'yahoo' },
];

const HL_ITEMS = SYMBOLS.filter(s => s.source === 'hl');
const YAHOO_ITEMS = SYMBOLS.filter(s => s.source === 'yahoo');
const HL_BY_SYMBOL = new Map(HL_ITEMS.map(s => [s.symbol, s]));

// ───────────────────────── 유틸 ─────────────────────────
const lastLog = new Map();
function logThrottled(key, ...args) {
  const now = Date.now();
  if (now - (lastLog.get(key) ?? 0) < 60_000) return;
  lastLog.set(key, now);
  console.error(...args);
}

process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));

// ───────────────────────── 장 시간 ─────────────────────────
// 휴장일 목록은 2026년분만 들어 있음 → 연말에 갱신 필요.
// 한국 휴장일은 직접 확인 후 사용 권장 (임시공휴일/대체공휴일 변동 가능).
const US_HOLIDAYS = new Set([ // 날짜는 미국 동부(ET) 기준
  '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
  '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
]);
const KR_HOLIDAYS = new Set([ // 날짜는 KST 기준 (주말은 따로 처리)
  '2026-01-01', '2026-02-16', '2026-02-17', '2026-02-18', '2026-03-02',
  '2026-05-01', '2026-05-05', '2026-05-25', '2026-06-03', '2026-08-17',
  '2026-09-24', '2026-09-25', '2026-09-26', '2026-10-05', '2026-10-09',
  '2026-12-25', '2026-12-31',
]);

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
function zonedParts(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone, year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', weekday: 'short', hour12: false,
  }).formatToParts(date);
  const p = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return {
    dateStr: p.year + '-' + p.month + '-' + p.day,
    dow: WEEKDAYS.indexOf(p.weekday),
    mins: (Number(p.hour) % 24) * 60 + Number(p.minute),
  };
}

function isMarketOpen(market, now = new Date()) {
  if (market === 'KR') {
    const z = zonedParts(now, 'Asia/Seoul');
    if (z.dow === 0 || z.dow === 6) return false;
    if (KR_HOLIDAYS.has(z.dateStr)) return false;
    return z.mins >= 530 && z.mins <= 940;            // 08:50 ~ 15:40 (여유 포함)
  }
  if (market === 'US') {
    const z = zonedParts(now, 'America/New_York');    // 서머타임 자동 반영
    if (z.dow === 0 || z.dow === 6) return false;
    if (US_HOLIDAYS.has(z.dateStr)) return false;
    return z.mins >= 570 && z.mins < 960;             // 09:30 ~ 16:00 ET 정규장
  }
  if (market === 'FX24') {                            // 금 17:00 ET ~ 일 17:00 ET 휴장
    const z = zonedParts(now, 'America/New_York');
    if (z.dow === 6) return false;
    if (z.dow === 5 && z.mins >= 17 * 60) return false;
    if (z.dow === 0 && z.mins < 17 * 60) return false;
    return true;
  }
  return true;                                        // 24H
}

// ───────────────────────── 히스토리 캐시 ─────────────────────────
const historyCache = new Map();
function cached(key, ttlMs, loader) {
  const hit = historyCache.get(key);
  if (hit && hit.exp > Date.now()) return hit.promise;
  const promise = loader().then(
    (data) => {
      if (!data.points.length) historyCache.delete(key); // 빈 결과는 캐시하지 않음
      return data;
    },
    (err) => {
      historyCache.delete(key);
      throw err;
    }
  );
  historyCache.set(key, { exp: Date.now() + ttlMs, promise });
  return promise;
}

// HL 캔들 조회: 일시 장애(타임아웃, 429, 빈 응답)에 대비해 3회 재시도하고, 실패 시 상태/본문을 남긴다.
async function fetchHLCandles(item, interval, startTime, endTime) {
  let lastErr;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(HL_REST_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: 'candleSnapshot',
          req: { coin: item.symbol, interval, startTime, endTime },
        }),
        signal: AbortSignal.timeout(8_000),
      });
      if (!response.ok) {
        const body = (await response.text().catch(() => '')).slice(0, 200);
        throw new Error('HL HTTP ' + response.status + ' ' + body);
      }
      const raw = await response.json();
      if (Array.isArray(raw) && raw.length > 0) return raw;
      lastErr = new Error('HL 빈 응답: ' + JSON.stringify(raw).slice(0, 200));
    } catch (e) {
      lastErr = e;
    }
    await new Promise(r => setTimeout(r, 400 * (attempt + 1)));
  }
  throw lastErr;
}

async function loadHistory(item, interval, days) {
  if (item.source === 'hl') {
    const lookbackMs = (days === 1 ? 36 : 60) * 3600 * 1000;
    const arr = await fetchHLCandles(item, interval, Date.now() - lookbackMs, Date.now());
    const points = arr
      .map(c => ({ time: Math.floor(c.t / 1000), value: parseFloat(c.c) }))
      .filter(p => Number.isFinite(p.value))
      .sort((a, b) => a.time - b.time);

    if (points.length === 0) return { points: [], prevClose: 0 };

    const now = new Date();
    const utcMidnightToday = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
    const utcMidnightYesterday = utcMidnightToday - 86400;

    const beforeToday = points.filter(p => p.time < utcMidnightToday);
    const prevClose = beforeToday.length > 0 ? beforeToday[beforeToday.length - 1].value : points[0].value;

    const startCut = days === 1 ? utcMidnightToday : utcMidnightYesterday;
    const displayPoints = points.filter(p => p.time >= startCut);

    return {
      points: displayPoints.length > 0 ? displayPoints : points.slice(-96),
      prevClose,
    };
  }

  // Yahoo: 1분봉은 최근 약 7일까지만 제공 → 6일로 제한
  const lookbackDays = interval === '1m' ? 6 : 8;
  const period1 = new Date(Date.now() - lookbackDays * 24 * 3600 * 1000);
  const result = await yahooFinance.chart(item.symbol, { period1, interval });
  const quotes = Array.isArray(result.quotes) ? result.quotes : [];
  const tz = item.market === 'KR' ? 'Asia/Seoul' : 'America/New_York';

  const valid = quotes
    .filter(q => q.close !== null && q.close !== undefined)
    .map(q => ({
      time: Math.floor(new Date(q.date).getTime() / 1000),
      value: q.close,
      dayStr: new Date(q.date).toLocaleDateString('en-CA', { timeZone: tz }),
    }))
    .sort((a, b) => a.time - b.time);

  if (valid.length === 0) return { points: [], prevClose: 0 };

  const uniqueDays = Array.from(new Set(valid.map(v => v.dayStr)));
  const targetDays = uniqueDays.slice(-days);
  const displayPoints = valid
    .filter(v => targetDays.includes(v.dayStr))
    .map(v => ({ time: v.time, value: v.value }));

  let prevClose = 0;
  if (uniqueDays.length >= 2) {
    const prevDayStr = uniqueDays[uniqueDays.length - 2];
    const prevDayPoints = valid.filter(v => v.dayStr === prevDayStr);
    prevClose = prevDayPoints[prevDayPoints.length - 1].value;
  } else {
    prevClose = result.meta?.chartPreviousClose || displayPoints[0].value;
  }

  return { points: displayPoints, prevClose };
}

// ───────────────────────── HTTP 라우트 ─────────────────────────
app.get('/chart-lib.js', (req, res) => res.sendFile(chartLibPath));

app.get('/api/symbols', (req, res) => {
  const now = new Date();
  res.json(SYMBOLS.map(s => ({ ...s, isOpen: isMarketOpen(s.market, now) })));
});

app.get('/api/history', async (req, res) => {
  const { id, interval } = req.query;
  const days = parseInt(req.query.days, 10) === 1 ? 1 : 2;
  const item = SYMBOLS.find(s => s.id === id);
  if (!item) return res.status(404).json({ error: 'Unknown symbol' });
  if (!INTERVALS.has(interval)) return res.status(400).json({ error: 'Invalid interval' });

  try {
    const data = await cached(id + '|' + interval + '|' + days, HISTORY_TTL_MS, () => loadHistory(item, interval, days));
    return res.json(data);
  } catch (err) {
    logThrottled('history:' + id, '[history] ' + id + ' 실패:', err?.message || err);
    return res.json({ points: [], prevClose: 0, error: true });
  }
});

// ───────────────────────── 클라이언트 페이지 ─────────────────────────
// 주의: 아래 템플릿 문자열 안에서는 백틱, 달러+중괄호, 역슬래시를 쓰지 말 것.
const htmlContent = `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>실시간 시세 대시보드</title>
<style>
* { box-sizing: border-box; }
body { margin: 0; background: #0b0e14; color: #d1d4dc; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }
header { position: sticky; top: 0; z-index: 100; display: flex; justify-content: space-between; align-items: center; padding: 8px 14px; background: #131722; border-bottom: 1px solid #222631; }
.controls { display: flex; gap: 10px; align-items: center; font-size: 12px; }
label { display: flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; }
select { padding: 4px 8px; background: #1e222d; color: #fff; border: 1px solid #323746; border-radius: 4px; font-size: 12px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 8px; padding: 10px; }
.card { background: #131722; border: 1px solid #222631; border-radius: 6px; padding: 8px 10px 6px; display: flex; flex-direction: column; }
.card.closed { opacity: 0.55; }
body.hide-closed .card.closed { display: none; }
.top { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 2px; }
.name { font-size: 13px; font-weight: 600; color: #f0f3fa; }
.ticker { font-size: 10px; color: #787b86; margin-left: 4px; }
.price { font-size: 13px; font-weight: 700; }
.sub { display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; font-size: 11px; }
.badge { font-size: 9px; padding: 1px 4px; border-radius: 3px; background: #1e222d; color: #848e9c; }
.badge.open { color: #26a63a; background: rgba(38,166,154,0.12); }
.chart { width: 100%; height: 115px; }
.up { color: #26a63a; }
.down { color: #ef5350; }
.flat { color: #d1d4dc; }
</style>
</head>
<body>
<header>
  <strong id="headerTitle" style="font-size:13px;">시장 모니터</strong>
  <div class="controls">
    <label><input type="checkbox" id="filterOpen" /> 개장 종목만</label>
    <select id="days">
      <option value="1">1일</option>
      <option value="2" selected>2일</option>
    </select>
    <select id="interval">
      <option value="1m">1분</option>
      <option value="5m">5분</option>
      <option value="15m" selected>15분</option>
    </select>
  </div>
</header>
<div class="grid" id="grid"></div>
<script type="module">
import { createChart, AreaSeries } from '/chart-lib.js?v=5';

const gridEl = document.getElementById('grid');
const daysEl = document.getElementById('days');
const intervalEl = document.getElementById('interval');
const filterOpenEl = document.getElementById('filterOpen');
const headerTitleEl = document.getElementById('headerTitle');
const cards = new Map();

const UP_COLOR = '#26a63a';
const DOWN_COLOR = '#ef5350';
const FLAT_COLOR = '#848e9c';
const CONCURRENCY = 4;

let loadSeq = 0;
let ws = null;
let retryTimer = null;
let retryCount = 0;

// KST 시간 포맷터 (크로스헤어 라벨 + 가로축 눈금 공용)
const kstTime = new Intl.DateTimeFormat('ko-KR', { timeZone: 'Asia/Seoul', hour: '2-digit', minute: '2-digit', hour12: false });
const kstDate = new Intl.DateTimeFormat('ko-KR', { timeZone: 'Asia/Seoul', month: 'numeric', day: 'numeric' });
// TickMarkType: 0 Year, 1 Month, 2 DayOfMonth, 3 Time, 4 TimeWithSeconds
function tickFormatter(time, type) {
  const d = new Date(Number(time) * 1000);
  return type <= 2 ? kstDate.format(d) : kstTime.format(d);
}

filterOpenEl.addEventListener('change', () => {
  document.body.classList.toggle('hide-closed', filterOpenEl.checked);
});

function fmt(v) {
  if (!v) return '-';
  return v >= 1000 ? v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : v.toFixed(v < 10 ? 4 : 2);
}

function updateHeader(ctx, price) {
  ctx.priceEl.textContent = fmt(price);
  if (!ctx.prevClose || !price) return;
  const diff = price - ctx.prevClose;
  const pct = (diff / ctx.prevClose) * 100;
  const sign = diff > 0 ? '+' : '';
  const cls = diff > 0 ? 'up' : (diff < 0 ? 'down' : 'flat');
  const color = diff > 0 ? UP_COLOR : (diff < 0 ? DOWN_COLOR : FLAT_COLOR);
  ctx.priceEl.className = 'price ' + cls;
  ctx.pctEl.className = cls;
  ctx.pctEl.textContent = sign + pct.toFixed(2) + '%';
  ctx.series.applyOptions({ lineColor: color, topColor: color + '33', bottomColor: color + '00' });
}

function applyStatus(ctx, isOpen, market) {
  ctx.card.classList.toggle('closed', !isOpen);
  ctx.badge.classList.toggle('open', isOpen);
  ctx.badge.textContent = isOpen ? market : '마감';
}

async function refreshStatus() {
  try {
    const res = await fetch('/api/symbols');
    const symbols = await res.json();
    for (const s of symbols) {
      const ctx = cards.get(s.id);
      if (ctx) applyStatus(ctx, s.isOpen, s.market);
    }
  } catch (e) {
    console.error('status refresh failed', e);
  }
}

async function init() {
  const res = await fetch('/api/symbols');
  const symbols = await res.json();

  for (const item of symbols) {
    const card = document.createElement('div');
    card.className = 'card' + (item.isOpen ? '' : ' closed');

    const top = document.createElement('div');
    top.className = 'top';

    const titleBox = document.createElement('div');
    const nameSpan = document.createElement('span');
    nameSpan.className = 'name';
    nameSpan.textContent = item.name;
    const tickerSpan = document.createElement('span');
    tickerSpan.className = 'ticker';
    tickerSpan.textContent = item.symbol;
    titleBox.appendChild(nameSpan);
    titleBox.appendChild(tickerSpan);

    const priceSpan = document.createElement('span');
    priceSpan.className = 'price flat';
    priceSpan.textContent = '-';

    top.appendChild(titleBox);
    top.appendChild(priceSpan);

    const sub = document.createElement('div');
    sub.className = 'sub';

    const badge = document.createElement('span');
    badge.className = 'badge' + (item.isOpen ? ' open' : '');
    badge.textContent = item.isOpen ? item.market : '마감';

    const pctSpan = document.createElement('span');
    pctSpan.className = 'flat';
    pctSpan.textContent = '0.00%';

    sub.appendChild(badge);
    sub.appendChild(pctSpan);

    const chEl = document.createElement('div');
    chEl.className = 'chart';

    card.appendChild(top);
    card.appendChild(sub);
    card.appendChild(chEl);
    gridEl.appendChild(card);

    const chart = createChart(chEl, {
      autoSize: true,
      layout: { background: { color: '#131722' }, textColor: '#6b7280', fontSize: 10 },
      grid: { vertLines: { visible: false }, horzLines: { color: '#1b202c' } },
      rightPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.12 } },
      timeScale: {
        visible: true, timeVisible: true, secondsVisible: false, borderColor: '#222631',
        fixLeftEdge: true, fixRightEdge: true,
        tickMarkFormatter: tickFormatter
      },
      crosshair: { mode: 0 },
      handleScroll: false,
      handleScale: false,
      localization: { timeFormatter: (ts) => kstTime.format(new Date(ts * 1000)) }
    });

    const series = chart.addSeries(AreaSeries, { lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });

    cards.set(item.id, {
      item, chart, series, card, badge, prevClose: 0, lastTime: 0, prevLine: null,
      priceEl: priceSpan,
      pctEl: pctSpan
    });
  }

  setInterval(refreshStatus, 60000);   // 개장/마감 배지 주기적 갱신
  await loadAll();
}

async function loadOne(id, ctx, interval, days, seq, attempt = 0) {
  try {
    const res = await fetch('/api/history?id=' + encodeURIComponent(id) + '&interval=' + interval + '&days=' + days);
    const data = await res.json();
    if (seq !== loadSeq) return;                       // 더 새로운 요청이 있으면 버림

    const points = Array.isArray(data.points) ? data.points : [];

    // HL은 24시간 거래라 빈 결과나 에러는 일시 장애일 가능성이 큼 → 백그라운드 재시도
    const needRetry = data.error || (ctx.item.source === 'hl' && points.length === 0);
    if (needRetry && attempt < 3) {
      setTimeout(() => loadOne(id, ctx, interval, days, seq, attempt + 1), 2000 * (attempt + 1));
    }
    ctx.prevClose = data.prevClose || 0;
    if (ctx.prevLine) { ctx.series.removePriceLine(ctx.prevLine); ctx.prevLine = null; }
    if (ctx.prevClose > 0) {
      ctx.prevLine = ctx.series.createPriceLine({ price: ctx.prevClose, color: '#6b7280', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '' });
    }
    ctx.series.setData(points);                        // 빈 배열이면 이전 간격의 잔상도 지움
    ctx.lastTime = 0;
    if (points.length > 0) {
      const last = points[points.length - 1];
      ctx.lastTime = last.time;
      updateHeader(ctx, last.value);
      ctx.chart.timeScale().fitContent();
    }
  } catch (e) {
    console.error('history load failed:', id, e);
  }
}

function closeWs() {
  clearTimeout(retryTimer);
  retryTimer = null;
  if (ws) {
    ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
    try { ws.close(); } catch (e) {}
    ws = null;
  }
}

function connectWs(seq, interval) {
  if (seq !== loadSeq) return;
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const sock = new WebSocket(protocol + '//' + location.host + '/ws');
  ws = sock;

  sock.onopen = () => {
    retryCount = 0;
    sock.send(JSON.stringify({ action: 'subscribeAll', interval }));
  };
  sock.onmessage = (e) => {
    let msg;
    try { msg = JSON.parse(e.data); } catch (err) { return; }
    const ctx = cards.get(msg.id);
    if (!ctx || !msg.point) return;
    if (msg.point.time >= ctx.lastTime) {
      ctx.series.update(msg.point);
      ctx.lastTime = msg.point.time;
      updateHeader(ctx, msg.point.value);
    }
  };
  sock.onerror = () => {};                              // close 이벤트에서 재연결 처리
  sock.onclose = () => {
    if (seq !== loadSeq || ws !== sock) return;
    const delay = Math.min(1000 * Math.pow(2, retryCount++), 15000);
    retryTimer = setTimeout(() => connectWs(seq, interval), delay);
  };
}

async function loadAll() {
  const seq = ++loadSeq;
  const interval = intervalEl.value;
  const days = daysEl.value;
  headerTitleEl.textContent = '시장 모니터 (' + days + '일)';
  closeWs();

  const queue = Array.from(cards.entries());
  let next = 0;
  const worker = async () => {
    while (next < queue.length) {
      if (seq !== loadSeq) return;
      const entry = queue[next++];
      await loadOne(entry[0], entry[1], interval, days, seq);
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  if (seq !== loadSeq) return;
  retryCount = 0;
  connectWs(seq, interval);
}

daysEl.addEventListener('change', loadAll);
intervalEl.addEventListener('change', loadAll);
init().catch((e) => console.error('init failed', e));
</script>
</body>
</html>`;

app.get('/', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.send(htmlContent);
});

// ───────────────────────── 업스트림 피드 (간격별 1세트 공유) ─────────────────────────
// 접속자가 몇 명이든 간격(1m/5m/15m)마다 HL 소켓 1개 + Yahoo 폴러 1개만 유지한다.
const feeds = new Map();

function broadcast(feed, obj) {
  const data = JSON.stringify(obj);
  for (const c of feed.clients) {
    if (c.readyState === WebSocket.OPEN) c.send(data);
  }
}

function connectHL(feed) {
  if (feed.closed) return;
  const hlWs = new WebSocket(HL_WS_URL);
  feed.hlWs = hlWs;
  feed.lastMsg = Date.now();

  hlWs.on('open', () => {
    feed.attempts = 0;
    feed.lastMsg = Date.now();
    for (const item of HL_ITEMS) {
      hlWs.send(JSON.stringify({
        method: 'subscribe',
        subscription: { type: 'candle', coin: item.symbol, interval: feed.interval },
      }));
    }
    clearInterval(feed.pingTimer);
    // keepalive: 30초마다 ping, 90초간 아무 메시지도 없으면 죽은 연결로 보고 재연결
    feed.pingTimer = setInterval(() => {
      if (hlWs.readyState !== WebSocket.OPEN) return;
      if (Date.now() - feed.lastMsg > 90_000) { hlWs.terminate(); return; }
      hlWs.send(JSON.stringify({ method: 'ping' }));
    }, 30_000);
  });

  hlWs.on('message', (data) => {
    feed.lastMsg = Date.now();
    let parsed;
    try { parsed = JSON.parse(data); } catch { return; }
    if (parsed.channel !== 'candle' || !parsed.data) return;

    const c = parsed.data;
    const target = HL_BY_SYMBOL.get(c.s);
    if (!target) {
      // xyz:KR200 같은 HIP-3 심볼이 다른 이름으로 오는지 확인용
      logThrottled('hl-unmatched:' + c.s, '[HL] 매칭되지 않는 캔들 심볼 수신:', c.s);
      return;
    }
    const value = parseFloat(c.c);
    if (!Number.isFinite(value)) return;
    broadcast(feed, { id: target.id, point: { time: Math.floor(c.t / 1000), value } });
  });

  hlWs.on('error', (e) => logThrottled('hl-error', '[HL] ws error:', e.message));

  hlWs.on('close', () => {
    clearInterval(feed.pingTimer);
    if (feed.closed || feed.hlWs !== hlWs) return;
    const delay = Math.min(1000 * 2 ** feed.attempts++, 30_000);
    feed.reconnectTimer = setTimeout(() => connectHL(feed), delay);
  });
}

function startYahooPoller(feed) {
  let idx = 0;
  let busy = false;
  feed.yahooTimer = setInterval(async () => {
    if (busy || feed.closed) return;
    const open = YAHOO_ITEMS.filter(s => isMarketOpen(s.market));   // 매 틱마다 개장 여부 재계산
    if (open.length === 0) return;
    const item = open[idx++ % open.length];

    busy = true;
    try {
      const period1 = new Date(Date.now() - 24 * 3600 * 1000);
      const result = await yahooFinance.chart(item.symbol, { period1, interval: feed.interval });
      const quotes = Array.isArray(result.quotes) ? result.quotes : [];
      const valid = quotes.filter(q => q.close !== null && q.close !== undefined);
      if (valid.length > 0 && !feed.closed) {
        const latest = valid[valid.length - 1];
        broadcast(feed, {
          id: item.id,
          point: { time: Math.floor(new Date(latest.date).getTime() / 1000), value: latest.close },
        });
      }
    } catch (e) {
      logThrottled('yahoo-live:' + item.id, '[Yahoo] ' + item.id + ' 폴링 실패:', e?.message || e);
    } finally {
      busy = false;
    }
  }, 1200);
}

function getFeed(interval) {
  let feed = feeds.get(interval);
  if (feed) return feed;
  feed = {
    interval, clients: new Set(), closed: false,
    hlWs: null, pingTimer: null, reconnectTimer: null, yahooTimer: null, idleTimer: null,
    attempts: 0, lastMsg: Date.now(),
  };
  feeds.set(interval, feed);
  if (HL_ITEMS.length > 0) connectHL(feed);
  startYahooPoller(feed);
  return feed;
}

function stopFeed(feed) {
  feed.closed = true;
  clearInterval(feed.pingTimer);
  clearInterval(feed.yahooTimer);
  clearTimeout(feed.reconnectTimer);
  clearTimeout(feed.idleTimer);
  if (feed.hlWs) { try { feed.hlWs.close(); } catch {} }
  feeds.delete(feed.interval);
}

function attach(clientWs, interval) {
  const feed = getFeed(interval);
  clearTimeout(feed.idleTimer);
  feed.clients.add(clientWs);
  clientWs.feed = feed;
}

function detach(clientWs) {
  const feed = clientWs.feed;
  if (!feed) return;
  feed.clients.delete(clientWs);
  clientWs.feed = null;
  if (feed.clients.size === 0) {
    // 새로고침 직후 재접속하는 경우 HL 소켓을 매번 끊고 새로 열지 않도록 10초 유예
    clearTimeout(feed.idleTimer);
    feed.idleTimer = setTimeout(() => {
      if (feed.clients.size === 0) stopFeed(feed);
    }, 10_000);
  }
}

// ───────────────────────── 클라이언트 WebSocket ─────────────────────────
wss.on('connection', (clientWs) => {
  clientWs.isAlive = true;
  clientWs.feed = null;

  clientWs.on('pong', () => { clientWs.isAlive = true; });
  clientWs.on('error', (e) => logThrottled('client-ws', '[client ws] error:', e.message));

  clientWs.on('message', (rawMsg) => {
    let msg;
    try { msg = JSON.parse(rawMsg); } catch { return; }
    if (!msg || msg.action !== 'subscribeAll' || !INTERVALS.has(msg.interval)) return;
    detach(clientWs);
    attach(clientWs, msg.interval);
  });

  clientWs.on('close', () => detach(clientWs));
});

// 끊긴 클라이언트 정리 (half-open 연결 방지)
const sweepTimer = setInterval(() => {
  for (const c of wss.clients) {
    if (!c.isAlive) { c.terminate(); continue; }
    c.isAlive = false;
    c.ping();
  }
}, 30_000);
wss.on('close', () => clearInterval(sweepTimer));

// ───────────────────────── 시작 / 종료 ─────────────────────────
server.listen(PORT, HOST, () => {
  console.log('시장 모니터 실행 중 -> http://' + (HOST === '0.0.0.0' ? 'localhost' : HOST) + ':' + PORT);
});

function shutdown() {
  for (const feed of [...feeds.values()]) stopFeed(feed);
  clearInterval(sweepTimer);
  wss.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
