import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import YahooFinance from 'yahoo-finance2';
import path from 'path';

const chartLibPath = path.resolve('node_modules/lightweight-charts/dist/lightweight-charts.standalone.production.mjs');
const yahooFinance = new YahooFinance();
const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const SYMBOLS = [
  { id: 'KS11',  symbol: '^KS11',     name: '코스피',     market: 'KR',  source: 'yahoo' },
  { id: 'KQ11',  symbol: '^KQ11',     name: '코스닥',     market: 'KR',  source: 'yahoo' },
  { id: 'KS200', symbol: '^KS200',    name: '코스피200',  market: 'KR',  source: 'yahoo' },
  { id: 'KR200', symbol: 'xyz:KR200', name: '코스피200',  market: '24H', source: 'hl' },
  { id: 'KRW',   symbol: 'KRW=X',     name: '달러/원',    market: '24H', source: 'yahoo' },
  { id: 'DXY',   symbol: 'DX-Y.NYB',  name: '달러인덱스', market: '24H', source: 'yahoo' },
  { id: 'BZF',   symbol: 'BZ=F',      name: '브렌트유',   market: '24H', source: 'yahoo' },
  { id: 'BTC',   symbol: 'BTC',       name: '비트코인',   market: '24H', source: 'hl' },
  { id: 'VIX',   symbol: '^VIX',      name: 'VIX',        market: 'US',  source: 'yahoo' },
  { id: 'GSPC',  symbol: '^GSPC',     name: 'S&P 500',    market: 'US',  source: 'yahoo' },
  { id: 'NDX',   symbol: '^NDX',      name: '나스닥100',  market: 'US',  source: 'yahoo' },
  { id: 'AAPL',  symbol: 'AAPL',      name: '애플',       market: 'US',  source: 'yahoo' },
  { id: 'NVDA',  symbol: 'NVDA',      name: '엔비디아',   market: 'US',  source: 'yahoo' },
  { id: 'SPCX',  symbol: 'SPCX',      name: 'SPCX',       market: 'US',  source: 'yahoo' },
  { id: 'SCHD',  symbol: 'SCHD',      name: 'SCHD',       market: 'US',  source: 'yahoo' },
  { id: 'JEPI',  symbol: 'JEPI',      name: 'JEPI',       market: 'US',  source: 'yahoo' },
  { id: 'SPYM',  symbol: 'SPYM',      name: 'SPYM',       market: 'US',  source: 'yahoo' },
  { id: 'QQQM',  symbol: 'QQQM',      name: 'QQQM',       market: 'US',  source: 'yahoo' },
];

function isMarketOpen(market) {
  if (market === '24H') return true;
  const now = new Date();
  const kst = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  const day = kst.getDay();
  const mins = kst.getHours() * 60 + kst.getMinutes();

  if (market === 'KR') {
    if (day === 0 || day === 6) return false;
    return mins >= 530 && mins <= 940;
  }
  if (market === 'US') {
    if (day === 0) return false;
    if (day === 1) return mins >= 1320;
    if (day === 6) return mins <= 390;
    return mins >= 1320 || mins <= 390;
  }
  return true;
}

app.get('/chart-lib.js', (req, res) => res.sendFile(chartLibPath));
app.get('/api/symbols', (req, res) => {
  res.json(SYMBOLS.map(s => ({ ...s, isOpen: isMarketOpen(s.market) })));
});

app.get('/api/history', async (req, res) => {
  const { id, interval } = req.query;
  const days = parseInt(req.query.days, 10) === 1 ? 1 : 2;
  const item = SYMBOLS.find(s => s.id === id);
  if (!item) return res.status(404).json({ error: 'Unknown symbol' });

  try {
    if (item.source === 'hl') {
      const lookbackMs = (days === 1 ? 36 : 60) * 3600 * 1000;
      const response = await fetch('https://api.hyperliquid.xyz/info', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: 'candleSnapshot',
          req: { coin: item.symbol, interval, startTime: Date.now() - lookbackMs, endTime: Date.now() }
        })
      });
      const raw = await response.json();
      const arr = Array.isArray(raw) ? raw : [];
      const points = arr
        .map(c => ({ time: Math.floor(c.t / 1000), value: parseFloat(c.c) }))
        .sort((a, b) => a.time - b.time);

      if (points.length === 0) return res.json({ points: [], prevClose: 0 });

      const now = new Date();
      const utcMidnightToday = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
      const utcMidnightYesterday = utcMidnightToday - 86400;

      const beforeToday = points.filter(p => p.time < utcMidnightToday);
      const prevClose = beforeToday.length > 0 ? beforeToday[beforeToday.length - 1].value : points[0].value;

      const startCut = days === 1 ? utcMidnightToday : utcMidnightYesterday;
      const displayPoints = points.filter(p => p.time >= startCut);

      return res.json({
        points: displayPoints.length > 0 ? displayPoints : points.slice(-96),
        prevClose
      });
    } else {
      const period1 = new Date(Date.now() - 8 * 24 * 3600 * 1000);
      const result = await yahooFinance.chart(item.symbol, { period1, interval });
      const quotes = Array.isArray(result.quotes) ? result.quotes : [];
      const tz = item.market === 'KR' ? 'Asia/Seoul' : 'America/New_York';

      const valid = quotes
        .filter(q => q.close !== null && q.close !== undefined)
        .map(q => ({
          time: Math.floor(new Date(q.date).getTime() / 1000),
          value: q.close,
          dayStr: new Date(q.date).toLocaleDateString('en-CA', { timeZone: tz })
        }))
        .sort((a, b) => a.time - b.time);

      if (valid.length === 0) return res.json({ points: [], prevClose: 0 });

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

      return res.json({ points: displayPoints, prevClose });
    }
  } catch (err) {
    return res.json({ points: [], prevClose: 0 });
  }
});

const htmlContent = `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8" />
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
.badge.open { color: #26a69a; background: rgba(38,166,154,0.12); }
.chart { width: 100%; height: 115px; }
.up { color: #26a649; }
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
      <option value="1" selected>1일</option>
      <option value="2">2일</option>
    </select>
    <select id="interval">
      <option value="1m" selected>1분</option>
      <option value="15m">15분</option>
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
let ws = null;

const UP_COLOR = '#26a69a';
const DOWN_COLOR = '#ef5350';
const FLAT_COLOR = '#848e9c';

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
    badge.textContent = item.isOpen ? '개장' : '마감';

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
      layout: { background: { color: '#131722' }, textColor: '#6b7280', fontSize: 10 },
      grid: { vertLines: { visible: false }, horzLines: { color: '#1b202c' } },
      rightPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.12 } },
      timeScale: { visible: true, timeVisible: true, secondsVisible: false, borderColor: '#222631', fixLeftEdge: true, fixRightEdge: true },
      crosshair: { mode: 0 },
      handleScroll: false,
      handleScale: false,
      localization: {
        timeFormatter: (ts) => new Date(ts * 1000).toLocaleTimeString('ko-KR', { timeZone: 'Asia/Seoul', hour: '2-digit', minute: '2-digit', hour12: false })
      }
    });

    const series = chart.addSeries(AreaSeries, { lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
    new ResizeObserver(() => chart.applyOptions({ width: chEl.clientWidth, height: chEl.clientHeight })).observe(chEl);

    cards.set(item.id, {
      item, chart, series, prevClose: 0, lastTime: 0, prevLine: null,
      priceEl: priceSpan,
      pctEl: pctSpan
    });
  }

  await loadAll();
}

async function loadAll() {
  const interval = intervalEl.value;
  const days = daysEl.value;
  headerTitleEl.textContent = '시장 모니터 (' + days + '일)';
  if (ws) ws.close();

  for (const [id, ctx] of cards) {
    try {
      const res = await fetch('/api/history?id=' + id + '&interval=' + interval + '&days=' + days);
      const { points, prevClose } = await res.json();
      ctx.prevClose = prevClose;
      if (ctx.prevLine) { ctx.series.removePriceLine(ctx.prevLine); ctx.prevLine = null; }
      if (prevClose > 0) {
        ctx.prevLine = ctx.series.createPriceLine({ price: prevClose, color: '#6b7280', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '' });
      }
      if (points && points.length > 0) {
        ctx.series.setData(points);
        const last = points[points.length - 1];
        ctx.lastTime = last.time;
        updateHeader(ctx, last.value);
        ctx.chart.timeScale().fitContent();
      }
    } catch (e) {}
    await new Promise(r => setTimeout(r, 120));
  }

  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(protocol + '//' + location.host + '/ws');
  ws.onopen = () => ws.send(JSON.stringify({ action: 'subscribeAll', interval }));
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    const ctx = cards.get(msg.id);
    if (!ctx || !msg.point) return;
    if (msg.point.time >= ctx.lastTime) {
      ctx.series.update(msg.point);
      ctx.lastTime = msg.point.time;
      updateHeader(ctx, msg.point.value);
    }
  };
}

daysEl.addEventListener('change', loadAll);
intervalEl.addEventListener('change', loadAll);
init();
</script>
</body>
</html>`;

app.get('/', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.send(htmlContent);
});

wss.on('connection', (clientWs) => {
  let cleanups = [];
  const clearAll = () => { cleanups.forEach(fn => fn()); cleanups = []; };

  clientWs.on('message', (rawMsg) => {
    const { action, interval } = JSON.parse(rawMsg);
    if (action !== 'subscribeAll') return;
    clearAll();

    const hlItems = SYMBOLS.filter(s => s.source === 'hl');
    if (hlItems.length > 0) {
      const hlWs = new WebSocket('wss://api.hyperliquid.xyz/ws');
      hlWs.on('open', () => {
        hlItems.forEach(item => {
          hlWs.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'candle', coin: item.symbol, interval } }));
        });
      });
      hlWs.on('message', (data) => {
        const parsed = JSON.parse(data);
        if (parsed.channel === 'candle' && parsed.data && clientWs.readyState === WebSocket.OPEN) {
          const c = parsed.data;
          const target = hlItems.find(item => item.symbol === c.s);
          if (target) {
            clientWs.send(JSON.stringify({
              id: target.id,
              point: { time: Math.floor(c.t / 1000), value: parseFloat(c.c) }
            }));
          }
        }
      });
      cleanups.push(() => hlWs.close());
    }

    const yahooOpenItems = SYMBOLS.filter(s => s.source === 'yahoo' && isMarketOpen(s.market));
    let idx = 0;

    const pollNext = async () => {
      if (clientWs.readyState !== WebSocket.OPEN || yahooOpenItems.length === 0) return;
      const item = yahooOpenItems[idx % yahooOpenItems.length];
      idx++;
      try {
        const period1 = new Date(Date.now() - 24 * 3600 * 1000);
        const result = await yahooFinance.chart(item.symbol, { period1, interval });
        const quotes = Array.isArray(result.quotes) ? result.quotes : [];
        const valid = quotes.filter(q => q.close !== null && q.close !== undefined);
        if (valid.length > 0 && clientWs.readyState === WebSocket.OPEN) {
          const latest = valid[valid.length - 1];
          clientWs.send(JSON.stringify({
            id: item.id,
            point: { time: Math.floor(new Date(latest.date).getTime() / 1000), value: latest.close }
          }));
        }
      } catch (e) {}
    };

    const timer = setInterval(pollNext, 1200);
    cleanups.push(() => clearInterval(timer));
  });

  clientWs.on('close', clearAll);
});

//server.listen(3000, () => {
//  console.log('시장 모니터 실행 중 -> http://localhost:3000');
//});
// [Render 배포용 코드로 교체]
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log('시장 모니터 실행 중 -> 포트:', PORT);
});
