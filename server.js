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
// HOST 우선순위: 환경변수 HOST > Render(RENDER 환경변수 자동 설정) 이면 0.0.0.0 > 기본 127.0.0.1(로컬 전용)
// 외부에 공개되는 환경(Render 등)에서는 아래 WebSocket Origin 검사가 적용된다.
const HOST = process.env.HOST || (process.env.RENDER ? '0.0.0.0' : '127.0.0.1');
const MAX_WS_CLIENTS = 200;

const HL_REST_URL = 'https://api.hyperliquid.xyz/info';
const HL_WS_URL = 'wss://api.hyperliquid.xyz/ws';
const NAVER_REALTIME_URL = 'https://polling.finance.naver.com/api/realtime?query=SERVICE_INDEX:';
const NAVER_INDEX_CHART_URL = 'https://api.stock.naver.com/chart/domestic/index/';
const CHART_INTERVAL = '15m';
const INTERVALS = new Set([CHART_INTERVAL]);
const HISTORY_TTL_MS = 45_000;
// Naver index history is returned as minute rows and aggregated into 15-minute bars.

const yahooFinance = new YahooFinance({ suppressNotices: ['yahooSurvey'] });
const app = express();
const server = createServer(app);
const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: 1024,
  // 동일 Origin(같은 host)에서 온 연결만 허용하고, 동시 접속 수를 제한한다.
  // Origin 헤더가 없는 클라이언트(브라우저가 아닌 도구)는 허용한다.
  verifyClient: ({ origin, req }) => {
    if (wss.clients.size >= MAX_WS_CLIENTS) return false;
    if (!origin) return true;
    try {
      return new URL(origin).host === req.headers.host;
    } catch {
      return false;
    }
  },
});

// market: KR(한국장) / US(미국 정규장) / FX24(평일 24시간, 주말 휴장) / 24H(상시)
const SYMBOLS = [
  { id: 'KS11',  symbol: 'KOSPI',     name: '코스피',     market: 'KR',   source: 'naver' },
  { id: 'KQ11',  symbol: 'KOSDAQ',    name: '코스닥',     market: 'KR',   source: 'naver' },
  { id: 'KS200', symbol: 'KPI200',    name: '코스피200',  market: 'KR',   source: 'naver' },
  { id: 'KR200', symbol: 'xyz:KR200', name: 'KR200영구선물', market: '24H', source: 'hl' },
  { id: 'KRW',   symbol: 'KRW=X',     name: '원달러환율',  market: 'FX24', source: 'yahoo' },
  { id: 'DXY',   symbol: 'DX-Y.NYB',  name: 'USD Index',  market: 'FX24', source: 'yahoo' },
  { id: 'BZF',   symbol: 'BZ=F',      name: 'Brent Oil',  market: 'FX24', source: 'yahoo' },
  { id: 'BTC',   symbol: 'BTC',       name: 'Bitcoin',    market: '24H',  source: 'hl' },
  { id: 'VIX',   symbol: '^VIX',      name: 'VIX',        market: 'US',   source: 'yahoo' },
  { id: 'GSPC',  symbol: '^GSPC',     name: 'S&P 500',    market: 'US',   source: 'yahoo' },
  { id: 'NDX',   symbol: '^NDX',      name: 'Nasdaq 100', market: 'US',   source: 'yahoo' },
  { id: 'ANT',   symbol: 'ANT',       name: 'Anthropic',  market: 'US',   source: 'yahoo' },
  { id: 'AAPL',  symbol: 'AAPL',      name: 'Apple',      market: 'US',   source: 'yahoo' },
  { id: 'GOOGL', symbol: 'GOOGL',     name: 'Google',     market: 'US',   source: 'yahoo' },
  { id: 'NVDA',  symbol: 'NVDA',      name: 'NVIDIA',     market: 'US',   source: 'yahoo' },
  { id: 'SPCX',  symbol: 'SPCX',      name: 'SPCX',       market: 'US',   source: 'yahoo' },
  { id: 'SCHD',  symbol: 'SCHD',      name: 'SCHD',       market: 'US',   source: 'yahoo' },
  { id: 'JEPI',  symbol: 'JEPI',      name: 'JEPI',       market: 'US',   source: 'yahoo' },
  { id: 'SPYM',  symbol: 'SPYM',      name: 'SPYM',       market: 'US',   source: 'yahoo' },
  { id: 'QQQM',  symbol: 'QQQM',      name: 'QQQM',       market: 'US',   source: 'yahoo' },
];

const HL_ITEMS = SYMBOLS.filter(s => s.source === 'hl');
const YAHOO_ITEMS = SYMBOLS.filter(s => s.source === 'yahoo');
const NAVER_ITEMS = SYMBOLS.filter(s => s.source === 'naver');
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
const US_EARLY_CLOSE = new Set([ // 13:00 ET 조기 폐장 (추수감사절 다음 날, 크리스마스 이브)
  '2026-11-27', '2026-12-24',
]);
const KR_HOLIDAYS = new Set([ // 날짜는 KST 기준 (주말은 따로 처리)
  '2026-01-01', '2026-02-16', '2026-02-17', '2026-02-18', '2026-03-02',
  '2026-05-01', '2026-05-05', '2026-05-25', '2026-06-03', '2026-08-17',
  '2026-09-24', '2026-09-25', '2026-09-26', '2026-10-05', '2026-10-09',
  '2026-12-25', '2026-12-31',
]);

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
function zonedParts(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-US', {
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
    const closeMins = US_EARLY_CLOSE.has(z.dateStr) ? 13 * 60 : 16 * 60;
    return z.mins >= 570 && z.mins < closeMins;       // 09:30 ~ 16:00 ET 정규장 (조기 폐장일은 13:00)
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

function getIntervalBucketTime(date, interval) {
  const timestamp = new Date(date).getTime();
  const intervalMs = Number.parseInt(interval, 10) * 60 * 1000;
  if (!Number.isFinite(timestamp) || !Number.isFinite(intervalMs) || intervalMs <= 0) return NaN;
  return Math.floor(timestamp / intervalMs) * intervalMs / 1000;
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

async function fetchNaverJson(url) {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0',
      Referer: 'https://stock.naver.com/',
    },
    signal: AbortSignal.timeout(8_000),
  });
  if (!response.ok) {
    const body = (await response.text().catch(() => '')).slice(0, 200);
    throw new Error('Naver HTTP ' + response.status + ' ' + body);
  }
  return response.json();
}

async function fetchNaverIndexHistory(code, startDate, endDate) {
  const url = NAVER_INDEX_CHART_URL + encodeURIComponent(code) + '/minute' +
    '?startDateTime=' + startDate + '0900&endDateTime=' + endDate + '1530';
  const json = await fetchNaverJson(url);
  if (!Array.isArray(json)) {
    throw new Error('Naver history unexpected response');
  }
  return json;
}

function parseNaverPrice(value) {
  if (typeof value === 'number') return value;
  if (typeof value !== 'string') return NaN;
  return Number(value.replaceAll(',', '').trim());
}

// 15분봉 기준 히스토리(표시 구간 + 전일 종가).
async function loadBaseHistory(item, days) {
  const interval = CHART_INTERVAL;
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

    // 시작가: 오늘(UTC) 첫 캔들의 시가. 아직 오늘 캔들이 없으면 전일 종가로 대체
    const todayFirst = arr
      .filter(c => Number(c.t) >= utcMidnightToday * 1000)
      .sort((a, b) => a.t - b.t)[0];
    const openRaw = todayFirst ? parseFloat(todayFirst.o) : NaN;
    const shown = displayPoints.length > 0 ? displayPoints : points.slice(-96);   // 15분봉 24시간치
    return {
      points: shown,
      prevClose,
      open: Number.isFinite(openRaw) && openRaw > 0 ? openRaw : prevClose,
      close: shown[shown.length - 1].value,
    };
  }

  if (item.source === 'naver') {
    const today = zonedParts(new Date(), 'Asia/Seoul').dateStr;
    const [year, month, day] = today.split('-').map(Number);
    const startDate = new Date(Date.UTC(year, month - 1, day - 7))
      .toISOString().slice(0, 10).replaceAll('-', '');
    const endDate = today.replaceAll('-', '');
    const rows = await fetchNaverIndexHistory(item.symbol, startDate, endDate);
    const minutePoints = rows
      .map(row => {
        const stamp = row.localDateTime;
        if (typeof stamp !== 'string' || !/^\d{14}$/.test(stamp)) return null;
        const value = parseNaverPrice(row.currentPrice);
        const day = stamp.slice(0, 8);
        const minuteOfDay = Number(stamp.slice(8, 10)) * 60 + Number(stamp.slice(10, 12));
        const intervalMinutes = Number.parseInt(interval, 10);
        const bucket = Math.floor((minuteOfDay - 540) / intervalMinutes);
        const bucketTime = Math.floor(new Date(
          day.slice(0, 4) + '-' + day.slice(4, 6) + '-' + day.slice(6, 8) + 'T09:00:00+09:00'
        ).getTime() / 1000) + bucket * intervalMinutes * 60;
        return {
          time: bucketTime,
          value,
          open: parseNaverPrice(row.openPrice),
          dayStr: day,
          bucket,
        };
      })
      .filter(p => p && Number.isFinite(p.time) && Number.isFinite(p.value) && p.bucket >= 0)
      .sort((a, b) => a.time - b.time);

    if (minutePoints.length === 0) return { points: [], prevClose: 0 };

    const buckets = new Map();
    for (const point of minutePoints) {
      buckets.set(point.dayStr + ':' + point.bucket, { time: point.time, value: point.value, dayStr: point.dayStr });
    }
    const points = Array.from(buckets.values()).sort((a, b) => a.time - b.time);
    const uniqueDays = Array.from(new Set(points.map(p => p.dayStr)));
    const targetDays = uniqueDays.slice(-days);
    const displayPoints = points
      .filter(p => targetDays.includes(p.dayStr))
      .map(({ time, value }) => ({ time, value }));
    const firstDisplayDay = targetDays[0];
    const previousDayPoints = points.filter(p => p.dayStr < firstDisplayDay);
    const prevClose = previousDayPoints.length > 0
      ? previousDayPoints[previousDayPoints.length - 1].value
      : displayPoints[0].value;

    // 시작가: 마지막 표시일 첫 분봉의 시가(없으면 그 분봉 값). 종가: 마지막 값
    const lastDisplayDay = targetDays[targetDays.length - 1];
    const firstOfDay = minutePoints.find(p => p.dayStr === lastDisplayDay);
    const open = firstOfDay && firstOfDay.open > 0 ? firstOfDay.open : (firstOfDay ? firstOfDay.value : 0);
    return { points: displayPoints, prevClose, open, close: displayPoints[displayPoints.length - 1].value };
  }

  // 최근 거래일과 전일 종가를 포함하도록 Yahoo 15분봉을 6일 조회한다.
  const lookbackDays = 6;
  const period1 = new Date(Date.now() - lookbackDays * 24 * 3600 * 1000);
  const result = await yahooFinance.chart(item.symbol, { period1, interval });
  const quotes = Array.isArray(result.quotes) ? result.quotes : [];
  const tz = item.market === 'KR' ? 'Asia/Seoul' : 'America/New_York';

  const bucketed = quotes
    .filter(q => q.close !== null && q.close !== undefined)
    .map(q => {
      const date = new Date(q.date);
      return {
        time: getIntervalBucketTime(date, interval),
        value: q.close,
        open: q.open,
        dayStr: date.toLocaleDateString('en-CA', { timeZone: tz }),
      };
    })
    .filter(point => Number.isFinite(point.time))
    .sort((a, b) => a.time - b.time);
  const pointsByTime = new Map();
  for (const point of bucketed) pointsByTime.set(point.time, point);
  const valid = Array.from(pointsByTime.values());

  if (valid.length === 0) return { points: [], prevClose: 0 };

  const uniqueDays = Array.from(new Set(valid.map(v => v.dayStr)));
  const targetDays = uniqueDays.slice(-days);
  // FX24: "지금" 기준이 아니라 마지막 데이터 시각 기준으로 N일치를 자른다 (주말에도 빈 차트가 되지 않도록)
  const lastTime = valid[valid.length - 1].time;
  const displayPoints = item.market === 'FX24'
    ? valid
      .filter(v => v.time >= lastTime - days * 24 * 3600)
      .map(v => ({ time: v.time, value: v.value }))
    : valid
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

  // 시작가: 마지막 날 첫 분봉의 시가(없으면 그 값). 종가: 마지막 값. (실제 응답에서는 아래 quote 스냅샷 값이 우선한다)
  const lastDay = uniqueDays[uniqueDays.length - 1];
  const firstOfDay = valid.find(v => v.dayStr === lastDay);
  const open = firstOfDay && firstOfDay.open > 0 ? firstOfDay.open : (firstOfDay ? firstOfDay.value : 0);
  return { points: displayPoints, prevClose, open, close: valid[valid.length - 1].value };
}

// ───────────────────────── Yahoo quote 스냅샷 (기준가 / 장외가격) ─────────────────────────
// quote 한 번에 시작가(regularMarketOpen), 정규장 종가(regularMarketPrice), 장외 가격(pre/postMarketPrice)이 들어 있다.
// 장외 가격은 marketState 가 REGULAR 가 아닐 때만 유효한 것으로 본다. 장외 가격이 없는 종목(지수, 환율 등)은 hasExt=false.
const yahooSnapshot = new Map();   // symbol -> { state, open, close, hasExt, price, time }
let yahooSnapshotAt = 0;
let yahooSnapshotPromise = null;

function positiveNumber(v) { return Number.isFinite(v) && v > 0 ? v : 0; }
function toMillis(t) { return t instanceof Date ? t.getTime() : new Date(t).getTime(); }

function summarizeYahooQuote(q) {
  const state = typeof q.marketState === 'string' ? q.marketState : '';
  const isPre = state === 'PRE' || state === 'PREPRE';
  const extPrice = positiveNumber(isPre ? q.preMarketPrice : q.postMarketPrice);
  const extTime = toMillis(isPre ? q.preMarketTime : q.postMarketTime);
  const hasExt = state !== 'REGULAR' && extPrice > 0;
  return {
    state,
    open: positiveNumber(q.regularMarketOpen),
    close: positiveNumber(q.regularMarketPrice),
    previousClose: positiveNumber(q.regularMarketPreviousClose),
    hasExt,
    price: hasExt ? extPrice : q.regularMarketPrice,       // 헤더에 보여줄 가격 (장외가격 우선)
    time: hasExt && Number.isFinite(extTime) ? extTime : toMillis(q.regularMarketTime),
  };
}

// 여러 호출(15초 폴러, 히스토리 요청)이 겹쳐도 quote 요청은 5초에 한 번만 나간다.
function refreshYahooSnapshot() {
  if (yahooSnapshotPromise) return yahooSnapshotPromise;
  if (Date.now() - yahooSnapshotAt < 5_000) return Promise.resolve();
  yahooSnapshotPromise = yahooFinance.quote(YAHOO_ITEMS.map(item => item.symbol))
    .then((quotes) => {
      for (const q of quotes) yahooSnapshot.set(q.symbol, summarizeYahooQuote(q));
      yahooSnapshotAt = Date.now();
    })
    .finally(() => { yahooSnapshotPromise = null; });
  return yahooSnapshotPromise;
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
    // 15분봉 히스토리는 종목/일수별로 캐시하고 그대로 반환한다.
    const base = await cached(id + '|' + days, HISTORY_TTL_MS, () => loadBaseHistory(item, days));
    // 기준가 재료: 시작가(open) / 정규장 종가(close) / 장외가격 존재 여부(ext). 어떤 값을 쓸지는 클라이언트가 장 상태로 고른다.
    let info = {
      open: base.open || 0,
      close: base.close || 0,
      previousClose: base.prevClose || 0,
      ext: false,
      regular: isMarketOpen(item.market),
    };
    if (item.source === 'yahoo') {
      try { await refreshYahooSnapshot(); } catch (e) { logThrottled('yahoo-snapshot', '[Yahoo] quote 스냅샷 실패:', e?.message || e); }
      const snap = yahooSnapshot.get(item.symbol);
      if (snap) {
        info = {
          open: snap.open || info.open,
          close: snap.close || info.close,
          previousClose: snap.previousClose || info.previousClose,
          ext: snap.hasExt,
          regular: isMarketOpen(item.market),
          price: snap.price,
          time: Math.floor(snap.time / 1000),
        };
      }
    }
    return res.json({ points: base.points, prevClose: base.prevClose, base: info });
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
.timeframe { padding: 4px 8px; background: #1e222d; color: #fff; border: 1px solid #323746; border-radius: 4px; font-size: 12px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 8px; padding: 10px; }
.card { background: #131722; border: 1px solid #222631; border-radius: 6px; padding: 8px 10px 6px; display: flex; flex-direction: column; }
.card.closed { opacity: 0.55; }
body.hide-closed .card.closed { display: none; }
.top { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 2px; }
.name { font-size: 13px; font-weight: 600; color: #f0f3fa; }
.ticker { font-size: 10px; color: #787b86; margin-left: 4px; }
.price-wrap { display: flex; align-items: baseline; gap: 5px; }
.last-time { font-size: 9px; font-weight: 400; color: #787b86; }
.price { font-size: 13px; font-weight: 700; }
.sub { display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; font-size: 11px; }
.badge { font-size: 9px; padding: 1px 4px; border-radius: 3px; background: #1e222d; color: #848e9c; }
.badge.open { color: #26a63a; background: rgba(38,166,154,0.12); }
.chart { position: relative; width: 100%; height: 115px; }
.session-close-marker-layer { position: absolute; inset: 0 54px 22px 0; overflow: hidden; pointer-events: none; z-index: 5; }
.session-close-marker { position: absolute; top: 0; bottom: 0; border-left: 1px dashed #9ca3af; opacity: 0.75; }
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
    <span class="timeframe">15분봉</span>
  </div>
</header>
<div class="grid" id="grid"></div>
<script type="module">
import { createChart, AreaSeries } from '/chart-lib.js?v=5';

const gridEl = document.getElementById('grid');
const daysEl = document.getElementById('days');
const filterOpenEl = document.getElementById('filterOpen');
const headerTitleEl = document.getElementById('headerTitle');
const cards = new Map();

const UP_COLOR = '#26a63a';
const DOWN_COLOR = '#ef5350';
const FLAT_COLOR = '#848e9c';
const CONCURRENCY = 4;
const CHART_INTERVAL = '15m';

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

function getZonedDateParts(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone, year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return {
    date: values.year + '-' + values.month + '-' + values.day,
    minutes: (Number(values.hour) % 24) * 60 + Number(values.minute),
  };
}

function getZonedCloseTime(date, timeZone, closeMinutes) {
  const [year, month, day] = date.split('-').map(Number);
  const target = Date.UTC(year, month - 1, day, Math.floor(closeMinutes / 60), closeMinutes % 60);
  let timestamp = target;
  for (let attempt = 0; attempt < 2; attempt++) {
    const local = getZonedDateParts(new Date(timestamp), timeZone);
    const [localYear, localMonth, localDay] = local.date.split('-').map(Number);
    const represented = Date.UTC(
      localYear, localMonth - 1, localDay,
      Math.floor(local.minutes / 60), local.minutes % 60
    );
    timestamp += target - represented;
  }
  return Math.floor(timestamp / 1000);
}

function getSessionCloseSettings(market) {
  if (market === 'KR') return { timeZone: 'Asia/Seoul', closeMinutes: 15 * 60 + 30 };
  if (market === 'US') return { timeZone: 'America/New_York', closeMinutes: 16 * 60 };
  return null;
}

function positionSessionCloseMarkers(ctx) {
  ctx.markerLayer.replaceChildren();
  if (!ctx.sessionDays.size) return;
  const settings = getSessionCloseSettings(ctx.item.market);
  if (!settings) return;

  const timeScale = ctx.chart.timeScale();
  const range = timeScale.getVisibleRange();
  if (!range) return;
  const from = Number(range.from);
  const to = Number(range.to);
  const intervalSeconds = Number.parseInt(CHART_INTERVAL, 10) * 60;

  for (const date of ctx.sessionDays) {
    const closeTime = getZonedCloseTime(date, settings.timeZone, settings.closeMinutes);
    if (closeTime < from - intervalSeconds || closeTime > to + intervalSeconds) continue;

    let x = timeScale.timeToCoordinate(closeTime);
    if (x === null) {
      const index = timeScale.timeToIndex(closeTime, true);
      if (index === null) continue;
      const nearestX = timeScale.logicalToCoordinate(index);
      if (nearestX === null) continue;
      const nearestTime = timeScale.coordinateToTime(nearestX);
      if (typeof nearestTime !== 'number' || Math.abs(nearestTime - closeTime) > intervalSeconds) continue;
      x = timeScale.logicalToCoordinate(index + (nearestTime < closeTime ? 0.5 : -0.5));
    }
    if (x === null || x < 0 || x > ctx.chartEl.clientWidth) continue;

    const marker = document.createElement('div');
    marker.className = 'session-close-marker';
    marker.style.left = x + 'px';
    ctx.markerLayer.appendChild(marker);
  }
}

filterOpenEl.addEventListener('change', () => {
  document.body.classList.toggle('hide-closed', filterOpenEl.checked);
});

function fmt(v) {
  if (!v) return '-';
  return v >= 1000 ? v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : v.toFixed(v < 10 ? 4 : 2);
}

function updateHeader(ctx, price, priceTime = ctx.lastTime) {
  ctx.currentPrice = price;
  ctx.currentPriceTime = priceTime;
  ctx.timeEl.textContent = priceTime ? kstTime.format(new Date(priceTime * 1000)) : '';
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

function getReferencePrice(ctx) {
  const base = ctx.yahooBase;
  const isRegular = typeof base?.regular === 'boolean' ? base.regular : ctx.isOpen;
  const previousClose = Number.isFinite(base?.previousClose) && base.previousClose > 0
    ? base.previousClose
    : ctx.previousClose;
  if (ctx.item.market === 'US' && !isRegular && base?.ext &&
      Number.isFinite(base.close) && base.close > 0) {
    return base.close;
  }
  return previousClose;
}

function setReferencePrice(ctx, price) {
  const nextPrice = Number.isFinite(price) && price > 0 ? price : 0;
  if (ctx.prevClose === nextPrice && (nextPrice === 0 || ctx.prevLine)) return;
  if (ctx.prevLine) {
    ctx.series.removePriceLine(ctx.prevLine);
    ctx.prevLine = null;
  }
  ctx.prevClose = nextPrice;
  if (nextPrice > 0) {
    ctx.prevLine = ctx.series.createPriceLine({
      price: nextPrice, color: '#6b7280', lineWidth: 1, lineStyle: 2,
      axisLabelVisible: true, title: '',
    });
  }
  if (ctx.currentPrice > 0) updateHeader(ctx, ctx.currentPrice, ctx.currentPriceTime);
}

function applyStatus(ctx, isOpen, market) {
  const changed = ctx.isOpen !== isOpen;
  ctx.isOpen = isOpen;
  ctx.card.classList.toggle('closed', !isOpen);
  ctx.badge.classList.toggle('open', isOpen);
  ctx.badge.textContent = isOpen ? market : '마감';
  if (changed && ctx.yahooBase) {
    ctx.yahooBase.regular = isOpen;
    setReferencePrice(ctx, getReferencePrice(ctx));
  }
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

    const timeSpan = document.createElement('span');
    timeSpan.className = 'last-time';

    const priceWrap = document.createElement('div');
    priceWrap.className = 'price-wrap';
    priceWrap.appendChild(timeSpan);
    priceWrap.appendChild(priceSpan);

    top.appendChild(titleBox);
    top.appendChild(priceWrap);

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
    const markerLayer = document.createElement('div');
    markerLayer.className = 'session-close-marker-layer';
    chEl.appendChild(markerLayer);

    const ctx = {
      item, chart, series, card, badge, prevClose: 0, previousClose: 0,
      currentPrice: 0, currentPriceTime: 0, isOpen: item.isOpen, yahooBase: null,
      lastTime: 0, prevLine: null,
      chartEl: chEl, markerLayer, sessionDays: new Set(), timeEl: timeSpan,
      priceEl: priceSpan,
      pctEl: pctSpan
    };
    cards.set(item.id, ctx);
    const updateMarkers = () => positionSessionCloseMarkers(ctx);
    chart.timeScale().subscribeVisibleTimeRangeChange(updateMarkers);
    chart.timeScale().subscribeSizeChange(updateMarkers);
  }

  setInterval(refreshStatus, 60000);   // 개장/마감 배지 주기적 갱신
  await loadAll();
}

async function loadOne(id, ctx, days, seq, attempt = 0) {
  try {
    const res = await fetch('/api/history?id=' + encodeURIComponent(id) + '&interval=' + CHART_INTERVAL + '&days=' + days);
    const data = await res.json();
    if (seq !== loadSeq) return;                       // 더 새로운 요청이 있으면 버림

    const points = Array.isArray(data.points) ? data.points : [];

    // HL/Naver는 장애 시 빈 결과가 나올 수 있어 짧게 재시도
    const needRetry = data.error || (ctx.item.source === 'hl' && points.length === 0) ||
      (ctx.item.source === 'naver' && points.length === 0);
    if (needRetry && attempt < 3) {
      setTimeout(() => {
        if (seq === loadSeq) loadOne(id, ctx, days, seq, attempt + 1);   // 낡은 요청이면 재시도 안 함
      }, 2000 * (attempt + 1));
    }
    ctx.previousClose = data.prevClose || 0;
    ctx.yahooBase = data.base || null;
    if (Number.isFinite(ctx.yahooBase?.previousClose) && ctx.yahooBase.previousClose > 0) {
      ctx.previousClose = ctx.yahooBase.previousClose;
    }
    if (typeof ctx.yahooBase?.regular === 'boolean') ctx.isOpen = ctx.yahooBase.regular;
    setReferencePrice(ctx, getReferencePrice(ctx));
    const closeSettings = getSessionCloseSettings(ctx.item.market);
    ctx.sessionDays = new Set(closeSettings
      ? points.map(point => getZonedDateParts(new Date(point.time * 1000), closeSettings.timeZone).date)
      : []);
    ctx.series.setData(points);                        // 빈 배열이면 이전 간격의 잔상도 지움
    ctx.lastTime = 0;
    positionSessionCloseMarkers(ctx);
    if (points.length > 0) {
      const last = points[points.length - 1];
      ctx.lastTime = last.time;
      updateHeader(ctx, last.value, last.time);
      ctx.chart.timeScale().fitContent();
      positionSessionCloseMarkers(ctx);
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

function connectWs(seq) {
  if (seq !== loadSeq) return;
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const sock = new WebSocket(protocol + '//' + location.host + '/ws');
  ws = sock;

  sock.onopen = () => {
    retryCount = 0;
    sock.send(JSON.stringify({ action: 'subscribeAll' }));
  };
  sock.onmessage = (e) => {
    let msg;
    try { msg = JSON.parse(e.data); } catch (err) { return; }
    const ctx = cards.get(msg.id);
    if (!ctx) return;
    if (msg.base) {
      ctx.yahooBase = msg.base;
      if (Number.isFinite(msg.base.previousClose) && msg.base.previousClose > 0) {
        ctx.previousClose = msg.base.previousClose;
      }
      if (typeof msg.base.regular === 'boolean') ctx.isOpen = msg.base.regular;
      setReferencePrice(ctx, getReferencePrice(ctx));
    }
    // 한 메시지에 quote(헤더용)와 point(차트용)가 함께 올 수 있으므로 둘 다 처리한다.
    if (msg.quote && Number.isFinite(msg.quote.value) && Number.isFinite(msg.quote.time)) {
      updateHeader(ctx, msg.quote.value, msg.quote.time);
    }
    if (msg.point && Number.isFinite(msg.point.time) && Number.isFinite(msg.point.value)) {
      if (msg.point.time >= ctx.lastTime) {
        ctx.series.update(msg.point);
        ctx.lastTime = msg.point.time;
        const closeSettings = getSessionCloseSettings(ctx.item.market);
        if (closeSettings) {
          ctx.sessionDays.add(getZonedDateParts(new Date(msg.point.time * 1000), closeSettings.timeZone).date);
          positionSessionCloseMarkers(ctx);
        }
      }
    }
  };
  sock.onerror = () => {};                              // close 이벤트에서 재연결 처리
  sock.onclose = () => {
    if (seq !== loadSeq || ws !== sock) return;
    const delay = Math.min(1000 * Math.pow(2, retryCount++), 15000);
    retryTimer = setTimeout(() => connectWs(seq), delay);
  };
}

async function loadAll() {
  const seq = ++loadSeq;
  const days = daysEl.value;
  headerTitleEl.textContent = '시장 모니터 (' + days + '일)';
  closeWs();

  const queue = Array.from(cards.entries());
  let next = 0;
  const worker = async () => {
    while (next < queue.length) {
      if (seq !== loadSeq) return;
      const entry = queue[next++];
      await loadOne(entry[0], entry[1], days, seq);
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  if (seq !== loadSeq) return;
  retryCount = 0;
  connectWs(seq);
}

daysEl.addEventListener('change', loadAll);
init().catch((e) => console.error('init failed', e));
</script>
</body>
</html>`;

app.get('/', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.send(htmlContent);
});

// ───────────────────────── 업스트림 피드 (15분봉 1세트 공유) ─────────────────────────
// 접속자가 몇 명이든 15분봉 HL 소켓과 Yahoo/Naver 폴러를 한 세트만 유지한다.
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
    const candleStart = Number(c.t);
    const candleEnd = Number(c.T);
    if (!Number.isFinite(value) || !Number.isFinite(candleStart)) return;
    broadcast(feed, {
      id: target.id,
      point: { time: Math.floor(candleStart / 1000), value },
    });

    const now = Date.now();
    if (Number.isFinite(candleEnd) && now <= candleEnd) {
      broadcast(feed, {
        id: target.id,
        quote: { time: Math.floor(now / 1000), value },
      });
    }
  });

  hlWs.on('error', (e) => logThrottled('hl-error', '[HL] ws error:', e.message));

  hlWs.on('close', () => {
    clearInterval(feed.pingTimer);
    if (feed.closed || feed.hlWs !== hlWs) return;
    const delay = Math.min(1000 * 2 ** feed.attempts++, 30_000);
    feed.reconnectTimer = setTimeout(() => connectHL(feed), delay);
  });
}

function startYahooChartPoller(feed) {
  let idx = 0;
  let busy = false;
  feed.yahooChartTimer = setInterval(async () => {
    if (busy || feed.closed) return;
    const open = YAHOO_ITEMS.filter(s => isMarketOpen(s.market));   // 매 틱마다 개장 여부 재계산
    if (open.length === 0) return;
    const item = open[idx++ % open.length];

    busy = true;
    try {
      const period1 = new Date(Date.now() - 60 * 60 * 1000);   // 최신 15분봉 확인용
      const result = await yahooFinance.chart(item.symbol, { period1, interval: feed.interval });
      const quotes = Array.isArray(result.quotes) ? result.quotes : [];
      const valid = quotes.filter(q => q.close !== null && q.close !== undefined);
      if (valid.length > 0 && !feed.closed) {
        const latest = valid[valid.length - 1];
        const time = getIntervalBucketTime(latest.date, feed.interval);
        if (Number.isFinite(time)) {
          broadcast(feed, { id: item.id, point: { time, value: latest.close } });
        }
      }
    } catch (e) {
      logThrottled('yahoo-live:' + item.id, '[Yahoo] ' + item.id + ' 폴링 실패:', e?.message || e);
    } finally {
      busy = false;
    }
  }, 10000);
}

function startYahooQuotePoller(feed) {
  let busy = false;
  const poll = async () => {
    if (busy || feed.closed) return;
    busy = true;
    try {
      await refreshYahooSnapshot();
      for (const item of YAHOO_ITEMS) {
        const snap = yahooSnapshot.get(item.symbol);
        if (!snap || !Number.isFinite(snap.price) || !Number.isFinite(snap.time)) {
          logThrottled('yahoo-quote:' + item.id, '[Yahoo] ' + item.id + ' quote 값 없음');
          continue;
        }
        if (!feed.closed) {
          broadcast(feed, {
            id: item.id,
            // 장외가격이 있으면 헤더 가격으로 쓰고, base 는 클라이언트가 기준선/등락률 기준을 고를 때 쓴다.
            quote: { time: Math.floor(snap.time / 1000), value: snap.price },
            base: {
              open: snap.open,
              close: snap.close,
              previousClose: snap.previousClose,
              ext: snap.hasExt,
              regular: isMarketOpen(item.market),
            },
          });
        }
      }
    } catch (e) {
      logThrottled('yahoo-quote', '[Yahoo] quote 폴링 실패:', e?.message || e);
    } finally {
      busy = false;
    }
  };
  feed.yahooQuoteTimer = setInterval(poll, 15_000);
  poll();
}

function startNaverPoller(feed) {
  let busy = false;
  feed.naverTimer = setInterval(async () => {
    if (busy || feed.closed || !isMarketOpen('KR')) return;
    busy = true;
    try {
      // 종목별로 독립 처리: 하나가 실패해도 나머지 업데이트는 그대로 전송한다.
      const results = await Promise.allSettled(NAVER_ITEMS.map(async item => {
        const json = await fetchNaverJson(NAVER_REALTIME_URL + encodeURIComponent(item.symbol));
        if (json.resultCode !== 'success' || !Array.isArray(json.result?.areas)) {
          throw new Error('Naver realtime unexpected response for ' + item.symbol);
        }
        const area = json.result.areas.find(a => a.name === 'SERVICE_INDEX');
        const quote = area?.datas?.find(d => d.cd === item.symbol);
        const value = Number(quote?.nv) / 100;
        const timestamp = Number(json.result.time);
        if (!quote || !Number.isFinite(value) || !Number.isFinite(timestamp)) {
          throw new Error('Naver realtime missing quote for ' + item.symbol);
        }
        const msg = { id: item.id, quote: { time: Math.floor(timestamp / 1000), value } };
        // 차트 갱신용 point: 정규장 구간(09:00~15:30 KST)일 때만 추가 (히스토리와 같은 버킷 규칙)
        const mins = zonedParts(new Date(timestamp), 'Asia/Seoul').mins;
        if (mins >= 540 && mins <= 930) {
          const bucketTime = getIntervalBucketTime(timestamp, feed.interval);
          if (Number.isFinite(bucketTime)) msg.point = { time: bucketTime, value };
        }
        return msg;
      }));
      for (const r of results) {
        if (r.status === 'fulfilled') {
          if (!feed.closed) broadcast(feed, r.value);
        } else {
          logThrottled('naver-live', '[Naver] 실시간 폴링 실패:', r.reason?.message || r.reason);
        }
      }
    } finally {
      busy = false;
    }
  }, 10000);
}

function getFeed(interval) {
  let feed = feeds.get(interval);
  if (feed) return feed;
  feed = {
    interval, clients: new Set(), closed: false,
    hlWs: null, pingTimer: null, reconnectTimer: null,
    yahooChartTimer: null, yahooQuoteTimer: null, naverTimer: null, idleTimer: null,
    attempts: 0, lastMsg: Date.now(),
  };
  feeds.set(interval, feed);
  if (HL_ITEMS.length > 0) connectHL(feed);
  startYahooChartPoller(feed);
  if (YAHOO_ITEMS.length > 0) startYahooQuotePoller(feed);
  if (NAVER_ITEMS.length > 0) startNaverPoller(feed);
  return feed;
}

function stopFeed(feed) {
  feed.closed = true;
  clearInterval(feed.pingTimer);
  clearInterval(feed.yahooChartTimer);
  clearInterval(feed.yahooQuoteTimer);
  clearInterval(feed.naverTimer);
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
    if (!msg || msg.action !== 'subscribeAll') return;
    detach(clientWs);
    attach(clientWs, CHART_INTERVAL);
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