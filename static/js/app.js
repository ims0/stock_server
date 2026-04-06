const form = document.getElementById('query-form');
const chartContainer = document.getElementById('chart');
const messageEl = document.getElementById('message');
const codeSelect = document.getElementById('code-select');
const codeInput = document.getElementById('code-input');
const sourceStatus = document.getElementById('source-status');
const checkSourcesButton = document.getElementById('check-sources');
const maControls = document.getElementById('ma-controls');
const toggleMa5 = document.getElementById('toggle-ma5');
const toggleMa10 = document.getElementById('toggle-ma10');
const tradeControls = document.getElementById('trade-controls');
const buyLineInput = document.getElementById('buy-line-input');
const sellLineInput = document.getElementById('sell-line-input');
const tradeStatsEl = document.getElementById('trade-stats');
const gridStepInput = document.getElementById('grid-step-input');
const optimizeBtn = document.getElementById('optimize-btn');
const rangeType = document.getElementById('range-type');
const datePresetGroup = document.getElementById('date-preset-group');
const startDateGroup = document.getElementById('start-date-group');
const endDateGroup = document.getElementById('end-date-group');
const datePreset = document.getElementById('date-preset');
const startDateInput = document.getElementById('start_date');
const endDateInput = document.getElementById('end_date');

function applyDatePreset(months) {
  if (!months) return;
  const end = new Date();
  const start = new Date();
  start.setMonth(start.getMonth() - months);
  const fmt = (d) => d.toISOString().slice(0, 10);
  startDateInput.value = fmt(start);
  endDateInput.value = fmt(end);
}

function updateRangeMode(mode) {
  const isRecent = mode !== 'custom';
  datePresetGroup.classList.toggle('is-hidden', !isRecent);
  startDateGroup.classList.toggle('is-hidden', isRecent);
  endDateGroup.classList.toggle('is-hidden', isRecent);
  datePreset.disabled = !isRecent;
  startDateInput.disabled = isRecent;
  endDateInput.disabled = isRecent;

  if (isRecent) {
    const months = parseInt(datePreset.value, 10);
    if (months) applyDatePreset(months);
  }
}

// Trade simulation state
let _tradeData = null;
let _buyPrice = null;
let _sellPrice = null;
let _suppressRelayout = false;
let _currentMarket = 'a';
let _feeConfig = null;

async function loadFeeConfig() {
  try {
    const resp = await fetch('/api/fees');
    if (redirectToLoginIfUnauthorized(resp)) {
      return;
    }
    if (resp.ok) {
      _feeConfig = await resp.json();
    }
  } catch (_) {
    _feeConfig = null;
  }
}

function getActiveFees() {
  return (_feeConfig && _currentMarket && _feeConfig[_currentMarket]) || null;
}

function calcTotalFeeRate(feeItems) {
  if (!feeItems || feeItems.length === 0) return 0;
  return feeItems.reduce((s, f) => s + (f.rate || 0), 0);
}

function showMessage(text, isError = false) {
  messageEl.textContent = text;
  messageEl.classList.toggle('error', isError);
}

function redirectToLoginIfUnauthorized(response) {
  if (response && response.status === 401) {
    window.location.href = '/login';
    return true;
  }
  return false;
}

function movingAverage(values, windowSize) {
  const result = [];
  const validValues = [];
  for (let i = 0; i < values.length; i += 1) {
    const v = values[i];
    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      validValues.push(v);
    }
    if (validValues.length < windowSize) {
      result.push(null);
    } else {
      const window = validValues.slice(-windowSize);
      const sum = window.reduce((acc, val) => acc + val, 0);
      result.push(sum / windowSize);
    }
  }
  return result;
}

const feeInfoEl = document.getElementById('fee-info');
const strategyTableEl = document.getElementById('strategy-table');

function renderFeeInfo() {
  if (!feeInfoEl) return;
  const fees = getActiveFees();
  if (!fees) { feeInfoEl.innerHTML = ''; return; }

  const bfr = calcTotalFeeRate(fees.buy);
  const sfr = calcTotalFeeRate(fees.sell);
  const mktLabel = _currentMarket === 'a' ? 'A股' : '港股通';

  const buildRows = (items) => items.map((f) => {
    const limits = [
      f.min_amount != null ? `最低 ${f.min_amount}` : '',
      f.max_amount != null ? `最高 ${f.max_amount}` : '',
    ].filter(Boolean).join('，');
    return `<li><span class="fee-item-name">${f.name}</span>`
      + `<span class="fee-item-rate">${(f.rate * 100).toFixed(4)}%</span>`
      + (limits ? `<span class="fee-item-note">${limits}</span>` : '')
      + `</li>`;
  }).join('');

  feeInfoEl.innerHTML = `
    <details class="fee-details">
      <summary class="fee-summary">
        ${mktLabel} 交易成本：买入共 <strong>${(bfr * 100).toFixed(3)}%</strong>，卖出共 <strong>${(sfr * 100).toFixed(3)}%</strong>（单边）&ensp;▾明细
      </summary>
      <div class="fee-detail-body">
        <div class="fee-col">
          <div class="fee-col-title">买入费用</div>
          <ul class="fee-list">${buildRows(fees.buy)}</ul>
        </div>
        <div class="fee-col">
          <div class="fee-col-title">卖出费用</div>
          <ul class="fee-list">${buildRows(fees.sell)}</ul>
        </div>
      </div>
    </details>`;
}

function buildCandlestick(data, title, market) {
  _currentMarket = market || 'a';
  _tradeData = data;
  if (strategyTableEl) strategyTableEl.innerHTML = '';

  // Initialize trade lines at median ±5%
  const validCloses = data.filter((r) => r.close != null).map((r) => r.close);
  const sorted = [...validCloses].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)] || 0;
  _buyPrice = Math.round(median * 0.95 * 10) / 10;
  _sellPrice = Math.round(median * 1.05 * 10) / 10;

  const x = data.map((row) => row.date);
  const open = data.map((row) => row.open);
  const high = data.map((row) => row.high);
  const low = data.map((row) => row.low);
  const close = data.map((row) => row.close);
  const ma5 = movingAverage(close, 5);
  const ma10 = movingAverage(close, 10);
  const signals = calcTradeSignals(data, _buyPrice, _sellPrice);

  const trace = {
    x,
    open,
    high,
    low,
    close,
    type: 'candlestick',
    whiskerwidth: 0,
    increasing: { line: { color: '#ef4444', width: 0.8 }, fillcolor: 'rgba(255,255,255,0)' },
    decreasing: { line: { color: '#22c55e', width: 0.8 }, fillcolor: 'rgba(255,255,255,0)' },
    name: 'K线',
  };

  const ma5Trace = {
    x,
    y: ma5,
    type: 'scatter',
    mode: 'lines',
    name: 'MA5',
    visible: toggleMa5.checked,
    line: { width: 1.5, color: '#f59e0b' },
  };

  const ma10Trace = {
    x,
    y: ma10,
    type: 'scatter',
    mode: 'lines',
    name: 'MA10',
    visible: toggleMa10.checked,
    line: { width: 1.5, color: '#3b82f6' },
  };

  // trace[3] = buy markers, trace[4] = sell markers
  const buyMarkerTrace = {
    x: signals.buyDates,
    y: signals.buyPrices,
    type: 'scatter',
    mode: 'markers',
    name: '买入',
    marker: { symbol: 'star', size: 14, color: '#16a34a' },
    hovertemplate: '买入 %{x}<br>价格: %{y:.2f}<extra></extra>',
  };

  const sellMarkerTrace = {
    x: signals.sellDates,
    y: signals.sellPrices,
    type: 'scatter',
    mode: 'markers',
    name: '卖出',
    marker: { symbol: 'star', size: 14, color: '#dc2626' },
    hovertemplate: '卖出 %{x}<br>价格: %{y:.2f}<extra></extra>',
  };

  const layout = {
    title,
    margin: { t: 50, r: 20, b: 60, l: 50 },
    xaxis: {
      title: '日期',
      rangeslider: { visible: true },
      type: 'date',
    },
    yaxis: {
      title: '价格',
      fixedrange: false,
    },
    bargap: 0.6,
    shapes: [
      {
        type: 'line',
        x0: x[0], x1: x[x.length - 1],
        y0: _buyPrice, y1: _buyPrice,
        xref: 'x', yref: 'y',
        line: { color: '#16a34a', width: 2, dash: 'dash' },
      },
      {
        type: 'line',
        x0: x[0], x1: x[x.length - 1],
        y0: _sellPrice, y1: _sellPrice,
        xref: 'x', yref: 'y',
        line: { color: '#dc2626', width: 2, dash: 'dash' },
      },
    ],
  };

  Plotly.newPlot(
    chartContainer,
    [trace, ma5Trace, ma10Trace, buyMarkerTrace, sellMarkerTrace],
    layout,
    { responsive: true, edits: { shapePosition: true } },
  );

  maControls.classList.add('visible');
  tradeControls.classList.add('visible');
  buyLineInput.value = _buyPrice.toFixed(1);
  sellLineInput.value = _sellPrice.toFixed(1);
  renderTradeStats(signals);
  renderFeeInfo();

  // Re-register each time the chart is rebuilt
  chartContainer.removeAllListeners('plotly_relayout');
  chartContainer.on('plotly_relayout', onTradeRelayout);
}

function calcTradeSignals(data, buyPrice, sellPrice) {
  const buyDates = [];
  const buyPrices = [];
  const sellDates = [];
  const sellPrices = [];
  let holding = false;

  for (const row of data) {
    if (!row.is_open || row.low == null || row.high == null) continue;
    // Buy: triggers when the day's low <= buyPrice (price dipped to or below limit)
    // Execution at min(buyPrice, high): if whole candle is below buy line → buy at day-high
    if (!holding && row.low <= buyPrice) {
      const execPrice = Math.min(buyPrice, row.high);
      buyDates.push(row.date);
      buyPrices.push(execPrice);
      holding = true;
    // Sell: triggers when the day's high >= sellPrice (price rose to or above limit)
    // Execution at max(sellPrice, low): if whole candle is above sell line → sell at day-low
    } else if (holding && row.high >= sellPrice) {
      const execPrice = Math.max(sellPrice, row.low);
      sellDates.push(row.date);
      sellPrices.push(execPrice);
      holding = false;
    }
  }
  return { buyDates, buyPrices, sellDates, sellPrices };
}

function findOptimalLines(data) {
  // Collect candidate price levels from all trading-day lows and highs
  const tradingRows = data.filter((r) => r.is_open && r.low != null && r.high != null);
  if (tradingRows.length === 0) return null;

  const rawLevels = [];
  for (const r of tradingRows) {
    rawLevels.push(r.low, r.high);
  }
  // Deduplicate and sort
  const levels = [...new Set(rawLevels.map((v) => Math.round(v * 10) / 10))].sort((a, b) => a - b);

  // Down-sample to at most 120 evenly-spaced candidates to keep O(N²) manageable
  const MAX = 120;
  let candidates = levels;
  if (levels.length > MAX) {
    const step = (levels.length - 1) / (MAX - 1);
    candidates = Array.from({ length: MAX }, (_, i) => levels[Math.round(i * step)]);
  }

  let bestBuy = null;
  let bestSell = null;
  let bestMultiplier = -Infinity;

  // Use fee-adjusted multiplier so the optimal pair accounts for real costs
  const fees = getActiveFees();
  const bfr = fees ? calcTotalFeeRate(fees.buy) : 0;
  const sfr = fees ? calcTotalFeeRate(fees.sell) : 0;

  for (let bi = 0; bi < candidates.length; bi += 1) {
    const bp = candidates[bi];
    for (let si = bi + 1; si < candidates.length; si += 1) {
      const sp = candidates[si];
      const signals = calcTradeSignals(data, bp, sp);
      const sells = signals.sellPrices.length;
      if (sells === 0) continue;
      let fund = 1;
      for (let k = 0; k < sells; k += 1) {
        fund *= (signals.sellPrices[k] * (1 - sfr)) / (signals.buyPrices[k] * (1 + bfr));
      }
      if (fund > bestMultiplier) {
        bestMultiplier = fund;
        bestBuy = bp;
        bestSell = sp;
      }
    }
  }

  return bestBuy !== null ? { buyPrice: bestBuy, sellPrice: bestSell, multiplier: bestMultiplier } : null;
}

function calcPairStats(data, buyPrice, sellPrice) {
  const signals = calcTradeSignals(data, buyPrice, sellPrice);
  const fees = getActiveFees();
  const bfr = fees ? calcTotalFeeRate(fees.buy) : 0;
  const sfr = fees ? calcTotalFeeRate(fees.sell) : 0;
  const sells = signals.sellPrices.length;
  let fund = 1.0;
  for (let k = 0; k < sells; k += 1) {
    fund *= (signals.sellPrices[k] * (1 - sfr)) / (signals.buyPrices[k] * (1 + bfr));
  }
  return { buyCount: signals.buyDates.length, sellCount: sells, mult: fund };
}

function findOptimalStrategy2(data) {
  const tradingRows = data.filter((r) => r.is_open && r.low != null && r.high != null);
  if (tradingRows.length === 0) return null;

  const rawLevels = [];
  for (const r of tradingRows) rawLevels.push(r.low, r.high);
  const allLevels = [...new Set(rawLevels.map((v) => Math.round(v * 10) / 10))].sort((a, b) => a - b);

  const MAX = 40;
  let candidates = allLevels;
  if (allLevels.length > MAX) {
    const step = (allLevels.length - 1) / (MAX - 1);
    candidates = Array.from({ length: MAX }, (_, i) => allLevels[Math.round(i * step)]);
  }

  const fees = getActiveFees();
  const bfr = fees ? calcTotalFeeRate(fees.buy) : 0;
  const sfr = fees ? calcTotalFeeRate(fees.sell) : 0;
  const n = candidates.length;

  // 预计算所有 (buy, sell) 对的资金倍数
  const pm = [];
  for (let i = 0; i < n; i += 1) pm.push(new Float64Array(n).fill(1.0));
  for (let bi = 0; bi < n; bi += 1) {
    for (let si = bi + 1; si < n; si += 1) {
      const signals = calcTradeSignals(data, candidates[bi], candidates[si]);
      const sells = signals.sellPrices.length;
      if (sells === 0) continue;
      let fund = 1.0;
      for (let k = 0; k < sells; k += 1) {
        fund *= (signals.sellPrices[k] * (1 - sfr)) / (signals.buyPrices[k] * (1 + bfr));
      }
      pm[bi][si] = fund;
    }
  }

  let bestTotal = -Infinity;
  let bestPairs = null;

  for (let b1i = 0; b1i < n; b1i += 1) {
    for (let b2i = b1i + 1; b2i < n; b2i += 1) {
      if (candidates[b2i] - candidates[b1i] < 3) continue;
      for (let s1i = 0; s1i < n; s1i += 1) {
        if (candidates[s1i] <= candidates[b2i]) continue; // 卖出必须高于所有买入线
        for (let s2i = s1i + 1; s2i < n; s2i += 1) {
          if (candidates[s2i] - candidates[s1i] < 3) continue;
          // 方案A：组1=(b1,s1) 组2=(b2,s2)
          const tA = 0.5 * pm[b1i][s1i] + 0.5 * pm[b2i][s2i];
          // 方案B：组1=(b1,s2) 组2=(b2,s1)
          const tB = 0.5 * pm[b1i][s2i] + 0.5 * pm[b2i][s1i];
          if (tA > bestTotal) {
            bestTotal = tA;
            bestPairs = [
              { buy: candidates[b1i], sell: candidates[s1i], mult: pm[b1i][s1i] },
              { buy: candidates[b2i], sell: candidates[s2i], mult: pm[b2i][s2i] },
            ];
          }
          if (tB > bestTotal) {
            bestTotal = tB;
            bestPairs = [
              { buy: candidates[b1i], sell: candidates[s2i], mult: pm[b1i][s2i] },
              { buy: candidates[b2i], sell: candidates[s1i], mult: pm[b2i][s1i] },
            ];
          }
        }
      }
    }
  }

  if (!bestPairs) return null;
  return {
    total: bestTotal,
    pairs: bestPairs.map((p) => ({ ...p, ...calcPairStats(data, p.buy, p.sell) })),
  };
}

function calcGridStrategy3(data, stepSize) {
  const tradingRows = data.filter((r) => r.is_open && r.low != null && r.high != null && r.close != null);
  if (tradingRows.length < 2) return null;

  const minLow = Math.min(...tradingRows.map((r) => r.low));
  const maxHigh = Math.max(...tradingRows.map((r) => r.high));
  const amplitude = maxHigh - minLow;
  if (amplitude <= 0 || stepSize <= 0) return null;

  const N = Math.max(1, Math.floor(amplitude / stepSize));
  const step = amplitude / N;

  // Grid levels: L[0]=minLow, L[1]=minLow+step, ..., L[N]=maxHigh
  const levels = Array.from({ length: N + 1 }, (_, i) => minLow + i * step);

  const fees = getActiveFees();
  const bfr = fees ? calcTotalFeeRate(fees.buy) : 0;
  const sfr = fees ? calcTotalFeeRate(fees.sell) : 0;

  const perSlotCapital = 1.0 / N;
  let cash = 1.0;
  let shares = 0;
  // slotShares[i]: shares currently held for slot i (0 = empty)
  const slotShares = new Float64Array(N);

  const buyDates = [];
  const buyPrices = [];
  const sellDates = [];
  const sellPrices = [];

  for (const row of tradingRows) {
    // ── Buy triggers (price falling to grid level) ─────────────────
    for (let i = 0; i < N; i += 1) {
      const buyLevel = levels[i];
      if (row.low > buyLevel) continue; // level not reached
      if (slotShares[i] > 0) continue;  // slot already filled

      // At the bottom level (i===0), use all remaining cash; otherwise use 1/N
      const spend = (i === 0 && cash > perSlotCapital) ? cash : Math.min(perSlotCapital, cash);
      if (spend < 1e-10) continue;

      // Execute price: if whole candle is below the level, we still buy at day-high
      const execPrice = Math.min(buyLevel, row.high);
      const acquired = spend / (execPrice * (1 + bfr));
      slotShares[i] = acquired;
      cash -= spend;
      shares += acquired;
      buyDates.push(row.date);
      buyPrices.push(execPrice);
    }

    // ── Sell triggers (price rising to next grid level) ────────────
    // If price hits the absolute maximum, sell everything at once
    if (row.high >= levels[N] && shares > 0) {
      const execPrice = Math.max(levels[N], row.low);
      cash += shares * execPrice * (1 - sfr);
      for (let i = 0; i < N; i += 1) slotShares[i] = 0;
      shares = 0;
      sellDates.push(row.date);
      sellPrices.push(execPrice);
    } else {
      for (let i = 0; i < N; i += 1) {
        const sellLevel = levels[i + 1];
        if (row.high < sellLevel) continue; // level not reached
        if (slotShares[i] <= 0) continue;   // nothing to sell in this slot

        const execPrice = Math.max(sellLevel, row.low);
        cash += slotShares[i] * execPrice * (1 - sfr);
        shares -= slotShares[i];
        slotShares[i] = 0;
        sellDates.push(row.date);
        sellPrices.push(execPrice);
      }
    }
  }

  // Liquidate any remaining position at the last close price
  if (shares > 1e-10) {
    const lastClose = tradingRows[tradingRows.length - 1].close;
    cash += shares * lastClose * (1 - sfr);
    shares = 0;
  }

  return {
    N,
    step,
    amplitude,
    minPrice: minLow,
    maxPrice: maxHigh,
    finalFund: cash,
    buyCount: buyDates.length,
    sellCount: sellDates.length,
    buyDates,
    buyPrices,
    sellDates,
    sellPrices,
  };
}

function calcGridStrategy4(data, stepSize) {
  const tradingRows = data.filter((r) => r.is_open && r.low != null && r.high != null && r.close != null);
  if (tradingRows.length < 2) return null;

  const minLow = Math.min(...tradingRows.map((r) => r.low));
  const maxHigh = Math.max(...tradingRows.map((r) => r.high));
  const amplitude = maxHigh - minLow;
  if (amplitude <= 0 || stepSize <= 0) return null;

  const halfAmp = amplitude / 2;
  const midline = (minLow + maxHigh) / 2;
  const N = Math.max(1, Math.floor(halfAmp / stepSize));
  const step = halfAmp / N;

  // buyLevels[k]  = midline - (k+1)*step  (k=0..N-1), buyLevels[N-1] ≈ minLow
  // sellLevels[k] = midline + (k+1)*step  (k=0..N-1), sellLevels[N-1] ≈ maxHigh
  // Slot k: buy at buyLevels[k], sell at sellLevels[k] (symmetric around midline)
  const buyLevels  = Array.from({ length: N }, (_, k) => midline - (k + 1) * step);
  const sellLevels = Array.from({ length: N }, (_, k) => midline + (k + 1) * step);

  const fees = getActiveFees();
  const bfr = fees ? calcTotalFeeRate(fees.buy) : 0;
  const sfr = fees ? calcTotalFeeRate(fees.sell) : 0;

  const perSlotCapital = 1.0 / N;
  let cash = 1.0;
  let shares = 0;
  const slotShares = new Float64Array(N);

  const buyDates = [];
  const buyPrices = [];
  const sellDates = [];
  const sellPrices = [];

  for (const row of tradingRows) {
    // ── Buy: price falls to each buy level below midline ───────────
    for (let k = 0; k < N; k += 1) {
      if (row.low > buyLevels[k]) continue;
      if (slotShares[k] > 0) continue;

      // At the deepest level (k===N-1), use all remaining cash
      const spend = (k === N - 1 && cash > perSlotCapital) ? cash : Math.min(perSlotCapital, cash);
      if (spend < 1e-10) continue;

      const execPrice = Math.min(buyLevels[k], row.high);
      slotShares[k] = spend / (execPrice * (1 + bfr));
      cash -= spend;
      shares += slotShares[k];
      buyDates.push(row.date);
      buyPrices.push(execPrice);
    }

    // ── Sell: price rises to each symmetric sell level above midline
    for (let k = 0; k < N; k += 1) {
      if (row.high < sellLevels[k]) continue;
      if (slotShares[k] <= 0) continue;

      const execPrice = Math.max(sellLevels[k], row.low);
      cash += slotShares[k] * execPrice * (1 - sfr);
      shares -= slotShares[k];
      slotShares[k] = 0;
      sellDates.push(row.date);
      sellPrices.push(execPrice);
    }
  }

  // Liquidate any remaining position at last close
  if (shares > 1e-10) {
    const lastClose = tradingRows[tradingRows.length - 1].close;
    cash += shares * lastClose * (1 - sfr);
    shares = 0;
  }

  return {
    N,
    step,
    amplitude,
    halfAmp,
    midline,
    minPrice: minLow,
    maxPrice: maxHigh,
    finalFund: cash,
    buyCount: buyDates.length,
    sellCount: sellDates.length,
    buyDates,
    buyPrices,
    sellDates,
    sellPrices,
  };
}

function renderStrategyTable(s1Result, s2Result, s3Result, s4Result) {
  if (!strategyTableEl) return;
  if (!s1Result && !s2Result && !s3Result && !s4Result) { strategyTableEl.innerHTML = ''; return; }

  const makeRow = (label, sub, configHtml, tradesHtml, mult, applyId) => {
    const cls = mult >= 1 ? 'profit' : 'loss';
    const applyCell = applyId
      ? `<button class="link-button ghost small-btn" id="${applyId}">应用到图</button>`
      : '—';
    return `<tr>
      <td><span class="strat-name">${label}</span><br><span class="strat-sub">${sub}</span></td>
      <td class="strat-config">${configHtml}</td>
      <td>${tradesHtml}</td>
      <td><strong class="${cls}">${mult.toFixed(3)}x</strong></td>
      <td>${applyCell}</td>
    </tr>`;
  };

  let rows = '';
  if (s1Result) {
    rows += makeRow(
      '策略1', '固定买卖点',
      `买&ensp;<strong>${s1Result.buyPrice.toFixed(1)}</strong>&emsp;卖&ensp;<strong>${s1Result.sellPrice.toFixed(1)}</strong>`,
      `${s1Result.buyCount}买 / ${s1Result.sellCount}卖`,
      s1Result.multiplier,
      'apply-s1-btn',
    );
  }

  if (s2Result) {
    const pairHtml = s2Result.pairs.map((p, i) => {
      const pcls = p.mult >= 1 ? 'profit' : 'loss';
      return `组${i + 1}：买&ensp;<strong>${p.buy.toFixed(1)}</strong>&emsp;卖&ensp;<strong>${p.sell.toFixed(1)}</strong>`
        + `&ensp;<span class="strat-sub">(${p.buyCount}买/${p.sellCount}卖&ensp;<span class="${pcls}">${p.mult.toFixed(3)}x</span>)</span>`;
    }).join('<br>');
    rows += makeRow(
      '策略2', '双线网格（各0.5仓）',
      pairHtml,
      '各组独立',
      s2Result.total,
      null,
    );
  }

  if (s3Result) {
    rows += makeRow(
      '策略3', '均匀网格（逐步建仓）',
      `幅度 ${s3Result.amplitude.toFixed(2)}，步长 ${s3Result.step.toFixed(2)}，N=${s3Result.N}`
        + `<br><span class="strat-sub">区间 ${s3Result.minPrice.toFixed(2)} ~ ${s3Result.maxPrice.toFixed(2)}</span>`,
      `${s3Result.buyCount}买 / ${s3Result.sellCount}卖`,
      s3Result.finalFund,
      'apply-s3-btn',
    );
  }

  if (s4Result) {
    rows += makeRow(
      '策略4', '中线对称网格',
      `幅度 ${s4Result.amplitude.toFixed(2)}，中线 ${s4Result.midline.toFixed(2)}，步长 ${s4Result.step.toFixed(2)}，N=${s4Result.N}`
        + `<br><span class="strat-sub">买入区 ${s4Result.minPrice.toFixed(2)} ~ ${s4Result.midline.toFixed(2)}`
        + `&ensp;卖出区 ${s4Result.midline.toFixed(2)} ~ ${s4Result.maxPrice.toFixed(2)}</span>`,
      `${s4Result.buyCount}买 / ${s4Result.sellCount}卖`,
      s4Result.finalFund,
      'apply-s4-btn',
    );
  }

  strategyTableEl.innerHTML = `
    <table class="strat-table">
      <thead><tr><th>策略</th><th>参数</th><th>交易次数</th><th>资金倍数</th><th>操作</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  const applyBtn = document.getElementById('apply-s1-btn');
  if (applyBtn && s1Result) {
    applyBtn.addEventListener('click', () => updateTradeLines(s1Result.buyPrice, s1Result.sellPrice));
  }
  const applyS3Btn = document.getElementById('apply-s3-btn');
  if (applyS3Btn && s3Result) {
    applyS3Btn.addEventListener('click', () => {
      Plotly.restyle(chartContainer, { x: [s3Result.buyDates], y: [s3Result.buyPrices] }, [3]);
      Plotly.restyle(chartContainer, { x: [s3Result.sellDates], y: [s3Result.sellPrices] }, [4]);
    });
  }
  const applyS4Btn = document.getElementById('apply-s4-btn');
  if (applyS4Btn && s4Result) {
    applyS4Btn.addEventListener('click', () => {
      Plotly.restyle(chartContainer, { x: [s4Result.buyDates], y: [s4Result.buyPrices] }, [3]);
      Plotly.restyle(chartContainer, { x: [s4Result.sellDates], y: [s4Result.sellPrices] }, [4]);
    });
  }
}

function optimizeBtnHandler() {
  if (!_tradeData) return;
  optimizeBtn.disabled = true;
  optimizeBtn.textContent = '计算中…';
  setTimeout(() => {
    const s1 = findOptimalLines(_tradeData);
    if (s1) {
      const stats = calcPairStats(_tradeData, s1.buyPrice, s1.sellPrice);
      s1.buyCount = stats.buyCount;
      s1.sellCount = stats.sellCount;
      updateTradeLines(s1.buyPrice, s1.sellPrice);
    }
    const s2 = findOptimalStrategy2(_tradeData);
    const stepSize = parseFloat(gridStepInput ? gridStepInput.value : 3) || 3;
    const s3 = calcGridStrategy3(_tradeData, stepSize);
    const s4 = calcGridStrategy4(_tradeData, stepSize);
    optimizeBtn.disabled = false;
    optimizeBtn.textContent = '最优策略';
    if (!s1 && !s2 && !s3 && !s4) {
      showMessage('未找到有效的买卖组合', true);
      return;
    }
    renderStrategyTable(s1, s2, s3, s4);
  }, 20);
}

function renderTradeStats(signals) {
  const buys = signals.buyDates.length;
  const sells = signals.sellDates.length;
  const fees = getActiveFees();
  const bfr = fees ? calcTotalFeeRate(fees.buy) : 0;
  const sfr = fees ? calcTotalFeeRate(fees.sell) : 0;

  let fund = 100;
  for (let i = 0; i < sells; i += 1) {
    // 含费用：实际成本 = 买入价 × (1 + 买入费率)，实际收益 = 卖出价 × (1 − 卖出费率)
    fund *= (signals.sellPrices[i] * (1 - sfr)) / (signals.buyPrices[i] * (1 + bfr));
  }
  const multiplier = fund / 100;
  const cls = multiplier >= 1 ? 'profit' : 'loss';
  const feeTag = fees
    ? `<span class="fee-tag">含费 买${(bfr * 100).toFixed(3)}%+卖${(sfr * 100).toFixed(3)}%/笔</span>`
    : '';
  tradeStatsEl.innerHTML =
    `<span>买入 <strong>${buys}</strong> 次</span>` +
    `<span>卖出 <strong>${sells}</strong> 次</span>` +
    `<span>资金 <strong class="${cls}">${multiplier.toFixed(2)}x</strong></span>` +
    feeTag;
}

function updateTradeLines(buyPrice, sellPrice) {
  _buyPrice = Math.round(buyPrice * 10) / 10;
  _sellPrice = Math.round(sellPrice * 10) / 10;
  buyLineInput.value = _buyPrice.toFixed(1);
  sellLineInput.value = _sellPrice.toFixed(1);
  if (!_tradeData) return;

  const signals = calcTradeSignals(_tradeData, _buyPrice, _sellPrice);
  Plotly.restyle(chartContainer, { x: [signals.buyDates], y: [signals.buyPrices] }, [3]);
  Plotly.restyle(chartContainer, { x: [signals.sellDates], y: [signals.sellPrices] }, [4]);

  const x0 = _tradeData[0].date;
  const x1 = _tradeData[_tradeData.length - 1].date;
  _suppressRelayout = true;
  Plotly.relayout(chartContainer, {
    shapes: [
      { type: 'line', x0, x1, y0: _buyPrice, y1: _buyPrice, xref: 'x', yref: 'y', line: { color: '#16a34a', width: 2, dash: 'dash' } },
      { type: 'line', x0, x1, y0: _sellPrice, y1: _sellPrice, xref: 'x', yref: 'y', line: { color: '#dc2626', width: 2, dash: 'dash' } },
    ],
  }).then(() => { _suppressRelayout = false; });

  renderTradeStats(signals);
}

function onTradeRelayout(eventData) {
  if (_suppressRelayout) return;

  // Respond to ANY shape key change (y-drag OR x-drag that shortens the line)
  const shapeKeys = Object.keys(eventData).filter((k) => k.startsWith('shapes['));
  if (shapeKeys.length === 0) return;

  let newBuy = _buyPrice;
  let newSell = _sellPrice;

  for (const key of shapeKeys) {
    const val = eventData[key];
    if (val == null) continue;
    if (key.startsWith('shapes[0]') && (key.endsWith('.y0') || key.endsWith('.y1'))) {
      newBuy = val;
    }
    if (key.startsWith('shapes[1]') && (key.endsWith('.y0') || key.endsWith('.y1'))) {
      newSell = val;
    }
  }
  // Always call updateTradeLines: restores full-width x0/x1 even after horizontal drag
  updateTradeLines(newBuy, newSell);
}

toggleMa5.addEventListener('change', () => {
  Plotly.restyle(chartContainer, { visible: toggleMa5.checked }, [1]);
});

toggleMa10.addEventListener('change', () => {
  Plotly.restyle(chartContainer, { visible: toggleMa10.checked }, [2]);
});

function setManualMode(enabled) {
  codeInput.classList.toggle('is-hidden', !enabled);
  codeInput.disabled = !enabled;
  if (enabled) {
    codeInput.required = true;
    codeInput.focus();
  } else {
    codeInput.required = false;
  }
}

function getSelectedCode() {
  if (codeSelect.value === 'manual') {
    return codeInput.value.trim();
  }
  return codeSelect.value.trim();
}

function buildOptionLabel(item) {
  const name = item.name ? `${item.name} ` : '';
  return `${item.market_label} ${name}${item.code}`;
}

function applyCacheOptions(items, keepValue) {
  codeSelect.innerHTML = '';
  const manualOption = document.createElement('option');
  manualOption.value = 'manual';
  manualOption.textContent = '手动输入';
  codeSelect.appendChild(manualOption);

  items.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.code;
    option.textContent = buildOptionLabel(item);
    codeSelect.appendChild(option);
  });

  if (keepValue && [...codeSelect.options].some((opt) => opt.value === keepValue)) {
    codeSelect.value = keepValue;
    setManualMode(keepValue === 'manual');
    return;
  }

  if (items.length) {
    codeSelect.value = items[0].code;
    setManualMode(false);
  } else {
    codeSelect.value = 'manual';
    setManualMode(true);
  }
}

async function refreshCacheOptions(keepValue = null) {
  try {
    const response = await fetch('/api/cache/summary');
    if (redirectToLoginIfUnauthorized(response)) {
      return;
    }
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || '缓存列表加载失败');
    }
    applyCacheOptions(result.items || [], keepValue);
  } catch (error) {
    showMessage(error.message, true);
  }
}

function renderSourceStatus(items) {
  if (!items || items.length === 0) {
    sourceStatus.innerHTML = '';
    return;
  }

  const rows = items
    .map((item) => {
      const statusClass = item.statusClass || (item.ok ? 'ok' : 'fail');
      const detail = item.ok ? `${item.rows} 条` : item.error || '失败';
      return `
        <div class="source-status-row ${statusClass}">
          <span class="source-name">${item.label}</span>
          <span class="source-detail">${detail}</span>
        </div>
      `;
    })
    .join('');

  sourceStatus.innerHTML = `<div class="source-status-card">${rows}</div>`;
}

function renderQueryProgress(steps) {
  if (!steps || steps.length === 0) {
    sourceStatus.innerHTML = '';
    return;
  }

  // Group steps by phase: "步骤N/M：..." lines start a new group.
  // Each group is collapsed into one info row.
  const groups = [];
  for (const step of steps) {
    if (/^步骤\d+\/\d+：/.test(step)) {
      groups.push([step.replace(/^步骤\d+\/\d+：/, '')]);
    } else if (groups.length === 0) {
      groups.push([step]);
    } else {
      groups[groups.length - 1].push(step);
    }
  }

  const rows = groups
    .map((parts) => `
        <div class="source-status-row info">
          <span class="source-detail">${parts.join('，')}</span>
        </div>
      `)
    .join('');

  sourceStatus.innerHTML = `<div class="source-status-card">${rows}</div>`;
}

function streamKline(payload, onProgress) {
  const params = new URLSearchParams(payload);
  const source = new EventSource(`/api/kline/stream?${params.toString()}`);

  return new Promise((resolve, reject) => {
    let settled = false;

    const finish = () => {
      if (!settled) {
        settled = true;
        source.close();
      }
    };

    source.addEventListener('progress', (event) => {
      const result = JSON.parse(event.data);
      if (onProgress) {
        onProgress(result.message);
      }
    });

    source.addEventListener('result', (event) => {
      const result = JSON.parse(event.data);
      finish();
      if (result.status >= 400) {
        const error = new Error(result.payload.error || '查询失败');
        error.progress = result.payload.progress || [];
        reject(error);
        return;
      }
      resolve(result.payload);
    });

    source.addEventListener('error', () => {
      if (settled) {
        return;
      }
      finish();
      reject(new Error('查询连接已中断'));
    });
  });
}

const autoSalesSection = document.getElementById('auto-sales-section');
const autoSalesTitle = document.getElementById('auto-sales-title');
const autoSalesStatus = document.getElementById('auto-sales-status');
const autoSalesChart = document.getElementById('auto-sales-chart');

/**
 * 从 K 线数据中提取每个月最后一个交易日（YYYY-MM-DD），以 YYYY-MM 为 key。
 */
function _monthEndDates(klineData) {
  const map = {};
  if (!klineData) return map;
  for (const row of klineData) {
    const d = row.date; // 'YYYY-MM-DD'
    if (!d) continue;
    const ym = d.slice(0, 7); // 'YYYY-MM'
    if (!map[ym] || d > map[ym]) map[ym] = d;
  }
  return map;
}

async function loadAutoSalesChart(code, startDate, endDate, klineData) {
  if (!autoSalesSection) return;
  autoSalesSection.style.display = 'none';
  if (autoSalesChart) Plotly.purge(autoSalesChart);

  const params = new URLSearchParams({ code, start_date: startDate, end_date: endDate });
  let result;
  try {
    const resp = await fetch(`/api/auto_sales?${params.toString()}`);
    if (redirectToLoginIfUnauthorized(resp)) return;
    result = await resp.json();
  } catch (err) {
    console.error('[auto_sales] fetch error:', err);
    return;
  }

  if (!result || !result.manufacturer || !result.items || result.items.length === 0) {
    return;
  }

  autoSalesSection.style.display = 'block';
  autoSalesTitle.textContent = `${result.manufacturer} 月度销量`;
  autoSalesStatus.textContent = `共 ${result.items.length} 个月`;

  // 将每月销量对齐到 K 线该月最后一个交易日
  const monthEnd = _monthEndDates(klineData);
  const kDates = klineData ? klineData.map((r) => r.date) : null;
  const xDates = result.items.map((r) => monthEnd[r.month] || (r.month + '-28'));
  const sales  = result.items.map((r) => r.sales);

  // x 轴范围与 K 线一致
  const xRange = kDates && kDates.length >= 2
    ? [kDates[0], kDates[kDates.length - 1]]
    : undefined;

  Plotly.newPlot(
    autoSalesChart,
    [{
      x: xDates,
      y: sales,
      type: 'scatter',
      mode: 'lines+markers',
      name: '月销量',
      line: { color: '#2563eb', width: 2 },
      marker: { size: 7, color: '#2563eb' },
      hovertemplate: result.items.map((r) => `${r.month}<br>销量: ${r.sales.toLocaleString()}<extra></extra>`),
    }],
    {
      margin: { t: 30, r: 20, b: 50, l: 70 },
      xaxis: {
        type: 'date',
        range: xRange,
        tickformat: '%Y-%m',
      },
      yaxis: { title: '销量（辆）' },
      height: 280,
    },
    { responsive: true },
  );
}

async function onSubmit(event) {
  event.preventDefault();
  if (rangeType.value !== 'custom') {
    const presetMonths = parseInt(datePreset.value, 10);
    if (presetMonths) applyDatePreset(presetMonths);
  }
  const progressHistory = [];
  showMessage('开始查询...');
  renderSourceStatus([]);

  const payload = {
    code: getSelectedCode(),
    start_date: form.start_date.value,
    end_date: form.end_date.value,
    source: form.source.value,
  };

  if (!payload.code) {
    showMessage('请输入股票代码', true);
    return;
  }

  try {
    const result = await streamKline(payload, (step) => {
      progressHistory.push(step);
      renderQueryProgress(progressHistory);
      showMessage(step);
    });
    const titleName = result.name ? `${result.name} ` : '';
    buildCandlestick(result.data, `${result.market_label} ${titleName}${result.code} K线图`, result.market);
    renderQueryProgress(result.progress || []);
    showMessage(
      `查询成功：${result.market_label} ${titleName}${result.code}，共 ${result.rows} 条数据（来源：${result.source_label}）`
    );
    await refreshCacheOptions(result.code);
    localStorage.setItem('lastCode', result.code);
    loadAutoSalesChart(result.code, startDateInput.value, endDateInput.value, result.data);
  } catch (error) {
    Plotly.purge(chartContainer);
    if (autoSalesSection) autoSalesSection.style.display = 'none';
    tradeControls.classList.remove('visible');
    if (strategyTableEl) strategyTableEl.innerHTML = '';
    renderQueryProgress(error.progress || []);
    showMessage(error.message, true);
  }
}

async function checkSources() {
  const code = getSelectedCode();
  if (!code) {
    showMessage('请输入股票代码', true);
    return;
  }

  showMessage('检测数据源中...');
  renderSourceStatus([]);

  const params = new URLSearchParams({
    code,
    start_date: form.start_date.value,
    end_date: form.end_date.value,
  });

  try {
    const response = await fetch(`/api/sources/health?${params.toString()}`);
    if (redirectToLoginIfUnauthorized(response)) {
      return;
    }
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || '检测失败');
    }
    renderSourceStatus(result.items || []);
    showMessage('检测完成');
  } catch (error) {
    showMessage(error.message, true);
  }
}

form.addEventListener('submit', onSubmit);
codeSelect.addEventListener('change', () => {
  setManualMode(codeSelect.value === 'manual');
});
checkSourcesButton.addEventListener('click', checkSources);

buyLineInput.addEventListener('change', () => {
  const v = parseFloat(buyLineInput.value);
  if (!isNaN(v) && v > 0 && _tradeData) updateTradeLines(v, _sellPrice);
});

sellLineInput.addEventListener('change', () => {
  const v = parseFloat(sellLineInput.value);
  if (!isNaN(v) && v > 0 && _tradeData) updateTradeLines(_buyPrice, v);
});

optimizeBtn.addEventListener('click', optimizeBtnHandler);

datePreset.addEventListener('change', () => {
  const months = parseInt(datePreset.value, 10);
  if (rangeType.value !== 'custom' && months) applyDatePreset(months);
});

rangeType.addEventListener('change', () => {
  updateRangeMode(rangeType.value);
});

setManualMode(true);
updateRangeMode(rangeType.value);
const _lastCode = localStorage.getItem('lastCode');
if (_lastCode) {
  codeInput.value = _lastCode;
}
refreshCacheOptions(_lastCode || 'manual');
loadFeeConfig();
