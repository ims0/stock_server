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
const optimizeBtn = document.getElementById('optimize-btn');

// Trade simulation state
let _tradeData = null;
let _buyPrice = null;
let _sellPrice = null;
let _suppressRelayout = false;

function showMessage(text, isError = false) {
  messageEl.textContent = text;
  messageEl.classList.toggle('error', isError);
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

function buildCandlestick(data, title) {
  _tradeData = data;

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
    increasing: { line: { color: '#ef4444' }, fillcolor: 'rgba(255,255,255,0)' },
    decreasing: { line: { color: '#22c55e' } },
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

  for (let bi = 0; bi < candidates.length; bi += 1) {
    const bp = candidates[bi];
    for (let si = bi + 1; si < candidates.length; si += 1) {
      const sp = candidates[si];
      const signals = calcTradeSignals(data, bp, sp);
      const sells = signals.sellPrices.length;
      if (sells === 0) continue;
      let fund = 1;
      for (let k = 0; k < sells; k += 1) {
        fund *= signals.sellPrices[k] / signals.buyPrices[k];
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

function optimizeBtnHandler() {
  if (!_tradeData) return;
  optimizeBtn.disabled = true;
  optimizeBtn.textContent = '计算中…';
  // Defer to next tick so the button state updates before heavy computation
  setTimeout(() => {
    const result = findOptimalLines(_tradeData);
    optimizeBtn.disabled = false;
    optimizeBtn.textContent = '最优策略';
    if (!result) {
      showMessage('未找到有效的买卖组合', true);
      return;
    }
    updateTradeLines(result.buyPrice, result.sellPrice);
  }, 20);
}

function renderTradeStats(signals) {
  const buys = signals.buyDates.length;
  const sells = signals.sellDates.length;
  let fund = 100;
  for (let i = 0; i < sells; i += 1) {
    fund *= signals.sellPrices[i] / signals.buyPrices[i];
  }
  const multiplier = fund / 100;
  const cls = multiplier >= 1 ? 'profit' : 'loss';
  tradeStatsEl.innerHTML =
    `<span>买入 <strong>${buys}</strong> 次</span>` +
    `<span>卖出 <strong>${sells}</strong> 次</span>` +
    `<span>资金 <strong class="${cls}">${multiplier.toFixed(2)}x</strong></span>`;
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

  const rows = steps
    .map(
      (step, index) => `
        <div class="source-status-row info">
          <span class="source-name">步骤 ${index + 1}</span>
          <span class="source-detail">${step}</span>
        </div>
      `
    )
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

async function onSubmit(event) {
  event.preventDefault();
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
    buildCandlestick(result.data, `${result.market_label} ${titleName}${result.code} K线图`);
    renderQueryProgress(result.progress || []);
    showMessage(
      `查询成功：${result.market_label} ${titleName}${result.code}，共 ${result.rows} 条数据（来源：${result.source_label}）`
    );
    await refreshCacheOptions(result.code);
    localStorage.setItem('lastCode', result.code);
  } catch (error) {
    Plotly.purge(chartContainer);
    tradeControls.classList.remove('visible');
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

setManualMode(true);
const _lastCode = localStorage.getItem('lastCode');
if (_lastCode) {
  codeInput.value = _lastCode;
}
refreshCacheOptions(_lastCode || 'manual');
