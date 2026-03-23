const form = document.getElementById('query-form');
const chartContainer = document.getElementById('chart');
const messageEl = document.getElementById('message');
const codeSelect = document.getElementById('code-select');
const codeInput = document.getElementById('code-input');
const sourceStatus = document.getElementById('source-status');
const checkSourcesButton = document.getElementById('check-sources');

function showMessage(text, isError = false) {
  messageEl.textContent = text;
  messageEl.classList.toggle('error', isError);
}

function movingAverage(values, windowSize) {
  const result = [];
  for (let i = 0; i < values.length; i += 1) {
    if (i + 1 < windowSize) {
      result.push(null);
      continue;
    }
    const slice = values.slice(i + 1 - windowSize, i + 1);
    const sum = slice.reduce((acc, value) => acc + value, 0);
    result.push(sum / windowSize);
  }
  return result;
}

function buildCandlestick(data, title) {
  const x = data.map((row) => row.date);
  const open = data.map((row) => row.open);
  const high = data.map((row) => row.high);
  const low = data.map((row) => row.low);
  const close = data.map((row) => row.close);
  const ma5 = movingAverage(close, 5);
  const ma10 = movingAverage(close, 10);

  const trace = {
    x,
    open,
    high,
    low,
    close,
    type: 'candlestick',
    increasing: { line: { color: '#ef4444' } },
    decreasing: { line: { color: '#22c55e' } },
    name: 'K线',
  };

  const ma5Trace = {
    x,
    y: ma5,
    type: 'scatter',
    mode: 'lines',
    name: 'MA5',
    line: { width: 1.5, color: '#f59e0b' },
  };

  const ma10Trace = {
    x,
    y: ma10,
    type: 'scatter',
    mode: 'lines',
    name: 'MA10',
    line: { width: 1.5, color: '#3b82f6' },
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
  };

  Plotly.newPlot(chartContainer, [trace, ma5Trace, ma10Trace], layout, { responsive: true });
}

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
  } catch (error) {
    Plotly.purge(chartContainer);
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

setManualMode(true);
refreshCacheOptions('manual');
