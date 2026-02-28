const form = document.getElementById('query-form');
const chartContainer = document.getElementById('chart');
const messageEl = document.getElementById('message');

function showMessage(text, isError = false) {
  messageEl.textContent = text;
  messageEl.classList.toggle('error', isError);
}

function buildCandlestick(data, title) {
  const x = data.map((row) => row.date);
  const open = data.map((row) => row.open);
  const high = data.map((row) => row.high);
  const low = data.map((row) => row.low);
  const close = data.map((row) => row.close);

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

  Plotly.newPlot(chartContainer, [trace], layout, { responsive: true });
}

async function fetchKline(payload) {
  const params = new URLSearchParams(payload);
  const response = await fetch(`/api/kline?${params.toString()}`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.error || '查询失败');
  }

  return result;
}

async function onSubmit(event) {
  event.preventDefault();
  showMessage('查询中...');

  const payload = {
    market: form.market.value,
    code: form.code.value,
    start_date: form.start_date.value,
    end_date: form.end_date.value,
  };

  try {
    const result = await fetchKline(payload);
    buildCandlestick(result.data, `${result.market.toUpperCase()} ${result.code} K线图`);
    showMessage(`查询成功：共 ${result.rows} 条数据`);
  } catch (error) {
    Plotly.purge(chartContainer);
    showMessage(error.message, true);
  }
}

form.addEventListener('submit', onSubmit);
