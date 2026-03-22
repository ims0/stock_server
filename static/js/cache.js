const tableBody = document.getElementById('cache-table-body');
const messageEl = document.getElementById('cache-message');
const refreshButton = document.getElementById('refresh-cache');
const clearAllButton = document.getElementById('clear-all-cache');

function setMessage(text, isError = false) {
  messageEl.textContent = text;
  messageEl.classList.toggle('error', isError);
}

function formatRange(start, end) {
  if (!start || !end) {
    return '-';
  }
  return `${start} ~ ${end}`;
}

function renderTable(items) {
  tableBody.innerHTML = '';
  if (!items.length) {
    tableBody.innerHTML = '<tr><td colspan="6" class="empty">暂无缓存数据</td></tr>';
    return;
  }

  items.forEach((item) => {
    const row = document.createElement('tr');
    const trading = item.trading_rows ?? 0;
    const nonTrading = item.non_trading_rows ?? 0;
    row.innerHTML = `
      <td>${item.market_label}</td>
      <td>${item.code}</td>
      <td>${formatRange(item.start_date, item.end_date)}</td>
      <td>${item.rows} (开盘 ${trading} / 休市 ${nonTrading})</td>
      <td>${item.updated_at || '-'}</td>
      <td><button class="link-button ghost" data-code="${item.code}">删除该股票</button></td>
    `;
    tableBody.appendChild(row);
  });
}

async function fetchSummary() {
  setMessage('加载中...');
  const response = await fetch('/api/cache/summary');
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.error || '加载失败');
  }

  renderTable(result.items || []);
  setMessage(`共 ${result.count || 0} 只股票有缓存`);
}

async function deleteCache(code) {
  const params = code ? `?code=${encodeURIComponent(code)}` : '';
  const response = await fetch(`/api/cache${params}`, { method: 'DELETE' });
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.error || '删除失败');
  }

  return result;
}

async function refresh() {
  try {
    await fetchSummary();
  } catch (error) {
    setMessage(error.message, true);
  }
}

refreshButton.addEventListener('click', refresh);

clearAllButton.addEventListener('click', async () => {
  const confirmed = window.confirm('确认清空全部缓存吗？');
  if (!confirmed) {
    return;
  }
  try {
    await deleteCache();
    await refresh();
  } catch (error) {
    setMessage(error.message, true);
  }
});

tableBody.addEventListener('click', async (event) => {
  const button = event.target.closest('button[data-code]');
  if (!button) {
    return;
  }
  const code = button.getAttribute('data-code');
  const confirmed = window.confirm(`确认删除 ${code} 的缓存吗？`);
  if (!confirmed) {
    return;
  }
  try {
    await deleteCache(code);
    await refresh();
  } catch (error) {
    setMessage(error.message, true);
  }
});

refresh();
