# 股票 K 线查看网站（Flask）

## 启动

```bash
python3 -m venv venv
# 激活虚拟环境（关键！激活后命令行开头会显示 (venv)）
source venv/bin/activate

python3 -m pip install -r requirements.txt
python3 app.py
```

浏览器打开：`http://127.0.0.1:5000`

## 功能

- 输入股票代码并选择市场（A股 / 港股）
- 自定义开始和结束日期（默认近半年）
- 展示日线 K 线图
- 图下方自带可左右拖动的时间范围条（Plotly rangeslider）

## 代码示例

- A股：`600519`
- 港股：`700` 或 `00700`

## 数据源

- 优先使用 `akshare`（免费）
- 失败时自动回退 `yfinance`（免费）
