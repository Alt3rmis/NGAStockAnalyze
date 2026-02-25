import akshare as ak
import yfinance as yf

# 通用工具
def pct_change(close: float, prev_close: float) -> float:
    return (close / prev_close - 1) * 100

def judge_signal(pct: float, threshold: float = 0) -> (str, int):
    """
    期货信号判断
    参数调整理由：
    - 原阈值0.5%对大盘期货来说偏严格
    - 用户规则是"多方占优+1分，空方占优+1分"，方向判断即可
    - 涨幅>0=多，跌幅<0=空，不再设置中间阈值
    """
    if pct > threshold:
        return "多", 1
    elif pct < -threshold:
        return "空", -1
    else:
        return "中性", 0

# 1. 沪深300 股指期货
def fetch_if_main():
    df = ak.futures_main_sina(symbol="IF0")
    latest = df.iloc[-1]
    prev   = df.iloc[-2]

    latest_close = latest["收盘价"]
    prev_close   = prev["收盘价"]
    pct = pct_change(latest_close, prev_close)

    signal, score = judge_signal(pct)

    return {
        "name": "沪深300 股指期货(IF)",
        "date": latest["日期"],
        "pct_change": round(pct, 2),
        "signal": signal,
        "score": score,
    }

# 2. 中证1000 股指期货
def fetch_im_main():
    df = ak.futures_main_sina(symbol="IM0")
    latest = df.iloc[-1]
    prev   = df.iloc[-2]

    latest_close = latest["收盘价"]
    prev_close   = prev["收盘价"]
    pct = pct_change(latest_close, prev_close)

    signal, score = judge_signal(pct)

    return {
        "name": "中证1000 股指期货(IM)",
        "date": latest["日期"],
        "pct_change": round(pct, 2),
        "signal": signal,
        "score": score,
    }

# 3. 富时中国 A50 指数
def fetch_a50_from_yf():
    # 这里的代码是示意，具体 ticker 要查 SGX 上 A50 对应代码
    ticker = yf.Ticker("FTXIN9")  # 比如某个 SGX A50 期货连续合约或ETF
    hist = ticker.history(period="5d")

    close_today = hist["Close"].iloc[-1]
    close_yesterday = hist["Close"].iloc[-2]

    pct = (close_today / close_yesterday - 1) * 100

    return {
        "name": "A50期货(来自yfinance)",
        "pct_change": pct,
    }

# ========= 主流程 =========
if __name__ == "__main__":
    results = [
        # fetch_a50_index(),
        fetch_if_main(),
        fetch_im_main(),
    ]

    print("\n====== 1–3 项指数 / 期货汇总 ======\n")
    for r in results:
        print(
            f"{r['name']} | {r['date']} | "
            f"{r['pct_change']:+.2f}% | "
            f"{r['signal']} | 分值 {r['score']}"
        )

    long_score = sum(r["score"] for r in results if r["score"] > 0)
    short_score = -sum(r["score"] for r in results if r["score"] < 0)

    print("\n====== 多空汇总 ======")
    print("多方分:", long_score)
    print("空方分:", short_score)

    if long_score == 0 and short_score == 0:
        print("结论：无方向 / 信号缺失")
    elif short_score >= 2 * long_score:
        print("结论：空方占优")
    elif long_score >= 2 * short_score:
        print("结论：多方占优")
    else:
        print("结论：震荡 / 分歧")
