import akshare as ak
import pandas as pd


def _find_total_margin_col(df: pd.DataFrame) -> str:
    """
    在 DataFrame 列名中寻找包含“融资融券余额”的列名
    """
    for col in df.columns:
        if "融资融券余额" in col:
            return col
    raise ValueError(f"未找到包含 '融资融券余额' 的列名，实际列名: {list(df.columns)}")


def fetch_total_margin_change(days: int = 3):
    """
    获取沪深两市融资融券余额合计，并计算最近 N 日的变化
    返回:
      - 最近日期
      - 起始 total_margin
      - 结束 total_margin
      - 近 N 日 total_margin 变化额
    """
    # 1. 获取深圳融资融券数据
    sz = ak.macro_china_market_margin_sz()
    # 2. 获取上海融资融券数据
    sh = ak.macro_china_market_margin_sh()

    # 3. 找到“融资融券余额”列名
    sz_col = _find_total_margin_col(sz)
    sh_col = _find_total_margin_col(sh)

    # 4. 日期转 DatetimeIndex
    sz["日期"] = pd.to_datetime(sz["日期"])
    sh["日期"] = pd.to_datetime(sh["日期"])

    sz = sz.set_index("日期")
    sh = sh.set_index("日期")

    # 5. 对齐日期并求和
    merged = pd.DataFrame({
        "sz": pd.to_numeric(sz[sz_col], errors="coerce"),
        "sh": pd.to_numeric(sh[sh_col], errors="coerce"),
    }).dropna()

    merged["total_margin"] = merged["sz"] + merged["sh"]

    # 6. 取最近 N+1 条（起点 + 终点）
    recent = merged.tail(days + 1)
    if recent.shape[0] < 2:
        raise ValueError("融资融券数据不足，无法计算变化")

    start = recent["total_margin"].iloc[0]
    end = recent["total_margin"].iloc[-1]
    delta = end - start
    latest_date = recent.index[-1].date()

    return latest_date, start, end, delta


def judge_margin_signal(delta: float, threshold_ratio: float = 0.005, base: float | None = None):
    """
    根据融资融券余额变化判断多空：
    - 如果提供 base（起点余额），用比例判断
    - 否则只看符号（正/负）
    """
    if base is not None:
        ratio = delta / base
        if ratio > threshold_ratio:
            return "融资余额明显增加 → 偏多"
        elif ratio < -threshold_ratio:
            return "融资余额明显减少 → 偏空"
        else:
            return "变化不大 → 中性"
    else:
        if delta > 0:
            return "融资余额增加 → 偏多"
        elif delta < 0:
            return "融资余额减少 → 偏空"
        else:
            return "中性"


if __name__ == "__main__":
    latest_date, start, end, delta = fetch_total_margin_change(days=3)

    print("\n====== 融资融券余额 - 近3日变化 ======\n")
    print(f"截至日期: {latest_date}")
    print(f"起始融资融券余额: {start:,.0f}")
    print(f"当前融资融券余额: {end:,.0f}")
    print(f"近3日余额变化: {delta:+,.0f}")

    signal = judge_margin_signal(delta, base=start)
    print("信号判定:", signal)
