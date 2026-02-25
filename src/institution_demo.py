import akshare as ak
import pandas as pd
import requests
from akshare.utils.tqdm import get_tqdm
import secrets
import string
from datetime import datetime
from typing import Tuple
from functools import lru_cache


def generate_nid() -> Tuple[str, int]:
    """
    生成 nid 和创建时间（毫秒时间戳）

    :return: (nid, create_time_ms)
    """
    alphabet = string.ascii_lowercase + string.digits  # a-z0-9
    nid = ''.join(secrets.choice(alphabet) for _ in range(32))

    create_time_ms = int(datetime.now().timestamp() * 1000)

    return nid, create_time_ms

def _find_col(df: pd.DataFrame, keyword: str) -> str:
    """
    在列名中模糊搜索包含 keyword 的列，找不到就抛异常
    """
    for col in df.columns:
        if keyword in col:
            return col
    raise ValueError(f"未找到包含 '{keyword}' 的列名，实际列名: {list(df.columns)}")

def stock_lhb_jgmmtj_em(
    start_date: str = "20240417", end_date: str = "20240430"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-机构买卖每日统计
    https://data.eastmoney.com/stock/jgmmtj.html
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :return: 机构买卖每日统计
    :rtype: pandas.DataFrame
    """
    start_date = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
    end_date = "-".join([end_date[:4], end_date[4:6], end_date[6:]])
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    nid, create_time_ms = generate_nid()
    headers = {
        "cookie": f"nid18={nid}; nid18_create_time={create_time_ms};",
        "host": "datacenter-web.eastmoney.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://data.eastmoney.com/stock/jgmmtj.html"
    }
    params = {
        "sortColumns": "NET_BUY_AMT,TRADE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1,1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_ORGANIZATION_TRADE_DETAILS",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE>='{start_date}')(TRADE_DATE<='{end_date}')",
    }
    r = requests.get(url, params=params, headers=headers, timeout=10)
    
    if r.status_code != 200:
        raise ValueError(f"API请求失败，状态码: {r.status_code}, 响应: {r.text[:500]}")
    
    try:
        data_json = r.json()
    except Exception as e:
        raise ValueError(f"JSON解析失败: {e}, 响应内容: {r.text[:500]}")
    
    if "result" not in data_json:
        raise ValueError(f"API返回数据格式异常，缺少result字段: {data_json}")
    
    if data_json["result"] is None:
        raise ValueError(f"API返回result为None，可能是日期参数错误或该日期无数据。完整响应: {data_json}")
    
    if "pages" not in data_json["result"]:
        raise ValueError(f"API返回数据格式异常，缺少pages字段: {data_json}")
    
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm = get_tqdm()
    for page in tqdm(range(1, total_page + 1), leave=False):
        params.update(
            {
                "pageNumber": page,
            }
        )
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)
    big_df.reset_index(inplace=True)
    big_df["index"] = big_df.index + 1
    big_df.columns = [
        "序号",
        "-",
        "名称",
        "代码",
        "上榜日期",
        "收盘价",
        "涨跌幅",
        "买方机构数",
        "卖方机构数",
        "机构买入总额",
        "机构卖出总额",
        "机构买入净额",
        "市场总成交额",
        "机构净买额占总成交额比",
        "换手率",
        "流通市值",
        "上榜原因",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    big_df = big_df[
        [
            "序号",
            "代码",
            "名称",
            "收盘价",
            "涨跌幅",
            "买方机构数",
            "卖方机构数",
            "机构买入总额",
            "机构卖出总额",
            "机构买入净额",
            "市场总成交额",
            "机构净买额占总成交额比",
            "换手率",
            "流通市值",
            "上榜原因",
            "上榜日期",
        ]
    ]
    big_df["上榜日期"] = pd.to_datetime(big_df["上榜日期"], errors="coerce").dt.date
    big_df["收盘价"] = pd.to_numeric(big_df["收盘价"], errors="coerce")
    big_df["涨跌幅"] = pd.to_numeric(big_df["涨跌幅"], errors="coerce")
    big_df["买方机构数"] = pd.to_numeric(big_df["买方机构数"], errors="coerce")
    big_df["卖方机构数"] = pd.to_numeric(big_df["卖方机构数"], errors="coerce")
    big_df["机构买入总额"] = pd.to_numeric(big_df["机构买入总额"], errors="coerce")
    big_df["机构卖出总额"] = pd.to_numeric(big_df["机构卖出总额"], errors="coerce")
    big_df["机构买入净额"] = pd.to_numeric(big_df["机构买入净额"], errors="coerce")
    big_df["市场总成交额"] = pd.to_numeric(big_df["市场总成交额"], errors="coerce")
    big_df["机构净买额占总成交额比"] = pd.to_numeric(
        big_df["机构净买额占总成交额比"], errors="coerce"
    )
    big_df["换手率"] = pd.to_numeric(big_df["换手率"], errors="coerce")
    big_df["流通市值"] = pd.to_numeric(big_df["流通市值"], errors="coerce")
    return big_df

def fetch_lhb_institution_flow(start_date: str, end_date: str | None = None):
    """
    东方财富-龙虎榜-机构买卖每日统计
    参数形如: start_date='20250101', end_date='20250131'
    如果 end_date 为 None，则只取单日
    返回:
      - 明细 DataFrame
      - 机构净买入总额
    """
    if end_date is None:
        end_date = start_date

    df = stock_lhb_jgmmtj_em(start_date=start_date, end_date=end_date)

    if df.empty:
        raise ValueError("机构买卖每日统计返回空数据")

    # 尝试找“净买入”相关列（不同版本字段可能略有差异）
    try:
        net_col = _find_col(df, "净买")
    except ValueError:
        buy_col = _find_col(df, "买入")
        sell_col = _find_col(df, "卖出")
        df["机构净买额"] = pd.to_numeric(df[buy_col], errors="coerce") - pd.to_numeric(df[sell_col], errors="coerce")
        net_col = "机构净买额"

    df[net_col] = pd.to_numeric(df[net_col], errors="coerce")

    total_net = df[net_col].sum()

    return df, total_net

@lru_cache(maxsize=512)
def get_stock_industry(code: str) -> str | None:
    try:
        info_df = ak.stock_individual_info_em(symbol=code)
    except Exception as e:
        # 打印一次就够了，可以根据需要加个开关
        # print(f"[WARN] 获取 {code} 行业信息失败: {e}")
        return None

    if info_df is None or info_df.empty:
        return None

    row = info_df[info_df["item"].str.contains("所属行业", na=False)]
    if row.empty:
        return None

    return str(row["value"].iloc[0]).strip() or None

def analyze_lhb_hot_industries(df: pd.DataFrame, top_n: int = 5):
    """
    基于当前已经抓到的机构龙虎榜 DataFrame，统计热门“方向”（第8项）

    优先按行业统计；
    如果行业接口不可用，则退化为按个股名称统计。
    """
    if df.empty:
        raise ValueError("空的龙虎榜数据，无法分析热门方向")

    # 1. 确保有“机构买入净额”这一列（你已有字段名就是这个）
    if "机构买入净额" not in df.columns:
        # 兼容写法，如果后续你改了列名
        net_col = _find_col(df, "净买")
        df["机构买入净额"] = pd.to_numeric(df[net_col], errors="coerce")
    else:
        df["机构买入净额"] = pd.to_numeric(df["机构买入净额"], errors="coerce")

    df = df.copy()

    # 2. 先尝试按行业统计（用 get_stock_industry）
    df["行业"] = df["代码"].astype(str).str.zfill(6).map(get_stock_industry)

    non_null_industry = df["行业"].notna().sum()

    if non_null_industry > 0:
        # 行业接口至少成功了一部分，按行业聚合
        grouped = (
            df[~df["行业"].isna()]
            .groupby("行业")["机构买入净额"]
            .agg(上榜次数="count", 净买入总额="sum")
            .sort_values(["净买入总额", "上榜次数"], ascending=[False, False])
        )
        key_type = "行业"
    else:
        # 行业接口完全不可用，退化为按个股名称聚合
        print("[WARN] 行业信息接口不可用，退化为按个股名称统计热门方向")
        grouped = (
            df.groupby("名称")["机构买入净额"]
            .agg(上榜次数="count", 净买入总额="sum")
            .sort_values(["净买入总额", "上榜次数"], ascending=[False, False])
        )
        key_type = "个股"

    # 正向热门（净买为正）
    hot_buy = grouped[grouped["净买入总额"] > 0].head(top_n)

    # 反向热门（机构大量净卖出）
    hot_sell = (
        grouped[grouped["净买入总额"] < 0]
        .sort_values("净买入总额", ascending=True)
        .head(top_n)
    )

    # 你可以在输出时根据 key_type 文案写成：
    # “热门行业” 或 “热门个股”
    hot_buy.key_type = key_type
    hot_sell.key_type = key_type

    return hot_buy, hot_sell

def judge_lhb_institution_signal(total_net: float, threshold: float = 0):
    """
    把机构净买入额转成一个简单的多空描述
    threshold 可以先用 0，后面你有历史回测再调
    """
    if total_net > threshold:
        return "机构整体净买入 → 偏多"
    elif total_net < -threshold:
        return "机构整体净卖出 → 偏空"
    else:
        return "机构买卖接近均衡 → 中性"


if __name__ == "__main__":
    from datetime import datetime, timedelta

    date = "20250123"

    try:
        df_lhb, total_net = fetch_lhb_institution_flow(start_date=date)

        print("\n====== 龙虎榜-机构买卖每日统计 ======\n")
        print(df_lhb.head())
        print(f"\n日期区间: {date} - {date}")
        print(f"机构净买入总额: {total_net:,.0f}")

        signal = judge_lhb_institution_signal(total_net)
        print("信号判定:", signal)

        # === 热门行业方向分析 ===
        try:
            hot_buy, hot_sell = analyze_lhb_hot_industries(df_lhb, top_n=5)

            print("\n====== 龙虎榜 热门做多行业（机构净买入 Top5）======\n")
            print(hot_buy)

            print("\n====== 龙虎榜 机构净卖出较多行业（回避方向 Top5）======\n")
            print(hot_sell)
        except ValueError as e:
            print(f"\n热门行业分析失败: {e}")

    except ValueError as e:
        print(f"获取数据失败: {e}")
        print(f"提示: 日期 {date} 可能没有龙虎榜数据，请尝试其他日期")
