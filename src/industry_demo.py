import akshare as ak

def fetch_industry_fund_flow_3d(top_n: int = 5):
    """
    获取同花顺-行业资金流 3日排行
    返回: 净流入 TopN 和 净流出 TopN
    """
    df = ak.stock_fund_flow_industry(symbol="3日排行")
    # 标准列名大致为: 序号, 行业, 公司家数, 行业指数, 阶段涨跌幅,
    #                流入资金, 流出资金, 净额  (净额单位: 亿)

    # 1. 净流入从大到小
    top_in = (
        df.sort_values("净额", ascending=False)
          .head(top_n)
          .loc[:, ["行业", "净额", "流入资金", "流出资金", "阶段涨跌幅"]]
    )

    # 2. 净流出从小到大（净额为负，最小的就是流出最大）
    top_out = (
        df.sort_values("净额", ascending=True)
          .head(top_n)
          .loc[:, ["行业", "净额", "流入资金", "流出资金", "阶段涨跌幅"]]
    )

    return top_in, top_out


if __name__ == "__main__":
    top_in, top_out = fetch_industry_fund_flow_3d(top_n=5)

    print("\n====== 行业资金流 - 近3日 净流入 TOP5 ======\n")
    print(top_in.to_string(index=False))

    print("\n====== 行业资金流 - 近3日 净流出 TOP5 ======\n")
    print(top_out.to_string(index=False))