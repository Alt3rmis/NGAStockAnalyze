import akshare as ak
import pandas as pd


def _find_col(df: pd.DataFrame, keyword: str) -> str:
    for col in df.columns:
        if keyword in col:
            return col
    raise ValueError(f"未找到包含 '{keyword}' 的列名, 实际列名: {list(df.columns)}")


def analyze_limitup_structure(date: str, top_n_industry: int = 5):
    """
    涨停板结构分析：
    - 当日涨停家数
    - 连板分布
    - 最高板高度
    - 行业集中度
    """
    df_zt = ak.stock_zt_pool_em(date=date)
    if df_zt.empty:
        raise ValueError(f"{date} 无涨停数据")

    lb_col = _find_col(df_zt, "连板")
    ind_col = _find_col(df_zt, "所属行业")  # 有的版本是“所属行业”、“所属概念”之类

    df_zt[lb_col] = pd.to_numeric(df_zt[lb_col], errors="coerce").fillna(1)

    total_zt = len(df_zt)
    max_lb = int(df_zt[lb_col].max())
    lb_dist = df_zt[lb_col].value_counts().sort_index()

    industry_top = (
        df_zt[ind_col]
        .value_counts()
        .head(top_n_industry)
    )

    return {
        "date": date,
        "total_zt": int(total_zt),
        "max_lb": max_lb,
        "lb_dist": lb_dist,
        "industry_top": industry_top,
        "df": df_zt,
    }


def analyze_limitup_break(date: str):
    """
    炸板股池分析：
    - 当日炸板家数
    - 行业分布
    """
    df_zb = ak.stock_zt_pool_zbgc_em(date=date)
    if df_zb.empty:
        # 没有炸板也算信息：环境很好
        return {
            "date": date,
            "total_zb": 0,
            "industry_dist": pd.Series(dtype=int),
            "df": df_zb,
        }

    ind_col = _find_col(df_zb, "所属行业")

    total_zb = len(df_zb)
    industry_dist = df_zb[ind_col].value_counts()

    return {
        "date": date,
        "total_zb": int(total_zb),
        "industry_dist": industry_dist,
        "df": df_zb,
    }


def analyze_tier_structure(lb_dist: pd.Series, max_lb: int) -> dict:
    """
    分析涨停梯队结构完整度
    梯队完整度判断：
    - 完整梯队：1板、2板、3板、4板及以上都有分布
    - 部分梯队：只有部分板位有分布
    - 单一梯队：主要集中在某个板位
    """
    tiers = {i: lb_dist.get(i, 0) for i in range(1, max_lb + 1)}
    
    active_tiers = [i for i, count in tiers.items() if count > 0]
    tier_count = len(active_tiers)
    
    if tier_count >= 4:
        tier_quality = "梯队结构完整，接力顺畅"
    elif tier_count >= 2:
        tier_quality = "梯队结构部分完整，存在接力机会"
    else:
        tier_quality = "梯队结构单一，接力困难"
    
    return {
        "tiers": tiers,
        "active_tiers": active_tiers,
        "tier_count": tier_count,
        "tier_quality": tier_quality,
        "dominant_tier": active_tiers[0] if active_tiers else 0
    }

def analyze_sector_graduation(struct: dict, breaks: dict | None) -> dict:
    """
    分析板块是否出现"毕业照"迹象
    毕业照特征：
    1. 最高标（max_lb）开始断板
    2. 涨停家数明显减少
    3. 炸板率上升
    4. 板块集中度下降（前5行业占比降低）
    
    参数调整理由：
    - 最高板<=3就要警惕(原<=2太严格)
    - 涨停家数<50就开始情绪降温(原<40)
    - 炸板率>35%就偏高(原>40%)
    - 行业集中度<35%说明资金分散(原<40%)
    - 毕业照判断：>=3分=明显，>=2分=疑似
    """
    total_zt = struct["total_zt"]
    max_lb = struct["max_lb"]
    industry_top = struct.get("industry_top", pd.Series())
    
    total_zb = 0
    if breaks is not None:
        total_zb = breaks["total_zb"]
    
    break_ratio = total_zb / (total_zt + 1e-6)
    
    if not industry_top.empty:
        top5_concentration = industry_top.head(5).sum() / total_zt
    else:
        top5_concentration = 0
    
    graduation_signals = []
    graduation_score = 0
    
    # 高标判断：<=2=严重断板，=3=承压
    if max_lb <= 2:
        graduation_signals.append("最高板<=2，高标严重断板")
        graduation_score += 2
    elif max_lb == 3:
        graduation_signals.append("最高板=3，高标承压")
        graduation_score += 1
    
    # 涨停家数：<50=情绪降温，<30=极弱
    if total_zt < 30:
        graduation_signals.append("涨停家数<30，情绪极弱")
        graduation_score += 2
    elif total_zt < 50:
        graduation_signals.append("涨停家数<50，情绪降温")
        graduation_score += 1
    
    # 炸板率：>35%=偏高，>50%=极高
    if break_ratio > 0.5:
        graduation_signals.append(f"炸板率{break_ratio:.1%}极高")
        graduation_score += 2
    elif break_ratio > 0.35:
        graduation_signals.append(f"炸板率{break_ratio:.1%}偏高")
        graduation_score += 1
    
    # 行业集中度：<35%=资金分散
    if top5_concentration < 0.35:
        graduation_signals.append(f"行业集中度{top5_concentration:.1%}较低")
        graduation_score += 1
    
    if graduation_score >= 4:
        graduation_status = "明显毕业照"
    elif graduation_score >= 2:
        graduation_status = "疑似毕业照"
    else:
        graduation_status = "梯队正常"
    
    return {
        "graduation_status": graduation_status,
        "graduation_score": graduation_score,
        "graduation_signals": graduation_signals,
        "break_ratio": break_ratio,
        "top5_concentration": top5_concentration
    }

def score_limitup_env(struct: dict, breaks: dict | None) -> tuple[str, int]:
    """
    根据涨停结构 + 炸板情况做一个简单评分：
    - 强: +1
    - 中性: 0
    - 弱: -1
    
    参数调整理由：
    - 强环境：涨停>=80(牛市常见100+)，高标>=4，炸板率<35%
    - 弱环境：涨停<=30(低于30情绪极差)，高标<=2(没有连板)，炸板率>50%
    - 炸板率35%是分界线，低于35%说明打板成功率高
    """
    total_zt = struct["total_zt"]
    max_lb = struct["max_lb"]
    lb_dist = struct["lb_dist"]

    total_zb = 0
    if breaks is not None:
        total_zb = breaks["total_zb"]

    break_ratio = total_zb / (total_zt + 1e-6)

    # 强环境：涨停家数>=80，高标>=4板，炸板率<35%
    if max_lb >= 4 and total_zt >= 80 and break_ratio < 0.35:
        return "涨停结构强，适合接力", +1

    # 弱环境：涨停家数<=30，或高标<=2，或炸板率>50%
    if max_lb <= 2 or total_zt <= 30 or break_ratio > 0.5:
        return "涨停结构弱，集中毕业照/上板失败多", -1

    # 其余情况视为中性
    return "涨停结构一般，分歧/震荡", 0

def build_core_sectors(
    top_in: pd.DataFrame,
    top_out: pd.DataFrame,
    lhb_hot_buy: pd.DataFrame,
    lhb_hot_sell: pd.DataFrame,
    limitup_industry_top: pd.Series,
):
    """
    三重信号交集：
    - 资金流入（top_in）
    - 龙虎榜净买入（lhb_hot_buy）
    - 涨停集中度（limitup_industry_top）

    返回:
      - long_candidates: 多头主线候选行业列表
      - short_avoid: 需要回避的方向列表
    """
    # 1. 多头：三方交集
    set_in = set(top_in["行业"] if "行业" in top_in.columns else top_in.index)
    set_lhb_buy = set(lhb_hot_buy.index)
    set_zt = set(limitup_industry_top.index)

    long_candidates = sorted(set_in & set_lhb_buy & set_zt)

    # 2. 空头/回避：资金流出 + 龙虎榜净卖出 交集
    set_out = set(top_out["行业"] if "行业" in top_out.columns else top_out.index)
    set_lhb_sell = set(lhb_hot_sell.index)

    short_avoid = sorted(set_out & set_lhb_sell)

    return long_candidates, short_avoid

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


