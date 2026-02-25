"""
A股多空情绪分析工具
整合多源数据，生成市场情绪分析报告
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

from src.margin_demo import fetch_total_margin_change, judge_margin_signal
from src.futures_demo import fetch_if_main, fetch_im_main
from src.limitup_demo import (
    analyze_limitup_structure, analyze_limitup_break, score_limitup_env,
    build_core_sectors, analyze_tier_structure, analyze_sector_graduation
)
from src.industry_demo import fetch_industry_fund_flow_3d
from src.institution_demo import (
    fetch_lhb_institution_flow, analyze_lhb_hot_industries, judge_lhb_institution_signal
)
from src.data_logger import (
    DataLogger, FileArchiver, init_directories, 
    LOGS_DIR, RESULTS_DIR, DATA_LOGS_DIR, ARCHIVE_DIR
)

BEIJING_TZ = timezone(timedelta(hours=8))


class Config:
    LOGS_DIR = LOGS_DIR
    RESULTS_DIR = RESULTS_DIR
    DATA_LOGS_DIR = DATA_LOGS_DIR
    ARCHIVE_DIR = ARCHIVE_DIR
    
    DATE_LOOKBACK_DAYS = 7
    MARGIN_CHANGE_DAYS = 3
    INDUSTRY_TOP_N = 5
    
    class Scoring:
        EXTERNAL_MARKET_WEIGHT = 0.1
        STRONG_RATIO = 2.0
        MIN_CONFIDENCE_SCORE = 3


_data_logger: Optional[DataLogger] = None
_main_logger = None


def get_data_logger() -> DataLogger:
    global _data_logger
    if _data_logger is None:
        _data_logger = DataLogger()
    return _data_logger


def get_main_logger():
    global _main_logger
    if _main_logger is None:
        import logging
        _main_logger = logging.getLogger('MainApp')
        if not _main_logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
            )
            _main_logger.addHandler(handler)
            _main_logger.setLevel(logging.INFO)
    return _main_logger


def get_trade_date() -> str:
    """
    获取最近的交易日日期（YYYYMMDD格式）
    """
    logger = get_data_logger()
    main_logger = get_main_logger()
    
    fetch_id = logger.log_data_fetch_start(
        "akshare.stock_zt_pool_em",
        {"purpose": "获取交易日日期"}
    )
    
    today = datetime.now()
    for i in range(Config.DATE_LOOKBACK_DAYS):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")
        try:
            df = ak.stock_zt_pool_em(date=date_str)
            if not df.empty:
                logger.log_data_fetch_success(fetch_id, df)
                main_logger.info(f"交易日检测完成: {date_str}")
                return date_str
        except Exception as e:
            continue
    
    fallback_date = today.strftime("%Y%m%d")
    logger.log_data_fetch_error(fetch_id, Exception(f"使用回退日期: {fallback_date}"))
    main_logger.warning(f"未找到有效交易日，使用今天: {fallback_date}")
    return fallback_date


def fetch_margin_data() -> Dict[str, Any]:
    """获取融资融券数据"""
    logger = get_data_logger()
    main_logger = get_main_logger()
    
    fetch_id = logger.log_data_fetch_start(
        "margin_data",
        {"days": Config.MARGIN_CHANGE_DAYS}
    )
    
    try:
        latest_date, start, end, delta = fetch_total_margin_change(days=Config.MARGIN_CHANGE_DAYS)
        
        result = {
            "latest_date": latest_date,
            "start": start,
            "end": end,
            "delta": delta,
            "signal": judge_margin_signal(delta, base=start)
        }
        
        logger.log_data_fetch_success(fetch_id, result)
        main_logger.info(f"融资融券数据获取成功: 日期={latest_date}, 变化={delta:+,.0f}")
        
        return result
    except Exception as e:
        logger.log_data_fetch_error(fetch_id, e)
        main_logger.error(f"融资融券数据获取失败: {e}")
        return {"error": str(e)}


def fetch_futures_data() -> Dict[str, Dict[str, Any]]:
    """获取股指期货数据"""
    logger = get_data_logger()
    main_logger = get_main_logger()
    result = {}
    
    for key, fetch_func in [("if_main", fetch_if_main), ("im_main", fetch_im_main)]:
        fetch_id = logger.log_data_fetch_start(
            f"futures_{key}",
            {"contract": key}
        )
        
        try:
            data = fetch_func()
            result[key] = data
            logger.log_data_fetch_success(fetch_id, data)
            main_logger.info(f"期货数据获取成功: {key}, 信号={data.get('signal', 'N/A')}")
        except Exception as e:
            result[key] = {"error": str(e)}
            logger.log_data_fetch_error(fetch_id, e)
            main_logger.error(f"期货数据获取失败: {key}, 错误={e}")
    
    return result


def fetch_industry_data() -> Dict[str, Any]:
    """获取行业资金流数据"""
    logger = get_data_logger()
    main_logger = get_main_logger()
    
    fetch_id = logger.log_data_fetch_start(
        "industry_fund_flow_3d",
        {"top_n": Config.INDUSTRY_TOP_N}
    )
    
    try:
        top_in, top_out = fetch_industry_fund_flow_3d(top_n=Config.INDUSTRY_TOP_N)
        
        result = {"top_in": top_in, "top_out": top_out}
        
        combined_data = {
            "top_in_count": len(top_in) if top_in is not None else 0,
            "top_out_count": len(top_out) if top_out is not None else 0,
            "top_in_sample": top_in.head(3).to_dict('records') if top_in is not None else [],
            "top_out_sample": top_out.head(3).to_dict('records') if top_out is not None else []
        }
        logger.log_data_fetch_success(fetch_id, combined_data)
        main_logger.info(f"行业资金流数据获取成功: 流入{len(top_in) if top_in is not None else 0}条, 流出{len(top_out) if top_out is not None else 0}条")
        
        return result
    except Exception as e:
        logger.log_data_fetch_error(fetch_id, e)
        main_logger.error(f"行业资金流数据获取失败: {e}")
        return {"error": str(e)}


def fetch_lhb_data(date_str: str) -> Dict[str, Any]:
    """获取龙虎榜数据"""
    logger = get_data_logger()
    main_logger = get_main_logger()
    
    fetch_id = logger.log_data_fetch_start(
        "lhb_institution_flow",
        {"start_date": date_str}
    )
    
    try:
        df_lhb, total_net = fetch_lhb_institution_flow(start_date=date_str)
        
        result = {
            "df": df_lhb,
            "total_net": total_net,
            "signal": judge_lhb_institution_signal(total_net),
            "hot_buy": None,
            "hot_sell": None
        }
        
        log_data = {
            "total_net": total_net,
            "signal": result["signal"],
            "record_count": len(df_lhb) if df_lhb is not None else 0
        }
        logger.log_data_fetch_success(fetch_id, log_data)
        main_logger.info(f"龙虎榜数据获取成功: 净买入={total_net:,.0f}, 信号={result['signal']}")
        
        return result
    except Exception as e:
        logger.log_data_fetch_error(fetch_id, e)
        main_logger.error(f"龙虎榜数据获取失败: {e}")
        return {"error": str(e)}


def fetch_limitup_data(date_str: str) -> Dict[str, Any]:
    """获取涨停板数据"""
    logger = get_data_logger()
    main_logger = get_main_logger()
    
    fetch_id = logger.log_data_fetch_start(
        "limitup_analysis",
        {"date": date_str}
    )
    
    try:
        struct = analyze_limitup_structure(date=date_str)
        breaks = analyze_limitup_break(date=date_str)
        env_desc, env_score = score_limitup_env(struct, breaks)
        tier_analysis = analyze_tier_structure(struct["lb_dist"], struct["max_lb"])
        graduation_analysis = analyze_sector_graduation(struct, breaks)
        
        result = {
            "struct": struct,
            "breaks": breaks,
            "env_desc": env_desc,
            "env_score": env_score,
            "tier": tier_analysis,
            "graduation": graduation_analysis
        }
        
        log_data = {
            "total_zt": struct.get("total_zt", 0),
            "max_lb": struct.get("max_lb", 0),
            "env_score": env_score,
            "env_desc": env_desc
        }
        logger.log_data_fetch_success(fetch_id, log_data)
        main_logger.info(f"涨停板数据获取成功: 涨停数={struct.get('total_zt', 0)}, 最高板={struct.get('max_lb', 0)}板")
        
        return result
    except Exception as e:
        logger.log_data_fetch_error(fetch_id, e)
        main_logger.error(f"涨停板数据获取失败: {e}")
        return {"error": str(e)}


def fetch_external_data() -> Dict[str, Any]:
    """获取外围市场数据"""
    logger = get_data_logger()
    main_logger = get_main_logger()
    
    fetch_id = logger.log_data_fetch_start(
        "akshare.stock_zh_index_daily",
        {"symbol": "sh000001"}
    )
    
    try:
        df_sh = ak.stock_zh_index_daily(symbol="sh000001")
        
        if len(df_sh) >= 2:
            close_today = df_sh["close"].iloc[-1]
            close_yesterday = df_sh["close"].iloc[-2]
            pct = (close_today / close_yesterday - 1) * 100
            
            result = {
                "sh_index": {
                    "pct_change": pct,
                    "signal": "多" if pct > 0 else "空" if pct < 0 else "中性"
                }
            }
            
            log_data = {
                "close_today": float(close_today),
                "close_yesterday": float(close_yesterday),
                "pct_change": pct,
                "signal": result["sh_index"]["signal"]
            }
            logger.log_data_fetch_success(fetch_id, log_data)
            main_logger.info(f"外围市场数据获取成功: 上证指数涨跌幅={pct:+.2f}%")
            
            return result
        
        error = Exception("数据不足")
        logger.log_data_fetch_error(fetch_id, error)
        main_logger.error("外围市场数据获取失败: 数据不足")
        return {"error": "数据不足"}
    except Exception as e:
        logger.log_data_fetch_error(fetch_id, e)
        main_logger.error(f"外围市场数据获取失败: {e}")
        return {"error": str(e)}


def fetch_all_data(date_str: str) -> Dict[str, Any]:
    """获取所有需要的数据"""
    main_logger = get_main_logger()
    main_logger.info("=" * 60)
    main_logger.info(f"开始获取所有数据 - 分析日期: {date_str}")
    main_logger.info("=" * 60)
    
    data = {
        "margin": fetch_margin_data(),
        "industry_flow": fetch_industry_data(),
        "lhb": fetch_lhb_data(date_str),
        "limitup": fetch_limitup_data(date_str),
        "external": fetch_external_data(),
    }
    data.update(fetch_futures_data())
    
    success_count = sum(1 for v in data.values() if "error" not in v)
    total_count = len(data)
    
    main_logger.info("=" * 60)
    main_logger.info(f"数据获取完成: 成功 {success_count}/{total_count}")
    main_logger.info("=" * 60)
    
    return data


def calculate_scores(data: Dict[str, Any]) -> Dict[str, float]:
    """计算多空分数"""
    scores = {"long": 0.0, "short": 0.0}
    
    score_rules = [
        ("margin", lambda d: d.get("delta", 0), 1.0),
        ("if_main", lambda d: d.get("signal", ""), 1.0),
        ("im_main", lambda d: d.get("signal", ""), 1.0),
        ("lhb", lambda d: d.get("total_net", 0), 1.0),
        ("limitup", lambda d: d.get("env_score", 0), 1.0),
    ]
    
    for key, getter, weight in score_rules:
        if key in data and "error" not in data[key]:
            value = getter(data[key])
            if isinstance(value, (int, float)):
                if value > 0:
                    scores["long"] += weight
                elif value < 0:
                    scores["short"] += weight
            elif isinstance(value, str):
                if value == "多":
                    scores["long"] += weight
                elif value == "空":
                    scores["short"] += weight
    
    if "external" in data and "error" not in data["external"]:
        pct = data["external"].get("sh_index", {}).get("pct_change", 0)
        w = Config.Scoring.EXTERNAL_MARKET_WEIGHT
        if pct > 0:
            scores["long"] += w
        elif pct < 0:
            scores["short"] += w
    
    return scores


def predict_market_opening(scores: Dict[str, float], data: Dict[str, Any]) -> Dict[str, str]:
    """
    预判明天开盘情况
    - 高开/低开判断：多方>=2倍空方且>=3分=高开
    - 黄线/白线策略：涨停环境强+高标>=4=做黄线
    """
    long_score = scores["long"]
    short_score = scores["short"]
    
    if long_score == 0 and short_score == 0:
        opening_pred, confidence = "平开", "低"
    elif long_score >= Config.Scoring.STRONG_RATIO * short_score and long_score >= Config.Scoring.MIN_CONFIDENCE_SCORE:
        opening_pred, confidence = "高开", "高"
    elif short_score >= Config.Scoring.STRONG_RATIO * long_score and short_score >= Config.Scoring.MIN_CONFIDENCE_SCORE:
        opening_pred, confidence = "低开", "高"
    elif long_score > short_score:
        opening_pred, confidence = "高开", "中"
    elif short_score > long_score:
        opening_pred, confidence = "低开", "中"
    else:
        opening_pred, confidence = "平开", "低"
    
    strategy, reason = _determine_strategy(data)
    
    return {
        "opening_pred": opening_pred,
        "opening_confidence": confidence,
        "strategy": strategy,
        "strategy_reason": reason
    }


def _determine_strategy(data: Dict[str, Any]) -> Tuple[str, str]:
    """确定操作策略"""
    if "limitup" not in data or "error" in data["limitup"]:
        return "震荡市", "涨停数据缺失"
    
    env_score = data["limitup"].get("env_score", 0)
    struct = data["limitup"].get("struct", {})
    max_lb = struct.get("max_lb", 0)
    
    if env_score > 0 and max_lb >= 4:
        return "做黄线", "涨停环境强+高标>=4板，题材接力顺畅"
    if env_score < 0:
        return "守白线", "涨停环境弱，题材退潮，关注权重防守"
    
    if_main = data.get("if_main", {})
    im_main = data.get("im_main", {})
    
    if "error" not in if_main and "error" not in im_main:
        im_signal = im_main.get("signal", "")
        if_signal = if_main.get("signal", "")
        
        if im_signal == "多" and if_signal != "空":
            return "做黄线", "中证1000+沪深300双多，小盘题材活跃"
        if im_signal == "多":
            return "做黄线", "中证1000偏多，小盘题材活跃"
        if if_signal == "多" and im_signal == "空":
            return "守白线", "沪深300偏多+中证1000偏空，权重主导"
    
    return "震荡市", "多空信号分歧，等待方向明确"


def analyze_sectors(data: Dict[str, Any]) -> Dict[str, Any]:
    """分析板块机会"""
    sectors = {}
    
    try:
        if all(k in data and "error" not in data[k] for k in ["industry_flow", "lhb", "limitup"]):
            top_in = data["industry_flow"].get("top_in")
            top_out = data["industry_flow"].get("top_out")
            hot_buy = data["lhb"].get("hot_buy")
            hot_sell = data["lhb"].get("hot_sell")
            limitup_industry_top = data["limitup"]["struct"].get("industry_top")
            
            if all(v is not None for v in [top_in, top_out, hot_buy, hot_sell, limitup_industry_top]):
                long_candidates, short_avoid = build_core_sectors(
                    top_in=top_in, top_out=top_out,
                    lhb_hot_buy=hot_buy, lhb_hot_sell=hot_sell,
                    limitup_industry_top=limitup_industry_top
                )
                sectors["long_candidates"] = long_candidates
                sectors["short_avoid"] = short_avoid
    except Exception as e:
        sectors["error"] = str(e)
    
    if "limitup" in data and "error" not in data["limitup"]:
        struct = data["limitup"].get("struct", {})
        sectors["limitup_struct"] = {
            "total_zt": struct.get("total_zt", 0),
            "max_lb": struct.get("max_lb", 0),
            "lb_dist": struct.get("lb_dist", {}),
            "industry_top": struct.get("industry_top", {})
        }
    
    return sectors


class ReportGenerator:
    """Markdown报告生成器"""
    
    def __init__(self, date_str: str):
        self.date_str = date_str
        self.lines = []
    
    def add_header(self):
        self.lines.extend([
            f"# 多空情绪分析报告",
            f"\n**日期**: {self.date_str}",
            f"\n**生成时间**: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')} (北京时间)",
            "\n---"
        ])
    
    def add_section(self, title: str, content_lines: list):
        self.lines.append(f"\n## {title}\n")
        self.lines.extend(content_lines)
    
    def add_table(self, headers: list, rows: list, align: str = "left"):
        align_map = {"left": ":-----", "center": ":----:", "right": "-----:"}
        self.lines.append("|" + "|".join(headers) + "|")
        self.lines.append("|" + "|".join([align_map.get(align, ":-----")] * len(headers)) + "|")
        for row in rows:
            self.lines.append("|" + "|".join(str(cell) for cell in row) + "|")
    
    def build(self) -> str:
        self.lines.append("\n---")
        self.lines.append("\n*报告结束*")
        return "\n".join(self.lines)


def generate_report(data: Dict, scores: Dict, sectors: Dict, date_str: str, opening_pred: Dict) -> Tuple[str, str]:
    """生成Markdown格式报告"""
    report = ReportGenerator(date_str)
    report.add_header()
    
    _add_opening_section(report, opening_pred)
    _add_scores_section(report, scores)
    _add_margin_section(report, data)
    _add_futures_section(report, data)
    _add_industry_section(report, data)
    _add_lhb_section(report, data)
    _add_limitup_section(report, data)
    _add_sectors_section(report, sectors)
    _add_external_section(report, data)
    
    report_str = report.build()
    file_name = save_report_to_file(report_str, date_str)
    return report_str, file_name


def _add_opening_section(report: ReportGenerator, opening_pred: Dict):
    report.add_section("📊 明日开盘预判", [])
    report.add_table(
        ["项目", "结果"],
        [
            ["开盘方向", f"**{opening_pred['opening_pred']}** (置信度: {opening_pred['opening_confidence']})"],
            ["操作策略", f"**{opening_pred['strategy']}**"],
            ["策略原因", opening_pred['strategy_reason']]
        ]
    )


def _add_scores_section(report: ReportGenerator, scores: Dict):
    report.add_section("📈 多空分数汇总", [])
    report.add_table(["方向", "分数"], [["多方", f"**{scores['long']:.1f}**"], ["空方", f"**{scores['short']:.1f}**"]], "center")
    
    if scores['long'] >= Config.Scoring.STRONG_RATIO * scores['short']:
        conclusion = "🟢 **多方占优**"
    elif scores['short'] >= Config.Scoring.STRONG_RATIO * scores['long']:
        conclusion = "🔴 **空方占优**"
    else:
        conclusion = "🟡 **震荡/分歧**"
    report.lines.append(f"\n**结论**: {conclusion}")


def _add_margin_section(report: ReportGenerator, data: Dict):
    report.add_section("💰 融资融券", [])
    margin = data.get("margin", {})
    
    if "error" not in margin:
        report.add_table(
            ["指标", "数值"],
            [
                ["最新日期", margin['latest_date']],
                ["起始余额", f"{margin['start']:,.0f}"],
                ["当前余额", f"{margin['end']:,.0f}"],
                ["3日变化", f"{margin['delta']:+,.0f}"],
                ["信号", margin['signal']]
            ]
        )
    else:
        report.lines.append("*数据获取失败*")


def _add_futures_section(report: ReportGenerator, data: Dict):
    report.add_section("📉 股指期货", [])
    rows = []
    for name, key in [("沪深300 (IF)", "if_main"), ("中证1000 (IM)", "im_main")]:
        if key in data and "error" not in data[key]:
            item = data[key]
            signal = item.get('signal', '')
            icon = "🟢" if signal == "多" else "🔴" if signal == "空" else "⚪"
            rows.append([name, item.get('date', ''), f"{item.get('pct_change', 0):+.2f}%", f"{icon} {signal}", str(item.get('score', 0))])
        else:
            rows.append([name, "-", "-", "❌", "-"])
    report.add_table(["品种", "日期", "涨跌幅", "信号", "分值"], rows)


def _add_industry_section(report: ReportGenerator, data: Dict):
    report.add_section("💹 行业资金流（近3日）", [])
    flow = data.get("industry_flow", {})
    
    if "error" not in flow:
        top_in = flow.get("top_in")
        top_out = flow.get("top_out")
        
        if top_in is not None:
            report.lines.append("### 🟢 净流入 TOP5\n")
            report.add_table(
                ["行业", "净额(亿)", "流入(亿)", "流出(亿)", "涨跌幅"],
                [[row['行业'], f"{row['净额']:+.2f}", f"{row['流入资金']:.2f}", f"{row['流出资金']:.2f}", str(row['阶段涨跌幅'])] for _, row in top_in.iterrows()]
            )
        
        if top_out is not None:
            report.lines.append("\n### 🔴 净流出 TOP5\n")
            report.add_table(
                ["行业", "净额(亿)", "流入(亿)", "流出(亿)", "涨跌幅"],
                [[row['行业'], f"{row['净额']:+.2f}", f"{row['流入资金']:.2f}", f"{row['流出资金']:.2f}", str(row['阶段涨跌幅'])] for _, row in top_out.iterrows()]
            )
    else:
        report.lines.append("*数据获取失败*")


def _add_lhb_section(report: ReportGenerator, data: Dict):
    report.add_section("🏢 龙虎榜机构资金", [])
    lhb = data.get("lhb", {})
    
    if "error" not in lhb:
        total_net = lhb.get("total_net", 0)
        signal = lhb.get("signal", "")
        icon = "🟢" if total_net > 0 else "🔴" if total_net < 0 else "⚪"
        
        report.add_table(["指标", "数值"], [["机构净买入总额", f"{total_net:,.0f}"], ["信号", f"{icon} {signal}"]])
        
        for title, key in [("热门做多行业", "hot_buy"), ("机构净卖出较多行业", "hot_sell")]:
            hot = lhb.get(key)
            if hot is not None and not hot.empty:
                report.lines.append(f"\n### {title}\n")
                report.lines.append("```")
                report.lines.append(hot.to_string())
                report.lines.append("```")
    else:
        report.lines.append("*数据获取失败*")


def _add_limitup_section(report: ReportGenerator, data: Dict):
    report.add_section("🚀 涨停板结构", [])
    limitup = data.get("limitup", {})
    
    if "error" not in limitup:
        struct = limitup.get("struct", {})
        breaks = limitup.get("breaks", {})
        env_score = limitup.get("env_score", 0)
        env_icon = "🟢" if env_score > 0 else "🔴" if env_score < 0 else "🟡"
        
        rows = [
            ["涨停家数", str(struct.get('total_zt', 0))],
            ["炸板家数", str(breaks.get('total_zb', 0))],
            ["最高板", f"{struct.get('max_lb', 0)}板"],
            ["涨停环境", f"{env_icon} {limitup.get('env_desc', '')}"]
        ]
        
        tier = limitup.get("tier", {})
        if tier:
            rows.extend([
                ["梯队结构", tier.get('tier_quality', '')],
                ["活跃板位", str(tier.get('active_tiers', []))],
                ["主导板位", f"{tier.get('dominant_tier', 0)}板"]
            ])
        
        graduation = limitup.get("graduation", {})
        if graduation:
            grad_status = graduation.get('graduation_status', '')
            grad_icon = "🔴" if "明显" in grad_status else "🟡" if "疑似" in grad_status else "🟢"
            rows.append(["毕业照判断", f"{grad_icon} {grad_status}"])
            
            if 'break_ratio' in graduation:
                rows.append(["炸板率", f"{graduation['break_ratio']:.1%}"])
            if 'top5_concentration' in graduation:
                rows.append(["行业集中度(前5)", f"{graduation['top5_concentration']:.1%}"])
        
        report.add_table(["指标", "数值"], rows)
        
        if graduation and graduation.get('graduation_signals'):
            report.lines.append("\n**毕业照信号**:")
            for signal in graduation['graduation_signals']:
                report.lines.append(f"- ⚠️ {signal}")
        
        lb_dist = struct.get("lb_dist", {})
        if lb_dist is not None and not lb_dist.empty:
            report.lines.append("\n### 连板分布\n")
            report.add_table(["连板数", "家数"], [[f"{int(lb)}板", str(int(count))] for lb, count in lb_dist.items()], "center")
        
        industry_top = struct.get("industry_top", {})
        if industry_top is not None and not industry_top.empty:
            report.lines.append("\n### 涨停行业集中度\n")
            report.add_table(["行业", "涨停数"], [[ind, str(int(count))] for ind, count in industry_top.items()])
    else:
        report.lines.append("*数据获取失败*")


def _add_sectors_section(report: ReportGenerator, sectors: Dict):
    report.add_section("🎯 板块机会分析", [])
    
    long_candidates = sectors.get("long_candidates", [])
    report.lines.append("### 🟢 三重共振的多头候选方向\n")
    if long_candidates:
        for ind in long_candidates:
            report.lines.append(f"- **{ind}**")
    else:
        report.lines.append("*无明显三重共振主线*")
    
    short_avoid = sectors.get("short_avoid", [])
    report.lines.append("\n### 🔴 需要回避的方向\n")
    if short_avoid:
        for ind in short_avoid:
            report.lines.append(f"- **{ind}**")
    else:
        report.lines.append("*暂无明显需要集体回避的行业*")
    
    if "limitup_struct" in sectors:
        struct = sectors["limitup_struct"]
        report.lines.append("\n### 📊 涨停板块结构\n")
        report.add_table(
            ["指标", "数值"],
            [["涨停家数", str(struct.get('total_zt', 0))], ["最高板", f"{struct.get('max_lb', 0)}板"]]
        )


def _add_external_section(report: ReportGenerator, data: Dict):
    report.add_section("🌏 外围市场", [])
    external = data.get("external", {})
    
    if "error" not in external:
        sh_index = external.get("sh_index", {})
        if sh_index:
            pct = sh_index.get('pct_change', 0)
            signal = sh_index.get('signal', '中性')
            icon = "🟢" if pct > 0 else "🔴" if pct < 0 else "⚪"
            report.add_table(["指数", "涨跌幅", "信号"], [["上证指数", f"{pct:+.2f}%", f"{icon} {signal}"]])
        else:
            report.lines.append("*数据获取失败*")
    else:
        report.lines.append("*数据获取失败*")


def save_report_to_file(report_str: str, date_str: str) -> str:
    """保存报告到Markdown文件"""
    main_logger = get_main_logger()
    
    Config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    
    file_name = f"{date_str}_market_sentiment.md"
    file_path = Config.RESULTS_DIR / file_name
    
    file_path.write_text(report_str, encoding="utf-8")
    main_logger.info(f"报告已保存到: {file_path}")
    print(f"报告已保存到: {file_path}")
    return file_name


def check_and_archive_folders():
    """检查文件夹大小并在需要时打包"""
    main_logger = get_main_logger()
    
    archiver = FileArchiver(
        logs_dir=Config.LOGS_DIR,
        results_dir=Config.RESULTS_DIR,
        archive_dir=Config.ARCHIVE_DIR
    )
    
    results = archiver.check_all_folders()
    
    for folder_name, archive_path in results.items():
        if archive_path:
            main_logger.info(f"{folder_name} 文件夹已打包: {archive_path}")
    
    return results


def main():
    init_directories()
    
    main_logger = get_main_logger()
    data_logger = get_data_logger()
    
    main_logger.info("=" * 60)
    main_logger.info("开始执行市场情绪分析")
    main_logger.info(f"北京时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
    main_logger.info("=" * 60)
    
    try:
        date_str = get_trade_date()
        print(f"分析日期: {date_str}")
        
        data = fetch_all_data(date_str)
        scores = calculate_scores(data)
        sectors = analyze_sectors(data)
        opening_pred = predict_market_opening(scores, data)
        
        report, file_name = generate_report(data, scores, sectors, date_str, opening_pred)
        print(report)
        
        record_file = data_logger.save_session_record()
        main_logger.info(f"数据获取记录已保存: {record_file}")
        
        check_and_archive_folders()
        
        main_logger.info("=" * 60)
        main_logger.info("市场情绪分析执行完成")
        main_logger.info("=" * 60)
        
    except Exception as e:
        main_logger.error(f"执行失败: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
