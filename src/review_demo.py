"""
复盘验证模块
用于验证前一天的分析预测是否准确
"""

import json
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass, asdict

PREDICTION_DIR = Path(__file__).parent.parent / "data" / "predictions"


@dataclass
class PredictionRecord:
    """
    预测记录数据结构
    """
    date: str
    opening_pred: str
    confidence: str
    strategy: str
    strategy_reason: str
    long_score: float
    short_score: float
    env_score: int
    conclusion: str


@dataclass
class ActualPerformance:
    """
    实际市场表现数据结构
    """
    date: str
    opening_pct: float
    closing_pct: float
    limitup_count: int
    limitdown_count: int
    market_sentiment: str
    sentiment_score: int
    im_pct: float
    if_pct: float


@dataclass
class ReviewResult:
    """
    复盘验证结果数据结构
    """
    pred_date: str
    actual_date: str
    opening_match: bool
    opening_score: int
    strategy_match: bool
    strategy_score: int
    direction_match: bool
    direction_score: int
    overall_score: int
    details: Dict[str, Any]


def init_prediction_dir():
    """初始化预测记录目录"""
    PREDICTION_DIR.mkdir(parents=True, exist_ok=True)


def save_prediction(record: PredictionRecord) -> str:
    """
    保存预测记录到JSON文件
    
    Args:
        record: 预测记录对象
    
    Returns:
        保存的文件路径
    """
    init_prediction_dir()
    file_path = PREDICTION_DIR / f"{record.date}_prediction.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(asdict(record), f, ensure_ascii=False, indent=2)
    return str(file_path)


def load_prediction(date_str: str) -> Optional[PredictionRecord]:
    """
    加载指定日期的预测记录
    
    Args:
        date_str: 日期字符串 (YYYYMMDD格式)
    
    Returns:
        预测记录对象，如果不存在返回None
    """
    file_path = PREDICTION_DIR / f"{date_str}_prediction.json"
    if not file_path.exists():
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return PredictionRecord(**data)


def get_previous_trade_date(current_date: str) -> Optional[str]:
    """
    获取前一个交易日
    
    Args:
        current_date: 当前日期 (YYYYMMDD格式)
    
    Returns:
        前一个交易日字符串，如果找不到返回None
    """
    try:
        date_obj = datetime.strptime(current_date, "%Y%m%d")
    except ValueError:
        return None
    
    for i in range(1, 15):
        check_date = date_obj - timedelta(days=i)
        check_str = check_date.strftime("%Y%m%d")
        try:
            df = ak.stock_zt_pool_em(date=check_str)
            if not df.empty:
                return check_str
        except Exception:
            continue
    
    return None


def fetch_actual_opening(date_str: str) -> Tuple[float, float]:
    """
    获取实际开盘涨跌幅和收盘涨跌幅
    
    Args:
        date_str: 日期字符串 (YYYYMMDD格式)
    
    Returns:
        (开盘涨跌幅%, 收盘涨跌幅%)
    """
    try:
        df = ak.stock_zh_index_daily(symbol="sh000001")
        df['date'] = pd.to_datetime(df['date'])
        target_date = pd.to_datetime(date_str)
        
        df_day = df[df['date'].dt.strftime('%Y%m%d') == date_str]
        
        if len(df_day) == 0:
            df_recent = df[df['date'] <= target_date].tail(2)
            if len(df_recent) >= 2:
                prev_close = df_recent.iloc[-2]['close']
                today_close = df_recent.iloc[-1]['close']
                closing_pct = (today_close / prev_close - 1) * 100
                opening_pct = closing_pct
                return opening_pct, closing_pct
            return 0.0, 0.0
        
        today_row = df_day.iloc[0]
        prev_row = df[df['date'] < df_day.iloc[0]['date']].iloc[-1]
        
        prev_close = prev_row['close']
        today_open = today_row['open']
        today_close = today_row['close']
        
        opening_pct = (today_open / prev_close - 1) * 100
        closing_pct = (today_close / prev_close - 1) * 100
        
        return opening_pct, closing_pct
    except Exception as e:
        print(f"获取开盘数据失败: {e}")
        return 0.0, 0.0


def fetch_limitup_limitdown_count(date_str: str) -> Tuple[int, int]:
    """
    获取涨停和跌停家数
    
    Args:
        date_str: 日期字符串 (YYYYMMDD格式)
    
    Returns:
        (涨停家数, 跌停家数)
    """
    limitup_count = 0
    limitdown_count = 0
    
    try:
        df_zt = ak.stock_zt_pool_em(date=date_str)
        if not df_zt.empty:
            limitup_count = len(df_zt)
    except Exception:
        pass
    
    try:
        df_dt = ak.stock_zt_pool_dtgc_em(date=date_str)
        if not df_dt.empty:
            limitdown_count = len(df_dt)
    except Exception:
        pass
    
    return limitup_count, limitdown_count


def fetch_futures_performance(date_str: str) -> Tuple[float, float]:
    """
    获取股指期货涨跌幅（IM和IF）
    
    Args:
        date_str: 日期字符串 (YYYYMMDD格式)
    
    Returns:
        (IM涨跌幅%, IF涨跌幅%)
    """
    im_pct = 0.0
    if_pct = 0.0
    
    try:
        df_im = ak.futures_main_sina(symbol="IM0")
        df_im['date'] = pd.to_datetime(df_im['date'])
        target_date = pd.to_datetime(date_str)
        df_day = df_im[df_im['date'].dt.strftime('%Y%m%d') == date_str]
        if len(df_day) >= 1:
            prev_row = df_im[df_im['date'] < df_day.iloc[0]['date']].iloc[-1]
            im_pct = (df_day.iloc[0]['close'] / prev_row['close'] - 1) * 100
    except Exception:
        pass
    
    try:
        df_if = ak.futures_main_sina(symbol="IF0")
        df_if['date'] = pd.to_datetime(df_if['date'])
        target_date = pd.to_datetime(date_str)
        df_day = df_if[df_if['date'].dt.strftime('%Y%m%d') == date_str]
        if len(df_day) >= 1:
            prev_row = df_if[df_if['date'] < df_day.iloc[0]['date']].iloc[-1]
            if_pct = (df_day.iloc[0]['close'] / prev_row['close'] - 1) * 100
    except Exception:
        pass
    
    return im_pct, if_pct


def evaluate_market_sentiment(limitup_count: int, limitdown_count: int, 
                               closing_pct: float) -> Tuple[str, int]:
    """
    评价大盘体感
    
    Args:
        limitup_count: 涨停家数
        limitdown_count: 跌停家数
        closing_pct: 收盘涨跌幅
    
    Returns:
        (体感等级, 体感分数0-100)
    """
    score = 50
    
    if limitup_count > 100:
        score += 20
    elif limitup_count > 80:
        score += 15
    elif limitup_count > 50:
        score += 5
    elif limitup_count < 30:
        score -= 15
    
    if limitdown_count < 10:
        score += 15
    elif limitdown_count < 20:
        score += 10
    elif limitdown_count < 50:
        score += 0
    elif limitdown_count > 100:
        score -= 20
    else:
        score -= 10
    
    if closing_pct > 1.5:
        score += 20
    elif closing_pct > 1.0:
        score += 15
    elif closing_pct > 0.5:
        score += 10
    elif closing_pct > 0:
        score += 5
    elif closing_pct < -1.5:
        score -= 20
    elif closing_pct < -1.0:
        score -= 15
    elif closing_pct < -0.5:
        score -= 10
    elif closing_pct < 0:
        score -= 5
    
    score = max(0, min(100, score))
    
    if score >= 80:
        sentiment = "很好"
    elif score >= 65:
        sentiment = "良好"
    elif score >= 45:
        sentiment = "一般"
    elif score >= 30:
        sentiment = "较差"
    else:
        sentiment = "很差"
    
    return sentiment, score


def fetch_actual_performance(date_str: str) -> ActualPerformance:
    """
    获取指定日期的实际市场表现
    
    Args:
        date_str: 日期字符串 (YYYYMMDD格式)
    
    Returns:
        实际表现数据对象
    """
    opening_pct, closing_pct = fetch_actual_opening(date_str)
    limitup_count, limitdown_count = fetch_limitup_limitdown_count(date_str)
    im_pct, if_pct = fetch_futures_performance(date_str)
    sentiment, sentiment_score = evaluate_market_sentiment(
        limitup_count, limitdown_count, closing_pct
    )
    
    return ActualPerformance(
        date=date_str,
        opening_pct=opening_pct,
        closing_pct=closing_pct,
        limitup_count=limitup_count,
        limitdown_count=limitdown_count,
        market_sentiment=sentiment,
        sentiment_score=sentiment_score,
        im_pct=im_pct,
        if_pct=if_pct
    )


def verify_opening_prediction(pred: PredictionRecord, actual: ActualPerformance) -> Tuple[bool, int]:
    """
    验证开盘预测是否准确
    
    Args:
        pred: 预测记录
        actual: 实际表现
    
    Returns:
        (是否准确, 得分0-100)
    """
    pred_opening = pred.opening_pred
    actual_opening_pct = actual.opening_pct
    
    if "高开" in pred_opening:
        if actual_opening_pct > 0.3:
            return True, 100
        elif actual_opening_pct > 0:
            return True, 70
        elif actual_opening_pct > -0.3:
            return False, 40
        else:
            return False, 0
    elif "低开" in pred_opening:
        if actual_opening_pct < -0.3:
            return True, 100
        elif actual_opening_pct < 0:
            return True, 70
        elif actual_opening_pct < 0.3:
            return False, 40
        else:
            return False, 0
    else:
        if abs(actual_opening_pct) < 0.3:
            return True, 100
        elif abs(actual_opening_pct) < 0.5:
            return True, 70
        else:
            return False, 30


def verify_strategy_prediction(pred: PredictionRecord, actual: ActualPerformance) -> Tuple[bool, int]:
    """
    验证策略预测是否有效
    
    Args:
        pred: 预测记录
        actual: 实际表现
    
    Returns:
        (是否有效, 得分0-100)
    """
    strategy = pred.strategy
    im_pct = actual.im_pct
    if_pct = actual.if_pct
    
    if "黄线" in strategy:
        if im_pct > if_pct and im_pct > 0:
            return True, 100
        elif im_pct > if_pct:
            return True, 70
        elif im_pct > 0:
            return True, 60
        else:
            return False, 30
    elif "白线" in strategy:
        if if_pct > im_pct and if_pct > 0:
            return True, 100
        elif if_pct > im_pct:
            return True, 70
        elif if_pct > 0:
            return True, 60
        else:
            return False, 30
    else:
        if abs(im_pct - if_pct) < 0.5:
            return True, 80
        else:
            return True, 50


def verify_direction_prediction(pred: PredictionRecord, actual: ActualPerformance) -> Tuple[bool, int]:
    """
    验证多空方向判断是否准确
    
    Args:
        pred: 预测记录
        actual: 实际表现
    
    Returns:
        (是否准确, 得分0-100)
    """
    long_score = pred.long_score
    short_score = pred.short_score
    closing_pct = actual.closing_pct
    
    if long_score >= 2 * short_score and long_score >= 3:
        if closing_pct > 0.5:
            return True, 100
        elif closing_pct > 0:
            return True, 70
        else:
            return False, 30
    elif short_score >= 2 * long_score and short_score >= 3:
        if closing_pct < -0.5:
            return True, 100
        elif closing_pct < 0:
            return True, 70
        else:
            return False, 30
    else:
        if abs(closing_pct) < 0.5:
            return True, 80
        elif closing_pct > 0 and long_score > short_score:
            return True, 70
        elif closing_pct < 0 and short_score > long_score:
            return True, 70
        else:
            return True, 50


def perform_review(pred_date: str, actual_date: str) -> Optional[ReviewResult]:
    """
    执行复盘验证
    
    Args:
        pred_date: 预测日期 (前一天的分析日期)
        actual_date: 实际日期 (今天的验证日期)
    
    Returns:
        复盘结果，如果预测记录不存在返回None
    """
    pred = load_prediction(pred_date)
    if pred is None:
        print(f"未找到 {pred_date} 的预测记录")
        return None
    
    actual = fetch_actual_performance(actual_date)
    
    opening_match, opening_score = verify_opening_prediction(pred, actual)
    strategy_match, strategy_score = verify_strategy_prediction(pred, actual)
    direction_match, direction_score = verify_direction_prediction(pred, actual)
    
    overall_score = int((opening_score * 0.35 + strategy_score * 0.35 + direction_score * 0.30))
    
    return ReviewResult(
        pred_date=pred_date,
        actual_date=actual_date,
        opening_match=opening_match,
        opening_score=opening_score,
        strategy_match=strategy_match,
        strategy_score=strategy_score,
        direction_match=direction_match,
        direction_score=direction_score,
        overall_score=overall_score,
        details={
            "prediction": asdict(pred),
            "actual": asdict(actual)
        }
    )


def get_review_history_stats(days: int = 7) -> Dict[str, Any]:
    """
    获取历史复盘统计
    
    Args:
        days: 统计天数
    
    Returns:
        统计结果
    """
    init_prediction_dir()
    
    today = datetime.now()
    stats = {
        "total_reviews": 0,
        "opening_accuracy": 0.0,
        "strategy_accuracy": 0.0,
        "direction_accuracy": 0.0,
        "avg_overall_score": 0.0,
        "reviews": []
    }
    
    opening_correct = 0
    strategy_correct = 0
    direction_correct = 0
    total_score = 0
    
    for i in range(1, days + 1):
        check_date = today - timedelta(days=i)
        check_str = check_date.strftime("%Y%m%d")
        
        pred = load_prediction(check_str)
        if pred is None:
            continue
        
        prev_date = check_date - timedelta(days=1)
        prev_str = prev_date.strftime("%Y%m%d")
        
        result = perform_review(prev_str, check_str)
        if result is None:
            continue
        
        stats["total_reviews"] += 1
        if result.opening_match:
            opening_correct += 1
        if result.strategy_match:
            strategy_correct += 1
        if result.direction_match:
            direction_correct += 1
        total_score += result.overall_score
        
        stats["reviews"].append({
            "pred_date": result.pred_date,
            "actual_date": result.actual_date,
            "opening_match": result.opening_match,
            "strategy_match": result.strategy_match,
            "direction_match": result.direction_match,
            "overall_score": result.overall_score
        })
    
    if stats["total_reviews"] > 0:
        stats["opening_accuracy"] = round(opening_correct / stats["total_reviews"] * 100, 1)
        stats["strategy_accuracy"] = round(strategy_correct / stats["total_reviews"] * 100, 1)
        stats["direction_accuracy"] = round(direction_correct / stats["total_reviews"] * 100, 1)
        stats["avg_overall_score"] = round(total_score / stats["total_reviews"], 1)
    
    return stats


def format_review_report(result: ReviewResult, stats: Dict[str, Any] = None) -> str:
    """
    格式化复盘报告为Markdown
    
    Args:
        result: 复盘结果
        stats: 历史统计（可选）
    
    Returns:
        Markdown格式的报告
    """
    lines = []
    
    lines.append("## 🔙 前一日复盘验证")
    lines.append("")
    
    pred = result.details["prediction"]
    actual = result.details["actual"]
    
    lines.append(f"### 预测回顾 ({result.pred_date})")
    lines.append("")
    lines.append("| 项目 | 预测内容 |")
    lines.append("|:-----|:---------|")
    lines.append(f"| 开盘方向 | **{pred['opening_pred']}** (置信度: {pred['confidence']}) |")
    lines.append(f"| 操作策略 | **{pred['strategy']}** |")
    lines.append(f"| 策略原因 | {pred['strategy_reason']} |")
    lines.append(f"| 多空分数 | 多方 {pred['long_score']:.1f} vs 空方 {pred['short_score']:.1f} |")
    lines.append(f"| 方向结论 | {pred['conclusion']} |")
    lines.append("")
    
    lines.append(f"### 今日实际 ({result.actual_date})")
    lines.append("")
    lines.append("| 项目 | 实际数值 |")
    lines.append("|:-----|:---------|")
    
    opening_icon = "🟢" if actual['opening_pct'] > 0 else "🔴" if actual['opening_pct'] < 0 else "⚪"
    lines.append(f"| 实际开盘 | {opening_icon} {actual['opening_pct']:+.2f}% |")
    
    closing_icon = "🟢" if actual['closing_pct'] > 0 else "🔴" if actual['closing_pct'] < 0 else "⚪"
    lines.append(f"| 收盘涨跌 | {closing_icon} {actual['closing_pct']:+.2f}% |")
    
    sentiment_icon = "🟢" if actual['sentiment_score'] >= 65 else "🟡" if actual['sentiment_score'] >= 45 else "🔴"
    lines.append(f"| 大盘体感 | {sentiment_icon} **{actual['market_sentiment']}** (分数: {actual['sentiment_score']}) |")
    lines.append(f"| 涨停/跌停 | {actual['limitup_count']} / {actual['limitdown_count']} |")
    lines.append(f"| 中证1000 | {actual['im_pct']:+.2f}% |")
    lines.append(f"| 沪深300 | {actual['if_pct']:+.2f}% |")
    lines.append("")
    
    lines.append("### 验证结果")
    lines.append("")
    lines.append("| 验证项 | 结果 | 得分 |")
    lines.append("|:-------|:-----|:----:|")
    
    opening_result = "✅ 准确" if result.opening_match else "❌ 不准确"
    lines.append(f"| 开盘预测 | {opening_result} | {result.opening_score} |")
    
    strategy_result = "✅ 有效" if result.strategy_match else "❌ 无效"
    lines.append(f"| 策略建议 | {strategy_result} | {result.strategy_score} |")
    
    direction_result = "✅ 准确" if result.direction_match else "❌ 不准确"
    lines.append(f"| 方向判断 | {direction_result} | {result.direction_score} |")
    lines.append("")
    
    overall_icon = "🟢" if result.overall_score >= 70 else "🟡" if result.overall_score >= 50 else "🔴"
    lines.append(f"### 综合评分: {overall_icon} **{result.overall_score}分**")
    lines.append("")
    
    if stats and stats["total_reviews"] > 0:
        lines.append("### 📊 历史准确率统计")
        lines.append("")
        lines.append("| 周期 | 开盘预测 | 策略建议 | 方向判断 | 综合均分 |")
        lines.append("|:----:|:--------:|:--------:|:--------:|:--------:|")
        lines.append(f"| 近{stats['total_reviews']}日 | {stats['opening_accuracy']}% | {stats['strategy_accuracy']}% | {stats['direction_accuracy']}% | {stats['avg_overall_score']} |")
    
    return "\n".join(lines)
