"""
自适应评分模块
基于历史复盘结果动态调整多空评分权重
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass, asdict
from collections import defaultdict

ADAPTIVE_DIR = Path(__file__).parent.parent / "data" / "adaptive"
METRICS = ["margin", "if_main", "im_main", "lhb", "limitup", "external"]

DEFAULT_WEIGHTS = {
    "margin": 1.0,
    "if_main": 1.0,
    "im_main": 1.0,
    "lhb": 1.0,
    "limitup": 1.0,
    "external": 0.1
}

MARKET_STATE_CONFIG = {
    "bull": {
        "margin": 1.2,
        "if_main": 0.8,
        "im_main": 1.3,
        "lhb": 1.2,
        "limitup": 1.3,
        "external": 0.1
    },
    "bear": {
        "margin": 1.3,
        "if_main": 1.3,
        "im_main": 0.8,
        "lhb": 0.8,
        "limitup": 0.7,
        "external": 0.15
    },
    "oscillation": {
        "margin": 1.0,
        "if_main": 1.0,
        "im_main": 1.0,
        "lhb": 1.0,
        "limitup": 1.0,
        "external": 0.1
    }
}


def get_optimized_base_weights() -> Dict[str, float]:
    """
    获取优化后的基础权重
    
    Returns:
        优化后的权重字典
    """
    try:
        from src.parameter_optimizer import load_optimized_params
        params = load_optimized_params()
        weights = params.get("weights", DEFAULT_WEIGHTS)
        return weights
    except Exception:
        return DEFAULT_WEIGHTS.copy()


@dataclass
class MetricRecord:
    """
    单个指标的预测记录
    """
    date: str
    metric: str
    prediction: str
    actual_result: str
    is_correct: bool


@dataclass
class AdaptiveWeights:
    """
    自适应权重配置
    """
    base_weights: Dict[str, float]
    accuracy_weights: Dict[str, float]
    market_state: str
    market_state_weights: Dict[str, float]
    final_weights: Dict[str, float]
    accuracy_stats: Dict[str, Any]
    adjustment_reason: str


def init_adaptive_dir():
    """初始化自适应数据目录"""
    ADAPTIVE_DIR.mkdir(parents=True, exist_ok=True)


def get_metric_history_file() -> Path:
    """获取指标历史记录文件路径"""
    return ADAPTIVE_DIR / "metric_history.json"


def load_metric_history() -> List[Dict]:
    """
    加载指标历史记录
    
    Returns:
        指标历史记录列表
    """
    init_adaptive_dir()
    file_path = get_metric_history_file()
    if not file_path.exists():
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        return []


def save_metric_history(history: List[Dict]):
    """
    保存指标历史记录
    
    Args:
        history: 指标历史记录列表
    """
    init_adaptive_dir()
    file_path = get_metric_history_file()
    
    def convert_types(obj):
        if isinstance(obj, (bool, int, float, str)):
            return obj
        if isinstance(obj, dict):
            return {k: convert_types(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert_types(i) for i in obj]
        return str(obj)
    
    serialized_history = convert_types(history)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(serialized_history, f, ensure_ascii=False, indent=2)


def record_metric_result(date: str, metric: str, prediction: str, 
                         actual_result: str, is_correct: bool):
    """
    记录单个指标的预测结果
    
    Args:
        date: 日期
        metric: 指标名称
        prediction: 预测方向 (多/空/中性)
        actual_result: 实际结果
        is_correct: 是否预测正确
    """
    history = load_metric_history()
    record = {
        "date": date,
        "metric": metric,
        "prediction": prediction,
        "actual_result": actual_result,
        "is_correct": bool(is_correct)
    }
    history.append(record)
    
    if len(history) > 500:
        history = history[-500:]
    
    save_metric_history(history)


def calculate_metric_accuracy(days: int = 30) -> Dict[str, Dict[str, float]]:
    """
    计算各指标的历史准确率
    
    Args:
        days: 统计天数
    
    Returns:
        各指标的准确率统计
    """
    history = load_metric_history()
    if not history:
        return {m: {"accuracy": 0.5, "count": 0, "streak": 0} for m in METRICS}
    
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    recent_history = [r for r in history if r["date"] >= cutoff_date]
    
    stats = {}
    for metric in METRICS:
        metric_records = [r for r in recent_history if r["metric"] == metric]
        
        if not metric_records:
            stats[metric] = {"accuracy": 0.5, "count": 0, "streak": 0}
            continue
        
        correct_count = sum(1 for r in metric_records if r["is_correct"])
        total_count = len(metric_records)
        accuracy = correct_count / total_count if total_count > 0 else 0.5
        
        streak = 0
        sorted_records = sorted(metric_records, key=lambda x: x["date"], reverse=True)
        for r in sorted_records:
            if r["is_correct"]:
                streak += 1
            else:
                break
        
        stats[metric] = {
            "accuracy": round(accuracy, 3),
            "count": total_count,
            "streak": streak
        }
    
    return stats


def detect_market_state(data: Dict[str, Any]) -> Tuple[str, str]:
    """
    检测当前市场状态
    
    Args:
        data: 市场数据
    
    Returns:
        (市场状态, 判断理由)
    """
    limitup = data.get("limitup", {})
    margin = data.get("margin", {})
    
    if "error" in limitup:
        return "oscillation", "数据不足，使用震荡市配置"
    
    total_zt = limitup.get("struct", {}).get("total_zt", 0)
    max_lb = limitup.get("struct", {}).get("max_lb", 0)
    env_score = limitup.get("env_score", 0)
    
    margin_delta = margin.get("delta", 0) if "error" not in margin else 0
    
    bull_signals = 0
    bear_signals = 0
    
    if total_zt > 80:
        bull_signals += 1
    elif total_zt < 40:
        bear_signals += 1
    
    if max_lb >= 5:
        bull_signals += 1
    elif max_lb <= 2:
        bear_signals += 1
    
    if env_score > 0:
        bull_signals += 1
    elif env_score < 0:
        bear_signals += 1
    
    if margin_delta > 0:
        bull_signals += 0.5
    else:
        bear_signals += 0.5
    
    if bull_signals >= 3:
        return "bull", f"牛市特征: 涨停{total_zt}家, 最高{max_lb}板, 情绪强"
    elif bear_signals >= 3:
        return "bear", f"熊市特征: 涨停{total_zt}家, 最高{max_lb}板, 情绪弱"
    else:
        return "oscillation", f"震荡市特征: 涨停{total_zt}家, 最高{max_lb}板"


def calculate_accuracy_based_weights(accuracy_stats: Dict[str, Dict]) -> Dict[str, float]:
    """
    基于历史准确率计算权重调整系数
    
    Args:
        accuracy_stats: 各指标的准确率统计
    
    Returns:
        各指标的权重调整系数
    """
    weights = {}
    
    for metric in METRICS:
        stats = accuracy_stats.get(metric, {"accuracy": 0.5, "count": 0, "streak": 0})
        accuracy = stats["accuracy"]
        count = stats["count"]
        streak = stats["streak"]
        
        if count < 3:
            weights[metric] = 1.0
            continue
        
        accuracy_factor = 0.5 + accuracy
        
        streak_factor = 1.0
        if streak >= 5:
            streak_factor = 1.15
        elif streak >= 3:
            streak_factor = 1.08
        elif streak <= -3:
            streak_factor = 0.85
        elif streak <= -5:
            streak_factor = 0.7
        
        confidence_factor = min(1.0, count / 10)
        
        weights[metric] = round(accuracy_factor * streak_factor * confidence_factor + 
                                1.0 * (1 - confidence_factor), 3)
    
    return weights


def get_adaptive_weights(data: Dict[str, Any], days: int = 30) -> AdaptiveWeights:
    """
    获取自适应权重配置
    
    Args:
        data: 市场数据
        days: 历史统计天数
    
    Returns:
        自适应权重配置对象
    """
    base_weights = get_optimized_base_weights()
    
    accuracy_stats = calculate_metric_accuracy(days=days)
    accuracy_weights = calculate_accuracy_based_weights(accuracy_stats)
    
    market_state, state_reason = detect_market_state(data)
    market_state_weights = MARKET_STATE_CONFIG.get(market_state, MARKET_STATE_CONFIG["oscillation"])
    
    final_weights = {}
    adjustment_parts = []
    
    for metric in METRICS:
        base = base_weights.get(metric, DEFAULT_WEIGHTS.get(metric, 1.0))
        acc_adj = accuracy_weights.get(metric, 1.0)
        state_adj = market_state_weights.get(metric, 1.0)
        
        final = round(base * acc_adj * state_adj, 3)
        final_weights[metric] = final
        
        if acc_adj != 1.0 or state_adj != 1.0:
            adjustments = []
            if acc_adj != 1.0:
                adjustments.append(f"准确率调整×{acc_adj}")
            if state_adj != 1.0:
                adjustments.append(f"市场状态调整×{state_adj}")
            adjustment_parts.append(f"{metric}: {' + '.join(adjustments)}")
    
    adjustment_reason = f"市场状态: {market_state} ({state_reason})"
    if adjustment_parts:
        adjustment_reason += f" | 权重调整: {'; '.join(adjustment_parts)}"
    
    return AdaptiveWeights(
        base_weights=base_weights.copy(),
        accuracy_weights=accuracy_weights,
        market_state=market_state,
        market_state_weights=market_state_weights,
        final_weights=final_weights,
        accuracy_stats=accuracy_stats,
        adjustment_reason=adjustment_reason
    )


def calculate_adaptive_scores(data: Dict[str, Any], 
                              adaptive_weights: AdaptiveWeights) -> Dict[str, float]:
    """
    使用自适应权重计算多空分数
    
    Args:
        data: 市场数据
        adaptive_weights: 自适应权重配置
    
    Returns:
        多空分数 {"long": x, "short": y}
    """
    scores = {"long": 0.0, "short": 0.0}
    weights = adaptive_weights.final_weights
    
    score_rules = [
        ("margin", lambda d: d.get("delta", 0)),
        ("if_main", lambda d: d.get("signal", "")),
        ("im_main", lambda d: d.get("signal", "")),
        ("lhb", lambda d: d.get("total_net", 0)),
        ("limitup", lambda d: d.get("env_score", 0)),
    ]
    
    for key, getter in score_rules:
        if key in data and "error" not in data[key]:
            value = getter(data[key])
            weight = weights.get(key, 1.0)
            
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
        weight = weights.get("external", 0.1)
        if pct > 0:
            scores["long"] += weight
        elif pct < 0:
            scores["short"] += weight
    
    return scores


def update_metric_history_from_review(pred_date: str, actual: Any, 
                                       data: Dict[str, Any]):
    """
    根据复盘结果更新各指标的历史记录
    
    Args:
        pred_date: 预测日期
        actual: 实际表现数据
        data: 预测时的市场数据
    """
    actual_closing_pct = actual.closing_pct if hasattr(actual, 'closing_pct') else 0
    
    for metric in METRICS:
        if metric not in data or "error" in data.get(metric, {}):
            continue
        
        metric_data = data[metric]
        
        if metric == "margin":
            prediction = "多" if metric_data.get("delta", 0) > 0 else "空"
        elif metric in ["if_main", "im_main"]:
            prediction = metric_data.get("signal", "中性")
        elif metric == "lhb":
            prediction = "多" if metric_data.get("total_net", 0) > 0 else "空"
        elif metric == "limitup":
            env_score = metric_data.get("env_score", 0)
            prediction = "多" if env_score > 0 else "空" if env_score < 0 else "中性"
        elif metric == "external":
            pct = metric_data.get("sh_index", {}).get("pct_change", 0)
            prediction = "多" if pct > 0 else "空" if pct < 0 else "中性"
        else:
            continue
        
        if prediction == "中性":
            is_correct = abs(actual_closing_pct) < 0.5
        elif prediction == "多":
            is_correct = actual_closing_pct > 0
        else:
            is_correct = actual_closing_pct < 0
        
        record_metric_result(
            date=pred_date,
            metric=metric,
            prediction=prediction,
            actual_result=f"收盘{actual_closing_pct:+.2f}%",
            is_correct=is_correct
        )


def format_adaptive_weights_report(weights: AdaptiveWeights) -> str:
    """
    格式化自适应权重报告
    
    Args:
        weights: 自适应权重配置
    
    Returns:
        Markdown格式的报告
    """
    lines = []
    
    lines.append("## ⚖️ 自适应权重调整")
    lines.append("")
    
    lines.append(f"**市场状态**: {weights.market_state}")
    lines.append(f"**调整原因**: {weights.adjustment_reason}")
    lines.append("")
    
    lines.append("### 权重对比")
    lines.append("")
    lines.append("| 指标 | 基础权重 | 准确率系数 | 状态系数 | 最终权重 |")
    lines.append("|:-----|:--------:|:----------:|:--------:|:--------:|")
    
    metric_names = {
        "margin": "融资融券",
        "if_main": "沪深300期货",
        "im_main": "中证1000期货",
        "lhb": "龙虎榜",
        "limitup": "涨停环境",
        "external": "外围市场"
    }
    
    for metric in METRICS:
        name = metric_names.get(metric, metric)
        base = weights.base_weights.get(metric, 1.0)
        acc = weights.accuracy_weights.get(metric, 1.0)
        state = weights.market_state_weights.get(metric, 1.0)
        final = weights.final_weights.get(metric, 1.0)
        
        changed = "✓" if abs(final - base) > 0.05 else ""
        lines.append(f"| {name} | {base} | ×{acc} | ×{state} | **{final}** {changed} |")
    
    lines.append("")
    
    stats = weights.accuracy_stats
    if any(s["count"] > 0 for s in stats.values()):
        lines.append("### 历史准确率统计 (近30日)")
        lines.append("")
        lines.append("| 指标 | 准确率 | 样本数 | 连续正确 |")
        lines.append("|:-----|:------:|:------:|:--------:|")
        
        for metric in METRICS:
            name = metric_names.get(metric, metric)
            s = stats.get(metric, {"accuracy": 0, "count": 0, "streak": 0})
            acc_pct = f"{s['accuracy']*100:.1f}%" if s['count'] > 0 else "-"
            lines.append(f"| {name} | {acc_pct} | {s['count']} | {s['streak']} |")
    
    return "\n".join(lines)
