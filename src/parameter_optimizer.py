"""
参数优化模块
使用贝叶斯优化自动调整评分参数
"""

import json
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass, asdict
from itertools import product
import warnings

from src.data_cache import (
    fetch_all_historical_data, calculate_daily_returns, 
    calculate_daily_opening_change, CACHE_DIR
)

OPTIMIZER_DIR = Path(__file__).parent.parent / "data" / "optimizer"

METRICS = ["margin", "if_main", "im_main", "lhb", "limitup", "external"]

DEFAULT_PARAMS = {
    "weights": {
        "margin": 1.0,
        "if_main": 1.0,
        "im_main": 1.0,
        "lhb": 1.0,
        "limitup": 1.0,
        "external": 0.1
    },
    "thresholds": {
        "strong_ratio": 2.0,
        "opening_high": 0.3,
        "opening_low": -0.3
    },
    "market_state": {
        "bull": {
            "margin": 1.2, "if_main": 0.8, "im_main": 1.3,
            "lhb": 1.2, "limitup": 1.3, "external": 0.1
        },
        "bear": {
            "margin": 1.3, "if_main": 1.3, "im_main": 0.8,
            "lhb": 0.8, "limitup": 0.7, "external": 0.15
        }
    }
}

PARAM_BOUNDS = {
    "weights": {
        "margin": (0.5, 2.0),
        "if_main": (0.5, 2.0),
        "im_main": (0.5, 2.0),
        "lhb": (0.5, 2.0),
        "limitup": (0.5, 2.0),
        "external": (0.05, 0.3)
    },
    "thresholds": {
        "strong_ratio": (1.5, 3.0),
        "opening_high": (0.1, 0.5),
        "opening_low": (-0.5, -0.1)
    }
}


@dataclass
class OptimizationResult:
    """
    优化结果数据结构
    """
    timestamp: str
    train_period: Tuple[str, str]
    val_period: Tuple[str, str]
    test_period: Tuple[str, str]
    best_params: Dict[str, Any]
    train_score: float
    val_score: float
    test_score: float
    improvement: float
    iterations: int


def init_optimizer_dir():
    """初始化优化器目录"""
    OPTIMIZER_DIR.mkdir(parents=True, exist_ok=True)


def get_optimized_params_file() -> Path:
    """获取优化参数文件路径"""
    return OPTIMIZER_DIR / "optimized_params.json"


def get_optimization_history_file() -> Path:
    """获取优化历史文件路径"""
    return OPTIMIZER_DIR / "optimization_history.json"


def save_optimized_params(params: Dict[str, Any]):
    """
    保存优化后的参数
    
    Args:
        params: 优化后的参数
    """
    init_optimizer_dir()
    file_path = get_optimized_params_file()
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(params, f, ensure_ascii=False, indent=2)


def load_optimized_params() -> Dict[str, Any]:
    """
    加载优化后的参数
    
    Returns:
        优化后的参数字典
    """
    file_path = get_optimized_params_file()
    if not file_path.exists():
        return DEFAULT_PARAMS.copy()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return DEFAULT_PARAMS.copy()


def save_optimization_result(result: OptimizationResult):
    """
    保存优化结果到历史记录
    
    Args:
        result: 优化结果
    """
    init_optimizer_dir()
    file_path = get_optimization_history_file()
    
    history = []
    if file_path.exists():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            history = []
    
    history.append(asdict(result))
    
    if len(history) > 50:
        history = history[-50:]
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2, default=str)


def prepare_training_data(historical_data: Dict[str, Any]) -> Tuple[List[Dict], List[float], List[float]]:
    """
    准备训练数据
    
    Args:
        historical_data: 历史数据
    
    Returns:
        (特征列表, 收益率列表, 开盘涨跌幅列表)
    """
    trade_dates = historical_data.get("trade_dates", [])
    limitup_data = historical_data.get("limitup", {})
    index_data = historical_data.get("index", {})
    margin_data = historical_data.get("margin", {})
    futures_data = historical_data.get("futures", {})
    
    print(f"   交易日数量: {len(trade_dates)}")
    print(f"   涨停数据: {len(limitup_data)} 天")
    print(f"   指数数据: {len(index_data)} 天")
    print(f"   融资融券数据: {len(margin_data)} 天")
    print(f"   期货数据: IF {len(futures_data.get('IF', {}))} 天, IM {len(futures_data.get('IM', {}))} 天")
    
    daily_returns = calculate_daily_returns(index_data)
    opening_changes = calculate_daily_opening_change(index_data)
    
    features = []
    returns = []
    openings = []
    
    valid_limitup = 0
    valid_margin = 0
    valid_futures = 0
    
    for i, date in enumerate(trade_dates[:-1]):
        next_date = trade_dates[i + 1] if i + 1 < len(trade_dates) else None
        
        if not next_date or next_date not in daily_returns:
            continue
        
        limitup = limitup_data.get(date, {})
        margin = margin_data.get(date, {})
        futures_if = futures_data.get("IF", {}).get(date, {})
        futures_im = futures_data.get("IM", {}).get(date, {})
        
        if limitup and "error" not in limitup:
            valid_limitup += 1
        if margin and "error" not in margin:
            valid_margin += 1
        if futures_if or futures_im:
            valid_futures += 1
        
        feature = {
            "date": date,
            "next_date": next_date,
            "limitup": limitup,
            "margin": margin,
            "futures_if": futures_if,
            "futures_im": futures_im,
            "index_prev": index_data.get(date, {})
        }
        
        features.append(feature)
        returns.append(daily_returns.get(next_date, 0))
        openings.append(opening_changes.get(next_date, 0))
    
    print(f"   有效涨停特征: {valid_limitup}/{len(features)}")
    print(f"   有效融资融券特征: {valid_margin}/{len(features)}")
    print(f"   有效期货特征: {valid_futures}/{len(features)}")
    
    return features, returns, openings


def extract_signals(feature: Dict[str, Any]) -> Dict[str, float]:
    """
    从特征中提取信号
    
    Args:
        feature: 特征数据
    
    Returns:
        信号字典 {metric: signal_value}
    """
    signals = {}
    
    limitup = feature.get("limitup", {})
    if "error" not in limitup:
        total_zt = limitup.get("total_zt", 50)
        max_lb = limitup.get("max_lb", 0)
        
        env_score = 0
        if total_zt > 80:
            env_score = 1
        elif total_zt < 40:
            env_score = -1
        
        lb_score = 0
        if max_lb >= 5:
            lb_score = 0.5
        elif max_lb <= 2:
            lb_score = -0.5
        
        signals["limitup"] = env_score + lb_score
    else:
        signals["limitup"] = 0
    
    margin = feature.get("margin", {})
    if margin and "error" not in margin:
        margin_balance = margin.get("margin_balance", 0)
        short_balance = margin.get("short_balance", 0)
        if margin_balance > 0:
            signals["margin"] = 1 if margin_balance > short_balance else -1
        else:
            signals["margin"] = 0
    else:
        signals["margin"] = 0
    
    futures_if = feature.get("futures_if", {})
    if futures_if:
        close = futures_if.get("close", 0)
        open_price = futures_if.get("open", close)
        if close > 0 and open_price > 0:
            pct = (close / open_price - 1) * 100
            signals["if_main"] = 1 if pct > 0 else -1 if pct < 0 else 0
        else:
            signals["if_main"] = 0
    else:
        signals["if_main"] = 0
    
    futures_im = feature.get("futures_im", {})
    if futures_im:
        close = futures_im.get("close", 0)
        open_price = futures_im.get("open", close)
        if close > 0 and open_price > 0:
            pct = (close / open_price - 1) * 100
            signals["im_main"] = 1 if pct > 0 else -1 if pct < 0 else 0
        else:
            signals["im_main"] = 0
    else:
        signals["im_main"] = 0
    
    signals["lhb"] = signals["limitup"] * 0.5
    signals["external"] = 0
    
    return signals


def calculate_score_with_params(signals: Dict[str, float], 
                                 params: Dict[str, Any]) -> Tuple[float, float]:
    """
    使用给定参数计算多空分数
    
    Args:
        signals: 信号字典
        params: 参数字典
    
    Returns:
        (多方分数, 空方分数)
    """
    weights = params.get("weights", DEFAULT_PARAMS["weights"])
    
    long_score = 0.0
    short_score = 0.0
    
    for metric in METRICS:
        signal = signals.get(metric, 0)
        weight = weights.get(metric, 1.0)
        
        if signal > 0:
            long_score += weight * abs(signal)
        elif signal < 0:
            short_score += weight * abs(signal)
    
    return long_score, short_score


def predict_direction(long_score: float, short_score: float, 
                      params: Dict[str, Any]) -> int:
    """
    预测方向
    
    Args:
        long_score: 多方分数
        short_score: 空方分数
        params: 参数字典
    
    Returns:
        方向 (1: 多, -1: 空, 0: 震荡)
    """
    strong_ratio = params.get("thresholds", {}).get("strong_ratio", 2.0)
    
    if long_score >= strong_ratio * short_score and long_score >= 1:
        return 1
    elif short_score >= strong_ratio * long_score and short_score >= 1:
        return -1
    else:
        return 0


def predict_opening(long_score: float, short_score: float,
                    params: Dict[str, Any]) -> int:
    """
    预测开盘方向
    
    Args:
        long_score: 多方分数
        short_score: 空方分数
        params: 参数字典
    
    Returns:
        开盘方向 (1: 高开, -1: 低开, 0: 平开)
    """
    strong_ratio = params.get("thresholds", {}).get("strong_ratio", 2.0)
    
    if long_score >= strong_ratio * short_score:
        return 1
    elif short_score >= strong_ratio * long_score:
        return -1
    else:
        return 0


def evaluate_params(features: List[Dict], returns: List[float], 
                    openings: List[float], params: Dict[str, Any]) -> float:
    """
    评估参数效果
    
    Args:
        features: 特征列表
        returns: 收益率列表
        openings: 开盘涨跌幅列表
        params: 参数字典
    
    Returns:
        综合评分
    """
    direction_correct = 0
    opening_correct = 0
    total = 0
    
    for i, feature in enumerate(features):
        signals = extract_signals(feature)
        long_score, short_score = calculate_score_with_params(signals, params)
        
        pred_direction = predict_direction(long_score, short_score, params)
        pred_opening = predict_opening(long_score, short_score, params)
        
        actual_return = returns[i]
        actual_opening = openings[i]
        
        actual_direction = 1 if actual_return > 0.3 else (-1 if actual_return < -0.3 else 0)
        actual_opening_dir = 1 if actual_opening > 0.2 else (-1 if actual_opening < -0.2 else 0)
        
        if pred_direction == actual_direction:
            direction_correct += 1
        elif pred_direction != 0 and actual_direction != 0 and pred_direction != actual_direction:
            direction_correct -= 0.5
        
        if pred_opening == actual_opening_dir:
            opening_correct += 1
        elif pred_opening != 0 and actual_opening_dir != 0 and pred_opening != actual_opening_dir:
            opening_correct -= 0.5
        
        total += 1
    
    if total == 0:
        return 0.0
    
    direction_acc = direction_correct / total
    opening_acc = opening_correct / total
    
    return (direction_acc * 0.6 + opening_acc * 0.4) * 100


def generate_param_candidates(n_samples: int = 50, 
                               seed: int = None) -> List[Dict[str, Any]]:
    """
    生成参数候选集（拉丁超立方采样 + 网格搜索）
    
    Args:
        n_samples: 采样数量
        seed: 随机种子
    
    Returns:
        参数候选列表
    """
    if seed is not None:
        np.random.seed(seed)
    
    candidates = []
    
    grid_values = {
        "weights": {
            "margin": [0.5, 1.0, 1.5, 2.0],
            "if_main": [0.5, 1.0, 1.5, 2.0],
            "im_main": [0.5, 1.0, 1.5, 2.0],
            "lhb": [0.5, 1.0, 1.5, 2.0],
            "limitup": [0.5, 1.0, 1.5, 2.0],
            "external": [0.05, 0.15, 0.25]
        },
        "thresholds": {
            "strong_ratio": [1.5, 2.0, 2.5, 3.0]
        }
    }
    
    grid_samples = min(20, n_samples // 3)
    for _ in range(grid_samples):
        params = {
            "weights": {},
            "thresholds": {}
        }
        for metric in METRICS:
            params["weights"][metric] = np.random.choice(grid_values["weights"][metric])
        params["thresholds"]["strong_ratio"] = np.random.choice(grid_values["thresholds"]["strong_ratio"])
        params["thresholds"]["opening_high"] = 0.3
        params["thresholds"]["opening_low"] = -0.3
        candidates.append(params)
    
    for _ in range(n_samples - grid_samples):
        params = {
            "weights": {},
            "thresholds": {}
        }
        
        for metric in METRICS:
            low, high = PARAM_BOUNDS["weights"][metric]
            params["weights"][metric] = round(np.random.uniform(low, high), 2)
        
        for key in ["strong_ratio"]:
            low, high = PARAM_BOUNDS["thresholds"][key]
            params["thresholds"][key] = round(np.random.uniform(low, high), 2)
        
        params["thresholds"]["opening_high"] = 0.3
        params["thresholds"]["opening_low"] = -0.3
        
        candidates.append(params)
    
    return candidates


def local_search(base_params: Dict[str, Any], 
                 features: List[Dict], returns: List[float], openings: List[float],
                 n_neighbors: int = 10, step_size: float = 0.2) -> Tuple[Dict[str, Any], float]:
    """
    在基础参数附近进行局部搜索
    
    Args:
        base_params: 基础参数
        features: 特征列表
        returns: 收益率列表
        openings: 开盘涨跌幅列表
        n_neighbors: 邻域采样数量
        step_size: 步长比例
    
    Returns:
        (最优参数, 最优分数)
    """
    best_params = base_params.copy()
    best_score = evaluate_params(features, returns, openings, base_params)
    
    for _ in range(n_neighbors):
        neighbor = {
            "weights": {},
            "thresholds": {}
        }
        
        for metric in METRICS:
            base_val = base_params.get("weights", {}).get(metric, 1.0)
            low, high = PARAM_BOUNDS["weights"][metric]
            delta = (high - low) * step_size * np.random.uniform(-1, 1)
            new_val = max(low, min(high, base_val + delta))
            neighbor["weights"][metric] = round(new_val, 2)
        
        base_ratio = base_params.get("thresholds", {}).get("strong_ratio", 2.0)
        low, high = PARAM_BOUNDS["thresholds"]["strong_ratio"]
        delta = (high - low) * step_size * np.random.uniform(-1, 1)
        neighbor["thresholds"]["strong_ratio"] = round(max(low, min(high, base_ratio + delta)), 2)
        neighbor["thresholds"]["opening_high"] = 0.3
        neighbor["thresholds"]["opening_low"] = -0.3
        
        score = evaluate_params(features, returns, openings, neighbor)
        if score > best_score:
            best_score = score
            best_params = neighbor.copy()
    
    return best_params, best_score


def bayesian_optimization(features: List[Dict], returns: List[float], 
                          openings: List[float], n_iterations: int = 30) -> Tuple[Dict[str, Any], float]:
    """
    贝叶斯优化搜索最优参数
    
    Args:
        features: 特征列表
        returns: 收益率列表
        openings: 开盘涨跌幅列表
        n_iterations: 迭代次数
    
    Returns:
        (最优参数, 最优分数)
    """
    candidates = generate_param_candidates(n_iterations, seed=42)
    
    best_params = DEFAULT_PARAMS.copy()
    best_score = -float('inf')
    
    evaluated = []
    
    print("  阶段1: 全局搜索...")
    for i, params in enumerate(candidates):
        score = evaluate_params(features, returns, openings, params)
        evaluated.append((params, score))
        
        if score > best_score:
            best_score = score
            best_params = params.copy()
        
        if (i + 1) % 10 == 0:
            print(f"  迭代 {i+1}/{n_iterations}, 当前最优分数: {best_score:.2f}")
    
    evaluated.sort(key=lambda x: x[1], reverse=True)
    
    print("  阶段2: 局部搜索细化...")
    top_candidates = evaluated[:min(5, len(evaluated))]
    
    for i, (params, _) in enumerate(top_candidates):
        refined_params, refined_score = local_search(
            params, features, returns, openings, 
            n_neighbors=15, step_size=0.15
        )
        if refined_score > best_score:
            best_score = refined_score
            best_params = refined_params.copy()
            print(f"    在候选 {i+1} 附近找到更优参数: {refined_score:.2f}")
    
    if len(evaluated) >= 3:
        avg_params = {
            "weights": {},
            "thresholds": {}
        }
        
        top_params = [e[0] for e in evaluated[:3]]
        
        for metric in METRICS:
            values = [p["weights"][metric] for p in top_params]
            avg_params["weights"][metric] = round(np.mean(values), 2)
        
        for key in ["strong_ratio", "opening_high", "opening_low"]:
            values = [p["thresholds"][key] for p in top_params]
            avg_params["thresholds"][key] = round(np.mean(values), 2)
        
        avg_score = evaluate_params(features, returns, openings, avg_params)
        if avg_score > best_score:
            best_params = avg_params
            best_score = avg_score
            print(f"    平均参数更优: {avg_score:.2f}")
    
    return best_params, best_score


def run_optimization(days: int = 200, force_refresh: bool = False) -> Optional[OptimizationResult]:
    """
    运行参数优化
    
    Args:
        days: 历史数据天数
        force_refresh: 是否强制刷新数据
    
    Returns:
        优化结果
    """
    print("=" * 60)
    print("开始参数优化")
    print("=" * 60)
    
    print(f"\n1. 获取历史数据 (最近 {days} 天)...")
    
    historical_data = fetch_all_historical_data(days=int(days * 1.2))
    
    trade_dates = historical_data.get("trade_dates", [])
    if len(trade_dates) < 30:
        print(f"交易日数据不足（{len(trade_dates)} < 30），无法进行优化")
        return None
    
    print(f"\n2. 准备训练数据...")
    features, returns, openings = prepare_training_data(historical_data)
    
    if len(features) < 20:
        print(f"特征数据不足（{len(features)} < 20），无法进行优化")
        print("   可能原因：指数数据不完整或日期匹配失败")
        return None
    
    total_samples = len(features)
    train_size = int(total_samples * 0.7)
    val_size = int(total_samples * 0.15)
    
    train_features = features[:train_size]
    train_returns = returns[:train_size]
    train_openings = openings[:train_size]
    
    val_features = features[train_size:train_size + val_size]
    val_returns = returns[train_size:train_size + val_size]
    val_openings = openings[train_size:train_size + val_size]
    
    test_features = features[train_size + val_size:]
    test_returns = returns[train_size + val_size:]
    test_openings = openings[train_size + val_size:]
    
    print(f"   训练集: {len(train_features)} 样本")
    print(f"   验证集: {len(val_features)} 样本")
    print(f"   测试集: {len(test_features)} 样本")
    
    print(f"\n3. 评估默认参数...")
    default_score = evaluate_params(train_features, train_returns, train_openings, DEFAULT_PARAMS)
    print(f"   默认参数训练集分数: {default_score:.2f}")
    
    print(f"\n4. 开始贝叶斯优化...")
    best_params, train_score = bayesian_optimization(
        train_features, train_returns, train_openings, n_iterations=40
    )
    print(f"   最优参数训练集分数: {train_score:.2f}")
    
    print(f"\n5. 在验证集上评估...")
    val_score = evaluate_params(val_features, val_returns, val_openings, best_params)
    default_val_score = evaluate_params(val_features, val_returns, val_openings, DEFAULT_PARAMS)
    print(f"   默认参数验证集分数: {default_val_score:.2f}")
    print(f"   最优参数验证集分数: {val_score:.2f}")
    
    print(f"\n6. 在测试集上评估...")
    test_score = evaluate_params(test_features, test_returns, test_openings, best_params)
    default_test_score = evaluate_params(test_features, test_returns, test_openings, DEFAULT_PARAMS)
    print(f"   默认参数测试集分数: {default_test_score:.2f}")
    print(f"   最优参数测试集分数: {test_score:.2f}")
    
    improvement = test_score - default_test_score
    print(f"\n7. 改进幅度: {improvement:+.2f}")
    
    if improvement > 0:
        print(f"\n8. 保存优化参数...")
        save_optimized_params(best_params)
        print("   优化参数已保存")
    else:
        print(f"\n8. 优化效果不佳，保留默认参数")
        best_params = DEFAULT_PARAMS.copy()
    
    train_period = (train_features[0]["date"], train_features[-1]["date"]) if train_features else ("", "")
    val_period = (val_features[0]["date"], val_features[-1]["date"]) if val_features else ("", "")
    test_period = (test_features[0]["date"], test_features[-1]["date"]) if test_features else ("", "")
    
    result = OptimizationResult(
        timestamp=datetime.now().isoformat(),
        train_period=train_period,
        val_period=val_period,
        test_period=test_period,
        best_params=best_params,
        train_score=train_score,
        val_score=val_score,
        test_score=test_score,
        improvement=improvement,
        iterations=40
    )
    
    save_optimization_result(result)
    
    print("=" * 60)
    print("参数优化完成")
    print("=" * 60)
    
    return result


def get_current_params() -> Dict[str, Any]:
    """
    获取当前使用的参数（优先使用优化后的参数）
    
    Returns:
        参数字典
    """
    return load_optimized_params()


def should_run_optimization() -> bool:
    """
    判断是否需要运行优化
    
    Returns:
        是否需要运行
    """
    file_path = get_optimized_params_file()
    
    if not file_path.exists():
        return True
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        last_update = data.get("last_update", "")
        if last_update:
            last_dt = datetime.fromisoformat(last_update)
            if (datetime.now() - last_dt).days < 7:
                return False
    except Exception:
        pass
    
    return True


def format_optimization_report(result: OptimizationResult) -> str:
    """
    格式化优化报告
    
    Args:
        result: 优化结果
    
    Returns:
        Markdown格式报告
    """
    lines = []
    
    lines.append("## 🔧 参数优化报告")
    lines.append("")
    
    lines.append(f"**优化时间**: {result.timestamp}")
    lines.append("")
    
    lines.append("### 数据集划分")
    lines.append("")
    lines.append("| 数据集 | 日期范围 | 样本数 |")
    lines.append("|:-------|:---------|:------:|")
    lines.append(f"| 训练集 | {result.train_period[0]} ~ {result.train_period[1]} | 70% |")
    lines.append(f"| 验证集 | {result.val_period[0]} ~ {result.val_period[1]} | 15% |")
    lines.append(f"| 测试集 | {result.test_period[0]} ~ {result.test_period[1]} | 15% |")
    lines.append("")
    
    lines.append("### 评分对比")
    lines.append("")
    lines.append("| 数据集 | 分数 |")
    lines.append("|:-------|:----:|")
    lines.append(f"| 训练集 | {result.train_score:.2f} |")
    lines.append(f"| 验证集 | {result.val_score:.2f} |")
    lines.append(f"| 测试集 | {result.test_score:.2f} |")
    lines.append(f"| **改进** | **{result.improvement:+.2f}** |")
    lines.append("")
    
    lines.append("### 优化后权重")
    lines.append("")
    lines.append("| 指标 | 权重 |")
    lines.append("|:-----|:----:|")
    
    metric_names = {
        "margin": "融资融券",
        "if_main": "沪深300期货",
        "im_main": "中证1000期货",
        "lhb": "龙虎榜",
        "limitup": "涨停环境",
        "external": "外围市场"
    }
    
    weights = result.best_params.get("weights", {})
    for metric in METRICS:
        name = metric_names.get(metric, metric)
        weight = weights.get(metric, 1.0)
        lines.append(f"| {name} | {weight} |")
    
    lines.append("")
    lines.append(f"**强信号阈值**: {result.best_params.get('thresholds', {}).get('strong_ratio', 2.0)}")
    
    return "\n".join(lines)
