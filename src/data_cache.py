"""
历史数据缓存模块
用于批量获取和缓存历史交易数据，防止频繁请求被封禁
支持增量获取和夜间慢速模式
"""

import json
import time
import random
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"
RATE_LIMIT_DELAY = (1.5, 3.0)
NIGHT_MODE_DELAY = (3.0, 6.0)  # 夜间模式更长的延迟


@dataclass
class CacheConfig:
    """
    缓存配置
    """
    cache_expire_days: int = 7
    max_retries: int = 3
    retry_delay: tuple = (2.0, 5.0)
    batch_size: int = 10
    batch_delay: tuple = (5.0, 10.0)
    night_mode: bool = False  # 夜间慢速模式


def init_cache_dir():
    """初始化缓存目录"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_cache_file(data_type: str) -> Path:
    """
    获取缓存文件路径
    
    Args:
        data_type: 数据类型
    
    Returns:
        缓存文件路径
    """
    return CACHE_DIR / f"{data_type}_cache.json"


def load_cache(data_type: str) -> Dict[str, Any]:
    """
    加载缓存数据
    
    Args:
        data_type: 数据类型
    
    Returns:
        缓存数据字典
    """
    init_cache_dir()
    file_path = get_cache_file(data_type)
    if not file_path.exists():
        return {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(data_type: str, data: Dict[str, Any]):
    """
    保存缓存数据
    
    Args:
        data_type: 数据类型
        data: 缓存数据
    """
    init_cache_dir()
    file_path = get_cache_file(data_type)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def random_delay(delay_range: tuple = RATE_LIMIT_DELAY, night_mode: bool = False):
    """
    随机延迟，防止被封
    
    Args:
        delay_range: 延迟范围（秒）
        night_mode: 是否使用夜间模式（更长延迟）
    """
    if night_mode:
        delay_range = NIGHT_MODE_DELAY
    delay = random.uniform(*delay_range)
    time.sleep(delay)


def get_cache_stats() -> Dict[str, Any]:
    """
    获取缓存统计信息
    
    Returns:
        缓存统计信息
    """
    stats = {}
    
    limitup_cache = load_cache("limitup")
    stats["limitup"] = {
        "total_days": len(limitup_cache),
        "valid_days": sum(1 for v in limitup_cache.values() if "error" not in v and v.get("total_zt", 0) >= 0),
        "error_days": sum(1 for v in limitup_cache.values() if "error" in v)
    }
    
    index_cache = load_cache("index")
    total_index = 0
    for key, value in index_cache.items():
        if isinstance(value, dict) and "close" in value:
            total_index += 1
        elif isinstance(value, dict):
            total_index += len(value)
    stats["index"] = {"total_days": total_index}
    
    margin_cache = load_cache("margin")
    stats["margin"] = {
        "total_days": len(margin_cache.get("data", {})),
        "last_update": margin_cache.get("last_update", "N/A")
    }
    
    futures_cache = load_cache("futures")
    futures_data = futures_cache.get("data", {})
    stats["futures"] = {
        "IF_days": len(futures_data.get("IF", {})),
        "IM_days": len(futures_data.get("IM", {})),
        "last_update": futures_cache.get("last_update", "N/A")
    }
    
    return stats


def print_cache_summary():
    """打印缓存摘要"""
    stats = get_cache_stats()
    
    print("\n" + "=" * 50)
    print("缓存数据统计")
    print("=" * 50)
    print(f"涨停数据: {stats['limitup']['valid_days']} 天有效, {stats['limitup']['error_days']} 天错误")
    print(f"指数数据: {stats['index']['total_days']} 天")
    print(f"融资融券: {stats['margin']['total_days']} 天 (更新于: {stats['margin']['last_update'][:10] if stats['margin']['last_update'] != 'N/A' else 'N/A'})")
    print(f"期货数据: IF {stats['futures']['IF_days']} 天, IM {stats['futures']['IM_days']} 天")
    print("=" * 50 + "\n")


def get_trade_dates_in_range(start_date: str, end_date: str) -> List[str]:
    """
    获取日期范围内的交易日列表
    
    Args:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    
    Returns:
        交易日列表
    """
    try:
        df = ak.tool_trade_date_hist_sina()
        trade_dates = df['trade_date'].tolist()
        
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(end_date, "%Y%m%d").date()
        
        filtered = []
        for d in trade_dates:
            if hasattr(d, 'strftime'):
                d_date = d if isinstance(d, type(start_dt)) else datetime.strptime(str(d), "%Y-%m-%d").date()
            else:
                d_str = str(d).replace('-', '')
                if len(d_str) == 8:
                    d_date = datetime.strptime(d_str, "%Y%m%d").date()
                else:
                    continue
            
            if start_dt <= d_date <= end_dt:
                filtered.append(d_date.strftime("%Y%m%d"))
        
        return filtered
    except Exception as e:
        print(f"获取交易日历失败: {e}")
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return dates


def fetch_historical_limitup_data(dates: List[str], 
                                   config: CacheConfig = CacheConfig()) -> Dict[str, Any]:
    """
    批量获取历史涨停数据（增量获取）
    
    Args:
        dates: 日期列表 (YYYYMMDD格式)
        config: 缓存配置
    
    Returns:
        历史涨停数据字典
    """
    cache = load_cache("limitup")
    results = {}
    today = datetime.now().strftime("%Y%m%d")
    
    normalized_cache = {}
    for k, v in cache.items():
        normalized_key = k.replace('-', '')
        normalized_cache[normalized_key] = v
    
    dates_to_fetch = []
    for date in dates:
        if date >= today:
            continue
        if date in normalized_cache:
            results[date] = normalized_cache[date]
        else:
            dates_to_fetch.append(date)
    
    if not dates_to_fetch:
        print(f"涨停数据已全部缓存，共 {len(results)} 天")
        return results
    
    print(f"需要获取 {len(dates_to_fetch)} 天的涨停数据（已缓存 {len(results)} 天）...")
    
    success_count = 0
    error_count = 0
    
    for i, date in enumerate(dates_to_fetch):
        try:
            progress = f"[{i+1}/{len(dates_to_fetch)}]"
            print(f"获取涨停数据 {progress}: {date}")
            
            df_zt = ak.stock_zt_pool_em(date=date)
            if not df_zt.empty:
                lb_dist = df_zt['连板数'].value_counts().to_dict() if '连板数' in df_zt.columns else {}
                max_lb = int(df_zt['连板数'].max()) if '连板数' in df_zt.columns and not df_zt.empty else 0
                total_zt = len(df_zt)
                
                industry_col = None
                for col in ['所属行业', '行业', 'sector']:
                    if col in df_zt.columns:
                        industry_col = col
                        break
                
                industry_top = {}
                if industry_col:
                    industry_top = df_zt[industry_col].value_counts().head(10).to_dict()
                
                results[date] = {
                    "total_zt": total_zt,
                    "max_lb": max_lb,
                    "lb_dist": {str(k): int(v) for k, v in lb_dist.items()},
                    "industry_top": {str(k): int(v) for k, v in industry_top.items()},
                    "fetch_time": datetime.now().isoformat()
                }
                success_count += 1
            else:
                results[date] = {"total_zt": 0, "max_lb": 0, "lb_dist": {}, "industry_top": {}}
                success_count += 1
            
            cache[date] = results[date]
            
            if (i + 1) % 5 == 0:
                save_cache("limitup", cache)
            
            if (i + 1) % config.batch_size == 0 and i + 1 < len(dates_to_fetch):
                elapsed = (i + 1) / len(dates_to_fetch) * 100
                print(f"  进度: {elapsed:.1f}%, 成功: {success_count}, 失败: {error_count}")
                print(f"  暂停 {config.batch_delay[0]:.0f}-{config.batch_delay[1]:.0f} 秒...")
                random_delay(config.batch_delay, config.night_mode)
            else:
                random_delay(night_mode=config.night_mode)
                
        except Exception as e:
            print(f"  获取 {date} 数据失败: {e}")
            results[date] = {"error": str(e)}
            cache[date] = results[date]
            error_count += 1
            random_delay(config.retry_delay, config.night_mode)
    
    save_cache("limitup", cache)
    print(f"涨停数据获取完成: 成功 {success_count}, 失败 {error_count}")
    return results


def fetch_historical_index_data(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    获取历史指数数据（一次性获取，减少请求次数）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        历史指数数据
    """
    cache = load_cache("index")
    cache_key = f"{start_date}_{end_date}"
    
    if cache_key in cache:
        return cache[cache_key]
    
    print("获取历史指数数据...")
    
    try:
        df_sh = ak.stock_zh_index_daily(symbol="sh000001")
        df_sh['date'] = pd.to_datetime(df_sh['date'])
        
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        df_filtered = df_sh[(df_sh['date'] >= start_dt) & (df_sh['date'] <= end_dt)]
        
        results = {}
        for _, row in df_filtered.iterrows():
            date_str = row['date'].strftime("%Y%m%d")
            results[date_str] = {
                "close": float(row['close']),
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "volume": float(row['volume']) if 'volume' in row else 0
            }
        
        cache[cache_key] = results
        save_cache("index", cache)
        
        return results
        
    except Exception as e:
        print(f"获取指数数据失败: {e}")
        return {}


def fetch_historical_margin_data() -> Dict[str, Any]:
    """
    获取历史融资融券数据
    """
    cache = load_cache("margin")
    
    if "data" in cache and len(cache.get("data", {})) > 0:
        last_update = cache.get("last_update", "")
        if last_update:
            last_dt = datetime.fromisoformat(last_update)
            if (datetime.now() - last_dt).days < 7:
                return cache["data"]
    
    print("获取历史融资融券数据...")
    results = {}
    
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
        
        df = ak.stock_margin_sse(start_date=start_date, end_date=end_date)
        
        if not df.empty:
            for _, row in df.iterrows():
                date_raw = str(row.get('信用交易日期', ''))
                date_str = date_raw.replace('-', '') if '-' in date_raw else date_raw
                if date_str and len(date_str) == 8:
                    margin_balance = row.get('融资余额', 0)
                    short_balance = row.get('融券余量金额', 0)
                    
                    try:
                        margin_balance = float(margin_balance) if margin_balance else 0
                        short_balance = float(short_balance) if short_balance else 0
                    except (ValueError, TypeError):
                        margin_balance = 0
                        short_balance = 0
                    
                    results[date_str] = {
                        "margin_balance": margin_balance,
                        "short_balance": short_balance
                    }
        
        cache["data"] = results
        cache["last_update"] = datetime.now().isoformat()
        save_cache("margin", cache)
        
        print(f"  获取到 {len(results)} 天的融资融券数据")
        return results
        
    except Exception as e:
        print(f"获取融资融券数据失败: {e}")
        return cache.get("data", {})


def fetch_historical_futures_data() -> Dict[str, Any]:
    """
    获取历史期货数据
    """
    cache = load_cache("futures")
    
    if "data" in cache:
        last_update = cache.get("last_update", "")
        if last_update:
            last_dt = datetime.fromisoformat(last_update)
            if (datetime.now() - last_dt).days < 1:
                return cache["data"]
    
    print("获取历史期货数据...")
    results = {"IF": {}, "IM": {}}
    
    try:
        df_if = ak.futures_main_sina(symbol="IF0")
        if not df_if.empty:
            for _, row in df_if.iterrows():
                date_raw = row.get('日期', row.get('date', ''))
                date_str = str(date_raw).replace('-', '') if date_raw else ''
                if date_str and len(date_str) == 8:
                    close_val = row.get('收盘价', row.get('close', 0))
                    open_val = row.get('开盘价', row.get('open', 0))
                    try:
                        results["IF"][date_str] = {
                            "close": float(close_val) if close_val else 0,
                            "open": float(open_val) if open_val else 0
                        }
                    except (ValueError, TypeError):
                        pass
        random_delay()
    except Exception as e:
        print(f"获取IF期货数据失败: {e}")
    
    try:
        df_im = ak.futures_main_sina(symbol="IM0")
        if not df_im.empty:
            for _, row in df_im.iterrows():
                date_raw = row.get('日期', row.get('date', ''))
                date_str = str(date_raw).replace('-', '') if date_raw else ''
                if date_str and len(date_str) == 8:
                    close_val = row.get('收盘价', row.get('close', 0))
                    open_val = row.get('开盘价', row.get('open', 0))
                    try:
                        results["IM"][date_str] = {
                            "close": float(close_val) if close_val else 0,
                            "open": float(open_val) if open_val else 0
                        }
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"获取IM期货数据失败: {e}")
    
    cache["data"] = results
    cache["last_update"] = datetime.now().isoformat()
    save_cache("futures", cache)
    
    print(f"  获取到 IF: {len(results['IF'])} 天, IM: {len(results['IM'])} 天")
    return results


def fetch_all_historical_data(days: int = 300, 
                               config: CacheConfig = CacheConfig(),
                               show_summary: bool = True) -> Dict[str, Any]:
    """
    批量获取所有历史数据（增量获取）
    
    Args:
        days: 获取天数
        config: 缓存配置
        show_summary: 是否显示缓存摘要
    
    Returns:
        所有历史数据
    """
    if show_summary:
        print_cache_summary()
    
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")
    
    mode_str = " (夜间慢速模式)" if config.night_mode else ""
    print(f"=" * 60)
    print(f"开始获取 {days} 天的历史数据{mode_str}")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"=" * 60)
    
    trade_dates = get_trade_dates_in_range(start_date, end_date)
    trade_dates = trade_dates[-days:] if len(trade_dates) > days else trade_dates
    
    print(f"目标交易日数量: {len(trade_dates)}")
    
    all_data = {
        "trade_dates": trade_dates,
        "limitup": {},
        "index": {},
        "margin": {},
        "futures": {}
    }
    
    all_data["limitup"] = fetch_historical_limitup_data(trade_dates, config)
    
    random_delay(config.batch_delay, config.night_mode)
    
    all_data["index"] = fetch_historical_index_data(trade_dates[0] if trade_dates else start_date, 
                                                     end_date)
    
    random_delay(night_mode=config.night_mode)
    
    all_data["margin"] = fetch_historical_margin_data()
    
    random_delay(night_mode=config.night_mode)
    
    all_data["futures"] = fetch_historical_futures_data()
    
    print(f"=" * 60)
    print("历史数据获取完成")
    if show_summary:
        print_cache_summary()
    print(f"=" * 60)
    
    return all_data


def fetch_history_nightly(days: int = 600) -> Dict[str, Any]:
    """
    夜间慢速获取历史数据（适合大量数据获取）
    
    Args:
        days: 获取天数（默认600个交易日）
    
    Returns:
        所有历史数据
    """
    config = CacheConfig(
        night_mode=True,
        batch_size=5,  # 更小的批次
        batch_delay=(10.0, 20.0),  # 更长的批次间隔
        retry_delay=(5.0, 10.0)  # 更长的重试间隔
    )
    
    print("\n🌙 夜间慢速模式启动")
    print("预计获取时间: 约 {:.1f} 小时".format(
        days * 4.5 / 3600  # 约4.5秒每条数据
    ))
    
    return fetch_all_historical_data(days=days, config=config)


def calculate_daily_returns(index_data: Dict[str, Any]) -> Dict[str, float]:
    """
    计算每日收益率
    
    Args:
        index_data: 指数数据
    
    Returns:
        每日收益率字典
    """
    returns = {}
    sorted_dates = sorted(index_data.keys())
    
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i-1]
        curr_date = sorted_dates[i]
        
        prev_close = index_data[prev_date].get("close", 0)
        curr_close = index_data[curr_date].get("close", 0)
        
        if prev_close > 0:
            returns[curr_date] = (curr_close / prev_close - 1) * 100
        else:
            returns[curr_date] = 0
    
    return returns


def calculate_daily_opening_change(index_data: Dict[str, Any]) -> Dict[str, float]:
    """
    计算每日开盘涨跌幅
    
    Args:
        index_data: 指数数据
    
    Returns:
        每日开盘涨跌幅字典
    """
    opening_changes = {}
    sorted_dates = sorted(index_data.keys())
    
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i-1]
        curr_date = sorted_dates[i]
        
        prev_close = index_data[prev_date].get("close", 0)
        curr_open = index_data[curr_date].get("open", 0)
        
        if prev_close > 0:
            opening_changes[curr_date] = (curr_open / prev_close - 1) * 100
        else:
            opening_changes[curr_date] = 0
    
    return opening_changes
