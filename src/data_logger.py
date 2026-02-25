"""
数据日志记录模块
提供详细的数据获取日志记录、数据完整性校验和自动打包功能
"""

import gzip
import hashlib
import json
import logging
import os
import shutil
import tarfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd

BEIJING_TZ = timezone(timedelta(hours=8))

MAX_FOLDER_SIZE_MB = 50
MAX_FOLDER_SIZE_BYTES = MAX_FOLDER_SIZE_MB * 1024 * 1024
KEEP_RECENT_FILES = 5

project_root = Path(__file__).parent.parent
LOGS_DIR = project_root / "logs"
RESULTS_DIR = project_root / "results"
DATA_LOGS_DIR = LOGS_DIR / "data"
ARCHIVE_DIR = project_root / "archives"


class DataLogger:
    """
    数据获取日志记录器
    记录从akshare获取的原始数据、数据完整性校验信息
    """
    
    def __init__(self, logs_dir: Path = DATA_LOGS_DIR):
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        self.session_id = datetime.now(BEIJING_TZ).strftime("%Y%m%d_%H%M%S")
        self.session_log_file = self.logs_dir / f"data_fetch_{self.session_id}.log"
        
        self.logger = logging.getLogger(f'DataLogger_{self.session_id}')
        self.logger.setLevel(logging.DEBUG)
        
        if not self.logger.handlers:
            file_handler = logging.FileHandler(
                self.session_log_file, 
                encoding='utf-8',
                mode='a'
            )
            file_handler.setLevel(logging.DEBUG)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
        
        self.data_records: List[Dict[str, Any]] = []
    
    def _get_beijing_time(self) -> str:
        return datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
    
    def _calculate_data_hash(self, data: Any) -> str:
        if isinstance(data, pd.DataFrame):
            data_str = data.to_json()
        elif isinstance(data, (dict, list)):
            data_str = json.dumps(data, ensure_ascii=False, default=str)
        else:
            data_str = str(data)
        
        return hashlib.sha256(data_str.encode('utf-8')).hexdigest()[:16]
    
    def _validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df is None or df.empty:
            return {
                "valid": False,
                "reason": "DataFrame为空或None",
                "rows": 0,
                "columns": 0,
                "null_counts": {},
                "duplicates": 0
            }
        
        null_counts = df.isnull().sum().to_dict()
        null_counts = {k: int(v) for k, v in null_counts.items() if v > 0}
        
        return {
            "valid": True,
            "reason": "数据验证通过",
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "null_counts": null_counts,
            "null_ratio": round(df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100, 2) if len(df) > 0 else 0,
            "duplicates": int(df.duplicated().sum()),
            "memory_usage": f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB"
        }
    
    def log_data_fetch_start(self, source: str, params: Dict[str, Any]) -> str:
        fetch_id = f"{source}_{datetime.now(BEIJING_TZ).strftime('%H%M%S_%f')}"
        
        record = {
            "fetch_id": fetch_id,
            "source": source,
            "params": params,
            "start_time": self._get_beijing_time(),
            "end_time": None,
            "status": "started",
            "data_hash": None,
            "validation": None,
            "error": None,
            "sample_data": None
        }
        
        self.data_records.append(record)
        
        self.logger.info(f"[数据获取开始] ID={fetch_id}")
        self.logger.info(f"  数据源: {source}")
        self.logger.info(f"  请求参数: {json.dumps(params, ensure_ascii=False, default=str)}")
        
        return fetch_id
    
    def log_data_fetch_success(
        self, 
        fetch_id: str, 
        data: Any,
        sample_rows: int = 3,
        log_raw_data: bool = False
    ) -> None:
        record = None
        for r in self.data_records:
            if r["fetch_id"] == fetch_id:
                record = r
                break
        
        if not record:
            self.logger.warning(f"未找到fetch_id: {fetch_id}")
            return
        
        record["end_time"] = self._get_beijing_time()
        record["status"] = "success"
        record["data_hash"] = self._calculate_data_hash(data)
        
        if isinstance(data, pd.DataFrame):
            record["validation"] = self._validate_dataframe(data)
            
            self.logger.info(f"[数据获取成功] ID={fetch_id}")
            self.logger.info(f"  数据类型: DataFrame")
            self.logger.info(f"  数据哈希: {record['data_hash']}")
            self.logger.info(f"  行数: {record['validation']['rows']}")
            self.logger.info(f"  列数: {record['validation']['columns']}")
            self.logger.info(f"  列名: {record['validation']['column_names']}")
            self.logger.info(f"  空值比例: {record['validation']['null_ratio']}%")
            self.logger.info(f"  重复行数: {record['validation']['duplicates']}")
            self.logger.info(f"  内存占用: {record['validation']['memory_usage']}")
            
            if log_raw_data and not data.empty:
                self.logger.debug(f"  原始数据预览:\n{data.head(sample_rows).to_string()}")
                record["sample_data"] = data.head(sample_rows).to_dict('records')
            else:
                record["sample_data"] = data.head(sample_rows).to_dict('records')
        
        elif isinstance(data, dict):
            record["validation"] = {
                "valid": True,
                "type": "dict",
                "keys": list(data.keys()),
                "key_count": len(data)
            }
            
            self.logger.info(f"[数据获取成功] ID={fetch_id}")
            self.logger.info(f"  数据类型: Dict")
            self.logger.info(f"  数据哈希: {record['data_hash']}")
            self.logger.info(f"  键数量: {len(data)}")
            self.logger.info(f"  键列表: {list(data.keys())}")
            
            if log_raw_data:
                self.logger.debug(f"  原始数据: {json.dumps(data, ensure_ascii=False, default=str)[:500]}")
        
        elif isinstance(data, (int, float, str)):
            record["validation"] = {
                "valid": True,
                "type": type(data).__name__,
                "value": str(data)[:200]
            }
            
            self.logger.info(f"[数据获取成功] ID={fetch_id}")
            self.logger.info(f"  数据类型: {type(data).__name__}")
            self.logger.info(f"  数据值: {str(data)[:200]}")
        
        else:
            record["validation"] = {
                "valid": True,
                "type": type(data).__name__
            }
            
            self.logger.info(f"[数据获取成功] ID={fetch_id}")
            self.logger.info(f"  数据类型: {type(data).__name__}")
    
    def log_data_fetch_error(self, fetch_id: str, error: Exception) -> None:
        record = None
        for r in self.data_records:
            if r["fetch_id"] == fetch_id:
                record = r
                break
        
        if not record:
            self.logger.warning(f"未找到fetch_id: {fetch_id}")
            return
        
        record["end_time"] = self._get_beijing_time()
        record["status"] = "error"
        record["error"] = str(error)
        
        self.logger.error(f"[数据获取失败] ID={fetch_id}")
        self.logger.error(f"  数据源: {record['source']}")
        self.logger.error(f"  错误类型: {type(error).__name__}")
        self.logger.error(f"  错误信息: {str(error)}")
    
    def save_session_record(self) -> Path:
        record_file = self.logs_dir / f"data_record_{self.session_id}.json"
        
        session_data = {
            "session_id": self.session_id,
            "start_time": self._get_beijing_time(),
            "total_fetches": len(self.data_records),
            "successful_fetches": sum(1 for r in self.data_records if r["status"] == "success"),
            "failed_fetches": sum(1 for r in self.data_records if r["status"] == "error"),
            "data_sources": list(set(r["source"] for r in self.data_records)),
            "records": self.data_records
        }
        
        with open(record_file, 'w', encoding='utf-8') as f:
            json.dump(session_data, f, ensure_ascii=False, indent=2, default=str)
        
        self.logger.info(f"[会话记录保存] 文件: {record_file}")
        self.logger.info(f"  总获取次数: {session_data['total_fetches']}")
        self.logger.info(f"  成功次数: {session_data['successful_fetches']}")
        self.logger.info(f"  失败次数: {session_data['failed_fetches']}")
        
        return record_file
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "total_fetches": len(self.data_records),
            "successful_fetches": sum(1 for r in self.data_records if r["status"] == "success"),
            "failed_fetches": sum(1 for r in self.data_records if r["status"] == "error"),
            "data_hashes": [r["data_hash"] for r in self.data_records if r.get("data_hash")]
        }


class FileArchiver:
    """
    文件自动打包器
    监控文件夹大小，超过阈值时自动打包
    """
    
    def __init__(
        self, 
        logs_dir: Path = LOGS_DIR,
        results_dir: Path = RESULTS_DIR,
        archive_dir: Path = ARCHIVE_DIR,
        max_size_mb: int = MAX_FOLDER_SIZE_MB,
        keep_recent: int = KEEP_RECENT_FILES
    ):
        self.logs_dir = logs_dir
        self.results_dir = results_dir
        self.archive_dir = archive_dir
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.keep_recent = keep_recent
        
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger('FileArchiver')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def get_folder_size(self, folder: Path) -> int:
        if not folder.exists():
            return 0
        
        total_size = 0
        for item in folder.rglob('*'):
            if item.is_file():
                total_size += item.stat().st_size
        return total_size
    
    def get_folder_size_mb(self, folder: Path) -> float:
        return self.get_folder_size(folder) / (1024 * 1024)
    
    def get_files_to_archive(self, folder: Path) -> List[Path]:
        if not folder.exists():
            return []
        
        all_files = sorted(
            [f for f in folder.iterdir() if f.is_file()],
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        files_to_archive = all_files[self.keep_recent:]
        
        return files_to_archive
    
    def create_archive(self, source_dir: Path, files: List[Path]) -> Optional[Path]:
        if not files:
            return None
        
        timestamp = datetime.now(BEIJING_TZ).strftime("%Y%m%d_%H%M%S")
        archive_name = f"{source_dir.name}_archive_{timestamp}.tar.gz"
        archive_path = self.archive_dir / archive_name
        
        try:
            with tarfile.open(archive_path, "w:gz") as tar:
                for file_path in files:
                    arcname = file_path.relative_to(source_dir.parent)
                    tar.add(file_path, arcname=arcname)
            
            archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"[打包完成] {archive_path}")
            self.logger.info(f"  原始文件数: {len(files)}")
            self.logger.info(f"  压缩包大小: {archive_size_mb:.2f} MB")
            
            for file_path in files:
                file_path.unlink()
            
            self.logger.info(f"  已删除原始文件: {len(files)} 个")
            
            return archive_path
            
        except Exception as e:
            self.logger.error(f"[打包失败] {e}")
            return None
    
    def check_and_archive(self, folder: Path) -> Optional[Path]:
        current_size = self.get_folder_size(folder)
        current_size_mb = current_size / (1024 * 1024)
        
        self.logger.info(f"[检查文件夹] {folder}")
        self.logger.info(f"  当前大小: {current_size_mb:.2f} MB")
        
        if current_size > self.max_size_bytes:
            self.logger.info(f"  超过阈值 ({MAX_FOLDER_SIZE_MB} MB)，开始打包...")
            
            files_to_archive = self.get_files_to_archive(folder)
            
            if files_to_archive:
                return self.create_archive(folder, files_to_archive)
            else:
                self.logger.info(f"  文件数量不足，跳过打包")
                return None
        else:
            self.logger.info(f"  未超过阈值，无需打包")
            return None
    
    def check_all_folders(self) -> Dict[str, Optional[Path]]:
        results = {}
        
        self.logger.info("=" * 60)
        self.logger.info("开始检查所有文件夹...")
        
        if self.logs_dir.exists():
            results['logs'] = self.check_and_archive(self.logs_dir)
        
        if self.results_dir.exists():
            results['results'] = self.check_and_archive(self.results_dir)
        
        self.logger.info("文件夹检查完成")
        self.logger.info("=" * 60)
        
        return results
    
    def get_archive_history(self) -> List[Dict[str, Any]]:
        if not self.archive_dir.exists():
            return []
        
        archives = []
        for archive_file in self.archive_dir.glob("*.tar.gz"):
            stat = archive_file.stat()
            archives.append({
                "name": archive_file.name,
                "path": str(archive_file),
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_mtime, BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return sorted(archives, key=lambda x: x['created'], reverse=True)
    
    def extract_archive(self, archive_path: Path, extract_to: Optional[Path] = None) -> Path:
        if extract_to is None:
            extract_to = archive_path.parent / f"extracted_{archive_path.stem}"
        
        extract_to.mkdir(parents=True, exist_ok=True)
        
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(extract_to)
        
        self.logger.info(f"[解压完成] {archive_path} -> {extract_to}")
        return extract_to


def init_directories():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    
    gitignore_path = project_root / ".gitignore"
    gitignore_entries = [
        "logs/",
        "results/",
        "archives/",
        "*.log",
        "*.tar.gz"
    ]
    
    if gitignore_path.exists():
        with open(gitignore_path, 'r', encoding='utf-8') as f:
            existing = f.read()
        
        with open(gitignore_path, 'a', encoding='utf-8') as f:
            for entry in gitignore_entries:
                if entry not in existing:
                    f.write(f"\n{entry}")


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        file_path = LOGS_DIR / log_file
        file_handler = logging.FileHandler(file_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger


if __name__ == "__main__":
    init_directories()
    
    data_logger = DataLogger()
    
    fetch_id = data_logger.log_data_fetch_start("test_source", {"param1": "value1"})
    
    test_df = pd.DataFrame({
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"]
    })
    
    data_logger.log_data_fetch_success(fetch_id, test_df, log_raw_data=True)
    
    record_file = data_logger.save_session_record()
    print(f"Session record saved to: {record_file}")
    
    archiver = FileArchiver()
    print(f"\nLogs folder size: {archiver.get_folder_size_mb(LOGS_DIR):.2f} MB")
    print(f"Results folder size: {archiver.get_folder_size_mb(RESULTS_DIR):.2f} MB")
