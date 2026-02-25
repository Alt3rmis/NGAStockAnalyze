"""
Git Uploader Module
Automatically uploads Markdown result files to a specified Git repository
with year/month directory structure
"""

import json
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BEIJING_TZ = timezone(timedelta(hours=8))

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / 'git_uploader.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('GitUploader')


@dataclass
class GitConfig:
    """Configuration for Git upload"""
    enabled: bool = False
    repo_url: str = ""
    branch: str = "main"
    local_path: str = ""
    commit_author: str = "StockAnalyzer Bot"
    commit_email: str = "bot@example.com"
    commit_message_template: str = "docs: update market sentiment report for {date}"


class GitUploader:
    """
    Handles uploading Markdown files to a Git repository
    Maintains year/month directory structure
    """
    
    def __init__(self, config: GitConfig):
        self.config = config
        self._repo_path: Optional[Path] = None
        self._is_initialized = False
    
    def initialize(self) -> Tuple[bool, str]:
        """
        Initialize the Git repository (clone or update)
        
        Returns:
            Tuple of (success, message)
        """
        if not self.config.enabled:
            return False, "Git upload is disabled"
        
        if not self.config.repo_url:
            return False, "Git repository URL not configured"
        
        if not self.config.local_path:
            return False, "Local path not configured"
        
        self._repo_path = Path(self.config.local_path)
        
        try:
            if self._repo_path.exists():
                if (self._repo_path / ".git").exists():
                    logger.info(f"Updating existing repository at {self._repo_path}")
                    result = subprocess.run(
                        ["git", "fetch", "origin"],
                        cwd=str(self._repo_path),
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    if result.returncode != 0:
                        logger.warning(f"Git fetch warning: {result.stderr}")
                    
                    result = subprocess.run(
                        ["git", "reset", "--hard", f"origin/{self.config.branch}"],
                        cwd=str(self._repo_path),
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    if result.returncode != 0:
                        logger.warning(f"Git reset warning: {result.stderr}")
                    
                    self._is_initialized = True
                    return True, "Repository updated successfully"
                else:
                    shutil.rmtree(str(self._repo_path))
            
            logger.info(f"Cloning repository {self.config.repo_url}")
            self._repo_path.parent.mkdir(parents=True, exist_ok=True)
            
            result = subprocess.run(
                ["git", "clone", "-b", self.config.branch, 
                 self.config.repo_url, str(self._repo_path)],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                logger.error(f"Git clone failed: {error_msg}")
                return False, f"Clone failed: {error_msg}"
            
            self._is_initialized = True
            logger.info("Repository cloned successfully")
            return True, "Repository cloned successfully"
            
        except subprocess.TimeoutExpired:
            logger.error("Git operation timed out")
            return False, "Git operation timed out"
        except Exception as e:
            logger.error(f"Failed to initialize repository: {e}")
            return False, str(e)
    
    def _get_target_directory(self, date_str: str) -> Path:
        """
        Get the target directory path based on date
        Structure: year/month/*.md
        
        Args:
            date_str: Date string in YYYYMMDD format
        
        Returns:
            Path object for the target directory
        """
        if len(date_str) != 8:
            raise ValueError(f"Invalid date format: {date_str}, expected YYYYMMDD")
        
        year = date_str[:4]
        month = date_str[4:6]
        
        target_dir = self._repo_path / year / month
        return target_dir
    
    def upload_markdown_file(
        self,
        source_file: Path,
        date_str: str
    ) -> Tuple[bool, str]:
        """
        Upload a Markdown file to the repository
        
        Args:
            source_file: Path to the source Markdown file
            date_str: Date string in YYYYMMDD format
        
        Returns:
            Tuple of (success, message)
        """
        if not self._is_initialized:
            success, msg = self.initialize()
            if not success:
                return False, msg
        
        if not source_file.exists():
            return False, f"Source file not found: {source_file}"
        
        if not source_file.suffix.lower() == '.md':
            return False, f"Only Markdown files (.md) are allowed, got: {source_file.suffix}"
        
        try:
            target_dir = self._get_target_directory(date_str)
            target_dir.mkdir(parents=True, exist_ok=True)
            
            target_file = target_dir / source_file.name
            
            shutil.copy2(str(source_file), str(target_file))
            logger.info(f"Copied {source_file} to {target_file}")
            
            return self._commit_and_push(date_str, target_file)
            
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            return False, str(e)
    
    def upload_multiple_files(
        self,
        files_with_dates: List[Tuple[Path, str]]
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Upload multiple Markdown files
        
        Args:
            files_with_dates: List of (source_file, date_str) tuples
        
        Returns:
            Tuple of (overall_success, results_dict)
        """
        if not self._is_initialized:
            success, msg = self.initialize()
            if not success:
                return False, {"error": msg}
        
        results = {}
        all_success = True
        
        for source_file, date_str in files_with_dates:
            success, msg = self.upload_markdown_file(source_file, date_str)
            results[str(source_file)] = msg
            if not success:
                all_success = False
        
        return all_success, results
    
    def _commit_and_push(
        self,
        date_str: str,
        target_file: Path
    ) -> Tuple[bool, str]:
        """
        Commit and push changes to the repository
        
        Args:
            date_str: Date string for commit message
            target_file: Path to the uploaded file
        
        Returns:
            Tuple of (success, message)
        """
        try:
            result = subprocess.run(
                ["git", "config", "user.name", self.config.commit_author],
                cwd=str(self._repo_path),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            result = subprocess.run(
                ["git", "config", "user.email", self.config.commit_email],
                cwd=str(self._repo_path),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            relative_path = target_file.relative_to(self._repo_path)
            
            result = subprocess.run(
                ["git", "add", str(relative_path)],
                cwd=str(self._repo_path),
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                logger.warning(f"Git add warning: {result.stderr}")
            
            result = subprocess.run(
                ["git", "diff", "--cached", "--quiet"],
                cwd=str(self._repo_path),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info("No changes to commit")
                return True, "No changes to commit (file already up to date)"
            
            commit_message = self.config.commit_message_template.format(date=date_str)
            
            result = subprocess.run(
                ["git", "commit", "-m", commit_message],
                cwd=str(self._repo_path),
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                logger.error(f"Git commit failed: {error_msg}")
                return False, f"Commit failed: {error_msg}"
            
            logger.info(f"Committed: {commit_message}")
            
            result = subprocess.run(
                ["git", "push", "origin", self.config.branch],
                cwd=str(self._repo_path),
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                logger.error(f"Git push failed: {error_msg}")
                return False, f"Push failed: {error_msg}"
            
            logger.info("Pushed successfully to remote repository")
            return True, f"Successfully uploaded to {relative_path}"
            
        except subprocess.TimeoutExpired:
            logger.error("Git operation timed out")
            return False, "Git operation timed out"
        except Exception as e:
            logger.error(f"Failed to commit/push: {e}")
            return False, str(e)
    
    def get_upload_history(self) -> List[Dict[str, Any]]:
        """
        Get the upload history from the repository
        
        Returns:
            List of uploaded files with their info
        """
        if not self._is_initialized or not self._repo_path:
            return []
        
        history = []
        
        try:
            for year_dir in sorted(self._repo_path.iterdir()):
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                
                for month_dir in sorted(year_dir.iterdir()):
                    if not month_dir.is_dir() or not month_dir.name.isdigit():
                        continue
                    
                    for md_file in sorted(month_dir.glob("*.md")):
                        stat = md_file.stat()
                        history.append({
                            "year": year_dir.name,
                            "month": month_dir.name,
                            "filename": md_file.name,
                            "path": str(md_file.relative_to(self._repo_path)),
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(
                                stat.st_mtime, BEIJING_TZ
                            ).isoformat()
                        })
        except Exception as e:
            logger.error(f"Failed to get upload history: {e}")
        
        return history


class GitUploadManager:
    """
    Central manager for Git upload operations
    Handles configuration and coordinates uploads
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the Git upload manager
        
        Args:
            config_path: Path to configuration file
        """
        self._config_path = config_path or (
            Path(__file__).parent.parent / "config" / "git_upload_config.json"
        )
        self.config: Optional[GitConfig] = None
        self.uploader: Optional[GitUploader] = None
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load configuration from JSON file"""
        if not self._config_path.exists():
            logger.warning(f"Configuration file not found: {self._config_path}")
            logger.info("Creating default configuration file...")
            self._create_default_config()
            return
        
        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            local_path = config.get("local_path", "")
            if local_path and not Path(local_path).is_absolute():
                local_path = str(Path(__file__).parent.parent / local_path)
            
            self.config = GitConfig(
                enabled=config.get("enabled", False),
                repo_url=config.get("repo_url", ""),
                branch=config.get("branch", "main"),
                local_path=local_path,
                commit_author=config.get("commit_author", "StockAnalyzer Bot"),
                commit_email=config.get("commit_email", "bot@example.com"),
                commit_message_template=config.get(
                    "commit_message_template", 
                    "docs: update market sentiment report for {date}"
                )
            )
            
            self.uploader = GitUploader(self.config)
            logger.info("Git upload configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _create_default_config(self) -> None:
        """Create default configuration file"""
        default_local_path = str(Path(__file__).parent.parent / "temp" / "StockAnalyzeResults")
        
        default_config = {
            "enabled": False,
            "repo_url": "https://github.com/Alt3rmis/StockAnalyzeResults.git",
            "branch": "main",
            "local_path": default_local_path,
            "commit_author": "StockAnalyzer Bot",
            "commit_email": "bot@stockanalyzer.local",
            "commit_message_template": "docs: update market sentiment report for {date}"
        }
        
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Default Git configuration created at: {self._config_path}")
        
        self.config = GitConfig(
            enabled=False,
            repo_url=default_config["repo_url"],
            branch=default_config["branch"],
            local_path=default_local_path,
            commit_author=default_config["commit_author"],
            commit_email=default_config["commit_email"],
            commit_message_template=default_config["commit_message_template"]
        )
        self.uploader = GitUploader(self.config)
    
    def upload_report(self, report_file: Path, date_str: str) -> Tuple[bool, str]:
        """
        Upload a report file to the Git repository
        
        Args:
            report_file: Path to the report file
            date_str: Date string in YYYYMMDD format
        
        Returns:
            Tuple of (success, message)
        """
        if not self.config or not self.config.enabled:
            logger.info("Git upload is disabled")
            return False, "Git upload is disabled"
        
        if not self.uploader:
            return False, "Git uploader not initialized"
        
        return self.uploader.upload_markdown_file(report_file, date_str)
    
    def is_enabled(self) -> bool:
        """Check if Git upload is enabled"""
        return self.config is not None and self.config.enabled
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the Git upload manager"""
        return {
            "enabled": self.config.enabled if self.config else False,
            "repo_url": self.config.repo_url if self.config else None,
            "branch": self.config.branch if self.config else None,
            "local_path": self.config.local_path if self.config else None,
            "is_initialized": self.uploader._is_initialized if self.uploader else False
        }


def upload_report_to_git(
    report_file: Path,
    date_str: str,
    config_path: Optional[Path] = None
) -> Tuple[bool, str]:
    """
    Convenience function to upload a report to Git
    
    Args:
        report_file: Path to the Markdown report file
        date_str: Date string in YYYYMMDD format
        config_path: Optional path to configuration file
    
    Returns:
        Tuple of (success, message)
    """
    manager = GitUploadManager(config_path)
    return manager.upload_report(report_file, date_str)


if __name__ == "__main__":
    print("Testing Git Uploader...")
    print("=" * 60)
    
    manager = GitUploadManager()
    
    print(f"\nGit Upload Status:")
    status = manager.get_status()
    for key, value in status.items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 60)
    print("Test completed")
