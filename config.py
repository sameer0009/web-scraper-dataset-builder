"""
Configuration Management for Web Scraper & Dataset Builder

This module handles application configuration, settings persistence,
and default values for the application.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class ScrapingDefaults:
    """Default settings for web scraping operations."""
    max_pages: int = 10
    delay_between_requests: float = 1.0
    timeout: int = 30
    max_retries: int = 3
    use_dynamic_scraper: bool = False
    respect_robots_txt: bool = True
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


@dataclass
class UIDefaults:
    """Default settings for the user interface."""
    theme: str = "dark"
    window_width: int = 1200
    window_height: int = 800
    auto_save_projects: bool = True
    show_advanced_options: bool = False
    log_level: str = "INFO"


@dataclass
class ExportDefaults:
    """Default settings for data export."""
    default_format: str = "xlsx"
    include_index: bool = False
    encoding: str = "utf-8"
    excel_sheet_name: str = "ScrapedData"
    csv_delimiter: str = ","
    json_orient: str = "records"


class AppConfig:
    """Main application configuration manager."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration with optional custom config file path."""
        self.config_file = config_file or self._get_default_config_path()
        self.config_dir = Path(self.config_file).parent
        
        # Default configuration sections
        self.scraping = ScrapingDefaults()
        self.ui = UIDefaults()
        self.export = ExportDefaults()
        
        # Application metadata
        self.app_name = "Web Scraper & Dataset Builder"
        self.app_version = "1.0.0"
        self.created_date = datetime.now()
        self.last_modified = datetime.now()
        
        # Runtime settings
        self.recent_projects: list = []
        self.recent_urls: list = []
        self.custom_headers: Dict[str, str] = {}
        
        # Ensure config directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing configuration
        self.load()
    
    def _get_default_config_path(self) -> str:
        """Get the default configuration file path based on the operating system."""
        app_name = "Web Scraper & Dataset Builder"
        if os.name == 'nt':  # Windows
            config_dir = Path(os.environ.get('APPDATA', '')) / app_name
        else:  # Unix-like systems
            config_dir = Path.home() / '.config' / 'web-scraper-dataset-builder'
        
        return str(config_dir / 'config.json')
    
    @property
    def log_file(self) -> str:
        """Get the log file path."""
        return str(self.config_dir / 'application.log')
    
    @property
    def log_level(self) -> str:
        """Get the current log level."""
        return self.ui.log_level
    
    @property
    def projects_dir(self) -> Path:
        """Get the projects directory path."""
        if hasattr(self, '_projects_dir'):
            return self._projects_dir
        projects_path = self.config_dir / 'projects'
        projects_path.mkdir(exist_ok=True)
        return projects_path
    
    @projects_dir.setter
    def projects_dir(self, value: Path) -> None:
        """Set the projects directory path."""
        self._projects_dir = Path(value)
    
    @property
    def temp_dir(self) -> Path:
        """Get the temporary files directory path."""
        temp_path = self.config_dir / 'temp'
        temp_path.mkdir(exist_ok=True)
        return temp_path
    
    def load(self) -> bool:
        """Load configuration from file."""
        try:
            if not os.path.exists(self.config_file):
                # Create default configuration file
                self.save()
                return True
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Load scraping settings
            if 'scraping' in config_data:
                scraping_data = config_data['scraping']
                for key, value in scraping_data.items():
                    if hasattr(self.scraping, key):
                        setattr(self.scraping, key, value)
            
            # Load UI settings
            if 'ui' in config_data:
                ui_data = config_data['ui']
                for key, value in ui_data.items():
                    if hasattr(self.ui, key):
                        setattr(self.ui, key, value)
            
            # Load export settings
            if 'export' in config_data:
                export_data = config_data['export']
                for key, value in export_data.items():
                    if hasattr(self.export, key):
                        setattr(self.export, key, value)
            
            # Load other settings
            self.recent_projects = config_data.get('recent_projects', [])
            self.recent_urls = config_data.get('recent_urls', [])
            self.custom_headers = config_data.get('custom_headers', {})
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}")
            return False
    
    def save(self) -> bool:
        """Save current configuration to file."""
        try:
            config_data = {
                'app_name': self.app_name,
                'app_version': self.app_version,
                'last_modified': self.last_modified.isoformat(),
                'scraping': asdict(self.scraping),
                'ui': asdict(self.ui),
                'export': asdict(self.export),
                'recent_projects': self.recent_projects,
                'recent_urls': self.recent_urls,
                'custom_headers': self.custom_headers
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            self.last_modified = datetime.now()
            return True
            
        except Exception as e:
            logging.error(f"Failed to save configuration: {e}")
            return False
    
    def add_recent_project(self, project_path: str) -> None:
        """Add a project to the recent projects list."""
        if project_path in self.recent_projects:
            self.recent_projects.remove(project_path)
        
        self.recent_projects.insert(0, project_path)
        
        # Keep only the last 10 recent projects
        self.recent_projects = self.recent_projects[:10]
        self.save()
    
    def add_recent_url(self, url: str) -> None:
        """Add a URL to the recent URLs list."""
        if url in self.recent_urls:
            self.recent_urls.remove(url)
        
        self.recent_urls.insert(0, url)
        
        # Keep only the last 20 recent URLs
        self.recent_urls = self.recent_urls[:20]
        self.save()
    
    def get_scraping_config(self) -> Dict[str, Any]:
        """Get scraping configuration as dictionary."""
        return asdict(self.scraping)
    
    def get_ui_config(self) -> Dict[str, Any]:
        """Get UI configuration as dictionary."""
        return asdict(self.ui)
    
    def get_export_config(self) -> Dict[str, Any]:
        """Get export configuration as dictionary."""
        return asdict(self.export)
    
    def update_scraping_config(self, **kwargs) -> None:
        """Update scraping configuration with provided values."""
        for key, value in kwargs.items():
            if hasattr(self.scraping, key):
                setattr(self.scraping, key, value)
        self.save()
    
    def update_ui_config(self, **kwargs) -> None:
        """Update UI configuration with provided values."""
        for key, value in kwargs.items():
            if hasattr(self.ui, key):
                setattr(self.ui, key, value)
        self.save()
    
    def update_export_config(self, **kwargs) -> None:
        """Update export configuration with provided values."""
        for key, value in kwargs.items():
            if hasattr(self.export, key):
                setattr(self.export, key, value)
        self.save()
    
    def reset_to_defaults(self) -> None:
        """Reset all configuration to default values."""
        self.scraping = ScrapingDefaults()
        self.ui = UIDefaults()
        self.export = ExportDefaults()
        self.recent_projects = []
        self.recent_urls = []
        self.custom_headers = {}
        self.save()
    
    def validate_config(self) -> bool:
        """Validate the current configuration."""
        try:
            # Validate scraping settings
            assert self.scraping.max_pages > 0, "max_pages must be positive"
            assert self.scraping.delay_between_requests >= 0, "delay must be non-negative"
            assert self.scraping.timeout > 0, "timeout must be positive"
            assert self.scraping.max_retries >= 0, "max_retries must be non-negative"
            
            # Validate UI settings
            assert self.ui.theme in ['light', 'dark'], "theme must be 'light' or 'dark'"
            assert self.ui.window_width > 0, "window_width must be positive"
            assert self.ui.window_height > 0, "window_height must be positive"
            assert self.ui.log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR'], "invalid log_level"
            
            # Validate export settings
            assert self.export.default_format in ['xlsx', 'csv', 'json'], "invalid default_format"
            assert self.export.encoding in ['utf-8', 'utf-16', 'ascii'], "invalid encoding"
            
            return True
            
        except AssertionError as e:
            logging.error(f"Configuration validation failed: {e}")
            return False