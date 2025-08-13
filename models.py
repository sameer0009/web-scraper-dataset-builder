"""
Core data models and validation for Web Scraper & Dataset Builder

This module defines the fundamental data structures used throughout
the application with comprehensive validation and type safety.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlparse
from pathlib import Path

from utils.logger import get_logger


class ContentType(Enum):
    """Types of web content that can be scraped."""
    STATIC = "static"
    DYNAMIC = "dynamic"
    MIXED = "mixed"


class ExportFormat(Enum):
    """Supported export formats."""
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    HTML = "html"
    XML = "xml"


@dataclass
class ScrapingConfig:
    """Configuration for web scraping operations."""
    url: str
    target_elements: List[str] = field(default_factory=lambda: ["table", "div", "span"])
    max_pages: int = 10
    delay_between_requests: float = 1.0
    use_dynamic_scraper: bool = False
    custom_headers: Dict[str, str] = field(default_factory=dict)
    timeout: int = 30
    max_retries: int = 3
    respect_robots_txt: bool = True
    follow_redirects: bool = True
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
    def validate(self) -> bool:
        """Validate the scraping configuration."""
        try:
            # Validate URL
            if not self.url or not isinstance(self.url, str):
                raise ValueError("URL must be a non-empty string")
            
            parsed_url = urlparse(self.url)
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValueError("URL must have a valid scheme and domain")
            
            # Validate numeric parameters
            if self.max_pages <= 0:
                raise ValueError("max_pages must be positive")
            
            if self.delay_between_requests < 0:
                raise ValueError("delay_between_requests must be non-negative")
            
            if self.timeout <= 0:
                raise ValueError("timeout must be positive")
            
            if self.max_retries < 0:
                raise ValueError("max_retries must be non-negative")
            
            # Validate target elements
            if not self.target_elements or not isinstance(self.target_elements, list):
                raise ValueError("target_elements must be a non-empty list")
            
            return True
            
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"ScrapingConfig validation failed: {e}")
            return False


@dataclass
class ScrapingResult:
    """Result of a scraping operation."""
    data: pd.DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: 'ScrapingStatus' = None
    error_message: Optional[str] = None
    scraping_timestamp: datetime = field(default_factory=datetime.now)
    errors: List[str] = field(default_factory=list)
    pages_scraped: int = 0
    total_records: int = 0
    
    def __post_init__(self):
        """Set default status based on data."""
        if self.status is None:
            if self.error_message or self.errors:
                self.status = ScrapingStatus.FAILED
            elif self.data is not None and not self.data.empty:
                self.status = ScrapingStatus.SUCCESS
            else:
                self.status = ScrapingStatus.NO_DATA
        
        # Set total_records from data if not provided
        if self.total_records == 0 and self.data is not None:
            self.total_records = len(self.data)

@dataclass
class ScrapedData:
    """Container for scraped data with metadata."""
    dataframe: pd.DataFrame
    source_url: str
    scraping_timestamp: datetime
    total_records: int = 0
    columns_detected: List[str] = field(default_factory=list)
    data_types: Dict[str, str] = field(default_factory=dict)
    content_type: ContentType = ContentType.STATIC
    pages_scraped: int = 1
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize computed fields."""
        if self.total_records == 0:
            self.total_records = len(self.dataframe) if not self.dataframe.empty else 0
        
        if not self.columns_detected:
            self.columns_detected = list(self.dataframe.columns) if not self.dataframe.empty else []
        
        if not self.data_types:
            self.data_types = {
                col: str(dtype) for col, dtype in self.dataframe.dtypes.items()
            } if not self.dataframe.empty else {}


@dataclass
class CleaningOperation:
    """Represents a data cleaning operation."""
    operation_type: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    target_columns: List[str] = field(default_factory=list)
    description: str = ""
    applied: bool = False
    records_affected: int = 0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ExportOptions:
    """Configuration options for data export."""
    format: ExportFormat = ExportFormat.EXCEL
    include_index: bool = False
    sheet_name: str = "ScrapedData"
    encoding: str = "utf-8"
    delimiter: str = ","
    json_orient: str = "records"
    compression: Optional[str] = None
    
    def validate(self) -> bool:
        """Validate export options."""
        try:
            # Validate format
            if not isinstance(self.format, ExportFormat):
                raise ValueError("format must be an ExportFormat enum")
            
            # Validate encoding
            valid_encodings = ['utf-8', 'utf-16', 'ascii', 'latin-1']
            if self.encoding not in valid_encodings:
                raise ValueError(f"encoding must be one of: {valid_encodings}")
            
            # Validate JSON orient
            valid_orients = ['records', 'index', 'values', 'split', 'table']
            if self.json_orient not in valid_orients:
                raise ValueError(f"json_orient must be one of: {valid_orients}")
            
            return True
            
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"ExportOptions validation failed: {e}")
            return False


@dataclass
class Project:
    """Complete project configuration and state."""
    name: str
    scraping_config: ScrapingConfig
    cleaning_operations: List[CleaningOperation] = field(default_factory=list)
    export_settings: ExportOptions = field(default_factory=ExportOptions)
    created_date: datetime = field(default_factory=datetime.now)
    last_modified: datetime = field(default_factory=datetime.now)
    description: str = ""
    tags: List[str] = field(default_factory=list)
    version: str = "1.0"
    
    def save_to_file(self, filepath: Path) -> None:
        """Save project to JSON file."""
        import json
        from pathlib import Path
        
        # Update last modified time
        self.last_modified = datetime.now()
        
        # Convert to dictionary for JSON serialization
        project_dict = {
            'name': self.name,
            'description': self.description,
            'tags': self.tags,
            'version': self.version,
            'created_date': self.created_date.isoformat(),
            'last_modified': self.last_modified.isoformat(),
            'scraping_config': {
                'url': self.scraping_config.url,
                'target_elements': self.scraping_config.target_elements,
                'max_pages': self.scraping_config.max_pages,
                'delay_between_requests': self.scraping_config.delay_between_requests,
                'use_dynamic_scraper': self.scraping_config.use_dynamic_scraper,
                'custom_headers': self.scraping_config.custom_headers,
                'timeout': self.scraping_config.timeout,
                'max_retries': self.scraping_config.max_retries,
                'respect_robots_txt': self.scraping_config.respect_robots_txt,
                'follow_redirects': self.scraping_config.follow_redirects,
                'user_agent': self.scraping_config.user_agent
            },
            'cleaning_operations': [
                {
                    'operation_type': op.operation_type,
                    'parameters': op.parameters,
                    'timestamp': op.timestamp.isoformat(),
                    'affected_columns': op.affected_columns,
                    'records_affected': op.records_affected
                } for op in self.cleaning_operations
            ],
            'export_settings': {
                'format': self.export_settings.format.value,
                'include_index': self.export_settings.include_index,
                'encoding': self.export_settings.encoding,
                'sheet_name': self.export_settings.sheet_name,
                'delimiter': self.export_settings.delimiter,
                'json_orient': self.export_settings.json_orient,
                'compression': self.export_settings.compression
            }
        }
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(project_dict, f, indent=2, ensure_ascii=False)
    
    @classmethod
    def load_from_file(cls, filepath: Path) -> 'Project':
        """Load project from JSON file."""
        import json
        from pathlib import Path
        
        with open(filepath, 'r', encoding='utf-8') as f:
            project_dict = json.load(f)
        
        # Parse dates
        created_date = datetime.fromisoformat(project_dict['created_date'])
        last_modified = datetime.fromisoformat(project_dict['last_modified'])
        
        # Reconstruct ScrapingConfig
        scraping_config = ScrapingConfig(
            url=project_dict['scraping_config']['url'],
            target_elements=project_dict['scraping_config']['target_elements'],
            max_pages=project_dict['scraping_config']['max_pages'],
            delay_between_requests=project_dict['scraping_config']['delay_between_requests'],
            use_dynamic_scraper=project_dict['scraping_config']['use_dynamic_scraper'],
            custom_headers=project_dict['scraping_config']['custom_headers'],
            timeout=project_dict['scraping_config']['timeout'],
            max_retries=project_dict['scraping_config']['max_retries'],
            respect_robots_txt=project_dict['scraping_config']['respect_robots_txt'],
            follow_redirects=project_dict['scraping_config']['follow_redirects'],
            user_agent=project_dict['scraping_config']['user_agent']
        )
        
        # Reconstruct cleaning operations
        cleaning_operations = []
        for op_dict in project_dict['cleaning_operations']:
            operation = CleaningOperation(
                operation_type=op_dict['operation_type'],
                parameters=op_dict['parameters'],
                timestamp=datetime.fromisoformat(op_dict['timestamp']),
                affected_columns=op_dict['affected_columns'],
                records_affected=op_dict['records_affected']
            )
            cleaning_operations.append(operation)
        
        # Reconstruct export settings
        export_settings = ExportOptions(
            format=ExportFormat(project_dict['export_settings']['format']),
            include_index=project_dict['export_settings']['include_index'],
            encoding=project_dict['export_settings']['encoding'],
            sheet_name=project_dict['export_settings']['sheet_name'],
            delimiter=project_dict['export_settings']['delimiter'],
            json_orient=project_dict['export_settings']['json_orient'],
            compression=project_dict['export_settings']['compression']
        )
        
        # Create and return project
        return cls(
            name=project_dict['name'],
            scraping_config=scraping_config,
            cleaning_operations=cleaning_operations,
            export_settings=export_settings,
            created_date=created_date,
            last_modified=last_modified,
            description=project_dict['description'],
            tags=project_dict['tags'],
            version=project_dict['version']
        )


# Validation functions
def validate_dataframe(df: pd.DataFrame) -> bool:
    """Validate that a DataFrame is suitable for processing."""
    try:
        if df is None:
            return False
        
        if not isinstance(df, pd.DataFrame):
            return False
        
        # Allow empty DataFrames
        return True
        
    except Exception:
        return False


def validate_file_path(filepath: str) -> bool:
    """Validate that a file path is valid and writable."""
    try:
        from pathlib import Path
        path = Path(filepath)
        
        # Check if parent directory exists or can be created
        parent = path.parent
        if not parent.exists():
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                return False
        
        return True
            
    except Exception:
        return False


def create_project(name: str, url: str, description: str = "", tags: List[str] = None) -> Project:
    """Factory function to create a new project with default settings."""
    scraping_config = ScrapingConfig(url=url)
    export_settings = ExportOptions()
    
    return Project(
        name=name,
        scraping_config=scraping_config,
        export_settings=export_settings,
        description=description,
        tags=tags or []
    )

# Abstract Interfaces for Components
from abc import ABC, abstractmethod


class ScraperInterface(ABC):
    """Abstract interface for web scrapers."""
    
    @abstractmethod
    def scrape_page(self, url: str) -> 'ScrapedData':
        """Scrape a single page and return structured data."""
        pass


class CleanerInterface(ABC):
    """Abstract interface for data cleaners."""
    
    @abstractmethod
    def remove_duplicates(self, strategy: str = 'first') -> pd.DataFrame:
        """Remove duplicate records from the dataset."""
        pass
    
    @abstractmethod
    def handle_missing_values(self, strategy: str, columns: List[str] = None) -> pd.DataFrame:
        """Handle missing values in the dataset."""
        pass


class ExporterInterface(ABC):
    """Abstract interface for data exporters."""
    
    @abstractmethod
    def export_to_excel(self, data: pd.DataFrame, filepath: str, options: 'ExportOptions') -> bool:
        """Export data to Excel format."""
        pass
    
    @abstractmethod
    def export_to_csv(self, data: pd.DataFrame, filepath: str, options: 'ExportOptions') -> bool:
        """Export data to CSV format."""
        pass
    
    @abstractmethod
    def export_to_json(self, data: pd.DataFrame, filepath: str, options: 'ExportOptions') -> bool:
        """Export data to JSON format."""
        pass


# Validation Functions
def validate_dataframe(df: pd.DataFrame) -> bool:
    """Validate that a DataFrame is suitable for processing."""
    if df is None:
        return False
    if not isinstance(df, pd.DataFrame):
        return False
    return True


def validate_file_path(filepath: str) -> bool:
    """Validate that a file path is valid."""
    if not filepath or not isinstance(filepath, str):
        return False
    if len(filepath.strip()) == 0:
        return False
    return True


def validate_project(project: 'Project') -> bool:
    """Validate that a project has all required fields."""
    if not project or not hasattr(project, 'name'):
        return False
    if not project.name or not project.name.strip():
        return False
    if not hasattr(project, 'scraping_config') or not project.scraping_config:
        return False
    return True


# Additional Enums and Status Classes
class ScrapingStatus(Enum):
    """Status of scraping operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NO_DATA = "no_data"


@dataclass
class ProjectTemplate:
    """Template for creating new projects."""
    name: str
    description: str
    scraping_config: ScrapingConfig
    cleaning_operations: List['CleaningOperation']
    export_settings: ExportOptions


@dataclass
class ProjectMetadata:
    """Metadata for project tracking."""
    total_records_scraped: int = 0
    last_scraping_date: Optional[datetime] = None
    export_count: int = 0
    data_quality_score: float = 0.0

@dataclass
class CleaningHistory:
    """Tracks cleaning operations for undo/redo functionality."""
    operations: List['CleaningOperation'] = field(default_factory=list)
    data_snapshots: List[pd.DataFrame] = field(default_factory=list)
    current_index: int = 0
    max_snapshots: int = 20  # Limit memory usage
    
    def add_operation(self, operation: 'CleaningOperation', data_snapshot: pd.DataFrame):
        """Add a new operation and data snapshot."""
        # Remove any operations after current index (for redo functionality)
        if self.current_index < len(self.operations):
            self.operations = self.operations[:self.current_index]
            self.data_snapshots = self.data_snapshots[:self.current_index + 1]
        
        # Add new operation and snapshot
        self.operations.append(operation)
        self.data_snapshots.append(data_snapshot.copy())
        self.current_index = len(self.operations)  # Point to the latest operation
        
        # Limit memory usage by removing old snapshots
        if len(self.data_snapshots) > self.max_snapshots:
            self.data_snapshots.pop(0)
            self.operations.pop(0)
            self.current_index -= 1
    
    def can_undo(self) -> bool:
        """Check if undo operation is possible."""
        return self.current_index > 0
    
    def can_redo(self) -> bool:
        """Check if redo operation is possible."""
        return self.current_index < len(self.operations)
    
    def get_current_data(self) -> pd.DataFrame:
        """Get the current data snapshot."""
        if 0 <= self.current_index < len(self.data_snapshots):
            return self.data_snapshots[self.current_index].copy()
        elif len(self.data_snapshots) > 0:
            return self.data_snapshots[-1].copy()  # Return latest if index is out of bounds
        return pd.DataFrame()
    
    def get_operation_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all operations."""
        return [
            {
                'index': i,
                'type': op.operation_type,
                'description': op.description,
                'timestamp': op.timestamp.isoformat(),
                'records_affected': op.records_affected,
                'is_current': i == self.current_index
            }
            for i, op in enumerate(self.operations)
        ]
        self.data_snapshots.append(data_snapshot.copy())
        self.current_index += 1
        
        # Limit history size to prevent memory issues
        max_history = 20
        if len(self.operations) > max_history:
            self.operations = self.operations[-max_history:]
            self.data_snapshots = self.data_snapshots[-max_history:]
            self.current_index = len(self.operations) - 1
    
    def can_undo(self) -> bool:
        """Check if undo is possible."""
        return self.current_index > 0
    
    def can_redo(self) -> bool:
        """Check if redo is possible."""
        return self.current_index < len(self.operations)
    
    def get_current_data(self) -> pd.DataFrame:
        """Get the current data snapshot."""
        if self.current_index < len(self.data_snapshots):
            return self.data_snapshots[self.current_index].copy()
        return pd.DataFrame()