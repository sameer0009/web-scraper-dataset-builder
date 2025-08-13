"""
Unit tests for core data models and validation.
"""

import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path
import tempfile
import json

from models import (
    ScrapingConfig, ScrapedData, ScrapingResult, CleaningOperation,
    CleaningHistory, ExportOptions, ApplicationState, Project,
    ContentType, ScrapingStatus, CleaningStrategy, ExportFormat,
    validate_url, validate_dataframe, validate_file_path,
    create_scraping_config, create_export_options, create_project
)


class TestScrapingConfig:
    """Test cases for ScrapingConfig model."""
    
    def test_valid_config_creation(self):
        """Test creating a valid scraping configuration."""
        config = ScrapingConfig(url="https://example.com")
        assert config.url == "https://example.com"
        assert config.max_pages == 10
        assert config.delay_between_requests == 1.0
        assert not config.use_dynamic_scraper
    
    def test_config_validation_invalid_url(self):
        """Test validation with invalid URL."""
        with pytest.raises(ValueError, match="URL must start with http"):
            ScrapingConfig(url="invalid-url")
    
    def test_config_validation_empty_url(self):
        """Test validation with empty URL."""
        with pytest.raises(ValueError, match="URL must be a non-empty string"):
            ScrapingConfig(url="")
    
    def test_config_validation_negative_pages(self):
        """Test validation with negative max_pages."""
        with pytest.raises(ValueError, match="max_pages must be positive"):
            ScrapingConfig(url="https://example.com", max_pages=-1)
    
    def test_config_validation_negative_delay(self):
        """Test validation with negative delay."""
        with pytest.raises(ValueError, match="delay_between_requests must be non-negative"):
            ScrapingConfig(url="https://example.com", delay_between_requests=-1.0)
    
    def test_config_to_dict(self):
        """Test converting config to dictionary."""
        config = ScrapingConfig(url="https://example.com", max_pages=5)
        config_dict = config.to_dict()
        
        assert config_dict['url'] == "https://example.com"
        assert config_dict['max_pages'] == 5
        assert isinstance(config_dict, dict)
    
    def test_config_from_dict(self):
        """Test creating config from dictionary."""
        data = {
            'url': 'https://example.com',
            'max_pages': 5,
            'delay_between_requests': 2.0,
            'use_dynamic_scraper': True
        }
        config = ScrapingConfig.from_dict(data)
        
        assert config.url == "https://example.com"
        assert config.max_pages == 5
        assert config.delay_between_requests == 2.0
        assert config.use_dynamic_scraper


class TestScrapedData:
    """Test cases for ScrapedData model."""
    
    def test_scraped_data_creation(self):
        """Test creating scraped data with DataFrame."""
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        scraped_data = ScrapedData(
            dataframe=df,
            source_url="https://example.com",
            scraping_timestamp=datetime.now()
        )
        
        assert scraped_data.total_records == 3
        assert scraped_data.columns_detected == ['col1', 'col2']
        assert len(scraped_data.data_types) == 2
    
    def test_scraped_data_empty_dataframe(self):
        """Test creating scraped data with empty DataFrame."""
        df = pd.DataFrame()
        scraped_data = ScrapedData(
            dataframe=df,
            source_url="https://example.com",
            scraping_timestamp=datetime.now()
        )
        
        assert scraped_data.total_records == 0
        assert scraped_data.columns_detected == []
        assert scraped_data.data_types == {}
    
    def test_scraped_data_summary(self):
        """Test getting summary of scraped data."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        scraped_data = ScrapedData(
            dataframe=df,
            source_url="https://example.com",
            scraping_timestamp=datetime.now(),
            errors=['test error']
        )
        
        summary = scraped_data.get_summary()
        assert summary['total_records'] == 3
        assert summary['total_columns'] == 1
        assert summary['has_errors'] is True
        assert summary['error_count'] == 1


class TestScrapingResult:
    """Test cases for ScrapingResult model."""
    
    def test_successful_result(self):
        """Test successful scraping result."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        result = ScrapingResult(
            data=df,
            metadata={'test': 'value'},
            errors=[],
            pages_scraped=1,
            total_records=3,
            scraping_timestamp=datetime.now(),
            status=ScrapingStatus.COMPLETED
        )
        
        assert result.is_successful()
        assert result.has_data()
    
    def test_failed_result(self):
        """Test failed scraping result."""
        result = ScrapingResult(
            data=pd.DataFrame(),
            metadata={},
            errors=['Connection failed'],
            pages_scraped=0,
            total_records=0,
            scraping_timestamp=datetime.now(),
            status=ScrapingStatus.FAILED
        )
        
        assert not result.is_successful()
        assert not result.has_data()


class TestCleaningOperation:
    """Test cases for CleaningOperation model."""
    
    def test_cleaning_operation_creation(self):
        """Test creating a cleaning operation."""
        operation = CleaningOperation(
            operation_type="remove_duplicates",
            parameters={'strategy': 'first'},
            target_columns=['col1', 'col2'],
            description="Remove duplicate rows"
        )
        
        assert operation.operation_type == "remove_duplicates"
        assert operation.parameters['strategy'] == 'first'
        assert not operation.applied
    
    def test_cleaning_operation_serialization(self):
        """Test serializing and deserializing cleaning operation."""
        operation = CleaningOperation(
            operation_type="remove_duplicates",
            parameters={'strategy': 'first'},
            target_columns=['col1'],
            description="Test operation"
        )
        
        # Serialize to dict
        op_dict = operation.to_dict()
        assert isinstance(op_dict['timestamp'], str)
        
        # Deserialize from dict
        restored_operation = CleaningOperation.from_dict(op_dict)
        assert restored_operation.operation_type == operation.operation_type
        assert restored_operation.parameters == operation.parameters


class TestCleaningHistory:
    """Test cases for CleaningHistory model."""
    
    def test_add_operation(self):
        """Test adding operations to history."""
        history = CleaningHistory()
        df = pd.DataFrame({'col1': [1, 2, 3]})
        operation = CleaningOperation(
            operation_type="test",
            parameters={},
            target_columns=[],
            description="Test"
        )
        
        history.add_operation(operation, df)
        
        assert len(history.operations) == 1
        assert len(history.data_snapshots) == 1
        assert history.current_index == 0
    
    def test_undo_redo_functionality(self):
        """Test undo/redo functionality."""
        history = CleaningHistory()
        df1 = pd.DataFrame({'col1': [1, 2, 3]})
        df2 = pd.DataFrame({'col1': [1, 2, 3, 4]})
        
        operation1 = CleaningOperation("op1", {}, [], "Op 1")
        operation2 = CleaningOperation("op2", {}, [], "Op 2")
        
        history.add_operation(operation1, df1)
        history.add_operation(operation2, df2)
        
        assert history.can_undo()
        assert not history.can_redo()
        assert history.current_index == 1
        
        # Simulate undo
        history.current_index -= 1
        assert history.can_redo()
    
    def test_max_history_limit(self):
        """Test that history respects maximum limit."""
        history = CleaningHistory(max_history=3)
        df = pd.DataFrame({'col1': [1]})
        
        # Add more operations than the limit
        for i in range(5):
            operation = CleaningOperation(f"op{i}", {}, [], f"Op {i}")
            history.add_operation(operation, df)
        
        assert len(history.operations) == 3
        assert len(history.data_snapshots) == 3


class TestExportOptions:
    """Test cases for ExportOptions model."""
    
    def test_valid_export_options(self):
        """Test creating valid export options."""
        options = ExportOptions(
            format=ExportFormat.EXCEL,
            include_index=True,
            encoding="utf-8"
        )
        
        assert options.format == ExportFormat.EXCEL
        assert options.include_index
        assert options.encoding == "utf-8"
    
    def test_export_options_validation(self):
        """Test export options validation."""
        options = ExportOptions(encoding="invalid-encoding")
        
        with pytest.raises(ValueError, match="Unsupported encoding"):
            options.validate()
    
    def test_json_orient_validation(self):
        """Test JSON orientation validation."""
        options = ExportOptions(json_orient="invalid-orient")
        
        with pytest.raises(ValueError, match="Invalid JSON orientation"):
            options.validate()


class TestProject:
    """Test cases for Project model."""
    
    def test_project_creation(self):
        """Test creating a project."""
        config = ScrapingConfig(url="https://example.com")
        export_options = ExportOptions()
        
        project = Project(
            name="Test Project",
            scraping_config=config,
            cleaning_operations=[],
            export_settings=export_options,
            created_date=datetime.now(),
            last_modified=datetime.now()
        )
        
        assert project.name == "Test Project"
        assert project.scraping_config.url == "https://example.com"
    
    def test_project_serialization(self):
        """Test project serialization to/from dictionary."""
        config = ScrapingConfig(url="https://example.com")
        export_options = ExportOptions()
        
        project = Project(
            name="Test Project",
            scraping_config=config,
            cleaning_operations=[],
            export_settings=export_options,
            created_date=datetime.now(),
            last_modified=datetime.now(),
            description="Test description",
            tags=["test", "example"]
        )
        
        # Serialize
        project_dict = project.to_dict()
        assert project_dict['name'] == "Test Project"
        assert project_dict['description'] == "Test description"
        assert project_dict['tags'] == ["test", "example"]
        
        # Deserialize
        restored_project = Project.from_dict(project_dict)
        assert restored_project.name == project.name
        assert restored_project.description == project.description
        assert restored_project.tags == project.tags
    
    def test_project_file_operations(self):
        """Test saving and loading project from file."""
        config = ScrapingConfig(url="https://example.com")
        export_options = ExportOptions()
        
        project = Project(
            name="File Test Project",
            scraping_config=config,
            cleaning_operations=[],
            export_settings=export_options,
            created_date=datetime.now(),
            last_modified=datetime.now()
        )
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            project.save_to_file(temp_path)
            
            # Load from file
            loaded_project = Project.load_from_file(temp_path)
            
            assert loaded_project.name == project.name
            assert loaded_project.scraping_config.url == project.scraping_config.url
            
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestValidationFunctions:
    """Test cases for validation functions."""
    
    def test_validate_url(self):
        """Test URL validation function."""
        assert validate_url("https://example.com")
        assert validate_url("http://example.com")
        assert not validate_url("invalid-url")
        assert not validate_url("")
        assert not validate_url(None)
    
    def test_validate_dataframe(self):
        """Test DataFrame validation function."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        assert validate_dataframe(df)
        assert not validate_dataframe(None)
        assert not validate_dataframe("not a dataframe")
    
    def test_validate_file_path(self):
        """Test file path validation function."""
        # Test with current directory
        assert validate_file_path("test.txt")
        
        # Test with existing directory
        temp_dir = tempfile.mkdtemp()
        try:
            test_path = Path(temp_dir) / "test.txt"
            assert validate_file_path(str(test_path))
        finally:
            Path(temp_dir).rmdir()


class TestFactoryFunctions:
    """Test cases for factory functions."""
    
    def test_create_scraping_config(self):
        """Test scraping config factory function."""
        config = create_scraping_config("https://example.com", max_pages=5)
        
        assert config.url == "https://example.com"
        assert config.max_pages == 5
    
    def test_create_export_options(self):
        """Test export options factory function."""
        options = create_export_options("csv", include_index=True)
        
        assert options.format == ExportFormat.CSV
        assert options.include_index
    
    def test_create_project(self):
        """Test project factory function."""
        project = create_project("Test Project", "https://example.com")
        
        assert project.name == "Test Project"
        assert project.scraping_config.url == "https://example.com"
        assert isinstance(project.created_date, datetime)


if __name__ == "__main__":
    pytest.main([__file__])