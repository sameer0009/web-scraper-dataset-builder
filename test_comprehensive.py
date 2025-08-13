#!/usr/bin/env python3
"""
Comprehensive test suite for Web Scraper & Dataset Builder

This test verifies all implemented functionality including models,
scraping, cleaning, export, and project management.
"""

import sys
import os
import pandas as pd
import numpy as np
import tempfile
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_models():
    """Test core data models."""
    print("\n🧪 Testing Core Data Models...")
    
    try:
        from models import (
            ScrapingConfig, ScrapedData, CleaningOperation, 
            ExportOptions, Project, create_project, ContentType
        )
        
        # Test ScrapingConfig
        config = ScrapingConfig(url="https://example.com", max_pages=5)
        assert config.url == "https://example.com"
        assert config.max_pages == 5
        config.validate()  # Should not raise exception
        print("✓ ScrapingConfig creation and validation")
        
        # Test ScrapedData
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        scraped_data = ScrapedData(
            dataframe=df,
            source_url="https://example.com",
            scraping_timestamp=datetime.now(),
            total_records=3,
            columns_detected=['col1', 'col2'],
            data_types={'col1': 'int64', 'col2': 'object'},
            content_type=ContentType.STATIC,
            pages_scraped=1,
            errors=[],
            warnings=[]
        )
        assert scraped_data.total_records == 3
        assert len(scraped_data.columns_detected) == 2
        print("✓ ScrapedData creation and metadata")
        
        # Test Project creation
        project = create_project("Test Project", "https://example.com")
        assert project.name == "Test Project"
        assert project.scraping_config.url == "https://example.com"
        print("✓ Project factory function")
        
        return True
        
    except Exception as e:
        print(f"❌ Models test failed: {e}")
        return False


def test_scraper():
    """Test web scraping functionality."""
    print("\n🌐 Testing Web Scraper...")
    
    try:
        from scraper import WebScraper, StaticScraper
        from models import ScrapingConfig, ContentType
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        config = ScrapingConfig(url="https://httpbin.org/html")
        
        # Test static scraper
        static_scraper = StaticScraper(config, error_handler)
        print("✓ StaticScraper initialization")
        
        # Test main scraper
        scraper = WebScraper(config, error_handler)
        print("✓ WebScraper initialization")
        
        # Test content type detection
        content_type = scraper.detect_content_type("https://httpbin.org/html")
        assert content_type in [ContentType.STATIC, ContentType.DYNAMIC]
        print(f"✓ Content type detection: {content_type}")
        
        return True
        
    except Exception as e:
        print(f"❌ Scraper test failed: {e}")
        return False


def test_cleaner():
    """Test data cleaning functionality."""
    print("\n🧹 Testing Data Cleaner...")
    
    try:
        from cleaner import DataCleaner
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        
        # Create test data
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Alice', 'Charlie', None],
            'age': [25, 30, 25, 35, None],
            'city': ['New York', 'London', 'New York', 'Paris', 'Tokyo']
        })
        
        cleaner = DataCleaner(df, error_handler)
        print("✓ DataCleaner initialization")
        
        # Test duplicate removal
        cleaned_df = cleaner.remove_duplicates(strategy='first')
        assert len(cleaned_df) < len(df)
        print("✓ Duplicate removal")
        
        # Test missing value handling
        cleaner.handle_missing_values(strategy='drop')
        print("✓ Missing value handling")
        
        # Test data summary
        summary = cleaner.get_data_summary()
        assert 'rows' in summary
        assert 'columns' in summary
        print("✓ Data summary generation")
        
        return True
        
    except Exception as e:
        print(f"❌ Cleaner test failed: {e}")
        return False


def test_export_manager():
    """Test export functionality."""
    print("\n📤 Testing Export Manager...")
    
    try:
        from export_manager import ExportManager
        from models import ExportOptions, ExportFormat
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        export_manager = ExportManager(error_handler)
        
        # Create test data
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'city': ['New York', 'London', 'Paris']
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Test Excel export
            excel_options = ExportOptions(format=ExportFormat.EXCEL)
            excel_file = temp_path / "test.xlsx"
            success = export_manager.export_to_excel(df, str(excel_file), excel_options)
            assert success
            assert excel_file.exists()
            print("✓ Excel export")
            
            # Test CSV export
            csv_options = ExportOptions(format=ExportFormat.CSV)
            csv_file = temp_path / "test.csv"
            success = export_manager.export_to_csv(df, str(csv_file), csv_options)
            assert success
            assert csv_file.exists()
            print("✓ CSV export")
            
            # Test JSON export
            json_options = ExportOptions(format=ExportFormat.JSON)
            json_file = temp_path / "test.json"
            success = export_manager.export_to_json(df, str(json_file), json_options)
            assert success
            assert json_file.exists()
            print("✓ JSON export")
            
            # Test export statistics
            stats = export_manager.get_export_statistics()
            assert 'total_exports' in stats
            print("✓ Export statistics")
        
        return True
        
    except Exception as e:
        print(f"❌ Export manager test failed: {e}")
        return False


def test_project_manager():
    """Test project management functionality."""
    print("\n📁 Testing Project Manager...")
    
    try:
        from project_manager import ProjectManager
        from config import AppConfig
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config with temp directory
            config = AppConfig()
            config.projects_dir = Path(temp_dir)
            
            # Create project manager
            pm = ProjectManager(config, error_handler)
            print("✓ ProjectManager initialization")
            
            # Test project creation
            project = pm.create_new_project(
                name="Test Project",
                url="https://example.com",
                description="A test project"
            )
            assert project.name == "Test Project"
            print("✓ Project creation")
            
            # Test project saving
            success = pm.save_project(project)
            assert success
            print("✓ Project saving")
            
            # Test project loading
            loaded_project = pm.load_project(pm._get_project_filepath("Test Project"))
            assert loaded_project is not None
            assert loaded_project.name == "Test Project"
            print("✓ Project loading")
            
            # Test project listing
            projects = pm.list_projects()
            assert len(projects) >= 1
            print("✓ Project listing")
            
            # Test project statistics
            stats = pm.get_project_statistics()
            assert 'total_projects' in stats
            print("✓ Project statistics")
        
        return True
        
    except Exception as e:
        print(f"❌ Project manager test failed: {e}")
        return False


def test_error_handling():
    """Test error handling system."""
    print("\n⚠️ Testing Error Handling...")
    
    try:
        from utils.error_handler import ErrorHandler, ErrorType
        from utils.logger import setup_logging
        import requests
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        
        # Test network error handling
        network_error = requests.exceptions.ConnectionError("Connection failed")
        response = error_handler.handle_network_error(network_error, "https://invalid-url.com")
        
        assert response.success == False
        assert response.error_type == ErrorType.NETWORK
        print("✓ Network error handling")
        
        # Test validation error handling
        validation_error = ValueError("Invalid input")
        response = error_handler.handle_validation_error(validation_error, "test_field", "invalid_value")
        
        assert response.success == False
        assert response.error_type == ErrorType.VALIDATION
        print("✓ Validation error handling")
        
        # Test error statistics
        stats = error_handler.get_error_statistics()
        assert 'total_errors' in stats
        print("✓ Error statistics")
        
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False


def test_integration():
    """Test complete integration workflow."""
    print("\n🔄 Testing Integration Workflow...")
    
    try:
        from config import AppConfig
        from utils.logger import setup_logging
        from utils.error_handler import ErrorHandler
        from project_manager import ProjectManager
        from cleaner import DataCleaner
        from export_manager import ExportManager
        from models import ExportOptions, ExportFormat
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config
            config = AppConfig()
            config.projects_dir = Path(temp_dir)
            
            # Create managers
            pm = ProjectManager(config, error_handler)
            export_manager = ExportManager(error_handler)
            
            # Create project
            project = pm.create_new_project(
                name="Integration Test",
                url="https://example.com",
                description="Integration test project"
            )
            print("✓ Project created for integration test")
            
            # Simulate scraped data
            scraped_data = pd.DataFrame({
                'title': ['Article 1', 'Article 2', 'Article 1', 'Article 3'],
                'author': ['John Doe', 'Jane Smith', 'John Doe', 'Bob Johnson'],
                'views': [1000, 1500, 1000, 800],
                'published': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03']
            })
            print("✓ Test data created")
            
            # Clean data
            cleaner = DataCleaner(scraped_data, error_handler)
            cleaner.remove_duplicates(strategy='first')
            cleaner.convert_data_types({'views': 'integer', 'published': 'datetime'})
            
            cleaned_data = cleaner.data
            assert len(cleaned_data) < len(scraped_data)  # Duplicates removed
            print("✓ Data cleaned successfully")
            
            # Export data
            export_path = Path(temp_dir) / "integration_test.xlsx"
            export_options = ExportOptions(
                format=ExportFormat.EXCEL,
                include_index=False,
                sheet_name="CleanedData"
            )
            
            success = export_manager.export_to_excel(cleaned_data, str(export_path), export_options)
            assert success
            assert export_path.exists()
            print("✓ Data exported successfully")
            
            # Save project
            assert pm.save_project(project)
            print("✓ Project saved")
            
            # Verify project can be loaded
            loaded_project = pm.get_project("Integration Test")
            assert loaded_project is not None
            assert loaded_project.name == "Integration Test"
            print("✓ Project loaded successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False


def test_performance():
    """Test performance with larger datasets."""
    print("\n⚡ Testing Performance...")
    
    try:
        from cleaner import DataCleaner
        from export_manager import ExportManager
        from models import ExportOptions, ExportFormat
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=False)
        error_handler = ErrorHandler(logger)
        
        # Create large test dataset
        large_data = pd.DataFrame({
            'id': range(5000),  # Reduced size for faster testing
            'name': [f'Item {i}' for i in range(5000)],
            'value': np.random.randn(5000),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 5000)
        })
        print(f"✓ Created test dataset with {len(large_data)} records")
        
        # Test data cleaning performance
        start_time = time.time()
        cleaner = DataCleaner(large_data, error_handler)
        cleaner.remove_duplicates()
        cleaning_time = time.time() - start_time
        
        assert cleaning_time < 5.0  # Should complete within 5 seconds
        print(f"✓ Data cleaning completed in {cleaning_time:.2f}s")
        
        # Test export performance
        export_manager = ExportManager(error_handler)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = Path(temp_dir) / "performance_test.xlsx"
            
            start_time = time.time()
            success = export_manager.export_to_excel(
                large_data, 
                str(export_path), 
                ExportOptions(format=ExportFormat.EXCEL)
            )
            export_time = time.time() - start_time
            
            assert success
            assert export_time < 15.0  # Should complete within 15 seconds
            assert export_path.exists()
            print(f"✓ Data export completed in {export_time:.2f}s")
            
            # Check file size is reasonable
            file_size = export_path.stat().st_size
            assert file_size > 50000  # At least 50KB for 5k records
            print(f"✓ Export file size: {file_size / 1024:.1f} KB")
        
        return True
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return False


def main():
    """Run all comprehensive tests."""
    print("=" * 70)
    print("🧪 Web Scraper & Dataset Builder - Comprehensive Test Suite")
    print("=" * 70)
    
    tests = [
        ("Core Data Models", test_models),
        ("Web Scraper", test_scraper),
        ("Data Cleaner", test_cleaner),
        ("Export Manager", test_export_manager),
        ("Project Manager", test_project_manager),
        ("Error Handling", test_error_handling),
        ("Integration Workflow", test_integration),
        ("Performance Tests", test_performance)
    ]
    
    passed = 0
    total = len(tests)
    failed_tests = []
    
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*20} {test_name} {'='*20}")
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                failed_tests.append(test_name)
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            failed_tests.append(test_name)
            print(f"❌ {test_name} FAILED with exception: {e}")
    
    # Final results
    print("\n" + "=" * 70)
    print("📊 COMPREHENSIVE TEST RESULTS")
    print("=" * 70)
    print(f"Tests Passed: {passed}/{total}")
    print(f"Success Rate: {(passed/total)*100:.1f}%")
    
    if failed_tests:
        print(f"\n❌ Failed Tests:")
        for test in failed_tests:
            print(f"  - {test}")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! The application is ready for production.")
        print("🚀 You can now run the application with: python main.py")
        return 0
    else:
        print(f"\n⚠️  {len(failed_tests)} test(s) failed. Please review and fix issues.")
        return 1


if __name__ == "__main__":
    sys.exit(main())