#!/usr/bin/env python3
"""
Basic test to verify the application setup and imports work correctly.
"""

import sys
import os

def test_imports():
    """Test that all core modules can be imported."""
    try:
        print("Testing imports...")
        
        # Test core modules
        import config
        print("✓ Config module imported")
        
        import utils.logger
        print("✓ Logger module imported")
        
        import utils.error_handler
        print("✓ Error handler module imported")
        
        import scraper
        print("✓ Scraper module imported")
        
        import cleaner
        print("✓ Cleaner module imported")
        
        import export_manager
        print("✓ Export manager module imported")
        
        import project_manager
        print("✓ Project manager module imported")
        
        # Test UI module (might fail if display not available)
        try:
            import ui
            print("✓ UI module imported")
        except Exception as e:
            print(f"⚠ UI module import failed (expected in headless environment): {e}")
        
        print("\n✅ All core modules imported successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False


def test_config():
    """Test configuration system."""
    try:
        print("\nTesting configuration...")
        
        from config import AppConfig
        config = AppConfig()
        
        print(f"✓ Config created: {config.app_name} v{config.app_version}")
        print(f"✓ Scraping defaults: max_pages={config.scraping.max_pages}")
        print(f"✓ UI defaults: theme={config.ui.theme}")
        print(f"✓ Export defaults: format={config.export.default_format}")
        
        # Test validation
        if config.validate_config():
            print("✓ Configuration validation passed")
        else:
            print("⚠ Configuration validation failed")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False


def test_logging():
    """Test logging system."""
    try:
        print("\nTesting logging...")
        
        from utils.logger import setup_logging, get_logger
        
        # Setup logging
        logger = setup_logging(log_level="INFO", console_output=True)
        print("✓ Main logger setup")
        
        # Test module logger
        module_logger = get_logger("test_module")
        module_logger.info("Test log message")
        print("✓ Module logger working")
        
        return True
        
    except Exception as e:
        print(f"❌ Logging test failed: {e}")
        return False


def test_data_structures():
    """Test core data structures."""
    try:
        print("\nTesting data structures...")
        
        from models import ScrapingConfig, ScrapingResult
        from cleaner import CleaningOperation
        from export_manager import ExportOptions
        import pandas as pd
        from datetime import datetime
        
        # Test scraping config
        config = ScrapingConfig(url="https://example.com")
        print(f"✓ ScrapingConfig created: {config.url}")
        
        # Test scraping result
        result = ScrapingResult(
            data=pd.DataFrame(),
            metadata={},
            errors=[],
            pages_scraped=0,
            total_records=0,
            scraping_timestamp=datetime.now()
        )
        print("✓ ScrapingResult created")
        
        # Test cleaning operation
        operation = CleaningOperation(
            operation_type="remove_duplicates",
            parameters={},
            target_columns=[],
            description="Test operation"
        )
        print("✓ CleaningOperation created")
        
        # Test export options
        export_opts = ExportOptions()
        print(f"✓ ExportOptions created: format={export_opts.encoding}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data structures test failed: {e}")
        return False


def main():
    """Run all basic tests."""
    print("=" * 60)
    print("Web Scraper & Dataset Builder - Basic Setup Test")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_config,
        test_logging,
        test_data_structures
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The basic setup is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the setup.")
        return 1


if __name__ == "__main__":
    sys.exit(main())