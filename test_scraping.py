#!/usr/bin/env python3
"""
Simple test to verify scraping functionality works
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_simple_scraping():
    """Test basic scraping functionality."""
    print("🧪 Testing Simple Web Scraping...")
    
    try:
        from scraper import StaticScraper
        from models import ScrapingConfig
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=True)
        error_handler = ErrorHandler(logger)
        
        # Test with a simple, reliable website
        config = ScrapingConfig(
            url="https://httpbin.org/html",
            target_elements=["h1", "p"],
            max_pages=1,
            delay_between_requests=1.0,
            use_dynamic_scraper=False
        )
        
        print(f"✓ Configuration created for URL: {config.url}")
        
        # Create scraper
        scraper = StaticScraper(config, error_handler)
        print("✓ Static scraper created")
        
        # Perform scraping
        print("🌐 Starting scraping...")
        result = scraper.scrape_page(config.url)
        
        if result and hasattr(result, 'dataframe') and not result.dataframe.empty:
            print(f"✅ SUCCESS! Scraped {len(result.dataframe)} records")
            print(f"📊 Columns: {list(result.dataframe.columns)}")
            print(f"📋 Sample data:")
            print(result.dataframe.head().to_string())
            return True
        else:
            print("❌ No data was scraped")
            return False
            
    except Exception as e:
        print(f"❌ Scraping test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_table_data():
    """Test scraping a page with table data."""
    print("\n🧪 Testing Table Scraping...")
    
    try:
        from scraper import StaticScraper
        from models import ScrapingConfig
        from utils.error_handler import ErrorHandler
        from utils.logger import setup_logging
        
        # Setup
        logger = setup_logging("INFO", console_output=True)
        error_handler = ErrorHandler(logger)
        
        # Test with a page that has tables
        config = ScrapingConfig(
            url="https://www.w3schools.com/html/html_tables.asp",
            target_elements=["table"],
            max_pages=1,
            delay_between_requests=1.0,
            use_dynamic_scraper=False
        )
        
        print(f"✓ Configuration created for URL: {config.url}")
        
        # Create scraper
        scraper = StaticScraper(config, error_handler)
        print("✓ Static scraper created")
        
        # Perform scraping
        print("🌐 Starting table scraping...")
        result = scraper.scrape_page(config.url)
        
        if result and hasattr(result, 'dataframe') and not result.dataframe.empty:
            print(f"✅ SUCCESS! Scraped {len(result.dataframe)} records")
            print(f"📊 Columns: {list(result.dataframe.columns)}")
            print(f"📋 Sample data:")
            print(result.dataframe.head().to_string())
            return True
        else:
            print("❌ No table data was scraped")
            return False
            
    except Exception as e:
        print(f"❌ Table scraping test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run scraping tests."""
    print("=" * 60)
    print("🕷️  Web Scraper - Functionality Test")
    print("=" * 60)
    
    tests = [
        ("Simple Scraping", test_simple_scraping),
        ("Table Scraping", test_with_table_data)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print("\n" + "=" * 60)
    print(f"📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All scraping tests passed! The scraper is working correctly.")
        return 0
    else:
        print("⚠️ Some tests failed. Check the error messages above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())