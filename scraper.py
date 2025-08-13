"""
Web Scraping Module for Web Scraper & Dataset Builder

This module provides web scraping functionality with support for
static and dynamic content extraction.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import time
import re
from urllib.parse import urljoin, urlparse

# Selenium imports (optional)
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from utils.logger import get_logger, log_performance
from utils.error_handler import ErrorHandler

# Import models with fallback
try:
    from models import ScrapingConfig, ScrapedData, ContentType
except ImportError:
    from dataclasses import dataclass
    from enum import Enum
    
    class ContentType(Enum):
        STATIC = "static"
        DYNAMIC = "dynamic"
    
    @dataclass
    class ScrapingConfig:
        url: str
        target_elements: List[str] = None
        max_pages: int = 10
        delay_between_requests: float = 1.0
        use_dynamic_scraper: bool = False
        custom_headers: Dict[str, str] = None
        timeout: int = 30
        max_retries: int = 3
        
        def __post_init__(self):
            if self.target_elements is None:
                self.target_elements = ['table', 'div', 'span']
            if self.custom_headers is None:
                self.custom_headers = {}
    
    @dataclass
    class ScrapedData:
        dataframe: pd.DataFrame
        source_url: str
        scraping_timestamp: datetime
        total_records: int = 0
        columns_detected: List[str] = None
        data_types: Dict[str, str] = None
        content_type: ContentType = ContentType.STATIC
        pages_scraped: int = 1
        errors: List[str] = None
        warnings: List[str] = None
        
        def __post_init__(self):
            if self.columns_detected is None:
                self.columns_detected = list(self.dataframe.columns) if not self.dataframe.empty else []
            if self.data_types is None:
                self.data_types = {col: str(dtype) for col, dtype in self.dataframe.dtypes.items()} if not self.dataframe.empty else {}
            if self.errors is None:
                self.errors = []
            if self.warnings is None:
                self.warnings = []
            if self.total_records == 0:
                self.total_records = len(self.dataframe)


class StaticScraper:
    """Scraper for static HTML content using BeautifulSoup."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
        """Initialize static scraper with configuration."""
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        self.session = requests.Session()
        
        # Set up session headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        if hasattr(config, 'custom_headers') and config.custom_headers:
            headers.update(config.custom_headers)
        
        self.session.headers.update(headers)
    
    def scrape_page(self, url: str) -> ScrapedData:
        """Scrape a single page and return structured data."""
        with log_performance(f"Scraping page: {url}"):
            try:
                # Make HTTP request
                response = self._make_request(url)
                if not response:
                    return self._create_empty_result(url, ["Failed to fetch page"])
                
                # Parse HTML content
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract data based on target elements
                extracted_data = self._extract_data(soup, url)
                
                return extracted_data
                
            except Exception as e:
                error_response = self.error_handler.handle_network_error(e, url)
                return self._create_empty_result(url, [error_response.message])
    
    def _make_request(self, url: str) -> Optional[requests.Response]:
        """Make HTTP request with retry logic."""
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    timeout=self.config.timeout,
                    allow_redirects=True
                )
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt == self.config.max_retries:
                    self.logger.error(f"Failed to fetch {url} after {attempt + 1} attempts: {e}")
                    return None
                
                # Wait before retry with exponential backoff
                wait_time = self.config.delay_between_requests * (2 ** attempt)
                self.logger.warning(f"Request failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
        
        return None
    
    def _extract_data(self, soup: BeautifulSoup, url: str) -> ScrapedData:
        """Extract structured data from parsed HTML."""
        all_data = []
        errors = []
        warnings = []
        
        try:
            # Extract tables
            tables_data = self._extract_tables(soup)
            if tables_data:
                all_data.extend(tables_data)
                self.logger.info(f"Extracted {len(tables_data)} tables from {url}")
            
            # Extract lists
            lists_data = self._extract_lists(soup)
            if lists_data:
                all_data.extend(lists_data)
                self.logger.info(f"Extracted {len(lists_data)} lists from {url}")
            
            # Extract custom elements
            custom_data = self._extract_custom_elements(soup)
            if custom_data:
                all_data.extend(custom_data)
                self.logger.info(f"Extracted {len(custom_data)} custom elements from {url}")
            
            # Combine all data into a single DataFrame
            if all_data:
                combined_df = self._combine_data(all_data)
            else:
                combined_df = pd.DataFrame()
                warnings.append("No structured data found on the page")
            
            return ScrapedData(
                dataframe=combined_df,
                source_url=url,
                scraping_timestamp=datetime.now(),
                total_records=len(combined_df) if not combined_df.empty else 0,
                columns_detected=list(combined_df.columns) if not combined_df.empty else [],
                data_types={col: str(dtype) for col, dtype in combined_df.dtypes.items()} if not combined_df.empty else {},
                content_type=ContentType.STATIC,
                pages_scraped=1,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            error_response = self.error_handler.handle_parsing_error(e, str(soup)[:1000])
            return self._create_empty_result(url, [error_response.message])
    
    def _extract_tables(self, soup: BeautifulSoup) -> List[pd.DataFrame]:
        """Extract data from HTML tables."""
        tables = []
        
        for table in soup.find_all('table'):
            try:
                # Try to parse table with pandas
                table_html = str(table)
                df_list = pd.read_html(table_html, header=0)
                
                for df in df_list:
                    if not df.empty:
                        # Clean column names
                        df.columns = [self._clean_column_name(col) for col in df.columns]
                        tables.append(df)
                        
            except Exception as e:
                self.logger.warning(f"Failed to parse table: {e}")
                continue
        
        return tables
    
    def _extract_lists(self, soup: BeautifulSoup) -> List[pd.DataFrame]:
        """Extract data from HTML lists (ul, ol)."""
        lists_data = []
        
        for list_element in soup.find_all(['ul', 'ol']):
            try:
                items = []
                for li in list_element.find_all('li'):
                    text = li.get_text(strip=True)
                    if text:
                        items.append({'item': text})
                
                if items:
                    df = pd.DataFrame(items)
                    lists_data.append(df)
                    
            except Exception as e:
                self.logger.warning(f"Failed to parse list: {e}")
                continue
        
        return lists_data
    
    def _extract_custom_elements(self, soup: BeautifulSoup) -> List[pd.DataFrame]:
        """Extract data from custom HTML elements."""
        custom_data = []
        
        for element_type in self.config.target_elements:
            if element_type in ['table', 'ul', 'ol']:
                continue  # Already handled
            
            try:
                elements = soup.find_all(element_type)
                if elements:
                    data = []
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        if text:
                            data.append({
                                'element_type': element_type,
                                'content': text,
                                'class': ' '.join(elem.get('class', [])),
                                'id': elem.get('id', '')
                            })
                    
                    if data:
                        df = pd.DataFrame(data)
                        custom_data.append(df)
                        
            except Exception as e:
                self.logger.warning(f"Failed to parse {element_type} elements: {e}")
                continue
        
        return custom_data
    
    def _combine_data(self, data_list: List[pd.DataFrame]) -> pd.DataFrame:
        """Combine multiple DataFrames into a single DataFrame."""
        if not data_list:
            return pd.DataFrame()
        
        if len(data_list) == 1:
            return data_list[0]
        
        # Try to concatenate DataFrames with similar structures
        try:
            # Group DataFrames by column structure
            grouped_dfs = {}
            for df in data_list:
                key = tuple(sorted(df.columns))
                if key not in grouped_dfs:
                    grouped_dfs[key] = []
                grouped_dfs[key].append(df)
            
            # Concatenate DataFrames with same structure
            combined_dfs = []
            for column_set, dfs in grouped_dfs.items():
                if len(dfs) == 1:
                    combined_dfs.append(dfs[0])
                else:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    combined_dfs.append(combined_df)
            
            # If we have multiple different structures, create a unified structure
            if len(combined_dfs) == 1:
                return combined_dfs[0]
            else:
                # Create a unified DataFrame with all columns
                all_columns = set()
                for df in combined_dfs:
                    all_columns.update(df.columns)
                
                unified_dfs = []
                for df in combined_dfs:
                    # Add missing columns with NaN values
                    for col in all_columns:
                        if col not in df.columns:
                            df[col] = pd.NA
                    
                    # Reorder columns
                    df = df.reindex(columns=sorted(all_columns))
                    unified_dfs.append(df)
                
                return pd.concat(unified_dfs, ignore_index=True)
                
        except Exception as e:
            self.logger.warning(f"Failed to combine DataFrames, using first one: {e}")
            return data_list[0]
    
    def _clean_column_name(self, name: str) -> str:
        """Clean and normalize column names."""
        if pd.isna(name) or name is None:
            return "unnamed_column"
        
        # Convert to string and clean
        name = str(name).strip()
        
        # Remove special characters and normalize
        name = re.sub(r'[^\w\s]', '', name)
        name = re.sub(r'\s+', '_', name)
        name = name.lower()
        
        # Ensure it's not empty
        if not name:
            return "unnamed_column"
        
        return name
    
    def _create_empty_result(self, url: str, errors: List[str]) -> ScrapedData:
        """Create an empty ScrapedData result with errors."""
        return ScrapedData(
            dataframe=pd.DataFrame(),
            source_url=url,
            scraping_timestamp=datetime.now(),
            total_records=0,
            columns_detected=[],
            data_types={},
            content_type=ContentType.STATIC,
            pages_scraped=0,
            errors=errors,
            warnings=[]
        )


# Dynamic scraper (only if Selenium is available)
if SELENIUM_AVAILABLE:
    class DynamicScraper:
        """Scraper for dynamic content using Selenium WebDriver."""
        
        def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
            """Initialize dynamic scraper with configuration."""
            self.config = config
            self.error_handler = error_handler
            self.logger = get_logger(__name__)
            self.driver = None
        
        def _setup_driver(self):
            """Set up and configure the WebDriver."""
            try:
                options = ChromeOptions()
                
                # Configure Chrome options for headless operation
                options.add_argument("--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--window-size=1920,1080")
                
                # Set user agent
                if hasattr(self.config, 'custom_headers') and 'User-Agent' in self.config.custom_headers:
                    options.add_argument(f"--user-agent={self.config.custom_headers['User-Agent']}")
                
                # Disable images and CSS for faster loading
                prefs = {
                    "profile.managed_default_content_settings.images": 2,
                    "profile.default_content_setting_values.notifications": 2
                }
                options.add_experimental_option("prefs", prefs)
                
                # Create driver
                self.driver = webdriver.Chrome(options=options)
                
                # Set timeouts
                self.driver.implicitly_wait(10)
                self.driver.set_page_load_timeout(self.config.timeout)
                
            except Exception as e:
                self.logger.error(f"Failed to setup WebDriver: {e}")
                raise
        
        def scrape_page(self, url: str) -> ScrapedData:
            """Scrape a single page using Selenium for dynamic content."""
            with log_performance(f"Dynamic scraping page: {url}"):
                try:
                    # Set up driver
                    self._setup_driver()
                    
                    # Navigate to page
                    self.driver.get(url)
                    
                    # Wait for page to load and execute JavaScript
                    self._wait_for_page_load()
                    
                    # Get page source after JavaScript execution
                    page_source = self.driver.page_source
                    
                    # Parse with BeautifulSoup
                    soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Use static scraper methods for extraction
                    static_scraper = StaticScraper(self.config, self.error_handler)
                    extracted_data = static_scraper._extract_data(soup, url)
                    
                    # Update content type
                    extracted_data.content_type = ContentType.DYNAMIC
                    
                    return extracted_data
                    
                except Exception as e:
                    error_response = self.error_handler.handle_network_error(e, url)
                    return self._create_empty_result(url, [error_response.message])
                
                finally:
                    if self.driver:
                        self.driver.quit()
                        self.driver = None
        
        def _wait_for_page_load(self):
            """Wait for page to fully load including JavaScript execution."""
            try:
                # Wait for document ready state
                WebDriverWait(self.driver, self.config.timeout).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                # Additional wait for dynamic content
                time.sleep(2)
                
            except TimeoutException:
                self.logger.warning("Page load timeout, proceeding with current content")
            except Exception as e:
                self.logger.warning(f"Error waiting for page load: {e}")
        
        def _create_empty_result(self, url: str, errors: List[str]) -> ScrapedData:
            """Create an empty ScrapedData result with errors."""
            return ScrapedData(
                dataframe=pd.DataFrame(),
                source_url=url,
                scraping_timestamp=datetime.now(),
                total_records=0,
                columns_detected=[],
                data_types={},
                content_type=ContentType.DYNAMIC,
                pages_scraped=0,
                errors=errors,
                warnings=[]
            )


class WebScraper:
    """Main web scraper class with comprehensive functionality."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler = None):
        """Initialize the web scraper with configuration."""
        self.config = config
        self.error_handler = error_handler or ErrorHandler(get_logger(__name__))
        self.logger = get_logger(__name__)
        
        # Initialize scrapers
        self.static_scraper = StaticScraper(config, self.error_handler)
        self.dynamic_scraper = None
        if config.use_dynamic_scraper and SELENIUM_AVAILABLE:
            self.dynamic_scraper = DynamicScraper(config, self.error_handler)
    
    def detect_content_type(self, url: str) -> ContentType:
        """Detect if the content is static or dynamic."""
        try:
            # Simple heuristic: check if the page has JavaScript
            response = requests.get(url, timeout=10)
            content = response.text.lower()
            
            # Look for JavaScript indicators
            js_indicators = ['<script', 'javascript:', 'document.', 'window.', 'jquery', 'angular', 'react', 'vue']
            
            if any(indicator in content for indicator in js_indicators):
                return ContentType.DYNAMIC
            else:
                return ContentType.STATIC
                
        except Exception:
            return ContentType.STATIC  # Default to static
    
    def scrape_url(self, url: str) -> ScrapedData:
        """Scrape data from the specified URL."""
        try:
            self.logger.info(f"Starting scraping operation for: {url}")
            
            # Choose scraper based on configuration
            if self.config.use_dynamic_scraper and self.dynamic_scraper:
                return self.dynamic_scraper.scrape_page(url)
            else:
                return self.static_scraper.scrape_page(url)
                
        except Exception as e:
            self.logger.error(f"Unexpected error during scraping: {e}", exc_info=True)
            return ScrapedData(
                dataframe=pd.DataFrame(),
                source_url=url,
                scraping_timestamp=datetime.now(),
                total_records=0,
                columns_detected=[],
                data_types={},
                content_type=ContentType.STATIC,
                pages_scraped=0,
                errors=[f"Unexpected error: {str(e)}"],
                warnings=[]
            )