"""
Web Scraping Module for Web Scraper & Dataset Builder

This module provides web scraping functionality with support for
static and dynamic content extraction.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import time
import re
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import json
import os
from pathlib import Path

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from webdriver_manager.chrome import ChromeDriverManager
    from webdriver_manager.firefox import GeckoDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

try:
    from models import (
        ScrapingConfig, ScrapingResult, ScrapedData, ContentType, 
        ScrapingStatus, ScraperInterface
    )
except ImportError:
    # Fallback definitions if models not available
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
        timeout: int = 30
        max_retries: int = 3
        
        def __post_init__(self):
            if self.target_elements is None:
                self.target_elements = ['table', 'div', 'span']
    
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
            if self.total_records == 0:
                self.total_records = len(self.dataframe)
            if self.columns_detected is None:
                self.columns_detected = list(self.dataframe.columns)
            if self.data_types is None:
                self.data_types = {col: str(dtype) for col, dtype in self.dataframe.dtypes.items()}
            if self.errors is None:
                self.errors = []
            if self.warnings is None:
                self.warnings = []
from utils.logger import get_logger, log_performance
from utils.error_handler import ErrorHandler


class StaticScraper:
    """Scraper for static HTML content using BeautifulSoup."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
        """Initialize static scraper with configuration."""
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        self.session = requests.Session()
        
        # Set up session headers
        self.session.headers.update({
            'User-Agent': config.user_agent,
            **config.custom_headers
        })
    
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


class PaginationHandler:
    """Handle automatic pagination detection and traversal."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
        """Initialize pagination handler."""
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
    
    def find_pagination_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find pagination links on the current page."""
        pagination_urls = []
        
        # Common pagination patterns
        pagination_selectors = [
            'a[href*="page"]',
            'a[href*="p="]',
            'a[href*="offset"]',
            '.pagination a',
            '.pager a',
            '.page-numbers a',
            'a:contains("Next")',
            'a:contains(">")',
            'a[rel="next"]'
        ]
        
        for selector in pagination_selectors:
            try:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(base_url, href)
                        if full_url not in pagination_urls and full_url != base_url:
                            pagination_urls.append(full_url)
            except Exception as e:
                self.logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        return pagination_urls[:self.config.max_pages - 1]  # Limit to max_pages


if SELENIUM_AVAILABLE:
    class DynamicScraper:
        """Scraper for dynamic content using Selenium WebDriver."""
        
        def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
            """Initialize dynamic scraper with configuration."""
        
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        self.driver = None
        self.driver_type = "chrome"  # Default to Chrome
    
    def _setup_driver(self) -> webdriver.Chrome:
        """Set up and configure the WebDriver."""
        try:
            if self.driver_type == "chrome":
                options = ChromeOptions()
                
                # Configure Chrome options for headless operation
                options.add_argument("--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--window-size=1920,1080")
                options.add_argument(f"--user-agent={self.config.user_agent}")
                
                # Disable images and CSS for faster loading
                prefs = {
                    "profile.managed_default_content_settings.images": 2,
                    "profile.default_content_setting_values.notifications": 2
                }
                options.add_experimental_option("prefs", prefs)
                
                # Set up Chrome service
                service = ChromeService(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                
            elif self.driver_type == "firefox":
                options = FirefoxOptions()
                options.add_argument("--headless")
                options.add_argument("--width=1920")
                options.add_argument("--height=1080")
                
                # Set up Firefox service
                service = FirefoxService(GeckoDriverManager().install())
                driver = webdriver.Firefox(service=service, options=options)
            
            else:
                raise ValueError(f"Unsupported driver type: {self.driver_type}")
            
            # Set timeouts
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(self.config.timeout)
            
            return driver
            
        except Exception as e:
            self.logger.error(f"Failed to setup WebDriver: {e}")
            raise
    
    def scrape_page(self, url: str) -> ScrapedData:
        """Scrape a single page using Selenium for dynamic content."""
        with log_performance(f"Dynamic scraping page: {url}"):
            try:
                # Set up driver
                self.driver = self._setup_driver()
                
                # Navigate to page
                self.driver.get(url)
                
                # Wait for page to load and execute JavaScript
                self._wait_for_page_load()
                
                # Get page source after JavaScript execution
                page_source = self.driver.page_source
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Extract data using the same methods as static scraper
                extracted_data = self._extract_data(soup, url)
                
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
            
            # Try to wait for common loading indicators to disappear
            loading_selectors = [
                ".loading", ".spinner", ".loader", "[data-loading]",
                ".fa-spinner", ".fa-circle-o-notch"
            ]
            
            for selector in loading_selectors:
                try:
                    WebDriverWait(self.driver, 5).until_not(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                except TimeoutException:
                    continue  # Loading indicator not found or still present
            
        except TimeoutException:
            self.logger.warning("Page load timeout, proceeding with current content")
        except Exception as e:
            self.logger.warning(f"Error waiting for page load: {e}")
    
    def _extract_data(self, soup: BeautifulSoup, url: str) -> ScrapedData:
        """Extract structured data from parsed HTML (same as StaticScraper)."""
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
            
            # Try to extract data from JavaScript-rendered content
            js_data = self._extract_javascript_data()
            if js_data:
                all_data.extend(js_data)
                self.logger.info(f"Extracted {len(js_data)} JavaScript data objects from {url}")
            
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
                content_type=ContentType.DYNAMIC,
                pages_scraped=1,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            error_response = self.error_handler.handle_parsing_error(e, str(soup)[:1000])
            return self._create_empty_result(url, [error_response.message])
    
    def _extract_javascript_data(self) -> List[pd.DataFrame]:
        """Extract data from JavaScript variables and JSON objects."""
        js_data = []
        
        try:
            # Look for common JavaScript data patterns
            scripts = self.driver.find_elements(By.TAG_NAME, "script")
            
            for script in scripts:
                script_content = script.get_attribute("innerHTML")
                if not script_content:
                    continue
                
                # Look for JSON data in script tags
                json_patterns = [
                    r'var\s+\w+\s*=\s*(\[.*?\]);',
                    r'let\s+\w+\s*=\s*(\[.*?\]);',
                    r'const\s+\w+\s*=\s*(\[.*?\]);',
                    r'window\.\w+\s*=\s*(\[.*?\]);',
                    r'data\s*:\s*(\[.*?\])',
                    r'"data"\s*:\s*(\[.*?\])'
                ]
                
                for pattern in json_patterns:
                    matches = re.findall(pattern, script_content, re.DOTALL)
                    for match in matches:
                        try:
                            # Clean up the JSON string
                            json_str = match.strip()
                            if json_str.startswith('[') and json_str.endswith(']'):
                                data = json.loads(json_str)
                                if isinstance(data, list) and data:
                                    df = pd.json_normalize(data)
                                    if not df.empty:
                                        js_data.append(df)
                        except (json.JSONDecodeError, ValueError):
                            continue
            
            # Try to execute JavaScript to get data
            try:
                # Look for common data variables
                js_commands = [
                    "return window.data || null;",
                    "return window.chartData || null;",
                    "return window.tableData || null;",
                    "return window.jsonData || null;"
                ]
                
                for command in js_commands:
                    try:
                        result = self.driver.execute_script(command)
                        if result and isinstance(result, list):
                            df = pd.json_normalize(result)
                            if not df.empty:
                                js_data.append(df)
                    except Exception:
                        continue
                        
            except Exception as e:
                self.logger.debug(f"Could not extract JavaScript data: {e}")
        
        except Exception as e:
            self.logger.warning(f"Error extracting JavaScript data: {e}")
        
        return js_data
    
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
            content_type=ContentType.DYNAMIC,
            pages_scraped=0,
            errors=errors,
            warnings=[]
        )


class WebScraper:
    """Main web scraper class with comprehensive functionality."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
        """Initialize the web scraper with configuration."""
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        
        # Initialize scrapers
        self.static_scraper = StaticScraper(config, error_handler)
        self.dynamic_scraper = DynamicScraper(config, error_handler) if SELENIUM_AVAILABLE else None
        self.pagination_handler = PaginationHandler(config, error_handler)
    
    def scrape_url(self, url: str) -> ScrapingResult:
        """Scrape data from the specified URL with pagination support."""
        start_time = datetime.now()
        
        with log_performance(f"Complete scraping operation for {url}"):
            try:
                # Check robots.txt if required
                if self.config.respect_robots_txt and not self._check_robots_txt(url):
                    return ScrapingResult(
                        data=pd.DataFrame(),
                        metadata={"url": url, "blocked_by_robots": True},
                        errors=["Scraping blocked by robots.txt"],
                        pages_scraped=0,
                        total_records=0,
                        scraping_timestamp=start_time,
                        status=ScrapingStatus.FAILED
                    )
                
                # Detect content type
                content_type = self.detect_content_type(url)
                
                # Scrape pages
                all_scraped_data = []
                errors = []
                pages_scraped = 0
                
                # Scrape initial page
                if self.config.use_dynamic_scraper and self.dynamic_scraper:
                    scraped_data = self.dynamic_scraper.scrape_page(url)
                    scraping_method = "dynamic"
                else:
                    scraped_data = self.static_scraper.scrape_page(url)
                    scraping_method = "static"
                
                all_scraped_data.append(scraped_data)
                pages_scraped += 1
                
                if scraped_data.errors:
                    errors.extend(scraped_data.errors)
                
                # Handle pagination if enabled and max_pages > 1
                if self.config.max_pages > 1 and not scraped_data.dataframe.empty:
                    additional_pages = self._scrape_paginated_content(url, scraped_data)
                    all_scraped_data.extend(additional_pages)
                    pages_scraped += len(additional_pages)
                
                # Combine all scraped data
                combined_data = self._combine_scraped_data(all_scraped_data)
                
                execution_time = (datetime.now() - start_time).total_seconds()
                
                return ScrapingResult(
                    data=combined_data,
                    metadata={
                        "url": url,
                        "content_type": content_type.value,
                        "scraping_method": scraping_method,
                        "execution_time": execution_time,
                        "selenium_available": SELENIUM_AVAILABLE
                    },
                    errors=errors,
                    pages_scraped=pages_scraped,
                    total_records=len(combined_data) if not combined_data.empty else 0,
                    scraping_timestamp=start_time,
                    status=ScrapingStatus.COMPLETED if not errors else ScrapingStatus.COMPLETED,
                    execution_time=execution_time
                )
                
            except Exception as e:
                error_response = self.error_handler.handle_unknown_error(e, {"url": url})
                execution_time = (datetime.now() - start_time).total_seconds()
                
                return ScrapingResult(
                    data=pd.DataFrame(),
                    metadata={"url": url, "error": str(e)},
                    errors=[error_response.message],
                    pages_scraped=0,
                    total_records=0,
                    scraping_timestamp=start_time,
                    status=ScrapingStatus.FAILED,
                    execution_time=execution_time
                )
    
    def detect_content_type(self, url: str) -> ContentType:
        """Detect the type of content at the URL."""
        try:
            response = requests.head(url, timeout=10)
            content_type = response.headers.get('content-type', '').lower()
            
            if 'text/html' in content_type:
                return ContentType.STATIC
            else:
                return ContentType.MIXED
                
        except Exception:
            return ContentType.STATIC  # Default assumption
    
    def _check_robots_txt(self, url: str) -> bool:
        """Check if scraping is allowed by robots.txt."""
        try:
            parsed_url = urlparse(url)
            robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
            
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            
            return rp.can_fetch(self.config.user_agent, url)
            
        except Exception as e:
            self.logger.warning(f"Could not check robots.txt: {e}")
            return True  # Allow scraping if robots.txt check fails
    
    def _scrape_paginated_content(self, base_url: str, initial_data: ScrapedData) -> List[ScrapedData]:
        """Scrape additional pages if pagination is detected."""
        additional_data = []
        
        try:
            # Parse initial page to find pagination links
            response = requests.get(base_url, timeout=self.config.timeout)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            pagination_urls = self.pagination_handler.find_pagination_links(soup, base_url)
            
            for i, page_url in enumerate(pagination_urls):
                if i >= self.config.max_pages - 1:  # -1 because we already scraped the first page
                    break
                
                self.logger.info(f"Scraping pagination page {i + 2}: {page_url}")
                
                # Add delay between requests
                if self.config.delay_between_requests > 0:
                    time.sleep(self.config.delay_between_requests)
                
                # Scrape the page
                page_data = self.static_scraper.scrape_page(page_url)
                if not page_data.dataframe.empty:
                    additional_data.append(page_data)
                
        except Exception as e:
            self.logger.warning(f"Error during pagination scraping: {e}")
        
        return additional_data
    
    def _combine_scraped_data(self, scraped_data_list: List[ScrapedData]) -> pd.DataFrame:
        """Combine multiple ScrapedData objects into a single DataFrame."""
        if not scraped_data_list:
            return pd.DataFrame()
        
        dataframes = [data.dataframe for data in scraped_data_list if not data.dataframe.empty]
        
        if not dataframes:
            return pd.DataFrame()
        
        if len(dataframes) == 1:
            return dataframes[0]
        
        try:
            # Try to concatenate all DataFrames
            combined_df = pd.concat(dataframes, ignore_index=True, sort=False)
            return combined_df
            
        except Exception as e:
            self.logger.warning(f"Failed to combine scraped data: {e}")
            return dataframes[0]  # Return first DataFrame as fallback


class WebScraper:
    """Simple web scraper class for UI integration."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
        """Initialize the web scraper."""
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
    
    def detect_content_type(self, url: str) -> ContentType:
        """Detect the type of content at the URL."""
        try:
            response = requests.head(url, timeout=10)
            content_type = response.headers.get('content-type', '').lower()
            
            if 'text/html' in content_type:
                return ContentType.STATIC
            else:
                return ContentType.STATIC  # Default assumption
                
        except Exception:
            return ContentType.STATIC  # Default assumption


class DynamicScraper:
    """Dynamic content scraper using Selenium."""
    
    def __init__(self, config: ScrapingConfig, error_handler: ErrorHandler):
        """Initialize dynamic scraper."""
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium is not available. Please install selenium and webdriver-manager.")
        
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        self.driver = None
    
    def scrape_page(self, url: str) -> ScrapedData:
        """Scrape a page using Selenium."""
        try:
            self.logger.info(f"Starting dynamic scraping of: {url}")
            
            # Setup Chrome driver
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = ChromeService(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception:
                # Fallback to system Chrome
                self.driver = webdriver.Chrome(options=chrome_options)
            
            self.driver.get(url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract data using static methods
            static_scraper = StaticScraper(self.config, self.error_handler)
            extracted_data = static_scraper._extract_data(soup, url)
            
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"Dynamic scraping failed: {e}")
            return ScrapedData(
                dataframe=pd.DataFrame(),
                source_url=url,
                scraping_timestamp=datetime.now(),
                content_type=ContentType.DYNAMIC,
                errors=[str(e)]
            )
        finally:
            if self.driver:
                self.driver.quit()