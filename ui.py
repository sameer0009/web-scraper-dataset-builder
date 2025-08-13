"""
User Interface for Web Scraper & Dataset Builder

This module contains the main GUI components using customtkinter
for a modern, professional interface with tabbed navigation.
"""

import customtkinter as ctk
from typing import Optional, Dict, Any, Callable
import logging
import threading
import pandas as pd
import numpy as np
from config import AppConfig
from utils.error_handler import ErrorHandler
from utils.logger import get_logger


class MainWindow(ctk.CTk):
    """Main application window with tabbed interface."""
    
    def __init__(self, config: AppConfig, error_handler: ErrorHandler):
        """Initialize the main window with configuration and error handling."""
        super().__init__()
        
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        
        # Initialize UI state
        self.current_data = None
        self.scraping_in_progress = False
        
        # Setup window
        self._setup_window()
        
        # Create UI components
        self._create_tabs()
        self._setup_status_bar()
        
        self.logger.info("Main window initialized successfully")
    
    def _setup_window(self):
        """Configure the main window properties."""
        # Set window properties
        self.title(self.config.app_name)
        self.geometry(f"{self.config.ui.window_width}x{self.config.ui.window_height}")
        
        # Set theme
        ctk.set_appearance_mode(self.config.ui.theme)
        ctk.set_default_color_theme("blue")
        
        # Configure grid
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        
        # Set minimum window size
        self.minsize(800, 600)
        
        # Center window on screen
        self._center_window()
    
    def _center_window(self):
        """Center the window on the screen."""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")
    
    def _create_tabs(self):
        """Create the main tabbed interface."""
        # Create tabview
        self.tabview = ctk.CTkTabview(self, width=780, height=520)
        self.tabview.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")
        
        # Create tabs
        self.scraper_tab = self.tabview.add("Web Scraper")
        self.cleaner_tab = self.tabview.add("Dataset Cleaner")
        self.export_tab = self.tabview.add("Export")
        
        # Initialize tab content (placeholder for now)
        self._setup_scraper_tab()
        self._setup_cleaner_tab()
        self._setup_export_tab()
    
    def _setup_scraper_tab(self):
        """Set up the web scraper tab interface."""
        # Configure grid
        self.scraper_tab.grid_columnconfigure(1, weight=1)
        
        # URL input section
        url_frame = ctk.CTkFrame(self.scraper_tab)
        url_frame.grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky="ew")
        url_frame.grid_columnconfigure(1, weight=1)
        
        ctk.CTkLabel(url_frame, text="URL:", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, padx=10, pady=10, sticky="w"
        )
        
        self.url_entry = ctk.CTkEntry(url_frame, placeholder_text="Enter URL to scrape...")
        self.url_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        
        self.scrape_button = ctk.CTkButton(
            url_frame, 
            text="Start Scraping", 
            command=self._start_scraping,
            width=120
        )
        self.scrape_button.grid(row=0, column=2, padx=10, pady=10)
        
        # Options section
        options_frame = ctk.CTkFrame(self.scraper_tab)
        options_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
        
        ctk.CTkLabel(options_frame, text="Scraping Options", font=ctk.CTkFont(size=14, weight="bold")).pack(
            pady=10
        )
        
        # Dynamic content checkbox
        self.dynamic_checkbox = ctk.CTkCheckBox(options_frame, text="Enable Dynamic Content (JavaScript)")
        self.dynamic_checkbox.pack(pady=5, padx=10, anchor="w")
        
        # Max pages
        pages_frame = ctk.CTkFrame(options_frame)
        pages_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(pages_frame, text="Max Pages:").pack(side="left", padx=5)
        self.max_pages_entry = ctk.CTkEntry(pages_frame, width=80)
        self.max_pages_entry.insert(0, str(self.config.scraping.max_pages))
        self.max_pages_entry.pack(side="left", padx=5)
        
        # Delay setting
        delay_frame = ctk.CTkFrame(options_frame)
        delay_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_frame, text="Delay (seconds):").pack(side="left", padx=5)
        self.delay_entry = ctk.CTkEntry(delay_frame, width=80)
        self.delay_entry.insert(0, str(self.config.scraping.delay_between_requests))
        self.delay_entry.pack(side="left", padx=5)
        
        # Preview section
        preview_frame = ctk.CTkFrame(self.scraper_tab)
        preview_frame.grid(row=1, column=1, padx=10, pady=10, sticky="nsew")
        preview_frame.grid_rowconfigure(1, weight=1)
        preview_frame.grid_columnconfigure(0, weight=1)
        
        ctk.CTkLabel(preview_frame, text="Data Preview", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, pady=10
        )
        
        # Placeholder for data preview
        self.preview_text = ctk.CTkTextbox(preview_frame, state="disabled")
        self.preview_text.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
        
        # Progress bar
        self.scraping_progress = ctk.CTkProgressBar(self.scraper_tab)
        self.scraping_progress.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky="ew")
        self.scraping_progress.set(0)
    
    def _setup_cleaner_tab(self):
        """Set up the dataset cleaner tab interface."""
        # Configure grid
        self.cleaner_tab.grid_columnconfigure(1, weight=1)
        self.cleaner_tab.grid_rowconfigure(0, weight=1)
        
        # Left panel - Cleaning tools
        tools_frame = ctk.CTkScrollableFrame(self.cleaner_tab, width=300)
        tools_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        
        # Data Quality Section
        quality_frame = ctk.CTkFrame(tools_frame)
        quality_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(quality_frame, text="Data Quality", font=ctk.CTkFont(size=14, weight="bold")).pack(pady=5)
        
        self.quality_button = ctk.CTkButton(
            quality_frame, text="Analyze Quality", state="disabled",
            command=self._analyze_data_quality
        )
        self.quality_button.pack(pady=5, padx=10, fill="x")
        
        self.auto_clean_button = ctk.CTkButton(
            quality_frame, text="Auto Clean (Smart)", state="disabled",
            command=lambda: self._auto_clean_data(aggressive=False)
        )
        self.auto_clean_button.pack(pady=2, padx=10, fill="x")
        
        self.auto_clean_aggressive_button = ctk.CTkButton(
            quality_frame, text="Auto Clean (Aggressive)", state="disabled",
            command=lambda: self._auto_clean_data(aggressive=True)
        )
        self.auto_clean_aggressive_button.pack(pady=2, padx=10, fill="x")
        
        # Duplicates Section
        duplicates_frame = ctk.CTkFrame(tools_frame)
        duplicates_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(duplicates_frame, text="Duplicates", font=ctk.CTkFont(size=12, weight="bold")).pack(pady=5)
        
        self.duplicate_strategy = ctk.StringVar(value="first")
        ctk.CTkRadioButton(duplicates_frame, text="Keep First", variable=self.duplicate_strategy, value="first").pack(anchor="w", padx=10)
        ctk.CTkRadioButton(duplicates_frame, text="Keep Last", variable=self.duplicate_strategy, value="last").pack(anchor="w", padx=10)
        ctk.CTkRadioButton(duplicates_frame, text="Remove All", variable=self.duplicate_strategy, value="all").pack(anchor="w", padx=10)
        
        self.remove_duplicates_button = ctk.CTkButton(
            duplicates_frame, text="Remove Duplicates", state="disabled",
            command=self._remove_duplicates
        )
        self.remove_duplicates_button.pack(pady=5, padx=10, fill="x")
        
        # Missing Values Section
        missing_frame = ctk.CTkFrame(tools_frame)
        missing_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(missing_frame, text="Missing Values", font=ctk.CTkFont(size=12, weight="bold")).pack(pady=5)
        
        self.missing_strategy = ctk.StringVar(value="fill_mean")
        strategies = [
            ("Drop Rows", "drop"),
            ("Fill Mean", "fill_mean"),
            ("Fill Median", "fill_median"),
            ("Fill Mode", "fill_mode"),
            ("Forward Fill", "forward_fill"),
            ("Interpolate", "interpolate")
        ]
        
        for text, value in strategies:
            ctk.CTkRadioButton(missing_frame, text=text, variable=self.missing_strategy, value=value).pack(anchor="w", padx=10)
        
        self.handle_missing_button = ctk.CTkButton(
            missing_frame, text="Handle Missing Values", state="disabled",
            command=self._handle_missing_values
        )
        self.handle_missing_button.pack(pady=5, padx=10, fill="x")
        
        # Text Cleaning Section
        text_frame = ctk.CTkFrame(tools_frame)
        text_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(text_frame, text="Text Cleaning", font=ctk.CTkFont(size=12, weight="bold")).pack(pady=5)
        
        # Text cleaning checkboxes
        self.text_operations = {}
        text_ops = [
            ("Remove Extra Spaces", "remove_extra_spaces"),
            ("Normalize Whitespace", "normalize_whitespace"),
            ("Remove Special Chars", "remove_special_chars"),
            ("Lowercase", "lowercase"),
            ("Remove HTML Tags", "remove_html_tags"),
            ("Fix Encoding", "fix_encoding")
        ]
        
        for text, value in text_ops:
            var = ctk.BooleanVar(value=value in ["remove_extra_spaces", "normalize_whitespace"])
            self.text_operations[value] = var
            ctk.CTkCheckBox(text_frame, text=text, variable=var).pack(anchor="w", padx=10, pady=2)
        
        self.clean_text_button = ctk.CTkButton(
            text_frame, text="Clean Text Data", state="disabled",
            command=self._clean_text_data
        )
        self.clean_text_button.pack(pady=5, padx=10, fill="x")
        
        # Data Types Section
        types_frame = ctk.CTkFrame(tools_frame)
        types_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(types_frame, text="Data Types", font=ctk.CTkFont(size=12, weight="bold")).pack(pady=5)
        
        self.auto_types_button = ctk.CTkButton(
            types_frame, text="Auto-Detect Types", state="disabled",
            command=self._auto_detect_types
        )
        self.auto_types_button.pack(pady=5, padx=10, fill="x")
        
        # Outliers Section
        outliers_frame = ctk.CTkFrame(tools_frame)
        outliers_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(outliers_frame, text="Outliers", font=ctk.CTkFont(size=12, weight="bold")).pack(pady=5)
        
        self.outlier_method = ctk.StringVar(value="iqr")
        ctk.CTkRadioButton(outliers_frame, text="IQR Method", variable=self.outlier_method, value="iqr").pack(anchor="w", padx=10)
        ctk.CTkRadioButton(outliers_frame, text="Z-Score Method", variable=self.outlier_method, value="zscore").pack(anchor="w", padx=10)
        
        self.outlier_threshold = ctk.DoubleVar(value=1.5)
        ctk.CTkLabel(outliers_frame, text="Threshold:").pack(anchor="w", padx=10)
        ctk.CTkEntry(outliers_frame, textvariable=self.outlier_threshold, width=100).pack(padx=10, pady=2)
        
        self.remove_outliers_button = ctk.CTkButton(
            outliers_frame, text="Remove Outliers", state="disabled",
            command=self._remove_outliers
        )
        self.remove_outliers_button.pack(pady=5, padx=10, fill="x")
        
        # History Section
        history_frame = ctk.CTkFrame(tools_frame)
        history_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(history_frame, text="History", font=ctk.CTkFont(size=12, weight="bold")).pack(pady=5)
        
        history_buttons_frame = ctk.CTkFrame(history_frame)
        history_buttons_frame.pack(fill="x", padx=5, pady=5)
        
        self.undo_button = ctk.CTkButton(
            history_buttons_frame, text="Undo", state="disabled", width=70,
            command=self._undo_operation
        )
        self.undo_button.pack(side="left", padx=2)
        
        self.redo_button = ctk.CTkButton(
            history_buttons_frame, text="Redo", state="disabled", width=70,
            command=self._redo_operation
        )
        self.redo_button.pack(side="left", padx=2)
        
        self.reset_button = ctk.CTkButton(
            history_buttons_frame, text="Reset", state="disabled", width=70,
            command=self._reset_data
        )
        self.reset_button.pack(side="left", padx=2)
        
        # Right panel - Data display and statistics
        data_frame = ctk.CTkFrame(self.cleaner_tab)
        data_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")
        data_frame.grid_rowconfigure(2, weight=1)
        data_frame.grid_columnconfigure(0, weight=1)
        
        # Data info header
        info_frame = ctk.CTkFrame(data_frame)
        info_frame.grid(row=0, column=0, padx=10, pady=5, sticky="ew")
        
        self.data_info_label = ctk.CTkLabel(
            info_frame, text="No data loaded", 
            font=ctk.CTkFont(size=14, weight="bold")
        )
        self.data_info_label.pack(pady=10)
        
        # Statistics frame
        stats_frame = ctk.CTkFrame(data_frame)
        stats_frame.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        
        self.stats_text = ctk.CTkTextbox(stats_frame, height=100)
        self.stats_text.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Data table
        table_frame = ctk.CTkFrame(data_frame)
        table_frame.grid(row=2, column=0, padx=10, pady=5, sticky="nsew")
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        
        self.data_text = ctk.CTkTextbox(table_frame, state="disabled")
        self.data_text.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
    
    def _setup_export_tab(self):
        """Set up the export tab interface."""
        # Configure grid
        self.export_tab.grid_columnconfigure(1, weight=1)
        
        # Export options section
        options_frame = ctk.CTkFrame(self.export_tab)
        options_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        
        ctk.CTkLabel(options_frame, text="Export Options", font=ctk.CTkFont(size=14, weight="bold")).pack(
            pady=10
        )
        
        # Format selection
        self.format_var = ctk.StringVar(value="xlsx")
        
        ctk.CTkRadioButton(options_frame, text="Excel (.xlsx)", variable=self.format_var, value="xlsx").pack(
            pady=5, padx=10, anchor="w"
        )
        ctk.CTkRadioButton(options_frame, text="CSV (.csv)", variable=self.format_var, value="csv").pack(
            pady=5, padx=10, anchor="w"
        )
        ctk.CTkRadioButton(options_frame, text="JSON (.json)", variable=self.format_var, value="json").pack(
            pady=5, padx=10, anchor="w"
        )
        
        # Export button
        self.export_button = ctk.CTkButton(
            options_frame, 
            text="Export Data", 
            command=self._export_data,
            state="disabled"
        )
        self.export_button.pack(pady=20, padx=10, fill="x")
        
        # File info section
        info_frame = ctk.CTkFrame(self.export_tab)
        info_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")
        info_frame.grid_rowconfigure(1, weight=1)
        info_frame.grid_columnconfigure(0, weight=1)
        
        ctk.CTkLabel(info_frame, text="Export Information", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, pady=10
        )
        
        # Export info display
        self.export_info_text = ctk.CTkTextbox(info_frame, state="disabled")
        self.export_info_text.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
        
        # Progress bar
        self.export_progress = ctk.CTkProgressBar(self.export_tab)
        self.export_progress.grid(row=1, column=0, columnspan=2, padx=10, pady=10, sticky="ew")
        self.export_progress.set(0)
    
    def _setup_status_bar(self):
        """Set up the status bar at the bottom of the window."""
        self.status_frame = ctk.CTkFrame(self, height=30)
        self.status_frame.grid(row=1, column=0, padx=20, pady=(0, 20), sticky="ew")
        self.status_frame.grid_columnconfigure(0, weight=1)
        
        self.status_label = ctk.CTkLabel(self.status_frame, text="Ready", anchor="w")
        self.status_label.grid(row=0, column=0, padx=10, pady=5, sticky="ew")
    
    def _start_scraping(self):
        """Handle start scraping button click."""
        url = self.url_entry.get().strip()
        if not url:
            self.show_error("Please enter a URL to scrape")
            return
        
        # Validate URL format
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            # Import scraping components
            from scraper import WebScraper, StaticScraper
            from models import ScrapingConfig
            
            # Get scraping options from UI
            max_pages = int(self.max_pages_entry.get() or "1")
            delay = float(self.delay_entry.get() or "1.0")
            use_dynamic = self.dynamic_checkbox.get()
            
            # Create scraping configuration
            config = ScrapingConfig(
                url=url,
                target_elements=["table", "div", "span", "p", "h1", "h2", "h3"],
                max_pages=max_pages,
                delay_between_requests=delay,
                use_dynamic_scraper=use_dynamic,
                timeout=30,
                max_retries=3
            )
            
            # Disable scraping button during operation
            self.scrape_button.configure(state="disabled", text="Scraping...")
            self.update_status("Starting web scraping...")
            self.scraping_progress.set(0.1)
            
            # Create scraper instance
            if use_dynamic:
                # Try dynamic scraper first
                try:
                    from scraper import DynamicScraper
                    scraper = DynamicScraper(config, self.error_handler)
                    self.update_status("Using dynamic scraper (Selenium)...")
                except ImportError:
                    self.show_error("Selenium not available. Please install selenium and webdriver-manager.")
                    self._reset_scraping_ui()
                    return
            else:
                # Use static scraper
                scraper = StaticScraper(config, self.error_handler)
                self.update_status("Using static scraper (BeautifulSoup)...")
            
            self.scraping_progress.set(0.3)
            
            # Perform scraping
            scraped_data = scraper.scrape_page(url)
            
            if scraped_data and not scraped_data.dataframe.empty:
                # Store scraped data
                self.current_data = scraped_data.dataframe
                
                # Update progress
                self.scraping_progress.set(0.8)
                self.update_status(f"Successfully scraped {len(self.current_data)} records")
                
                # Show preview in the preview text box
                self._show_data_preview(self.current_data)
                
                # Enable cleaning and export tabs
                self._enable_data_tabs()
                
                self.scraping_progress.set(1.0)
                self.update_status(f"Scraping completed! Found {len(self.current_data)} records")
                
            else:
                self.show_error("No data found on the webpage. Try adjusting your scraping settings.")
                self.update_status("No data found")
                
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}", exc_info=True)
            error_msg = f"Scraping failed: {str(e)}"
            self.show_error(error_msg)
            self.update_status("Scraping failed")
            
        finally:
            # Re-enable scraping button
            self._reset_scraping_ui()
    
    def _reset_scraping_ui(self):
        """Reset scraping UI elements."""
        self.scrape_button.configure(state="normal", text="Start Scraping")
        self.scraping_progress.set(0)
    
    def _show_data_preview(self, data):
        """Show data preview in the preview text box."""
        try:
            # Enable text box for editing
            self.preview_text.configure(state="normal")
            self.preview_text.delete("1.0", "end")
            
            # Create preview text
            preview_lines = []
            preview_lines.append(f"Data Preview ({len(data)} records, {len(data.columns)} columns)")
            preview_lines.append("=" * 50)
            preview_lines.append("")
            
            # Show column names
            preview_lines.append("Columns:")
            for i, col in enumerate(data.columns):
                preview_lines.append(f"  {i+1}. {col}")
            preview_lines.append("")
            
            # Show first few rows
            preview_lines.append("Sample Data (first 5 rows):")
            preview_lines.append("-" * 30)
            
            # Convert to string representation
            sample_data = data.head(5).to_string(max_cols=5, max_colwidth=20)
            preview_lines.append(sample_data)
            
            # Show data types
            preview_lines.append("")
            preview_lines.append("Data Types:")
            for col, dtype in data.dtypes.items():
                preview_lines.append(f"  {col}: {dtype}")
            
            # Insert preview text
            preview_text = "\n".join(preview_lines)
            self.preview_text.insert("1.0", preview_text)
            
            # Disable text box
            self.preview_text.configure(state="disabled")
            
        except Exception as e:
            self.logger.error(f"Failed to show preview: {e}")
    
    def _enable_data_tabs(self):
        """Enable data cleaning and export functionality."""
        try:
            # Update cleaner tab with data
            if hasattr(self, 'current_data') and self.current_data is not None:
                # Enable all cleaning buttons
                cleaning_buttons = [
                    'quality_button', 'auto_clean_button', 'auto_clean_aggressive_button',
                    'remove_duplicates_button', 'handle_missing_button', 'clean_text_button',
                    'auto_types_button', 'remove_outliers_button'
                ]
                
                for button_name in cleaning_buttons:
                    if hasattr(self, button_name):
                        getattr(self, button_name).configure(state="normal")
                
                # Initialize cleaner
                self._initialize_cleaner()
                
                # Show data in cleaner tab
                self._update_cleaner_data_display()
                self._update_data_info()
                self._update_history_buttons()
                
                # Enable export button
                self.export_button.configure(state="normal")
                
                # Show export info
                self._update_export_info()
                
        except Exception as e:
            self.logger.error(f"Failed to enable data tabs: {e}")
    
    def _update_cleaner_data_display(self):
        """Update the data display in the cleaner tab."""
        try:
            if hasattr(self, 'current_data') and self.current_data is not None:
                # Enable text box
                self.data_text.configure(state="normal")
                self.data_text.delete("1.0", "end")
                
                # Create data summary
                summary_lines = []
                summary_lines.append(f"Dataset Summary")
                summary_lines.append("=" * 40)
                summary_lines.append(f"Rows: {len(self.current_data)}")
                summary_lines.append(f"Columns: {len(self.current_data.columns)}")
                summary_lines.append(f"Memory Usage: {self.current_data.memory_usage(deep=True).sum() / 1024:.1f} KB")
                summary_lines.append("")
                
                # Show missing values
                missing = self.current_data.isnull().sum()
                if missing.sum() > 0:
                    summary_lines.append("Missing Values:")
                    for col, count in missing.items():
                        if count > 0:
                            summary_lines.append(f"  {col}: {count}")
                else:
                    summary_lines.append("No missing values found")
                
                summary_lines.append("")
                summary_lines.append("Data Preview:")
                summary_lines.append("-" * 20)
                
                # Add data preview
                data_preview = self.current_data.head(10).to_string(max_cols=5, max_colwidth=15)
                summary_lines.append(data_preview)
                
                # Insert text
                summary_text = "\n".join(summary_lines)
                self.data_text.insert("1.0", summary_text)
                
                # Disable text box
                self.data_text.configure(state="disabled")
                
        except Exception as e:
            self.logger.error(f"Failed to update cleaner display: {e}")
    
    def _update_export_info(self):
        """Update export information display."""
        try:
            if hasattr(self, 'current_data') and self.current_data is not None:
                # Enable text box
                self.export_info_text.configure(state="normal")
                self.export_info_text.delete("1.0", "end")
                
                # Create export info
                info_lines = []
                info_lines.append("Export Information")
                info_lines.append("=" * 30)
                info_lines.append(f"Records to export: {len(self.current_data)}")
                info_lines.append(f"Columns to export: {len(self.current_data.columns)}")
                info_lines.append("")
                
                # Estimate file sizes
                memory_usage = self.current_data.memory_usage(deep=True).sum()
                info_lines.append("Estimated Export Sizes:")
                info_lines.append(f"  Excel (.xlsx): ~{memory_usage * 0.8 / 1024:.1f} KB")
                info_lines.append(f"  CSV (.csv): ~{memory_usage * 1.2 / 1024:.1f} KB")
                info_lines.append(f"  JSON (.json): ~{memory_usage * 1.5 / 1024:.1f} KB")
                info_lines.append("")
                
                info_lines.append("Available Formats:")
                info_lines.append("  ✓ Excel (.xlsx) - Recommended")
                info_lines.append("  ✓ CSV (.csv) - Universal")
                info_lines.append("  ✓ JSON (.json) - Structured")
                
                # Insert text
                info_text = "\n".join(info_lines)
                self.export_info_text.insert("1.0", info_text)
                
                # Disable text box
                self.export_info_text.configure(state="disabled")
                
        except Exception as e:
            self.logger.error(f"Failed to update export info: {e}")
    
    def _export_data(self):
        """Handle export data button click."""
        if not hasattr(self, 'current_data') or self.current_data is None:
            self.show_error("No data to export. Please scrape some data first.")
            return
        
        try:
            from tkinter import filedialog
            from export_manager import ExportManager
            from models import ExportOptions, ExportFormat
            
            # Get selected format
            selected_format = self.format_var.get()
            
            # Set up file dialog
            if selected_format == "xlsx":
                file_types = [("Excel files", "*.xlsx"), ("All files", "*.*")]
                default_ext = ".xlsx"
                export_format = ExportFormat.EXCEL
            elif selected_format == "csv":
                file_types = [("CSV files", "*.csv"), ("All files", "*.*")]
                default_ext = ".csv"
                export_format = ExportFormat.CSV
            elif selected_format == "json":
                file_types = [("JSON files", "*.json"), ("All files", "*.*")]
                default_ext = ".json"
                export_format = ExportFormat.JSON
            else:
                file_types = [("All files", "*.*")]
                default_ext = ".xlsx"
                export_format = ExportFormat.EXCEL
            
            # Show file save dialog
            filename = filedialog.asksaveasfilename(
                title="Save exported data",
                defaultextension=default_ext,
                filetypes=file_types,
                initialfile=f"scraped_data{default_ext}"
            )
            
            if not filename:
                return  # User cancelled
            
            # Disable export button during operation
            self.export_button.configure(state="disabled", text="Exporting...")
            self.export_progress.set(0.1)
            self.update_status("Starting export...")
            
            # Create export manager
            export_manager = ExportManager(self.error_handler)
            
            # Create export options
            export_options = ExportOptions(
                format=export_format,
                include_index=False,
                sheet_name="ScrapedData",
                encoding="utf-8"
            )
            
            self.export_progress.set(0.3)
            self.update_status(f"Exporting to {selected_format.upper()}...")
            
            # Perform export
            if selected_format == "xlsx":
                success = export_manager.export_to_excel(self.current_data, filename, export_options)
            elif selected_format == "csv":
                success = export_manager.export_to_csv(self.current_data, filename, export_options)
            elif selected_format == "json":
                success = export_manager.export_to_json(self.current_data, filename, export_options)
            else:
                success = export_manager.export_to_excel(self.current_data, filename, export_options)
            
            self.export_progress.set(0.8)
            
            if success:
                self.export_progress.set(1.0)
                self.update_status(f"Successfully exported {len(self.current_data)} records to {filename}")
                
                # Show success dialog
                import tkinter.messagebox as msgbox
                result = msgbox.askyesno(
                    "Export Successful", 
                    f"Data exported successfully to:\n{filename}\n\nWould you like to open the file location?",
                    icon="question"
                )
                
                if result:
                    # Open file location
                    import os
                    import subprocess
                    import platform
                    
                    folder_path = os.path.dirname(filename)
                    if platform.system() == "Windows":
                        os.startfile(folder_path)
                    elif platform.system() == "Darwin":  # macOS
                        subprocess.run(["open", folder_path])
                    else:  # Linux
                        subprocess.run(["xdg-open", folder_path])
            else:
                self.show_error("Export failed. Please check the file path and try again.")
                self.update_status("Export failed")
                
        except Exception as e:
            self.logger.error(f"Export failed: {e}", exc_info=True)
            self.show_error(f"Export failed: {str(e)}")
            self.update_status("Export failed")
            
        finally:
            # Re-enable export button
            self.export_button.configure(state="normal", text="Export Data")
            self.export_progress.set(0)
    
    def show_progress(self, message: str, progress: float):
        """Show progress information to the user."""
        self.update_status(message)
        # Update appropriate progress bar based on current tab
        current_tab = self.tabview.get()
        if current_tab == "Web Scraper":
            self.scraping_progress.set(progress)
        elif current_tab == "Export":
            self.export_progress.set(progress)
    
    def show_error(self, message: str):
        """Show error message to the user."""
        self.logger.error(f"UI Error: {message}")
        # Create error dialog
        error_dialog = ctk.CTkToplevel(self)
        error_dialog.title("Error")
        error_dialog.geometry("400x150")
        error_dialog.transient(self)
        error_dialog.grab_set()
        
        # Center the dialog
        error_dialog.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() // 2) - (400 // 2)
        y = self.winfo_y() + (self.winfo_height() // 2) - (150 // 2)
        error_dialog.geometry(f"400x150+{x}+{y}")
        
        # Error message
        ctk.CTkLabel(error_dialog, text=message, wraplength=350).pack(pady=20, padx=20)
        
        # OK button
        ctk.CTkButton(error_dialog, text="OK", command=error_dialog.destroy).pack(pady=10)
    
    def update_log(self, message: str):
        """Update the log display (placeholder for now)."""
        self.logger.info(f"Log: {message}")
    
    def update_status(self, message: str):
        """Update the status bar message."""
        self.status_label.configure(text=message)
        self.update_idletasks()
    
    # Data Cleaning Methods
    def _analyze_data_quality(self):
        """Analyze and display data quality metrics."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            self.update_status("Analyzing data quality...")
            quality_report = self.data_cleaner.validate_data_quality()
            
            # Update statistics display
            self.stats_text.configure(state="normal")
            self.stats_text.delete("1.0", "end")
            
            stats_lines = []
            stats_lines.append(f"Data Quality Score: {quality_report.get('overall_score', 0):.1f}/100")
            stats_lines.append("=" * 50)
            
            if 'metrics' in quality_report:
                metrics = quality_report['metrics']
                stats_lines.append(f"Missing Data: {metrics.get('missing_percentage', 0):.1f}%")
                stats_lines.append(f"Duplicates: {metrics.get('duplicate_percentage', 0):.1f}%")
            
            if quality_report.get('issues'):
                stats_lines.append("\nIssues Found:")
                for issue in quality_report['issues']:
                    stats_lines.append(f"• {issue}")
            
            if quality_report.get('recommendations'):
                stats_lines.append("\nRecommendations:")
                for rec in quality_report['recommendations']:
                    stats_lines.append(f"• {rec}")
            
            self.stats_text.insert("1.0", "\n".join(stats_lines))
            self.stats_text.configure(state="disabled")
            
            self.update_status("Data quality analysis completed")
            
        except Exception as e:
            self.show_error(f"Failed to analyze data quality: {str(e)}")
            self.logger.error(f"Data quality analysis failed: {e}")
    
    def _auto_clean_data(self, aggressive: bool = False):
        """Automatically clean the dataset."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            mode = "aggressive" if aggressive else "smart"
            self.update_status(f"Auto-cleaning data ({mode} mode)...")
            
            cleaning_report = self.data_cleaner.auto_clean_dataset(aggressive=aggressive)
            
            if 'error' in cleaning_report:
                self.show_error(f"Auto-cleaning failed: {cleaning_report['error']}")
                return
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            # Show cleaning report
            report_lines = []
            report_lines.append(f"Auto-Cleaning Report ({mode} mode)")
            report_lines.append("=" * 40)
            report_lines.append(f"Records: {cleaning_report['records_before']} → {cleaning_report['records_after']}")
            report_lines.append(f"Columns: {cleaning_report['columns_before']} → {cleaning_report['columns_after']}")
            report_lines.append(f"Records Removed: {cleaning_report['records_removed']}")
            
            if cleaning_report.get('operations_performed'):
                report_lines.append("\nOperations Performed:")
                for op in cleaning_report['operations_performed']:
                    report_lines.append(f"• {op}")
            
            if cleaning_report.get('issues_fixed'):
                report_lines.append("\nIssues Fixed:")
                for issue in cleaning_report['issues_fixed']:
                    report_lines.append(f"• {issue}")
            
            self.stats_text.configure(state="normal")
            self.stats_text.delete("1.0", "end")
            self.stats_text.insert("1.0", "\n".join(report_lines))
            self.stats_text.configure(state="disabled")
            
            self.update_status(f"Auto-cleaning completed ({mode} mode)")
            
        except Exception as e:
            self.show_error(f"Auto-cleaning failed: {str(e)}")
            self.logger.error(f"Auto-cleaning failed: {e}")
    
    def _remove_duplicates(self):
        """Remove duplicate records from the dataset."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            strategy = self.duplicate_strategy.get()
            self.update_status(f"Removing duplicates ({strategy})...")
            
            initial_count = len(self.current_data)
            self.data_cleaner.remove_duplicates(strategy=strategy)
            final_count = len(self.data_cleaner.data)
            removed_count = initial_count - final_count
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status(f"Removed {removed_count} duplicate records")
            
        except Exception as e:
            self.show_error(f"Failed to remove duplicates: {str(e)}")
            self.logger.error(f"Remove duplicates failed: {e}")
    
    def _handle_missing_values(self):
        """Handle missing values in the dataset."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            strategy = self.missing_strategy.get()
            self.update_status(f"Handling missing values ({strategy})...")
            
            self.data_cleaner.handle_missing_values(strategy=strategy)
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status(f"Missing values handled using {strategy}")
            
        except Exception as e:
            self.show_error(f"Failed to handle missing values: {str(e)}")
            self.logger.error(f"Handle missing values failed: {e}")
    
    def _clean_text_data(self):
        """Clean text data based on selected operations."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            # Get selected operations
            selected_ops = []
            for op_name, var in self.text_operations.items():
                if var.get():
                    selected_ops.append(op_name)
            
            if not selected_ops:
                self.show_error("Please select at least one text cleaning operation")
                return
            
            self.update_status("Cleaning text data...")
            
            # Get text columns
            text_cols = self.current_data.select_dtypes(include=['object', 'string']).columns.tolist()
            
            if not text_cols:
                self.show_error("No text columns found in the dataset")
                return
            
            # Handle encoding fix separately
            if "fix_encoding" in selected_ops:
                self.data_cleaner.detect_and_fix_encoding_issues(text_cols)
                selected_ops.remove("fix_encoding")
            
            # Apply other text cleaning operations
            if selected_ops:
                self.data_cleaner.clean_text(text_cols, selected_ops)
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status("Text cleaning completed")
            
        except Exception as e:
            self.show_error(f"Failed to clean text data: {str(e)}")
            self.logger.error(f"Text cleaning failed: {e}")
    
    def _auto_detect_types(self):
        """Automatically detect and convert data types."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            self.update_status("Auto-detecting data types...")
            
            # Analyze columns for type conversion
            type_mapping = {}
            for col in self.current_data.columns:
                if self.current_data[col].dtype == 'object':
                    # Try to convert to numeric
                    numeric_series = pd.to_numeric(self.current_data[col], errors='coerce')
                    numeric_ratio = numeric_series.notna().sum() / len(self.current_data)
                    
                    if numeric_ratio > 0.8:  # 80% convertible to numeric
                        # Check if integers
                        if numeric_series.dropna().apply(lambda x: x.is_integer()).all():
                            type_mapping[col] = "integer"
                        else:
                            type_mapping[col] = "float"
                    else:
                        # Try datetime conversion
                        try:
                            pd.to_datetime(self.current_data[col].dropna().head(100), errors='raise')
                            type_mapping[col] = "datetime"
                        except:
                            # Check for boolean-like values
                            unique_vals = self.current_data[col].dropna().str.lower().unique()
                            if len(unique_vals) <= 10 and all(val in ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n'] for val in unique_vals):
                                type_mapping[col] = "boolean"
            
            if type_mapping:
                self.data_cleaner.convert_data_types(type_mapping)
                
                # Update current data
                self.current_data = self.data_cleaner.data
                self._update_cleaner_data_display()
                self._update_data_info()
                self._update_history_buttons()
                
                # Show conversion report
                report_lines = [f"Converted {len(type_mapping)} columns:"]
                for col, dtype in type_mapping.items():
                    report_lines.append(f"• {col} → {dtype}")
                
                self.stats_text.configure(state="normal")
                self.stats_text.delete("1.0", "end")
                self.stats_text.insert("1.0", "\n".join(report_lines))
                self.stats_text.configure(state="disabled")
                
                self.update_status(f"Auto-detected and converted {len(type_mapping)} column types")
            else:
                self.update_status("No type conversions needed")
            
        except Exception as e:
            self.show_error(f"Failed to auto-detect types: {str(e)}")
            self.logger.error(f"Auto-detect types failed: {e}")
    
    def _remove_outliers(self):
        """Remove outliers from numeric columns."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self._initialize_cleaner()
            
            method = self.outlier_method.get()
            threshold = self.outlier_threshold.get()
            
            self.update_status(f"Removing outliers ({method} method)...")
            
            # Get numeric columns
            numeric_cols = self.current_data.select_dtypes(include=[np.number]).columns.tolist()
            
            if not numeric_cols:
                self.show_error("No numeric columns found for outlier removal")
                return
            
            initial_count = len(self.current_data)
            self.data_cleaner.remove_outliers(numeric_cols, method=method, threshold=threshold)
            final_count = len(self.data_cleaner.data)
            removed_count = initial_count - final_count
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status(f"Removed {removed_count} outliers from {len(numeric_cols)} columns")
            
        except Exception as e:
            self.show_error(f"Failed to remove outliers: {str(e)}")
            self.logger.error(f"Remove outliers failed: {e}")
    
    def _undo_operation(self):
        """Undo the last cleaning operation."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self.show_error("No cleaner initialized")
                return
            
            self.data_cleaner.undo_last_operation()
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status("Operation undone")
            
        except Exception as e:
            self.show_error(f"Failed to undo operation: {str(e)}")
            self.logger.error(f"Undo operation failed: {e}")
    
    def _redo_operation(self):
        """Redo the last undone cleaning operation."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self.show_error("No cleaner initialized")
                return
            
            self.data_cleaner.redo_last_operation()
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status("Operation redone")
            
        except Exception as e:
            self.show_error(f"Failed to redo operation: {str(e)}")
            self.logger.error(f"Redo operation failed: {e}")
    
    def _reset_data(self):
        """Reset data to original state."""
        try:
            if not hasattr(self, 'data_cleaner') or self.data_cleaner is None:
                self.show_error("No cleaner initialized")
                return
            
            self.data_cleaner.reset_to_original()
            
            # Update current data
            self.current_data = self.data_cleaner.data
            self._update_cleaner_data_display()
            self._update_data_info()
            self._update_history_buttons()
            
            self.update_status("Data reset to original state")
            
        except Exception as e:
            self.show_error(f"Failed to reset data: {str(e)}")
            self.logger.error(f"Reset data failed: {e}")
    
    def _initialize_cleaner(self):
        """Initialize the data cleaner with current data."""
        try:
            if not hasattr(self, 'current_data') or self.current_data is None:
                raise ValueError("No data available for cleaning")
            
            from cleaner import DataCleaner
            self.data_cleaner = DataCleaner(self.current_data, self.error_handler)
            
        except Exception as e:
            self.logger.error(f"Failed to initialize cleaner: {e}")
            raise
    
    def _update_data_info(self):
        """Update the data information display."""
        try:
            if hasattr(self, 'current_data') and self.current_data is not None:
                rows, cols = self.current_data.shape
                memory_mb = self.current_data.memory_usage(deep=True).sum() / 1024 / 1024
                
                info_text = f"Dataset: {rows:,} rows × {cols} columns ({memory_mb:.1f} MB)"
                self.data_info_label.configure(text=info_text)
            else:
                self.data_info_label.configure(text="No data loaded")
                
        except Exception as e:
            self.logger.error(f"Failed to update data info: {e}")
    
    def _update_history_buttons(self):
        """Update the state of history buttons (undo/redo/reset)."""
        try:
            if hasattr(self, 'data_cleaner') and self.data_cleaner is not None:
                # Update undo button
                if self.data_cleaner.cleaning_history.can_undo():
                    self.undo_button.configure(state="normal")
                else:
                    self.undo_button.configure(state="disabled")
                
                # Update redo button
                if self.data_cleaner.cleaning_history.can_redo():
                    self.redo_button.configure(state="normal")
                else:
                    self.redo_button.configure(state="disabled")
                
                # Reset button is always enabled if we have a cleaner
                self.reset_button.configure(state="normal")
            else:
                # Disable all history buttons if no cleaner
                self.undo_button.configure(state="disabled")
                self.redo_button.configure(state="disabled")
                self.reset_button.configure(state="disabled")
                
        except Exception as e:
            self.logger.error(f"Failed to update history buttons: {e}")

    def cleanup(self):
        """Clean up resources before closing."""
        try:
            self.logger.info("Cleaning up main window resources")
            # Save window state only if window still exists
            if self.winfo_exists():
                self.config.update_ui_config(
                    window_width=self.winfo_width(),
                    window_height=self.winfo_height()
                )
        except Exception as e:
            # Ignore cleanup errors during shutdown
            if self.logger:
                self.logger.debug(f"Cleanup error (ignored): {e}")