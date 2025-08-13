#!/usr/bin/env python3
"""
Web Scraper & Dataset Builder - Main Application Entry Point

This is the main entry point for the Web Scraper & Dataset Builder application.
It initializes the application, sets up logging, and launches the GUI.
"""

import sys
import os
import logging
from pathlib import Path
from typing import Optional

# Add the current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ui import MainWindow
from config import AppConfig
from utils.logger import setup_logging
from utils.error_handler import ErrorHandler


class WebScraperApp:
    """Main application class that orchestrates the entire application."""
    
    def __init__(self):
        """Initialize the application with configuration and logging."""
        self.config: Optional[AppConfig] = None
        self.main_window: Optional[MainWindow] = None
        self.error_handler: Optional[ErrorHandler] = None
        self.logger: Optional[logging.Logger] = None
        
        # Initialize core components
        self._setup_application()
    
    def _setup_application(self):
        """Set up application configuration, logging, and error handling."""
        try:
            # Load configuration
            self.config = AppConfig()
            
            # Setup logging
            self.logger = setup_logging(
                log_level=self.config.log_level,
                log_file=self.config.log_file
            )
            
            # Initialize error handler
            self.error_handler = ErrorHandler(self.logger)
            
            self.logger.info("Application initialized successfully")
            
        except Exception as e:
            print(f"Failed to initialize application: {e}")
            sys.exit(1)
    
    def run(self):
        """Launch the main application window and start the GUI event loop."""
        try:
            self.logger.info("Starting Web Scraper & Dataset Builder")
            
            # Create and configure main window
            self.main_window = MainWindow(
                config=self.config,
                error_handler=self.error_handler
            )
            
            # Start the GUI event loop
            self.main_window.mainloop()
            
        except Exception as e:
            error_msg = f"Failed to start application: {e}"
            self.logger.error(error_msg, exc_info=True)
            if self.error_handler:
                self.error_handler.handle_ui_error(e, "Application startup")
            else:
                print(error_msg)
            sys.exit(1)
    
    def shutdown(self):
        """Clean shutdown of the application."""
        try:
            if self.logger:
                self.logger.info("Shutting down Web Scraper & Dataset Builder")
            
            # Clean up resources
            if self.main_window:
                self.main_window.cleanup()
            
            # Save configuration if needed
            if self.config:
                self.config.save()
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error during shutdown: {e}", exc_info=True)
            print(f"Error during shutdown: {e}")


def main():
    """Main entry point for the application."""
    try:
        # Create and run the application
        app = WebScraperApp()
        app.run()
        
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Ensure clean shutdown
        if 'app' in locals():
            app.shutdown()


if __name__ == "__main__":
    main()