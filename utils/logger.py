"""
Logging Configuration for Web Scraper & Dataset Builder

This module provides centralized logging configuration and utilities
for the application with support for file and console logging.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    console_output: bool = True
) -> logging.Logger:
    """
    Set up comprehensive logging configuration for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup log files to keep
        console_output: Whether to output logs to console
    
    Returns:
        Configured logger instance
    """
    # Create main logger
    logger = logging.getLogger("WebScraperApp")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if log_file:
        try:
            # Ensure log directory exists
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            logger.error(f"Failed to set up file logging: {e}")
    
    # Log startup information
    logger.info("=" * 60)
    logger.info("Web Scraper & Dataset Builder - Logging Started")
    logger.info(f"Log Level: {log_level}")
    logger.info(f"Log File: {log_file or 'Console only'}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module or component.
    
    Args:
        name: Name of the logger (usually __name__)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(f"WebScraperApp.{name}")


class LogCapture:
    """Context manager for capturing log messages during operations."""
    
    def __init__(self, logger_name: str = "WebScraperApp", level: int = logging.INFO):
        """Initialize log capture with specified logger and level."""
        self.logger_name = logger_name
        self.level = level
        self.messages = []
        self.handler = None
    
    def __enter__(self):
        """Start capturing log messages."""
        self.handler = LogCaptureHandler(self.messages)
        self.handler.setLevel(self.level)
        
        logger = logging.getLogger(self.logger_name)
        logger.addHandler(self.handler)
        
        return self.messages
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop capturing log messages."""
        if self.handler:
            logger = logging.getLogger(self.logger_name)
            logger.removeHandler(self.handler)


class LogCaptureHandler(logging.Handler):
    """Custom logging handler that captures messages to a list."""
    
    def __init__(self, message_list):
        """Initialize with a list to store messages."""
        super().__init__()
        self.message_list = message_list
    
    def emit(self, record):
        """Capture log record to the message list."""
        try:
            message = self.format(record)
            self.message_list.append({
                'timestamp': datetime.fromtimestamp(record.created),
                'level': record.levelname,
                'message': message,
                'module': record.module,
                'function': record.funcName,
                'line': record.lineno
            })
        except Exception:
            self.handleError(record)


def log_function_call(func):
    """Decorator to log function calls with parameters and execution time."""
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = datetime.now()
        
        # Log function entry
        logger.debug(f"Entering {func.__name__} with args={args}, kwargs={kwargs}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.debug(f"Exiting {func.__name__} successfully (took {execution_time:.3f}s)")
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error in {func.__name__} after {execution_time:.3f}s: {e}", exc_info=True)
            raise
    
    return wrapper


def log_performance(operation_name: str):
    """Context manager for logging performance of operations."""
    return PerformanceLogger(operation_name)


class PerformanceLogger:
    """Context manager for measuring and logging operation performance."""
    
    def __init__(self, operation_name: str):
        """Initialize with operation name."""
        self.operation_name = operation_name
        self.start_time = None
        self.logger = get_logger("Performance")
    
    def __enter__(self):
        """Start performance measurement."""
        self.start_time = datetime.now()
        self.logger.info(f"Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End performance measurement and log results."""
        if self.start_time:
            execution_time = (datetime.now() - self.start_time).total_seconds()
            
            if exc_type is None:
                self.logger.info(f"Completed operation: {self.operation_name} (took {execution_time:.3f}s)")
            else:
                self.logger.error(f"Failed operation: {self.operation_name} after {execution_time:.3f}s - {exc_val}")
    
    def checkpoint(self, message: str):
        """Log a checkpoint during the operation."""
        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            self.logger.info(f"{self.operation_name} - {message} (elapsed: {elapsed:.3f}s)")