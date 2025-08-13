"""
Error Handling System for Web Scraper & Dataset Builder

This module provides centralized error handling with user-friendly messages,
logging, and recovery suggestions for different types of errors.
"""

import logging
import traceback
import sys
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import requests
from urllib.parse import urlparse


class ErrorType(Enum):
    """Enumeration of different error types in the application."""
    NETWORK = "network"
    PARSING = "parsing"
    DATA_PROCESSING = "data_processing"
    UI = "ui"
    FILE_IO = "file_io"
    CONFIGURATION = "configuration"
    VALIDATION = "validation"
    UNKNOWN = "unknown"


@dataclass
class ErrorResponse:
    """Response object containing error information and suggested actions."""
    success: bool
    error_type: ErrorType
    message: str
    technical_details: str
    suggested_action: str
    retry_possible: bool
    context: Dict[str, Any]


class ErrorHandler:
    """Centralized error handling system with logging and user notifications."""
    
    def __init__(self, logger: logging.Logger):
        """Initialize error handler with logger instance."""
        self.logger = logger
        self.error_callbacks: Dict[ErrorType, Callable] = {}
        self.error_count = 0
        self.recent_errors = []
    
    def register_error_callback(self, error_type: ErrorType, callback: Callable):
        """Register a callback function for specific error types."""
        self.error_callbacks[error_type] = callback
    
    def handle_network_error(self, error: Exception, url: str = "", context: Dict[str, Any] = None) -> ErrorResponse:
        """Handle network-related errors with specific suggestions."""
        context = context or {}
        context.update({"url": url, "error_type": "network"})
        
        # Determine specific network error type
        if isinstance(error, requests.exceptions.ConnectionError):
            message = "Unable to connect to the website. Please check your internet connection."
            suggested_action = "Verify your internet connection and try again. The website might be temporarily unavailable."
            retry_possible = True
            
        elif isinstance(error, requests.exceptions.Timeout):
            message = "The request timed out. The website is taking too long to respond."
            suggested_action = "Try again with a longer timeout setting or check if the website is responding slowly."
            retry_possible = True
            
        elif isinstance(error, requests.exceptions.HTTPError):
            status_code = getattr(error.response, 'status_code', 'unknown')
            if status_code == 404:
                message = "The requested page was not found (404 error)."
                suggested_action = "Check the URL for typos or try a different page on the same website."
                retry_possible = False
            elif status_code == 403:
                message = "Access to this page is forbidden (403 error)."
                suggested_action = "The website may be blocking automated requests. Try adding custom headers or using a different approach."
                retry_possible = True
            elif status_code >= 500:
                message = f"The website is experiencing server problems ({status_code} error)."
                suggested_action = "This is a temporary server issue. Try again later."
                retry_possible = True
            else:
                message = f"HTTP error occurred (status code: {status_code})."
                suggested_action = "Check the URL and try again. The website may have specific access requirements."
                retry_possible = True
                
        elif isinstance(error, requests.exceptions.InvalidURL):
            message = "The provided URL is not valid."
            suggested_action = "Please check the URL format and ensure it includes http:// or https://"
            retry_possible = False
            
        else:
            message = "An unexpected network error occurred."
            suggested_action = "Check your internet connection and try again."
            retry_possible = True
        
        return self._create_error_response(
            error_type=ErrorType.NETWORK,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=retry_possible,
            context=context,
            exception=error
        )
    
    def handle_parsing_error(self, error: Exception, content: str = "", context: Dict[str, Any] = None) -> ErrorResponse:
        """Handle HTML parsing and data extraction errors."""
        context = context or {}
        context.update({"content_length": len(content), "error_type": "parsing"})
        
        if "lxml" in str(error).lower():
            message = "Error parsing HTML content. The page structure may be invalid."
            suggested_action = "Try using a different parser or check if the page loads correctly in a browser."
            
        elif "encoding" in str(error).lower():
            message = "Character encoding issue detected while parsing the page."
            suggested_action = "The page may use a different character encoding. Try specifying the encoding manually."
            
        elif "empty" in str(error).lower() or len(content) == 0:
            message = "The page appears to be empty or contains no parseable content."
            suggested_action = "Check if the URL is correct and the page loads content. It might require JavaScript to load data."
            
        else:
            message = "Unable to parse the webpage content."
            suggested_action = "The page structure may be complex or use dynamic content. Try enabling dynamic scraping."
        
        return self._create_error_response(
            error_type=ErrorType.PARSING,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=True,
            context=context,
            exception=error
        )
    
    def handle_data_error(self, error: Exception, operation: str = "", context: Dict[str, Any] = None) -> ErrorResponse:
        """Handle data processing and manipulation errors."""
        context = context or {}
        context.update({"operation": operation, "error_type": "data_processing"})
        
        if "memory" in str(error).lower() or isinstance(error, MemoryError):
            message = "Not enough memory to process this dataset."
            suggested_action = "Try processing the data in smaller chunks or close other applications to free up memory."
            retry_possible = True
            
        elif "dtype" in str(error).lower() or "type" in str(error).lower():
            message = "Data type conversion error occurred."
            suggested_action = "Check the data format and ensure all values are compatible with the target data type."
            retry_possible = True
            
        elif "index" in str(error).lower() or isinstance(error, (IndexError, KeyError)):
            message = "Error accessing data columns or rows."
            suggested_action = "Verify that the expected columns exist in the dataset and check for empty data."
            retry_possible = True
            
        elif "duplicate" in str(error).lower():
            message = "Duplicate data handling error."
            suggested_action = "Check the duplicate removal settings and ensure the data structure is consistent."
            retry_possible = True
            
        else:
            message = f"Data processing error during {operation}."
            suggested_action = "Check the data format and try a different processing approach."
            retry_possible = True
        
        return self._create_error_response(
            error_type=ErrorType.DATA_PROCESSING,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=retry_possible,
            context=context,
            exception=error
        )
    
    def handle_ui_error(self, error: Exception, context: str = "", context_data: Dict[str, Any] = None) -> ErrorResponse:
        """Handle user interface related errors."""
        context_data = context_data or {}
        context_data.update({"ui_context": context, "error_type": "ui"})
        
        if "tkinter" in str(error).lower() or "customtkinter" in str(error).lower():
            message = "User interface error occurred."
            suggested_action = "Try restarting the application. If the problem persists, check your display settings."
            
        elif "permission" in str(error).lower():
            message = "Permission denied for the requested operation."
            suggested_action = "Check file permissions or run the application with appropriate privileges."
            
        elif "file" in str(error).lower() and "not found" in str(error).lower():
            message = "Required file not found."
            suggested_action = "Ensure all application files are present and the installation is complete."
            
        else:
            message = f"Interface error in {context}."
            suggested_action = "Try the operation again or restart the application if the problem persists."
        
        return self._create_error_response(
            error_type=ErrorType.UI,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=True,
            context=context_data,
            exception=error
        )
    
    def handle_file_error(self, error: Exception, filepath: str = "", operation: str = "", context: Dict[str, Any] = None) -> ErrorResponse:
        """Handle file I/O related errors."""
        context = context or {}
        context.update({"filepath": filepath, "operation": operation, "error_type": "file_io"})
        
        if isinstance(error, FileNotFoundError):
            message = f"File not found: {filepath}"
            suggested_action = "Check that the file path is correct and the file exists."
            retry_possible = False
            
        elif isinstance(error, PermissionError):
            message = f"Permission denied accessing file: {filepath}"
            suggested_action = "Check file permissions or close the file if it's open in another application."
            retry_possible = True
            
        elif isinstance(error, OSError) and "disk" in str(error).lower():
            message = "Insufficient disk space for the operation."
            suggested_action = "Free up disk space and try again."
            retry_possible = True
            
        elif "encoding" in str(error).lower():
            message = "File encoding error."
            suggested_action = "Try saving the file with UTF-8 encoding or specify a different encoding."
            retry_possible = True
            
        else:
            message = f"File operation error during {operation}."
            suggested_action = "Check the file path and permissions, then try again."
            retry_possible = True
        
        return self._create_error_response(
            error_type=ErrorType.FILE_IO,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=retry_possible,
            context=context,
            exception=error
        )
    
    def handle_validation_error(self, error: Exception, field: str = "", value: Any = None, context: Dict[str, Any] = None) -> ErrorResponse:
        """Handle input validation errors."""
        context = context or {}
        context.update({"field": field, "value": str(value), "error_type": "validation"})
        
        message = f"Invalid value for {field}: {value}"
        suggested_action = "Please check the input format and try again with a valid value."
        
        return self._create_error_response(
            error_type=ErrorType.VALIDATION,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=True,
            context=context,
            exception=error
        )
    
    def handle_unknown_error(self, error: Exception, context: Dict[str, Any] = None) -> ErrorResponse:
        """Handle unexpected errors that don't fit other categories."""
        context = context or {}
        context.update({"error_type": "unknown"})
        
        message = "An unexpected error occurred."
        suggested_action = "Please try the operation again. If the problem persists, restart the application."
        
        return self._create_error_response(
            error_type=ErrorType.UNKNOWN,
            message=message,
            technical_details=str(error),
            suggested_action=suggested_action,
            retry_possible=True,
            context=context,
            exception=error
        )
    
    def _create_error_response(
        self,
        error_type: ErrorType,
        message: str,
        technical_details: str,
        suggested_action: str,
        retry_possible: bool,
        context: Dict[str, Any],
        exception: Exception
    ) -> ErrorResponse:
        """Create a standardized error response object."""
        
        # Log the error
        self.log_error(exception, context)
        
        # Create response
        response = ErrorResponse(
            success=False,
            error_type=error_type,
            message=message,
            technical_details=technical_details,
            suggested_action=suggested_action,
            retry_possible=retry_possible,
            context=context
        )
        
        # Track error statistics
        self.error_count += 1
        self.recent_errors.append(response)
        
        # Keep only last 50 errors
        if len(self.recent_errors) > 50:
            self.recent_errors = self.recent_errors[-50:]
        
        # Call registered callback if available
        if error_type in self.error_callbacks:
            try:
                self.error_callbacks[error_type](response)
            except Exception as callback_error:
                self.logger.error(f"Error in error callback: {callback_error}")
        
        return response
    
    def log_error(self, error: Exception, context: Dict[str, Any]):
        """Log error with full context and stack trace."""
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "stack_trace": traceback.format_exc()
        }
        
        self.logger.error(f"Error occurred: {error_info}")
    
    def show_user_error(self, message: str, error_type: str = "Error"):
        """Display user-friendly error message (to be implemented by UI)."""
        # This method should be overridden by the UI component
        # to show actual error dialogs to the user
        print(f"{error_type}: {message}")
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error statistics for debugging and monitoring."""
        error_types = {}
        for error in self.recent_errors:
            error_type = error.error_type.value
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            "total_errors": self.error_count,
            "recent_errors_count": len(self.recent_errors),
            "error_types": error_types,
            "recent_errors": [
                {
                    "type": err.error_type.value,
                    "message": err.message,
                    "retry_possible": err.retry_possible
                }
                for err in self.recent_errors[-10:]  # Last 10 errors
            ]
        }
    
    def clear_error_history(self):
        """Clear the error history and reset counters."""
        self.recent_errors.clear()
        self.error_count = 0
        self.logger.info("Error history cleared")