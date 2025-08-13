"""
Data Cleaning Module for Web Scraper & Dataset Builder

This module provides comprehensive data cleaning and transformation
capabilities for scraped datasets with advanced operations and validation.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
import re
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import StandardScaler, LabelEncoder

from models import (
    CleaningOperation, CleaningHistory, 
    CleanerInterface, validate_dataframe
)
from utils.logger import get_logger, log_performance
from utils.error_handler import ErrorHandler


class DataCleaner:
    """Comprehensive data cleaning and transformation class."""
    
    def __init__(self, data: pd.DataFrame, error_handler: ErrorHandler):
        """Initialize the data cleaner with a dataset."""
        if not validate_dataframe(data):
            raise ValueError("Invalid DataFrame provided")
        
        self.data = data.copy()
        self.original_data = data.copy()
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        self.cleaning_history = CleaningHistory()
        
        # Store original data as first snapshot
        self.cleaning_history.add_operation(
            CleaningOperation(
                operation_type="initial_state",
                parameters={},
                target_columns=[],
                description="Initial dataset state"
            ),
            self.original_data
        )
    
    def remove_duplicates(self, strategy: str = 'first', subset: Optional[List[str]] = None) -> pd.DataFrame:
        """Remove duplicate records from the dataset."""
        with log_performance("Removing duplicates"):
            try:
                initial_count = len(self.data)
                
                # Validate strategy
                if strategy not in ['first', 'last', 'all']:
                    raise ValueError(f"Invalid strategy: {strategy}. Must be 'first', 'last', or 'all'")
                
                # Remove duplicates
                if strategy == 'all':
                    # Remove all duplicate rows (keep none)
                    self.data = self.data.drop_duplicates(subset=subset, keep=False)
                else:
                    self.data = self.data.drop_duplicates(subset=subset, keep=strategy)
                
                final_count = len(self.data)
                records_affected = initial_count - final_count
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="remove_duplicates",
                    parameters={"strategy": strategy, "subset": subset},
                    target_columns=subset or list(self.data.columns),
                    description=f"Removed {records_affected} duplicate records using '{strategy}' strategy",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Removed {records_affected} duplicates from {initial_count} records")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "remove_duplicates")
                self.logger.error(f"Failed to remove duplicates: {error_response.message}")
                raise
    
    def handle_missing_values(
        self, 
        strategy: str, 
        columns: Optional[List[str]] = None,
        fill_value: Any = None
    ) -> pd.DataFrame:
        """Handle missing values in the dataset with various strategies."""
        with log_performance("Handling missing values"):
            try:
                if columns is None:
                    columns = list(self.data.columns)
                
                # Validate columns exist
                missing_cols = [col for col in columns if col not in self.data.columns]
                if missing_cols:
                    raise ValueError(f"Columns not found: {missing_cols}")
                
                initial_missing = self.data[columns].isnull().sum().sum()
                
                if strategy == "drop":
                    self.data = self.data.dropna(subset=columns)
                    
                elif strategy == "fill_mean":
                    for col in columns:
                        if self.data[col].dtype in ['int64', 'float64']:
                            self.data[col].fillna(self.data[col].mean(), inplace=True)
                        else:
                            self.logger.warning(f"Cannot calculate mean for non-numeric column: {col}")
                
                elif strategy == "fill_median":
                    for col in columns:
                        if self.data[col].dtype in ['int64', 'float64']:
                            self.data[col].fillna(self.data[col].median(), inplace=True)
                        else:
                            self.logger.warning(f"Cannot calculate median for non-numeric column: {col}")
                
                elif strategy == "fill_mode":
                    for col in columns:
                        mode_value = self.data[col].mode()
                        if not mode_value.empty:
                            self.data[col].fillna(mode_value[0], inplace=True)
                
                elif strategy == "fill_custom":
                    if fill_value is None:
                        raise ValueError("fill_value must be provided for 'fill_custom' strategy")
                    for col in columns:
                        self.data[col].fillna(fill_value, inplace=True)
                
                elif strategy == "forward_fill":
                    self.data[columns] = self.data[columns].fillna(method='ffill')
                
                elif strategy == "backward_fill":
                    self.data[columns] = self.data[columns].fillna(method='bfill')
                
                elif strategy == "interpolate":
                    for col in columns:
                        if self.data[col].dtype in ['int64', 'float64']:
                            self.data[col] = self.data[col].interpolate()
                        else:
                            self.logger.warning(f"Cannot interpolate non-numeric column: {col}")
                
                elif strategy == "knn_impute":
                    numeric_cols = [col for col in columns if self.data[col].dtype in ['int64', 'float64']]
                    if numeric_cols:
                        imputer = KNNImputer(n_neighbors=5)
                        self.data[numeric_cols] = imputer.fit_transform(self.data[numeric_cols])
                
                else:
                    raise ValueError(f"Unknown strategy: {strategy}")
                
                final_missing = self.data[columns].isnull().sum().sum()
                records_affected = initial_missing - final_missing
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="handle_missing_values",
                    parameters={
                        "strategy": strategy, 
                        "columns": columns, 
                        "fill_value": fill_value
                    },
                    target_columns=columns,
                    description=f"Handled {records_affected} missing values using '{strategy}' strategy",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Handled {records_affected} missing values using {strategy}")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "handle_missing_values")
                self.logger.error(f"Failed to handle missing values: {error_response.message}")
                raise
    
    def clean_text(self, columns: List[str], operations: List[str]) -> pd.DataFrame:
        """Clean text data with various operations."""
        with log_performance("Cleaning text data"):
            try:
                # Validate columns
                missing_cols = [col for col in columns if col not in self.data.columns]
                if missing_cols:
                    raise ValueError(f"Columns not found: {missing_cols}")
                
                records_affected = 0
                
                for col in columns:
                    if self.data[col].dtype != 'object':
                        self.logger.warning(f"Column {col} is not text type, converting to string")
                        self.data[col] = self.data[col].astype(str)
                    
                    original_values = self.data[col].copy()
                    
                    for operation in operations:
                        if operation == "remove_special_chars":
                            self.data[col] = self.data[col].str.replace(r'[^\w\s]', '', regex=True)
                        
                        elif operation == "remove_extra_spaces":
                            self.data[col] = self.data[col].str.replace(r'\s+', ' ', regex=True).str.strip()
                        
                        elif operation == "lowercase":
                            self.data[col] = self.data[col].str.lower()
                        
                        elif operation == "uppercase":
                            self.data[col] = self.data[col].str.upper()
                        
                        elif operation == "title_case":
                            self.data[col] = self.data[col].str.title()
                        
                        elif operation == "remove_numbers":
                            self.data[col] = self.data[col].str.replace(r'\d+', '', regex=True)
                        
                        elif operation == "remove_html_tags":
                            self.data[col] = self.data[col].str.replace(r'<[^>]+>', '', regex=True)
                        
                        elif operation == "normalize_whitespace":
                            self.data[col] = self.data[col].str.replace(r'[\n\r\t]', ' ', regex=True)
                            self.data[col] = self.data[col].str.replace(r'\s+', ' ', regex=True).str.strip()
                        
                        else:
                            self.logger.warning(f"Unknown text cleaning operation: {operation}")
                    
                    # Always clean up extra spaces at the end if any removal operations were performed
                    removal_ops = ['remove_special_chars', 'remove_numbers', 'remove_html_tags']
                    if any(op in operations for op in removal_ops):
                        self.data[col] = self.data[col].str.replace(r'\s+', ' ', regex=True).str.strip()
                    
                    # Count changes
                    changed_mask = original_values != self.data[col]
                    records_affected += changed_mask.sum()
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="clean_text",
                    parameters={"columns": columns, "operations": operations},
                    target_columns=columns,
                    description=f"Applied text cleaning operations: {', '.join(operations)}",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Cleaned text in {len(columns)} columns, {records_affected} values changed")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "clean_text")
                self.logger.error(f"Failed to clean text: {error_response.message}")
                raise
    
    def convert_data_types(self, type_mapping: Dict[str, str]) -> pd.DataFrame:
        """Convert data types of specified columns."""
        with log_performance("Converting data types"):
            try:
                records_affected = 0
                conversion_errors = []
                
                for column, target_type in type_mapping.items():
                    if column not in self.data.columns:
                        self.logger.warning(f"Column {column} not found, skipping")
                        continue
                    
                    try:
                        original_type = str(self.data[column].dtype)
                        
                        if target_type == "numeric":
                            self.data[column] = pd.to_numeric(self.data[column], errors='coerce')
                        
                        elif target_type == "integer":
                            self.data[column] = pd.to_numeric(self.data[column], errors='coerce')
                            self.data[column] = self.data[column].astype('Int64')  # Nullable integer
                        
                        elif target_type == "float":
                            self.data[column] = pd.to_numeric(self.data[column], errors='coerce')
                            self.data[column] = self.data[column].astype('float64')
                        
                        elif target_type == "string":
                            self.data[column] = self.data[column].astype('string')
                        
                        elif target_type == "datetime":
                            self.data[column] = pd.to_datetime(self.data[column], errors='coerce')
                        
                        elif target_type == "category":
                            self.data[column] = self.data[column].astype('category')
                        
                        elif target_type == "boolean":
                            # Convert common boolean representations
                            bool_map = {
                                'true': True, 'false': False, 'yes': True, 'no': False,
                                '1': True, '0': False, 'y': True, 'n': False
                            }
                            self.data[column] = self.data[column].astype(str).str.lower().map(bool_map)
                            self.data[column] = self.data[column].astype('boolean')
                        
                        else:
                            self.logger.warning(f"Unknown target type: {target_type}")
                            continue
                        
                        new_type = str(self.data[column].dtype)
                        if original_type != new_type:
                            records_affected += len(self.data)
                            self.logger.info(f"Converted {column}: {original_type} -> {new_type}")
                        
                    except Exception as e:
                        conversion_errors.append(f"{column}: {str(e)}")
                        self.logger.error(f"Failed to convert {column} to {target_type}: {e}")
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="convert_data_types",
                    parameters={"type_mapping": type_mapping, "errors": conversion_errors},
                    target_columns=list(type_mapping.keys()),
                    description=f"Converted data types for {len(type_mapping)} columns",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                if conversion_errors:
                    self.logger.warning(f"Conversion errors: {conversion_errors}")
                
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "convert_data_types")
                self.logger.error(f"Failed to convert data types: {error_response.message}")
                raise
    
    def rename_columns(self, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """Rename columns in the dataset."""
        with log_performance("Renaming columns"):
            try:
                # Validate that source columns exist
                missing_cols = [col for col in column_mapping.keys() if col not in self.data.columns]
                if missing_cols:
                    raise ValueError(f"Source columns not found: {missing_cols}")
                
                # Check for duplicate target names
                target_names = list(column_mapping.values())
                if len(target_names) != len(set(target_names)):
                    raise ValueError("Duplicate target column names detected")
                
                old_columns = list(self.data.columns)
                self.data = self.data.rename(columns=column_mapping)
                new_columns = list(self.data.columns)
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="rename_columns",
                    parameters={"column_mapping": column_mapping},
                    target_columns=list(column_mapping.keys()),
                    description=f"Renamed {len(column_mapping)} columns",
                    applied=True,
                    records_affected=0  # Column renaming doesn't affect record count
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Renamed columns: {column_mapping}")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "rename_columns")
                self.logger.error(f"Failed to rename columns: {error_response.message}")
                raise
    
    def reorder_columns(self, column_order: List[str]) -> pd.DataFrame:
        """Reorder columns in the dataset."""
        with log_performance("Reordering columns"):
            try:
                # Validate that all specified columns exist
                missing_cols = [col for col in column_order if col not in self.data.columns]
                if missing_cols:
                    raise ValueError(f"Columns not found: {missing_cols}")
                
                # Add any columns not specified in the order to the end
                remaining_cols = [col for col in self.data.columns if col not in column_order]
                final_order = column_order + remaining_cols
                
                self.data = self.data[final_order]
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="reorder_columns",
                    parameters={"column_order": column_order},
                    target_columns=column_order,
                    description=f"Reordered columns",
                    applied=True,
                    records_affected=0
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Reordered columns: {column_order}")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "reorder_columns")
                self.logger.error(f"Failed to reorder columns: {error_response.message}")
                raise
    
    def remove_outliers(self, columns: List[str], method: str = "iqr", threshold: float = 1.5) -> pd.DataFrame:
        """Remove outliers from numeric columns."""
        with log_performance("Removing outliers"):
            try:
                # Validate columns
                numeric_cols = [col for col in columns if col in self.data.columns and 
                              self.data[col].dtype in ['int64', 'float64']]
                
                if not numeric_cols:
                    raise ValueError("No valid numeric columns found for outlier removal")
                
                initial_count = len(self.data)
                outlier_mask = pd.Series([False] * len(self.data))
                
                for col in numeric_cols:
                    if method == "iqr":
                        Q1 = self.data[col].quantile(0.25)
                        Q3 = self.data[col].quantile(0.75)
                        IQR = Q3 - Q1
                        lower_bound = Q1 - threshold * IQR
                        upper_bound = Q3 + threshold * IQR
                        
                        col_outliers = (self.data[col] < lower_bound) | (self.data[col] > upper_bound)
                        outlier_mask = outlier_mask | col_outliers
                    
                    elif method == "zscore":
                        z_scores = np.abs((self.data[col] - self.data[col].mean()) / self.data[col].std())
                        col_outliers = z_scores > threshold
                        outlier_mask = outlier_mask | col_outliers
                    
                    else:
                        raise ValueError(f"Unknown outlier detection method: {method}")
                
                # Remove outliers
                self.data = self.data[~outlier_mask]
                final_count = len(self.data)
                records_affected = initial_count - final_count
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="remove_outliers",
                    parameters={"columns": columns, "method": method, "threshold": threshold},
                    target_columns=numeric_cols,
                    description=f"Removed {records_affected} outliers using {method} method",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Removed {records_affected} outliers from {len(numeric_cols)} columns")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "remove_outliers")
                self.logger.error(f"Failed to remove outliers: {error_response.message}")
                raise
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary statistics for the dataset."""
        try:
            if self.data.empty:
                return {"message": "No data available"}
            
            # Basic info
            summary = {
                "shape": self.data.shape,
                "rows": len(self.data),
                "columns": len(self.data.columns),
                "memory_usage_mb": self.data.memory_usage(deep=True).sum() / 1024 / 1024,
                "dtypes": self.data.dtypes.to_dict()
            }
            
            # Missing values
            missing_values = self.data.isnull().sum()
            summary["missing_values"] = {
                "total": missing_values.sum(),
                "by_column": missing_values[missing_values > 0].to_dict(),
                "percentage": (missing_values / len(self.data) * 100).round(2).to_dict()
            }
            
            # Duplicate rows
            summary["duplicates"] = {
                "count": self.data.duplicated().sum(),
                "percentage": (self.data.duplicated().sum() / len(self.data) * 100).round(2)
            }
            
            # Column types summary
            type_counts = self.data.dtypes.value_counts()
            summary["column_types"] = type_counts.to_dict()
            
            # Numeric columns statistics
            numeric_cols = self.data.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                summary["numeric_summary"] = self.data[numeric_cols].describe().to_dict()
            
            # Text columns statistics
            text_cols = self.data.select_dtypes(include=['object', 'string']).columns
            if len(text_cols) > 0:
                text_stats = {}
                for col in text_cols:
                    text_stats[col] = {
                        "unique_values": self.data[col].nunique(),
                        "most_common": self.data[col].value_counts().head(3).to_dict()
                    }
                summary["text_summary"] = text_stats
            
            # Cleaning history
            summary["cleaning_history"] = {
                "operations_count": len(self.cleaning_history.operations),
                "can_undo": self.cleaning_history.can_undo(),
                "can_redo": self.cleaning_history.can_redo(),
                "recent_operations": [
                    {
                        "type": op.operation_type,
                        "description": op.description,
                        "records_affected": op.records_affected
                    }
                    for op in self.cleaning_history.operations[-5:]  # Last 5 operations
                ]
            }
            
            return summary
            
        except Exception as e:
            error_response = self.error_handler.handle_data_error(e, "get_data_summary")
            self.logger.error(f"Failed to generate data summary: {error_response.message}")
            return {"error": error_response.message}
    
    def undo_last_operation(self) -> pd.DataFrame:
        """Undo the last cleaning operation."""
        try:
            if not self.cleaning_history.can_undo():
                raise ValueError("No operations to undo")
            
            # Get the operation that will be undone
            if self.cleaning_history.current_index > 0 and self.cleaning_history.current_index <= len(self.cleaning_history.operations):
                undone_operation = self.cleaning_history.operations[self.cleaning_history.current_index - 1]
                operation_desc = undone_operation.description
            else:
                operation_desc = "Unknown operation"
            
            self.cleaning_history.current_index -= 1
            self.data = self.cleaning_history.get_current_data()
            
            self.logger.info(f"Undone operation: {operation_desc}")
            
            return self.data
            
        except Exception as e:
            error_response = self.error_handler.handle_data_error(e, "undo_operation")
            self.logger.error(f"Failed to undo operation: {error_response.message}")
            raise
    
    def redo_last_operation(self) -> pd.DataFrame:
        """Redo the last undone cleaning operation."""
        try:
            if not self.cleaning_history.can_redo():
                raise ValueError("No operations to redo")
            
            self.cleaning_history.current_index += 1
            self.data = self.cleaning_history.get_current_data()
            
            # Get the operation that was redone
            if self.cleaning_history.current_index > 0 and self.cleaning_history.current_index <= len(self.cleaning_history.operations):
                redone_operation = self.cleaning_history.operations[self.cleaning_history.current_index - 1]
                operation_desc = redone_operation.description
            else:
                operation_desc = "Unknown operation"
            
            self.logger.info(f"Redone operation: {operation_desc}")
            
            return self.data
            
        except Exception as e:
            error_response = self.error_handler.handle_data_error(e, "redo_operation")
            self.logger.error(f"Failed to redo operation: {error_response.message}")
            raise
    
    def reset_to_original(self) -> pd.DataFrame:
        """Reset data to original state."""
        try:
            self.data = self.original_data.copy()
            self.cleaning_history = CleaningHistory()
            
            # Add initial state back
            self.cleaning_history.add_operation(
                CleaningOperation(
                    operation_type="reset_to_original",
                    parameters={},
                    target_columns=[],
                    description="Reset to original dataset state"
                ),
                self.original_data
            )
            
            self.logger.info("Reset data to original state")
            return self.data
            
        except Exception as e:
            error_response = self.error_handler.handle_data_error(e, "reset_to_original")
            self.logger.error(f"Failed to reset data: {error_response.message}")
            raise
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Perform comprehensive data quality validation."""
        with log_performance("Validating data quality"):
            try:
                quality_report = {
                    "overall_score": 0.0,
                    "issues": [],
                    "recommendations": [],
                    "metrics": {}
                }
                
                total_cells = len(self.data) * len(self.data.columns)
                if total_cells == 0:
                    return {"overall_score": 0.0, "issues": ["Dataset is empty"]}
                
                # Check for missing values
                missing_count = self.data.isnull().sum().sum()
                missing_percentage = (missing_count / total_cells) * 100
                quality_report["metrics"]["missing_percentage"] = missing_percentage
                
                if missing_percentage > 20:
                    quality_report["issues"].append(f"High missing data: {missing_percentage:.1f}%")
                    quality_report["recommendations"].append("Consider imputation or removing columns with excessive missing values")
                
                # Check for duplicates
                duplicate_count = self.data.duplicated().sum()
                duplicate_percentage = (duplicate_count / len(self.data)) * 100
                quality_report["metrics"]["duplicate_percentage"] = duplicate_percentage
                
                if duplicate_percentage > 5:
                    quality_report["issues"].append(f"High duplicate records: {duplicate_percentage:.1f}%")
                    quality_report["recommendations"].append("Remove duplicate records")
                
                # Check data type consistency
                inconsistent_types = []
                for col in self.data.columns:
                    if self.data[col].dtype == 'object':
                        # Check if numeric data is stored as text
                        numeric_count = pd.to_numeric(self.data[col], errors='coerce').notna().sum()
                        if numeric_count > len(self.data) * 0.8:  # 80% numeric
                            inconsistent_types.append(col)
                
                if inconsistent_types:
                    quality_report["issues"].append(f"Potential type issues in columns: {inconsistent_types}")
                    quality_report["recommendations"].append("Convert numeric columns from text to numeric types")
                
                # Check for outliers in numeric columns
                numeric_cols = self.data.select_dtypes(include=[np.number]).columns
                outlier_columns = []
                
                for col in numeric_cols:
                    Q1 = self.data[col].quantile(0.25)
                    Q3 = self.data[col].quantile(0.75)
                    IQR = Q3 - Q1
                    outliers = ((self.data[col] < (Q1 - 1.5 * IQR)) | 
                               (self.data[col] > (Q3 + 1.5 * IQR))).sum()
                    
                    if outliers > len(self.data) * 0.05:  # More than 5% outliers
                        outlier_columns.append(col)
                
                if outlier_columns:
                    quality_report["issues"].append(f"High outlier count in columns: {outlier_columns}")
                    quality_report["recommendations"].append("Review and potentially remove outliers")
                
                # Check text data quality
                text_cols = self.data.select_dtypes(include=['object', 'string']).columns
                text_issues = []
                
                for col in text_cols:
                    # Check for excessive whitespace
                    if self.data[col].astype(str).str.contains(r'\s{2,}').any():
                        text_issues.append(f"{col}: excessive whitespace")
                    
                    # Check for special characters
                    if self.data[col].astype(str).str.contains(r'[^\w\s]').sum() > len(self.data) * 0.3:
                        text_issues.append(f"{col}: many special characters")
                
                if text_issues:
                    quality_report["issues"].extend(text_issues)
                    quality_report["recommendations"].append("Clean text data (remove extra spaces, special characters)")
                
                # Calculate overall quality score
                score = 100.0
                score -= min(missing_percentage, 30)  # Penalize up to 30 points for missing data
                score -= min(duplicate_percentage, 20)  # Penalize up to 20 points for duplicates
                score -= len(inconsistent_types) * 5  # 5 points per inconsistent column
                score -= len(outlier_columns) * 3  # 3 points per outlier column
                score -= len(text_issues) * 2  # 2 points per text issue
                
                quality_report["overall_score"] = max(score, 0.0)
                
                # Add general recommendations based on score
                if quality_report["overall_score"] < 60:
                    quality_report["recommendations"].insert(0, "Data quality is poor - significant cleaning required")
                elif quality_report["overall_score"] < 80:
                    quality_report["recommendations"].insert(0, "Data quality is moderate - some cleaning recommended")
                else:
                    quality_report["recommendations"].insert(0, "Data quality is good - minimal cleaning needed")
                
                self.logger.info(f"Data quality score: {quality_report['overall_score']:.1f}/100")
                return quality_report
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "validate_data_quality")
                self.logger.error(f"Failed to validate data quality: {error_response.message}")
                return {"error": error_response.message}
    
    def standardize_formats(self, format_rules: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """Standardize data formats based on rules."""
        with log_performance("Standardizing formats"):
            try:
                records_affected = 0
                
                for column, rules in format_rules.items():
                    if column not in self.data.columns:
                        self.logger.warning(f"Column {column} not found, skipping")
                        continue
                    
                    original_values = self.data[column].copy()
                    
                    # Phone number standardization
                    if rules.get('type') == 'phone':
                        pattern = rules.get('pattern', r'(\d{3})(\d{3})(\d{4})')
                        format_str = rules.get('format', r'(\1) \2-\3')
                        self.data[column] = self.data[column].astype(str).str.replace(r'\D', '', regex=True)
                        self.data[column] = self.data[column].str.replace(pattern, format_str, regex=True)
                    
                    # Date standardization
                    elif rules.get('type') == 'date':
                        target_format = rules.get('format', '%Y-%m-%d')
                        self.data[column] = pd.to_datetime(self.data[column], errors='coerce')
                        self.data[column] = self.data[column].dt.strftime(target_format)
                    
                    # Currency standardization
                    elif rules.get('type') == 'currency':
                        # Remove currency symbols and convert to float
                        self.data[column] = self.data[column].astype(str).str.replace(r'[$,€£¥]', '', regex=True)
                        self.data[column] = pd.to_numeric(self.data[column], errors='coerce')
                        
                        # Apply formatting if specified
                        if rules.get('format'):
                            currency_symbol = rules.get('symbol', '$')
                            self.data[column] = self.data[column].apply(
                                lambda x: f"{currency_symbol}{x:,.2f}" if pd.notna(x) else x
                            )
                    
                    # Email standardization
                    elif rules.get('type') == 'email':
                        self.data[column] = self.data[column].astype(str).str.lower().str.strip()
                        # Validate email format
                        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                        invalid_emails = ~self.data[column].str.match(email_pattern, na=False)
                        if invalid_emails.any():
                            self.logger.warning(f"Found {invalid_emails.sum()} invalid email addresses in {column}")
                    
                    # Custom regex pattern
                    elif rules.get('type') == 'regex':
                        pattern = rules.get('pattern')
                        replacement = rules.get('replacement', '')
                        if pattern:
                            self.data[column] = self.data[column].astype(str).str.replace(pattern, replacement, regex=True)
                    
                    # Count changes
                    changed_mask = original_values.astype(str) != self.data[column].astype(str)
                    records_affected += changed_mask.sum()
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="standardize_formats",
                    parameters={"format_rules": format_rules},
                    target_columns=list(format_rules.keys()),
                    description=f"Standardized formats for {len(format_rules)} columns",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Standardized formats, {records_affected} values changed")
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "standardize_formats")
                self.logger.error(f"Failed to standardize formats: {error_response.message}")
                raise
    
    def detect_and_fix_encoding_issues(self, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Detect and fix common encoding issues in text data."""
        with log_performance("Fixing encoding issues"):
            try:
                if columns is None:
                    columns = self.data.select_dtypes(include=['object', 'string']).columns.tolist()
                
                records_affected = 0
                encoding_fixes = []
                
                for col in columns:
                    if col not in self.data.columns:
                        continue
                    
                    original_values = self.data[col].copy()
                    
                    # Common encoding fixes
                    encoding_map = {
                        'â€™': "'",  # Smart apostrophe
                        'â€œ': '"',  # Smart quote left
                        'â€': '"',   # Smart quote right
                        'â€"': '–',  # En dash
                        'â€"': '—',  # Em dash
                        'Ã¡': 'á',   # á with encoding issue
                        'Ã©': 'é',   # é with encoding issue
                        'Ã­': 'í',   # í with encoding issue
                        'Ã³': 'ó',   # ó with encoding issue
                        'Ãº': 'ú',   # ú with encoding issue
                        'Ã±': 'ñ',   # ñ with encoding issue
                    }
                    
                    for bad_char, good_char in encoding_map.items():
                        if self.data[col].astype(str).str.contains(bad_char, na=False).any():
                            self.data[col] = self.data[col].astype(str).str.replace(bad_char, good_char)
                            encoding_fixes.append(f"{col}: {bad_char} -> {good_char}")
                    
                    # Remove non-printable characters
                    self.data[col] = self.data[col].astype(str).str.replace(r'[^\x20-\x7E]', '', regex=True)
                    
                    # Count changes
                    changed_mask = original_values.astype(str) != self.data[col].astype(str)
                    records_affected += changed_mask.sum()
                
                # Record operation
                operation = CleaningOperation(
                    operation_type="fix_encoding_issues",
                    parameters={"columns": columns, "fixes_applied": encoding_fixes},
                    target_columns=columns,
                    description=f"Fixed encoding issues in {len(columns)} columns",
                    applied=True,
                    records_affected=records_affected
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                if encoding_fixes:
                    self.logger.info(f"Fixed encoding issues: {encoding_fixes}")
                else:
                    self.logger.info("No encoding issues detected")
                
                return self.data
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "fix_encoding_issues")
                self.logger.error(f"Failed to fix encoding issues: {error_response.message}")
                raise
    
    def auto_clean_dataset(self, aggressive: bool = False) -> Dict[str, Any]:
        """Automatically clean the dataset based on detected issues."""
        with log_performance("Auto-cleaning dataset"):
            try:
                cleaning_report = {
                    "operations_performed": [],
                    "records_before": len(self.data),
                    "records_after": 0,
                    "columns_before": len(self.data.columns),
                    "columns_after": 0,
                    "issues_fixed": []
                }
                
                # Step 1: Fix encoding issues
                self.detect_and_fix_encoding_issues()
                cleaning_report["operations_performed"].append("Fixed encoding issues")
                
                # Step 2: Remove completely empty rows and columns
                initial_shape = self.data.shape
                
                # Remove empty columns
                empty_cols = self.data.columns[self.data.isnull().all()].tolist()
                if empty_cols:
                    self.data = self.data.drop(columns=empty_cols)
                    cleaning_report["issues_fixed"].append(f"Removed {len(empty_cols)} empty columns")
                
                # Remove empty rows
                empty_rows = self.data.isnull().all(axis=1).sum()
                if empty_rows > 0:
                    self.data = self.data.dropna(how='all')
                    cleaning_report["issues_fixed"].append(f"Removed {empty_rows} empty rows")
                
                # Step 3: Handle duplicates
                duplicate_count = self.data.duplicated().sum()
                if duplicate_count > 0:
                    self.remove_duplicates(strategy='first')
                    cleaning_report["operations_performed"].append("Removed duplicates")
                    cleaning_report["issues_fixed"].append(f"Removed {duplicate_count} duplicate records")
                
                # Step 4: Clean text data
                text_cols = self.data.select_dtypes(include=['object', 'string']).columns.tolist()
                if text_cols:
                    text_operations = ["remove_extra_spaces", "normalize_whitespace"]
                    if aggressive:
                        text_operations.extend(["remove_special_chars", "lowercase"])
                    
                    self.clean_text(text_cols, text_operations)
                    cleaning_report["operations_performed"].append("Cleaned text data")
                
                # Step 5: Handle missing values (conservative approach)
                missing_threshold = 0.5 if aggressive else 0.8  # Remove columns with >50% or >80% missing
                
                for col in self.data.columns:
                    missing_pct = self.data[col].isnull().sum() / len(self.data)
                    if missing_pct > missing_threshold:
                        self.data = self.data.drop(columns=[col])
                        cleaning_report["issues_fixed"].append(f"Removed column '{col}' ({missing_pct:.1%} missing)")
                
                # For remaining missing values, use appropriate strategies
                numeric_cols = self.data.select_dtypes(include=[np.number]).columns.tolist()
                if numeric_cols:
                    missing_numeric = [col for col in numeric_cols if self.data[col].isnull().any()]
                    if missing_numeric:
                        self.handle_missing_values("fill_median", missing_numeric)
                        cleaning_report["operations_performed"].append("Filled missing numeric values with median")
                
                text_cols = self.data.select_dtypes(include=['object', 'string']).columns.tolist()
                if text_cols:
                    missing_text = [col for col in text_cols if self.data[col].isnull().any()]
                    if missing_text:
                        self.handle_missing_values("fill_mode", missing_text)
                        cleaning_report["operations_performed"].append("Filled missing text values with mode")
                
                # Step 6: Remove outliers (only if aggressive)
                if aggressive:
                    numeric_cols = self.data.select_dtypes(include=[np.number]).columns.tolist()
                    if numeric_cols:
                        self.remove_outliers(numeric_cols, method="iqr", threshold=2.0)
                        cleaning_report["operations_performed"].append("Removed outliers")
                
                # Step 7: Optimize data types
                type_mapping = {}
                for col in self.data.columns:
                    if self.data[col].dtype == 'object':
                        # Try to convert to numeric
                        numeric_series = pd.to_numeric(self.data[col], errors='coerce')
                        if numeric_series.notna().sum() > len(self.data) * 0.8:  # 80% convertible
                            # Check if integers
                            if numeric_series.dropna().apply(lambda x: x.is_integer()).all():
                                type_mapping[col] = "integer"
                            else:
                                type_mapping[col] = "float"
                
                if type_mapping:
                    self.convert_data_types(type_mapping)
                    cleaning_report["operations_performed"].append("Optimized data types")
                
                # Final statistics
                cleaning_report["records_after"] = len(self.data)
                cleaning_report["columns_after"] = len(self.data.columns)
                cleaning_report["records_removed"] = cleaning_report["records_before"] - cleaning_report["records_after"]
                cleaning_report["columns_removed"] = cleaning_report["columns_before"] - cleaning_report["columns_after"]
                
                # Record the auto-clean operation
                operation = CleaningOperation(
                    operation_type="auto_clean_dataset",
                    parameters={"aggressive": aggressive},
                    target_columns=list(self.data.columns),
                    description=f"Auto-cleaned dataset ({'aggressive' if aggressive else 'conservative'} mode)",
                    applied=True,
                    records_affected=cleaning_report["records_removed"]
                )
                
                self.cleaning_history.add_operation(operation, self.data)
                
                self.logger.info(f"Auto-cleaning completed: {len(cleaning_report['operations_performed'])} operations")
                return cleaning_report
                
            except Exception as e:
                error_response = self.error_handler.handle_data_error(e, "auto_clean_dataset")
                self.logger.error(f"Failed to auto-clean dataset: {error_response.message}")
                return {"error": error_response.message}