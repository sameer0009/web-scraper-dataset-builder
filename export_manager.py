"""
Export Manager for Web Scraper & Dataset Builder

This module handles data export in multiple formats with comprehensive
configuration options, validation, and error handling.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
import xlsxwriter

from models import (
    ExportOptions, ExportFormat, ExporterInterface,
    validate_dataframe, validate_file_path
)
from utils.logger import get_logger, log_performance
from utils.error_handler import ErrorHandler


class ExcelExporter:
    """Specialized Excel exporter with advanced formatting options."""
    
    def __init__(self, error_handler: ErrorHandler):
        """Initialize Excel exporter."""
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
    
    def export(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to Excel with formatting."""
        with log_performance(f"Exporting to Excel: {filepath}"):
            try:
                # Ensure directory exists
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                
                # Use xlsxwriter for better formatting control
                with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                    # Write main data
                    data.to_excel(
                        writer,
                        sheet_name=options.sheet_name,
                        index=options.include_index
                    )
                    
                    # Get workbook and worksheet objects
                    workbook = writer.book
                    worksheet = writer.sheets[options.sheet_name]
                    
                    # Apply formatting
                    self._apply_excel_formatting(workbook, worksheet, data, options)
                    
                    # Add metadata sheet
                    self._add_metadata_sheet(writer, data, options)
                
                self.logger.info(f"Successfully exported {len(data)} records to Excel: {filepath}")
                return True
                
            except Exception as e:
                error_response = self.error_handler.handle_file_error(e, filepath, "Excel export")
                self.logger.error(f"Failed to export to Excel: {error_response.message}")
                return False
    
    def _apply_excel_formatting(self, workbook, worksheet, data: pd.DataFrame, options: ExportOptions):
        """Apply professional formatting to Excel worksheet."""
        try:
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            cell_format = workbook.add_format({
                'text_wrap': True,
                'valign': 'top',
                'border': 1
            })
            
            number_format = workbook.add_format({
                'num_format': '#,##0.00',
                'border': 1
            })
            
            date_format = workbook.add_format({
                'num_format': 'yyyy-mm-dd',
                'border': 1
            })
            
            # Set column widths and apply formats
            for idx, column in enumerate(data.columns):
                col_letter = chr(65 + idx)  # A, B, C, etc.
                
                # Calculate optimal column width
                max_length = max(
                    len(str(column)),
                    data[column].astype(str).str.len().max() if not data.empty else 0
                )
                optimal_width = min(max_length + 2, 50)  # Cap at 50 characters
                
                worksheet.set_column(f'{col_letter}:{col_letter}', optimal_width)
                
                # Apply format based on data type
                if data[column].dtype in ['int64', 'float64']:
                    worksheet.set_column(f'{col_letter}:{col_letter}', optimal_width, number_format)
                elif data[column].dtype == 'datetime64[ns]':
                    worksheet.set_column(f'{col_letter}:{col_letter}', optimal_width, date_format)
                else:
                    worksheet.set_column(f'{col_letter}:{col_letter}', optimal_width, cell_format)
            
            # Format header row
            for col_num, value in enumerate(data.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Freeze header row
            worksheet.freeze_panes(1, 0)
            
        except Exception as e:
            self.logger.warning(f"Failed to apply Excel formatting: {e}")
    
    def _add_metadata_sheet(self, writer, data: pd.DataFrame, options: ExportOptions):
        """Add metadata sheet with export information."""
        try:
            metadata = {
                'Export Information': [
                    f'Export Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                    f'Total Records: {len(data)}',
                    f'Total Columns: {len(data.columns)}',
                    f'File Format: Excel (.xlsx)',
                    f'Encoding: {options.encoding}',
                    f'Include Index: {options.include_index}'
                ],
                'Column Information': [
                    f'{col}: {dtype}' for col, dtype in data.dtypes.items()
                ],
                'Data Summary': []
            }
            
            # Add data summary
            if not data.empty:
                numeric_cols = data.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    summary = data[numeric_cols].describe()
                    metadata['Data Summary'] = [
                        f'{col} - Mean: {summary.loc["mean", col]:.2f}, Std: {summary.loc["std", col]:.2f}'
                        for col in numeric_cols
                    ]
            
            # Create metadata DataFrame
            max_len = max(len(v) for v in metadata.values())
            for key, values in metadata.items():
                values.extend([''] * (max_len - len(values)))
            
            metadata_df = pd.DataFrame(metadata)
            metadata_df.to_excel(writer, sheet_name='Export_Metadata', index=False)
            
        except Exception as e:
            self.logger.warning(f"Failed to add metadata sheet: {e}")


class CSVExporter:
    """Specialized CSV exporter with encoding and delimiter options."""
    
    def __init__(self, error_handler: ErrorHandler):
        """Initialize CSV exporter."""
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
    
    def export(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to CSV format."""
        with log_performance(f"Exporting to CSV: {filepath}"):
            try:
                # Ensure directory exists
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                
                # Export to CSV
                data.to_csv(
                    filepath,
                    index=options.include_index,
                    encoding=options.encoding,
                    sep=options.delimiter,
                    na_rep='',
                    float_format='%.2f',
                    date_format='%Y-%m-%d %H:%M:%S'
                )
                
                self.logger.info(f"Successfully exported {len(data)} records to CSV: {filepath}")
                return True
                
            except Exception as e:
                error_response = self.error_handler.handle_file_error(e, filepath, "CSV export")
                self.logger.error(f"Failed to export to CSV: {error_response.message}")
                return False


class JSONExporter:
    """Specialized JSON exporter with structure options."""
    
    def __init__(self, error_handler: ErrorHandler):
        """Initialize JSON exporter."""
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
    
    def export(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to JSON format."""
        with log_performance(f"Exporting to JSON: {filepath}"):
            try:
                # Ensure directory exists
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                
                # Convert DataFrame to JSON
                if options.json_orient == 'records':
                    json_data = data.to_dict('records')
                elif options.json_orient == 'index':
                    json_data = data.to_dict('index')
                elif options.json_orient == 'values':
                    json_data = data.values.tolist()
                elif options.json_orient == 'split':
                    json_data = data.to_dict('split')
                elif options.json_orient == 'table':
                    json_data = data.to_dict('table')
                else:
                    json_data = data.to_dict('records')  # Default
                
                # Add metadata
                export_data = {
                    'metadata': {
                        'export_date': datetime.now().isoformat(),
                        'total_records': len(data),
                        'total_columns': len(data.columns),
                        'columns': list(data.columns),
                        'data_types': {col: str(dtype) for col, dtype in data.dtypes.items()},
                        'orientation': options.json_orient
                    },
                    'data': json_data
                }
                
                # Write to file
                with open(filepath, 'w', encoding=options.encoding) as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
                
                self.logger.info(f"Successfully exported {len(data)} records to JSON: {filepath}")
                return True
                
            except Exception as e:
                error_response = self.error_handler.handle_file_error(e, filepath, "JSON export")
                self.logger.error(f"Failed to export to JSON: {error_response.message}")
                return False


class ParquetExporter:
    """Specialized Parquet exporter for efficient data storage."""
    
    def __init__(self, error_handler: ErrorHandler):
        """Initialize Parquet exporter."""
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
    
    def export(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to Parquet format."""
        with log_performance(f"Exporting to Parquet: {filepath}"):
            try:
                # Ensure directory exists
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                
                # Export to Parquet
                data.to_parquet(
                    filepath,
                    index=options.include_index,
                    compression=options.compression or 'snappy',
                    engine='pyarrow'
                )
                
                self.logger.info(f"Successfully exported {len(data)} records to Parquet: {filepath}")
                return True
                
            except Exception as e:
                error_response = self.error_handler.handle_file_error(e, filepath, "Parquet export")
                self.logger.error(f"Failed to export to Parquet: {error_response.message}")
                return False


class ExportManager:
    """Comprehensive export manager supporting multiple formats."""
    
    def __init__(self, error_handler: ErrorHandler):
        """Initialize the export manager."""
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        
        # Initialize format-specific exporters
        self.excel_exporter = ExcelExporter(error_handler)
        self.csv_exporter = CSVExporter(error_handler)
        self.json_exporter = JSONExporter(error_handler)
        self.parquet_exporter = ParquetExporter(error_handler)
        
        # Export statistics
        self.export_history = []
    
    def export_data(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to the specified format and location."""
        start_time = datetime.now()
        
        try:
            # Validate inputs
            if not validate_dataframe(data):
                raise ValueError("Invalid DataFrame provided")
            
            if data.empty:
                raise ValueError("Cannot export empty DataFrame")
            
            if not validate_file_path(filepath):
                raise ValueError(f"Invalid file path: {filepath}")
            
            # Validate export options
            options.validate()
            
            # Determine format from file extension if not specified
            file_path = Path(filepath)
            if options.format == ExportFormat.EXCEL or file_path.suffix.lower() in ['.xlsx', '.xls']:
                success = self.excel_exporter.export(data, filepath, options)
                format_used = ExportFormat.EXCEL
                
            elif options.format == ExportFormat.CSV or file_path.suffix.lower() == '.csv':
                success = self.csv_exporter.export(data, filepath, options)
                format_used = ExportFormat.CSV
                
            elif options.format == ExportFormat.JSON or file_path.suffix.lower() == '.json':
                success = self.json_exporter.export(data, filepath, options)
                format_used = ExportFormat.JSON
                
            elif options.format == ExportFormat.PARQUET or file_path.suffix.lower() == '.parquet':
                success = self.parquet_exporter.export(data, filepath, options)
                format_used = ExportFormat.PARQUET
                
            else:
                raise ValueError(f"Unsupported export format: {options.format}")
            
            # Record export statistics
            execution_time = (datetime.now() - start_time).total_seconds()
            export_record = {
                'timestamp': start_time,
                'filepath': filepath,
                'format': format_used.value,
                'records_exported': len(data),
                'columns_exported': len(data.columns),
                'file_size_mb': self._get_file_size(filepath) if success else 0,
                'execution_time': execution_time,
                'success': success
            }
            
            self.export_history.append(export_record)
            
            # Keep only last 100 export records
            if len(self.export_history) > 100:
                self.export_history = self.export_history[-100:]
            
            if success:
                self.logger.info(f"Export completed successfully in {execution_time:.2f}s")
            
            return success
            
        except Exception as e:
            error_response = self.error_handler.handle_file_error(e, filepath, "data export")
            self.logger.error(f"Export failed: {error_response.message}")
            return False
    
    def export_to_excel(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to Excel format."""
        options.format = ExportFormat.EXCEL
        return self.export_data(data, filepath, options)
    
    def export_to_csv(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to CSV format."""
        options.format = ExportFormat.CSV
        return self.export_data(data, filepath, options)
    
    def export_to_json(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to JSON format."""
        options.format = ExportFormat.JSON
        return self.export_data(data, filepath, options)
    
    def export_to_parquet(self, data: pd.DataFrame, filepath: str, options: ExportOptions) -> bool:
        """Export data to Parquet format."""
        options.format = ExportFormat.PARQUET
        return self.export_data(data, filepath, options)
    
    def validate_export_path(self, filepath: str) -> bool:
        """Validate the export file path."""
        return validate_file_path(filepath)
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported export formats."""
        return [format.value for format in ExportFormat]
    
    def get_export_statistics(self) -> Dict[str, Any]:
        """Get export statistics and history."""
        if not self.export_history:
            return {"message": "No exports performed yet"}
        
        successful_exports = [record for record in self.export_history if record['success']]
        failed_exports = [record for record in self.export_history if not record['success']]
        
        # Calculate statistics
        total_records_exported = sum(record['records_exported'] for record in successful_exports)
        total_file_size_mb = sum(record['file_size_mb'] for record in successful_exports)
        avg_execution_time = sum(record['execution_time'] for record in successful_exports) / len(successful_exports) if successful_exports else 0
        
        # Format breakdown
        format_counts = {}
        for record in self.export_history:
            format_name = record['format']
            format_counts[format_name] = format_counts.get(format_name, 0) + 1
        
        return {
            'total_exports': len(self.export_history),
            'successful_exports': len(successful_exports),
            'failed_exports': len(failed_exports),
            'success_rate': (len(successful_exports) / len(self.export_history) * 100) if self.export_history else 0,
            'total_records_exported': total_records_exported,
            'total_file_size_mb': round(total_file_size_mb, 2),
            'average_execution_time': round(avg_execution_time, 2),
            'format_breakdown': format_counts,
            'recent_exports': self.export_history[-10:]  # Last 10 exports
        }
    
    def _get_file_size(self, filepath: str) -> float:
        """Get file size in MB."""
        try:
            file_path = Path(filepath)
            if file_path.exists():
                return file_path.stat().st_size / 1024 / 1024
        except Exception:
            pass
        return 0.0
    
    def clear_export_history(self):
        """Clear export history."""
        self.export_history.clear()
        self.logger.info("Export history cleared")
    
    def export_multiple_formats(
        self, 
        data: pd.DataFrame, 
        base_filepath: str, 
        formats: List[ExportFormat],
        options: ExportOptions
    ) -> Dict[str, bool]:
        """Export data to multiple formats simultaneously."""
        results = {}
        base_path = Path(base_filepath)
        
        for format_type in formats:
            try:
                # Generate filepath for each format
                if format_type == ExportFormat.EXCEL:
                    filepath = base_path.with_suffix('.xlsx')
                elif format_type == ExportFormat.CSV:
                    filepath = base_path.with_suffix('.csv')
                elif format_type == ExportFormat.JSON:
                    filepath = base_path.with_suffix('.json')
                elif format_type == ExportFormat.PARQUET:
                    filepath = base_path.with_suffix('.parquet')
                else:
                    continue
                
                # Set format and export
                export_options = ExportOptions(
                    format=format_type,
                    include_index=options.include_index,
                    sheet_name=options.sheet_name,
                    encoding=options.encoding,
                    delimiter=options.delimiter,
                    json_orient=options.json_orient,
                    compression=options.compression
                )
                
                success = self.export_data(data, str(filepath), export_options)
                results[format_type.value] = success
                
            except Exception as e:
                self.logger.error(f"Failed to export to {format_type.value}: {e}")
                results[format_type.value] = False
        
        return results