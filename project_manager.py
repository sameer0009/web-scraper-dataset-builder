"""
Project Manager for Web Scraper & Dataset Builder

This module handles saving and loading of scraping configurations,
cleaning rules, and project settings with comprehensive project management.
"""

import json
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
import zipfile
import tempfile

from models import (
    Project, ScrapingConfig, CleaningOperation, ExportOptions,
    create_project, validate_file_path
)
from config import AppConfig
from utils.logger import get_logger, log_performance
from utils.error_handler import ErrorHandler


class ProjectManager:
    """Comprehensive project management with versioning and backup support."""
    
    def __init__(self, config: AppConfig, error_handler: ErrorHandler):
        """Initialize the project manager."""
        self.config = config
        self.error_handler = error_handler
        self.logger = get_logger(__name__)
        
        # Ensure projects directory exists
        self.projects_dir = config.projects_dir
        self.projects_dir.mkdir(parents=True, exist_ok=True)
        
        # Project cache for quick access
        self.project_cache = {}
        self.recent_projects = self._load_recent_projects()
    
    def create_new_project(
        self, 
        name: str, 
        url: str, 
        description: str = "",
        tags: List[str] = None
    ) -> Project:
        """Create a new project with default settings."""
        try:
            # Validate project name
            if not name or not name.strip():
                raise ValueError("Project name cannot be empty")
            
            # Check if project already exists
            if self.project_exists(name):
                raise ValueError(f"Project '{name}' already exists")
            
            # Create project
            project = create_project(
                name=name.strip(),
                url=url,
                description=description,
                tags=tags or []
            )
            
            self.logger.info(f"Created new project: {name}")
            return project
            
        except Exception as e:
            error_response = self.error_handler.handle_validation_error(e, "project_name", name)
            self.logger.error(f"Failed to create project: {error_response.message}")
            raise
    
    def save_project(self, project: Project, filepath: Optional[str] = None) -> bool:
        """Save a project to file with backup and versioning."""
        with log_performance(f"Saving project: {project.name}"):
            try:
                # Determine filepath
                if filepath is None:
                    filepath = self._get_project_filepath(project.name)
                else:
                    filepath = Path(filepath)
                
                # Ensure directory exists
                filepath.parent.mkdir(parents=True, exist_ok=True)
                
                # Create backup if file exists
                if filepath.exists():
                    self._create_backup(filepath)
                
                # Update last modified time
                project.last_modified = datetime.now()
                
                # Save project
                project.save_to_file(filepath)
                
                # Update cache and recent projects
                self.project_cache[project.name] = project
                self._add_to_recent_projects(str(filepath))
                
                # Save project metadata
                self._save_project_metadata(project, filepath)
                
                self.logger.info(f"Successfully saved project: {project.name}")
                return True
                
            except Exception as e:
                error_response = self.error_handler.handle_file_error(e, str(filepath), "save project")
                self.logger.error(f"Failed to save project: {error_response.message}")
                return False
    
    def load_project(self, filepath: Union[str, Path]) -> Optional[Project]:
        """Load a project from file with validation."""
        with log_performance(f"Loading project from: {filepath}"):
            try:
                filepath = Path(filepath)
                
                if not filepath.exists():
                    raise FileNotFoundError(f"Project file not found: {filepath}")
                
                # Load project
                project = Project.load_from_file(filepath)
                
                # Validate project
                self._validate_project(project)
                
                # Update cache and recent projects
                self.project_cache[project.name] = project
                self._add_to_recent_projects(str(filepath))
                
                self.logger.info(f"Successfully loaded project: {project.name}")
                return project
                
            except Exception as e:
                error_response = self.error_handler.handle_file_error(e, str(filepath), "load project")
                self.logger.error(f"Failed to load project: {error_response.message}")
                return None
    
    def delete_project(self, name: str) -> bool:
        """Delete a project and its associated files."""
        try:
            filepath = self._get_project_filepath(name)
            
            if not filepath.exists():
                raise FileNotFoundError(f"Project '{name}' not found")
            
            # Create final backup before deletion
            backup_path = self._create_backup(filepath, suffix="_deleted")
            
            # Delete project file
            filepath.unlink()
            
            # Delete metadata file
            metadata_path = filepath.with_suffix('.meta.json')
            if metadata_path.exists():
                metadata_path.unlink()
            
            # Remove from cache and recent projects
            if name in self.project_cache:
                del self.project_cache[name]
            
            self._remove_from_recent_projects(str(filepath))
            
            self.logger.info(f"Deleted project: {name} (backup saved to {backup_path})")
            return True
            
        except Exception as e:
            error_response = self.error_handler.handle_file_error(e, name, "delete project")
            self.logger.error(f"Failed to delete project: {error_response.message}")
            return False
    
    def duplicate_project(self, source_name: str, new_name: str) -> Optional[Project]:
        """Create a duplicate of an existing project."""
        try:
            # Load source project
            source_project = self.get_project(source_name)
            if not source_project:
                raise ValueError(f"Source project '{source_name}' not found")
            
            # Check if new name already exists
            if self.project_exists(new_name):
                raise ValueError(f"Project '{new_name}' already exists")
            
            # Create duplicate
            duplicate_project = Project(
                name=new_name,
                scraping_config=source_project.scraping_config,
                cleaning_operations=source_project.cleaning_operations.copy(),
                export_settings=source_project.export_settings,
                created_date=datetime.now(),
                last_modified=datetime.now(),
                description=f"Copy of {source_project.description}",
                tags=source_project.tags.copy(),
                version="1.0"
            )
            
            # Save duplicate
            if self.save_project(duplicate_project):
                self.logger.info(f"Duplicated project: {source_name} -> {new_name}")
                return duplicate_project
            else:
                return None
                
        except Exception as e:
            error_response = self.error_handler.handle_validation_error(e, "project_name", new_name)
            self.logger.error(f"Failed to duplicate project: {error_response.message}")
            return None
    
    def get_project(self, name: str) -> Optional[Project]:
        """Get a project by name (from cache or file)."""
        try:
            # Check cache first
            if name in self.project_cache:
                return self.project_cache[name]
            
            # Try to load from file
            filepath = self._get_project_filepath(name)
            if filepath.exists():
                return self.load_project(filepath)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get project '{name}': {e}")
            return None
    
    def list_projects(self) -> List[Dict[str, Any]]:
        """List all available projects with metadata."""
        projects = []
        
        try:
            for project_file in self.projects_dir.glob("*.json"):
                if project_file.name.endswith('.meta.json'):
                    continue  # Skip metadata files
                
                try:
                    # Try to load project metadata first
                    metadata_file = project_file.with_suffix('.meta.json')
                    if metadata_file.exists():
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                        projects.append(metadata)
                    else:
                        # Load project to get basic info
                        project = Project.load_from_file(project_file)
                        projects.append({
                            'name': project.name,
                            'description': project.description,
                            'created_date': project.created_date.isoformat(),
                            'last_modified': project.last_modified.isoformat(),
                            'tags': project.tags,
                            'url': project.scraping_config.url,
                            'filepath': str(project_file),
                            'file_size': project_file.stat().st_size
                        })
                        
                except Exception as e:
                    self.logger.warning(f"Failed to load project metadata for {project_file}: {e}")
                    continue
            
            # Sort by last modified date (newest first)
            projects.sort(key=lambda x: x['last_modified'], reverse=True)
            
        except Exception as e:
            self.logger.error(f"Failed to list projects: {e}")
        
        return projects
    
    def search_projects(self, query: str, search_fields: List[str] = None) -> List[Dict[str, Any]]:
        """Search projects by name, description, tags, or URL."""
        if search_fields is None:
            search_fields = ['name', 'description', 'tags', 'url']
        
        query = query.lower().strip()
        if not query:
            return self.list_projects()
        
        all_projects = self.list_projects()
        matching_projects = []
        
        for project in all_projects:
            match_found = False
            
            for field in search_fields:
                if field in project:
                    field_value = project[field]
                    
                    if isinstance(field_value, str):
                        if query in field_value.lower():
                            match_found = True
                            break
                    elif isinstance(field_value, list):
                        if any(query in str(item).lower() for item in field_value):
                            match_found = True
                            break
            
            if match_found:
                matching_projects.append(project)
        
        return matching_projects
    
    def get_recent_projects(self) -> List[str]:
        """Get list of recent project file paths."""
        return self.recent_projects.copy()
    
    def project_exists(self, name: str) -> bool:
        """Check if a project exists."""
        filepath = self._get_project_filepath(name)
        return filepath.exists()
    
    def export_project(self, name: str, export_path: str, include_data: bool = False) -> bool:
        """Export a project as a zip file with all associated files."""
        try:
            project = self.get_project(name)
            if not project:
                raise ValueError(f"Project '{name}' not found")
            
            export_path = Path(export_path)
            export_path.parent.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(export_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add project file
                project_file = self._get_project_filepath(name)
                zipf.write(project_file, f"{name}.json")
                
                # Add metadata file if exists
                metadata_file = project_file.with_suffix('.meta.json')
                if metadata_file.exists():
                    zipf.write(metadata_file, f"{name}.meta.json")
                
                # Add export info
                export_info = {
                    'exported_by': 'Web Scraper & Dataset Builder',
                    'export_date': datetime.now().isoformat(),
                    'project_name': name,
                    'project_version': project.version,
                    'includes_data': include_data
                }
                
                zipf.writestr('export_info.json', json.dumps(export_info, indent=2))
            
            self.logger.info(f"Exported project '{name}' to {export_path}")
            return True
            
        except Exception as e:
            error_response = self.error_handler.handle_file_error(e, export_path, "export project")
            self.logger.error(f"Failed to export project: {error_response.message}")
            return False
    
    def import_project(self, import_path: str, overwrite: bool = False) -> Optional[Project]:
        """Import a project from a zip file."""
        try:
            import_path = Path(import_path)
            
            if not import_path.exists():
                raise FileNotFoundError(f"Import file not found: {import_path}")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Extract zip file
                with zipfile.ZipFile(import_path, 'r') as zipf:
                    zipf.extractall(temp_path)
                
                # Find project file
                project_files = list(temp_path.glob("*.json"))
                project_files = [f for f in project_files if not f.name.endswith('.meta.json') and f.name != 'export_info.json']
                
                if not project_files:
                    raise ValueError("No project file found in import archive")
                
                project_file = project_files[0]
                
                # Load project
                project = Project.load_from_file(project_file)
                
                # Check if project already exists
                if self.project_exists(project.name) and not overwrite:
                    raise ValueError(f"Project '{project.name}' already exists. Use overwrite=True to replace.")
                
                # Save imported project
                if self.save_project(project):
                    self.logger.info(f"Imported project: {project.name}")
                    return project
                else:
                    return None
                    
        except Exception as e:
            error_response = self.error_handler.handle_file_error(e, import_path, "import project")
            self.logger.error(f"Failed to import project: {error_response.message}")
            return None
    
    def get_project_statistics(self) -> Dict[str, Any]:
        """Get statistics about all projects."""
        try:
            projects = self.list_projects()
            
            if not projects:
                return {"message": "No projects found"}
            
            # Calculate statistics
            total_projects = len(projects)
            
            # Tag analysis
            all_tags = []
            for project in projects:
                all_tags.extend(project.get('tags', []))
            
            tag_counts = {}
            for tag in all_tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
            # Date analysis
            creation_dates = [datetime.fromisoformat(p['created_date']) for p in projects]
            oldest_project = min(creation_dates) if creation_dates else None
            newest_project = max(creation_dates) if creation_dates else None
            
            # Size analysis
            total_size = sum(p.get('file_size', 0) for p in projects)
            
            return {
                'total_projects': total_projects,
                'oldest_project_date': oldest_project.isoformat() if oldest_project else None,
                'newest_project_date': newest_project.isoformat() if newest_project else None,
                'total_size_mb': round(total_size / 1024 / 1024, 2),
                'average_size_kb': round(total_size / total_projects / 1024, 2) if total_projects > 0 else 0,
                'most_common_tags': sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10],
                'projects_by_month': self._group_projects_by_month(projects),
                'recent_activity': projects[:5]  # 5 most recently modified
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get project statistics: {e}")
            return {"error": str(e)}
    
    def cleanup_old_backups(self, days_to_keep: int = 30) -> int:
        """Clean up old backup files."""
        try:
            backup_dir = self.projects_dir / 'backups'
            if not backup_dir.exists():
                return 0
            
            cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
            deleted_count = 0
            
            for backup_file in backup_dir.glob("*.backup.*"):
                if backup_file.stat().st_mtime < cutoff_date:
                    backup_file.unlink()
                    deleted_count += 1
            
            self.logger.info(f"Cleaned up {deleted_count} old backup files")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old backups: {e}")
            return 0
    
    def _get_project_filepath(self, name: str) -> Path:
        """Get the file path for a project."""
        # Sanitize project name for filename
        safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_name = safe_name.replace(' ', '_')
        return self.projects_dir / f"{safe_name}.json"
    
    def _create_backup(self, filepath: Path, suffix: str = "") -> Path:
        """Create a backup of a project file."""
        backup_dir = self.projects_dir / 'backups'
        backup_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{filepath.stem}.backup.{timestamp}{suffix}.json"
        backup_path = backup_dir / backup_name
        
        shutil.copy2(filepath, backup_path)
        return backup_path
    
    def _save_project_metadata(self, project: Project, filepath: Path):
        """Save project metadata for quick access."""
        try:
            metadata = {
                'name': project.name,
                'description': project.description,
                'created_date': project.created_date.isoformat(),
                'last_modified': project.last_modified.isoformat(),
                'tags': project.tags,
                'url': project.scraping_config.url,
                'filepath': str(filepath),
                'file_size': filepath.stat().st_size if filepath.exists() else 0,
                'version': project.version
            }
            
            metadata_path = filepath.with_suffix('.meta.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.warning(f"Failed to save project metadata: {e}")
    
    def _validate_project(self, project: Project):
        """Validate a loaded project."""
        if not project.name or not project.name.strip():
            raise ValueError("Project name cannot be empty")
        
        if not project.scraping_config or not project.scraping_config.url:
            raise ValueError("Project must have a valid scraping configuration")
        
        # Validate scraping config
        project.scraping_config.validate()
    
    def _load_recent_projects(self) -> List[str]:
        """Load recent projects list from config."""
        return self.config.recent_projects.copy()
    
    def _add_to_recent_projects(self, filepath: str):
        """Add a project to the recent projects list."""
        self.config.add_recent_project(filepath)
        self.recent_projects = self.config.recent_projects.copy()
    
    def _remove_from_recent_projects(self, filepath: str):
        """Remove a project from the recent projects list."""
        if filepath in self.config.recent_projects:
            self.config.recent_projects.remove(filepath)
            self.config.save()
            self.recent_projects = self.config.recent_projects.copy()
    
    def _group_projects_by_month(self, projects: List[Dict[str, Any]]) -> Dict[str, int]:
        """Group projects by creation month for statistics."""
        monthly_counts = {}
        
        for project in projects:
            try:
                date = datetime.fromisoformat(project['created_date'])
                month_key = date.strftime("%Y-%m")
                monthly_counts[month_key] = monthly_counts.get(month_key, 0) + 1
            except Exception:
                continue
        
        return monthly_counts