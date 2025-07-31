"""
Power BI PBIX File Automation
Programmatically update data sources and refresh Power BI files
"""

import os
import json
import zipfile
import tempfile
import shutil
from path_config import get_powerbi_paths
from pathlib import Path
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PowerBIAutomation:
    def __init__(self, pbix_path):
        self.pbix_path = Path(pbix_path)
        self.temp_dir = None
        self.extracted_path = None
        
    def __enter__(self):
        """Context manager entry"""
        self.temp_dir = tempfile.mkdtemp()
        self.extracted_path = Path(self.temp_dir) / "pbix_contents"
        self.extracted_path.mkdir(exist_ok=True)
        
        # Extract PBIX file (it's a ZIP file)
        with zipfile.ZipFile(self.pbix_path, 'r') as zip_ref:
            zip_ref.extractall(str(self.extracted_path))
        
        logger.info(f"Extracted PBIX to: {self.extracted_path}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def update_data_source(self, old_path, new_path):
        """Update data source path in the PBIX file"""
        if self.extracted_path is None:
            raise RuntimeError("PowerBIAutomation must be used within a context manager (with statement)")
        
        try:
            # Find and update DataModelSchema
            schema_path = self.extracted_path / "DataModelSchema"
            if schema_path.exists():
                self._update_schema_data_source(schema_path, old_path, new_path)
            
            # Find and update Connections
            connections_path = self.extracted_path / "Connections"
            if connections_path.exists():
                self._update_connections_data_source(connections_path, old_path, new_path)
            
            # Find and update Settings
            settings_path = self.extracted_path / "Settings"
            if settings_path.exists():
                self._update_settings_data_source(settings_path, old_path, new_path)
            
            logger.info(f"Updated data source from {old_path} to {new_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update data source: {e}")
            return False
    
    def _update_schema_data_source(self, schema_path, old_path, new_path):
        """Update data source in DataModelSchema"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Replace file paths
            content = content.replace(old_path.replace('\\', '/'), new_path.replace('\\', '/'))
            content = content.replace(old_path.replace('/', '\\'), new_path.replace('/', '\\'))
            content = content.replace(old_path, new_path)
            
            with open(schema_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
        except Exception as e:
            logger.warning(f"Could not update DataModelSchema: {e}")
    
    def _update_connections_data_source(self, connections_path, old_path, new_path):
        """Update data source in Connections"""
        try:
            with open(connections_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Replace file paths
            content = content.replace(old_path.replace('\\', '/'), new_path.replace('\\', '/'))
            content = content.replace(old_path.replace('/', '\\'), new_path.replace('/', '\\'))
            content = content.replace(old_path, new_path)
            
            with open(connections_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
        except Exception as e:
            logger.warning(f"Could not update Connections: {e}")
    
    def _update_settings_data_source(self, settings_path, old_path, new_path):
        """Update data source in Settings"""
        try:
            with open(settings_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Replace file paths
            content = content.replace(old_path.replace('\\', '/'), new_path.replace('\\', '/'))
            content = content.replace(old_path.replace('/', '\\'), new_path.replace('/', '\\'))
            content = content.replace(old_path, new_path)
            
            with open(settings_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
        except Exception as e:
            logger.warning(f"Could not update Settings: {e}")
    
    def save_updated_pbix(self, output_path=None):
        """Save the updated PBIX file"""
        if output_path is None:
            output_path = self.pbix_path
        
        if self.extracted_path is None:
            raise RuntimeError("PowerBIAutomation must be used within a context manager (with statement)")
        
        try:
            # Create new ZIP file with updated contents
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                for root, dirs, files in os.walk(self.extracted_path):
                    for file in files:
                        file_path = Path(root) / file
                        arc_name = file_path.relative_to(self.extracted_path)
                        zip_ref.write(file_path, arc_name)
            
            logger.info(f"Saved updated PBIX to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save PBIX: {e}")
            return False
    
    def get_current_data_sources(self):
        """Get list of current data sources in the PBIX file"""
        if self.extracted_path is None:
            raise RuntimeError("PowerBIAutomation must be used within a context manager (with statement)")
        
        data_sources = []
        
        try:
            # Check DataModelSchema
            schema_path = self.extracted_path / "DataModelSchema"
            if schema_path.exists():
                with open(schema_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Extract file paths (simplified pattern matching)
                    import re
                    file_patterns = re.findall(r'[A-Z]:\\[^"]*\.csv', content)
                    data_sources.extend(file_patterns)
            
            # Check Connections
            connections_path = self.extracted_path / "Connections"
            if connections_path.exists():
                with open(connections_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    import re
                    file_patterns = re.findall(r'[A-Z]:\\[^"]*\.csv', content)
                    data_sources.extend(file_patterns)
        
        except Exception as e:
            logger.error(f"Failed to get data sources: {e}")
        
        return list(set(data_sources))  # Remove duplicates

def update_powerbi_data_source(pbix_file, old_data_source, new_data_source):
    try:
        # Create backup
        backup_path = f"{pbix_file}.backup"
        shutil.copy2(pbix_file, backup_path)
        logger.info(f"Created backup: {backup_path}")
        
        # Update data source
        with PowerBIAutomation(pbix_file) as pbi:
            # Get current data sources
            current_sources = pbi.get_current_data_sources()
            logger.info(f"Current data sources: {current_sources}")
            
            # Update data source
            if pbi.update_data_source(old_data_source, new_data_source):
                if pbi.save_updated_pbix():
                    logger.info("✅ Power BI file updated successfully!")
                    return True
            
        return False
        
    except Exception as e:
        logger.error(f"Failed to update Power BI file: {e}")
        return False

if __name__ == "__main__":
    # Example usage - using portable paths
    paths = get_powerbi_paths()
    pbix_path = paths['pbix_file']
    old_source = r"d:\projects\cleaned_superstore_dataset.csv"
    new_source = paths['processed_data']
    
    print("🔄 Updating Power BI data source...")
    success = update_powerbi_data_source(pbix_path, old_source, new_source)
    
    if success:
        print("✅ Power BI file updated successfully!")
        print("📊 Open the PBIX file in Power BI Desktop to see the updated data")
    else:
        print("❌ Failed to update Power BI file")
        print("💡 You may need to update the data source manually in Power BI Desktop")
