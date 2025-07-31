"""
Portable Path Configuration
Handles cross-platform and user-independent paths
"""
import os
from pathlib import Path

def get_base_dirs():
    """Get list of possible base directories for data files"""
    current_dir = Path.cwd()
    
    # Try to find data directories in order of preference
    possible_dirs = [
        # Relative paths (most portable)
        current_dir / ".." / "ECOMMERCE HAMMAD",
        current_dir.parent / "ECOMMERCE HAMMAD", 
        current_dir / "data",
        current_dir / ".." / "data",
        
        # User home directory fallback
        Path.home() / "Desktop" / "Pipeline" / "ECOMMERCE HAMMAD",
        Path.home() / "Documents" / "Pipeline" / "ECOMMERCE HAMMAD",
        Path.home() / "Pipeline" / "ECOMMERCE HAMMAD",
        
        # Current directory fallback
        current_dir / "ECOMMERCE HAMMAD",
    ]
    
    return [str(p) for p in possible_dirs]

def find_data_directory():
    """Find the first existing data directory"""
    for dir_path in get_base_dirs():
        if os.path.exists(dir_path):
            return dir_path
    return None

def get_data_file_path(filename):
    """Get full path to a data file"""
    data_dir = find_data_directory()
    if data_dir:
        return os.path.join(data_dir, filename)
    
    # Fallback to current directory
    return filename

def get_powerbi_paths():
    """Get Power BI related file paths"""
    base_dir = find_data_directory()
    
    if base_dir:
        return {
            'pbix_file': os.path.join(base_dir, "ECOMMERCE HAMMAD.pbix"),
            'processed_data': os.path.join(base_dir, "pipeline_processed_data.csv"),
            'metadata': os.path.join(base_dir, "pipeline_metadata.json"),
            'cleaned_data': os.path.join(base_dir, "cleaned_superstore_dataset.csv"),
            'raw_data': os.path.join(base_dir, "HAMMAD ECOMMERCE.csv"),
            'base_directory': base_dir
        }
    
    # Fallback paths
    return {
        'pbix_file': "ECOMMERCE HAMMAD.pbix",
        'processed_data': "pipeline_processed_data.csv", 
        'metadata': "pipeline_metadata.json",
        'cleaned_data': "cleaned_superstore_dataset.csv",
        'raw_data': "HAMMAD ECOMMERCE.csv",
        'base_directory': "."
    }

def ensure_output_directory(subdir=""):
    """Ensure output directory exists"""
    output_dir = Path("data") / "output" / subdir if subdir else Path("data") / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)

# Environment-based configuration
DEFAULT_CONFIG = {
    'input_dir': 'data/input',
    'output_dir': 'data/output', 
    'logs_dir': 'data/logs',
    'temp_dir': 'data/temp'
}

def get_config():
    """Get configuration with environment variable overrides"""
    config = DEFAULT_CONFIG.copy()
    
    # Allow environment variable overrides
    for key in config:
        env_key = f"PIPELINE_{key.upper()}"
        if env_key in os.environ:
            config[key] = os.environ[env_key]
    
    return config
