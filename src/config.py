"""
Configuration Module

Central configuration management for the orderly project.
Handles paths, database connections, and environment settings.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class ProjectPaths:
    """Project directory structure configuration."""
    root: Path
    data: Path
    data_raw: Path
    data_staged: Path
    data_final: Path
    data_simulated: Path
    dbt: Path
    dbt_models: Path
    dbt_target: Path
    src: Path
    tests: Path
    notebooks: Path
    
    @classmethod
    def from_root(cls, root_path: str) -> 'ProjectPaths':
        """Create paths configuration from project root."""
        root = Path(root_path)
        data = root / "data"
        
        return cls(
            root=root,
            data=data,
            data_raw=data / "raw",
            data_staged=data / "staged", 
            data_final=data / "final",
            data_simulated=data / "simulated",
            dbt=root / "dbt",
            dbt_models=root / "dbt" / "models",
            dbt_target=root / "dbt" / "target",
            src=root / "src",
            tests=root / "tests",
            notebooks=root / "notebooks"
        )

@dataclass 
class DatabaseConfig:
    """Database connection configuration."""
    type: str = "duckdb"
    path: str = "../data/orderly.duckdb"
    schema: str = "main"
    threads: int = 4
    
    def get_connection_string(self) -> str:
        """Get database connection string."""
        if self.type == "duckdb":
            return f"duckdb:///{self.path}"
        else:
            raise ValueError(f"Unsupported database type: {self.type}")

@dataclass
class ProcessingConfig:
    """Data processing configuration."""
    # Fuzzy matching thresholds
    min_similarity_threshold: float = 0.3
    high_confidence_threshold: float = 0.8
    brand_boost_factor: float = 0.2
    
    # Data quality thresholds
    min_quality_score: float = 0.25
    auto_process_threshold: float = 0.75
    
    # Validation settings
    max_price: float = 100000.0
    min_price: float = 0.01
    max_quantity: int = 10000
    
    # Supported values
    valid_currencies: list = None
    valid_countries: list = None
    valid_units: list = None
    
    def __post_init__(self):
        """Set default lists if not provided."""
        if self.valid_currencies is None:
            self.valid_currencies = ['USD', 'AUD', 'GBP', 'EUR', 'SGD', 'CAD', 'JPY']
        
        if self.valid_countries is None:
            self.valid_countries = ['US', 'AU', 'UK', 'DE', 'SG', 'CA', 'FR', 'JP']
        
        if self.valid_units is None:
            self.valid_units = ['EACH', 'BOX', 'CASE', 'KG', 'LB', 'METER', 'LITER']

class Config:
    """Main configuration class for the orderly project."""
    
    def __init__(self, root_path: Optional[str] = None):
        """Initialize configuration."""
        if root_path is None:
            # Auto-detect project root
            current_file = Path(__file__)
            self.root_path = current_file.parent.parent.parent
        else:
            self.root_path = Path(root_path)
        
        self.paths = ProjectPaths.from_root(str(self.root_path))
        self.database = DatabaseConfig()
        self.processing = ProcessingConfig()
        
        # Load environment-specific overrides
        self._load_environment_config()
    
    def _load_environment_config(self) -> None:
        """Load environment-specific configuration from .env file."""
        env_file = self.root_path / ".env"
        if env_file.exists():
            # Simple .env parser
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
        
        # Override database config from environment
        if os.getenv('DUCKDB_PATH'):
            self.database.path = os.getenv('DUCKDB_PATH')
        
        if os.getenv('DUCKDB_THREADS'):
            self.database.threads = int(os.getenv('DUCKDB_THREADS'))
        
        # Override processing config from environment
        if os.getenv('MIN_SIMILARITY_THRESHOLD'):
            self.processing.min_similarity_threshold = float(os.getenv('MIN_SIMILARITY_THRESHOLD'))
    
    def ensure_directories(self) -> None:
        """Create all necessary directories."""
        directories = [
            self.paths.data_raw,
            self.paths.data_staged,
            self.paths.data_final,
            self.paths.data_simulated,
            self.paths.dbt_target,
            self.paths.tests,
            self.paths.notebooks
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_dbt_vars(self) -> Dict[str, str]:
        """Get variables for dbt configuration."""
        return {
            'database_path': str(self.paths.data / "orderly.duckdb"),
            'raw_data_path': str(self.paths.data_raw),
            'staged_data_path': str(self.paths.data_staged),
            'final_data_path': str(self.paths.data_final)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'root_path': str(self.root_path),
            'database': {
                'type': self.database.type,
                'path': self.database.path,
                'schema': self.database.schema,
                'threads': self.database.threads
            },
            'processing': {
                'min_similarity_threshold': self.processing.min_similarity_threshold,
                'high_confidence_threshold': self.processing.high_confidence_threshold,
                'brand_boost_factor': self.processing.brand_boost_factor,
                'min_quality_score': self.processing.min_quality_score,
                'auto_process_threshold': self.processing.auto_process_threshold,
                'max_price': self.processing.max_price,
                'min_price': self.processing.min_price,
                'max_quantity': self.processing.max_quantity
            },
            'paths': {
                'root': str(self.paths.root),
                'data': str(self.paths.data),
                'data_raw': str(self.paths.data_raw),
                'data_staged': str(self.paths.data_staged),
                'data_final': str(self.paths.data_final),
                'dbt': str(self.paths.dbt),
                'src': str(self.paths.src)
            }
        }

# Global configuration instance
_config = None

def get_config() -> Config:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = Config()
    return _config

def set_config(config: Config) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config

# Environment detection
def is_development() -> bool:
    """Check if running in development environment."""
    return os.getenv('ENVIRONMENT', 'development').lower() == 'development'

def is_production() -> bool:
    """Check if running in production environment."""
    return os.getenv('ENVIRONMENT', 'development').lower() == 'production'

def is_testing() -> bool:
    """Check if running in test environment."""
    return os.getenv('ENVIRONMENT', 'development').lower() == 'testing'
