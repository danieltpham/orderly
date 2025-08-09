"""
Production staging layer runner script.

Orchestrates the complete staging transformation pipeline with proper error handling
and dependency management.
"""

import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from transform.stg_orders import StgOrdersTransform


def setup_logging():
    """Configure logging for production use."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('staging_pipeline.log')
        ]
    )


def main():
    """Main entry point for staging pipeline."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting production staging pipeline...")
    
    try:
        # Get project paths
        project_root = Path(__file__).parent
        warehouse_path = project_root / "warehouse" / "orderly.duckdb"
        data_path = project_root / "data"
        
        # Verify warehouse exists
        if not warehouse_path.exists():
            raise FileNotFoundError(f"DuckDB warehouse not found: {warehouse_path}")
        
        # Initialize and run transformer
        transformer = StgOrdersTransform(
            warehouse_path=str(warehouse_path),
            data_path=str(data_path)
        )
        
        transformer.run_transform()
        
        logger.info("✅ Production staging pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Production staging pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
