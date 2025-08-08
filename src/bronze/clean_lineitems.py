"""
Line Items Cleaning Module

Pandas-based data cleaning and transformation for messy procurement line items.
This module bridges the gap between raw AI-generated data and clean dbt models.
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class LineItemCleaner:
    """Clean and standardize messy procurement line items using pandas."""
    
    def __init__(self, raw_data_path: str, staged_data_path: str):
        self.raw_data_path = Path(raw_data_path)
        self.staged_data_path = Path(staged_data_path)
        
    def load_raw_line_items(self) -> pd.DataFrame:
        """Load raw line items from JSONL file."""
        jsonl_path = self.raw_data_path / "../simulated/line_items.jsonl"
        df = pd.read_json(jsonl_path, lines=True)
        logger.info(f"Loaded {len(df)} raw line items")
        return df
    
    def clean_user_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize user input text."""
        df = df.copy()
        
        # Basic text cleaning
        df['user_input_clean'] = (df['user_input']
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)  # Normalize whitespace
            .str.replace(r'[^\w\s-]', '', regex=True)  # Remove special chars except hyphens
        )
        
        # Extract potential SKU patterns
        df['extracted_sku'] = df['user_input'].str.extract(r'([A-Z]{2,4}-[0-9]{3,6})')
        
        # Normalize quantities
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(1)
        df['quantity'] = df['quantity'].clip(lower=1, upper=10000)  # Reasonable bounds
        
        # Clean estimated prices
        df['estimated_price'] = pd.to_numeric(df['estimated_price'], errors='coerce')
        df['estimated_price'] = df['estimated_price'].clip(lower=0.01, upper=100000)
        
        return df
    
    def standardize_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize unit of measure values."""
        df = df.copy()
        
        unit_mappings = {
            'ea': 'EACH', 'each': 'EACH', 'piece': 'EACH', 'pcs': 'EACH',
            'box': 'BOX', 'boxes': 'BOX', 'case': 'CASE', 'cases': 'CASE',
            'kg': 'KG', 'kilogram': 'KG', 'kilograms': 'KG',
            'lb': 'LB', 'pound': 'LB', 'pounds': 'LB', 'lbs': 'LB',
            'meter': 'METER', 'metres': 'METER', 'm': 'METER',
            'liter': 'LITER', 'litre': 'LITER', 'l': 'LITER'
        }
        
        df['unit_of_measure'] = (df['unit_of_measure']
            .str.lower()
            .str.strip()
            .map(unit_mappings)
            .fillna('EACH')
        )
        
        return df
    
    def validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add data quality assessments and flags."""
        df = df.copy()
        
        # Calculate quality scores
        df['has_sku_pattern'] = df['extracted_sku'].notna()
        df['has_reasonable_price'] = df['estimated_price'].between(0.01, 50000)
        df['has_clean_text'] = df['user_input_clean'].str.len() >= 3
        df['has_valid_email'] = df['requester_email'].str.contains('@', na=False)
        
        # Overall quality score (0-1)
        quality_columns = ['has_sku_pattern', 'has_reasonable_price', 'has_clean_text', 'has_valid_email']
        df['quality_score'] = df[quality_columns].sum(axis=1) / len(quality_columns)
        
        # Quality categories for processing
        df['processing_category'] = pd.cut(
            df['quality_score'],
            bins=[0, 0.25, 0.5, 0.75, 0.9, 1.0],
            labels=['needs_manual_review', 'fuzzy_matching_required', 
                   'standard_processing', 'direct_matching', 'exact_match']
        )
        
        return df
    
    def save_staged_data(self, df: pd.DataFrame) -> None:
        """Save cleaned data to staged directory."""
        self.staged_data_path.mkdir(parents=True, exist_ok=True)
        
        # Save as both CSV and Parquet for flexibility
        csv_path = self.staged_data_path / "line_items_cleaned.csv"
        parquet_path = self.staged_data_path / "line_items_cleaned.parquet"
        
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)
        
        logger.info(f"Saved cleaned data to {csv_path} and {parquet_path}")
        logger.info(f"Processing categories: {df['processing_category'].value_counts().to_dict()}")
    
    def run_full_pipeline(self) -> pd.DataFrame:
        """Execute the complete cleaning pipeline."""
        logger.info("Starting line items cleaning pipeline...")
        
        # Load and process data
        df = self.load_raw_line_items()
        df = self.clean_user_input(df)
        df = self.standardize_units(df)
        df = self.validate_data_quality(df)
        
        # Save results
        self.save_staged_data(df)
        
        logger.info("Cleaning pipeline completed successfully")
        return df

def main():
    """Main entry point for running the cleaner."""
    import sys
    from pathlib import Path
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configure paths
    project_root = Path(__file__).parent.parent.parent
    raw_data_path = project_root / "data" / "raw"
    staged_data_path = project_root / "data" / "staged"
    
    # Run cleaner
    cleaner = LineItemCleaner(str(raw_data_path), str(staged_data_path))
    result_df = cleaner.run_full_pipeline()
    
    print(f"✅ Processed {len(result_df)} line items")
    print(f"📊 Quality distribution: {result_df['processing_category'].value_counts().to_dict()}")

if __name__ == "__main__":
    main()
