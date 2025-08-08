"""
Test suite for ETL cleaning functionality.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import json

# Adjust import path for the module structure
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl_core.clean_lineitems import LineItemCleaner

class TestLineItemCleaner:
    """Test cases for LineItemCleaner."""
    
    @pytest.fixture
    def sample_line_items(self):
        """Create sample line items data for testing."""
        return pd.DataFrame([
            {
                'request_id': 'REQ001',
                'user_input': '  TechFlow Laptop Computer  ',
                'quantity': '2',
                'unit_of_measure': 'ea',
                'estimated_price': '1200.50',
                'currency': 'USD',
                'cost_center': 'IT-001',
                'requester_email': 'user@company.com',
                'urgency': 'MEDIUM',
                'preferred_vendor': 'TechFlow',
                'data_quality_flag': 'GOOD'
            },
            {
                'request_id': 'REQ002', 
                'user_input': 'Office supplies - pens, paper, etc.',
                'quantity': '',  # Missing quantity
                'unit_of_measure': 'box',
                'estimated_price': '45.99',
                'currency': 'USD',
                'cost_center': 'ADMIN-002',
                'requester_email': 'admin@company.com',
                'urgency': 'LOW',
                'preferred_vendor': '',
                'data_quality_flag': 'FAIR'
            },
            {
                'request_id': 'REQ003',
                'user_input': 'CF-12345 Cable Connector!!!',
                'quantity': '10',
                'unit_of_measure': 'pieces',
                'estimated_price': 'invalid_price',
                'currency': 'USD', 
                'cost_center': 'IT-001',
                'requester_email': 'invalid_email',
                'urgency': 'HIGH',
                'preferred_vendor': 'ConnectPro Inc',
                'data_quality_flag': 'POOR'
            }
        ])
    
    @pytest.fixture
    def temp_directories(self):
        """Create temporary directories for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            raw_dir = temp_path / "raw"
            staged_dir = temp_path / "staged"
            simulated_dir = temp_path / "simulated"
            
            raw_dir.mkdir()
            staged_dir.mkdir()
            simulated_dir.mkdir()
            
            yield {
                'raw': str(raw_dir),
                'staged': str(staged_dir),
                'simulated': str(simulated_dir)
            }
    
    def test_clean_user_input(self, sample_line_items, temp_directories):
        """Test user input cleaning functionality."""
        cleaner = LineItemCleaner(temp_directories['raw'], temp_directories['staged'])
        
        cleaned_df = cleaner.clean_user_input(sample_line_items)
        
        # Test whitespace normalization
        assert cleaned_df.loc[0, 'user_input_clean'] == 'TechFlow Laptop Computer'
        
        # Test SKU extraction
        assert cleaned_df.loc[2, 'extracted_sku'] == 'CF-12345'
        assert pd.isna(cleaned_df.loc[0, 'extracted_sku'])
        
        # Test quantity handling
        assert cleaned_df.loc[0, 'quantity'] == 2
        assert cleaned_df.loc[1, 'quantity'] == 1  # Default for missing
        assert cleaned_df.loc[2, 'quantity'] == 10
        
        # Test price cleaning
        assert cleaned_df.loc[0, 'estimated_price'] == 1200.50
        assert cleaned_df.loc[1, 'estimated_price'] == 45.99
        assert pd.isna(cleaned_df.loc[2, 'estimated_price'])  # Invalid price
    
    def test_standardize_units(self, sample_line_items, temp_directories):
        """Test unit of measure standardization."""
        cleaner = LineItemCleaner(temp_directories['raw'], temp_directories['staged'])
        
        standardized_df = cleaner.standardize_units(sample_line_items)
        
        assert standardized_df.loc[0, 'unit_of_measure'] == 'EACH'  # 'ea' -> 'EACH'
        assert standardized_df.loc[1, 'unit_of_measure'] == 'BOX'   # 'box' -> 'BOX'
        assert standardized_df.loc[2, 'unit_of_measure'] == 'EACH'  # 'pieces' -> 'EACH'
    
    def test_validate_data_quality(self, sample_line_items, temp_directories):
        """Test data quality validation and scoring."""
        cleaner = LineItemCleaner(temp_directories['raw'], temp_directories['staged'])
        
        # First clean the data
        cleaned_df = cleaner.clean_user_input(sample_line_items)
        validated_df = cleaner.validate_data_quality(cleaned_df)
        
        # Check quality flags
        assert validated_df.loc[0, 'has_valid_email'] == True
        assert validated_df.loc[2, 'has_valid_email'] == False
        
        # Check quality scores
        assert validated_df.loc[0, 'quality_score'] > validated_df.loc[2, 'quality_score']
        
        # Check processing categories
        assert validated_df.loc[0, 'processing_category'] in ['direct_matching', 'exact_match', 'standard_processing']
        assert validated_df.loc[2, 'processing_category'] in ['needs_manual_review', 'fuzzy_matching_required']
    
    def test_full_pipeline_with_mock_data(self, temp_directories):
        """Test the complete pipeline with mock JSONL input."""
        # Create mock JSONL file
        simulated_dir = Path(temp_directories['staged']).parent / "simulated"
        simulated_dir.mkdir(exist_ok=True)
        
        jsonl_path = simulated_dir / "line_items.jsonl"
        sample_data = [
            {
                'request_id': 'REQ001',
                'user_input': 'TechFlow Laptop TF-5000',
                'quantity': 1,
                'unit_of_measure': 'each',
                'estimated_price': 1500.00,
                'currency': 'USD',
                'cost_center': 'IT-001',
                'requester_email': 'user@company.com',
                'urgency': 'MEDIUM',
                'preferred_vendor': 'TechFlow Corp',
                'data_quality_flag': 'EXCELLENT'
            }
        ]
        
        with open(jsonl_path, 'w') as f:
            for item in sample_data:
                f.write(json.dumps(item) + '\n')
        
        # Run pipeline
        cleaner = LineItemCleaner(temp_directories['raw'], temp_directories['staged'])
        result_df = cleaner.run_full_pipeline()
        
        # Verify results
        assert len(result_df) == 1
        assert 'user_input_clean' in result_df.columns
        assert 'quality_score' in result_df.columns
        assert 'processing_category' in result_df.columns
        
        # Check output files were created
        staged_path = Path(temp_directories['staged'])
        assert (staged_path / "line_items_cleaned.csv").exists()
        assert (staged_path / "line_items_cleaned.parquet").exists()

def test_unit_mappings():
    """Test unit of measure mapping logic."""
    from etl_core.clean_lineitems import LineItemCleaner
    
    # Create a minimal test case
    test_df = pd.DataFrame({
        'unit_of_measure': ['ea', 'box', 'kg', 'unknown_unit', 'pieces']
    })
    
    with tempfile.TemporaryDirectory() as temp_dir:
        cleaner = LineItemCleaner(temp_dir, temp_dir)
        result_df = cleaner.standardize_units(test_df)
    
    expected_units = ['EACH', 'BOX', 'KG', 'EACH', 'EACH']  # unknown_unit -> EACH (default)
    assert result_df['unit_of_measure'].tolist() == expected_units

def test_quality_score_calculation():
    """Test quality score calculation logic."""
    test_df = pd.DataFrame({
        'extracted_sku': ['TF-1234', None, 'CF-5678'],
        'estimated_price': [100.0, 50.0, None],
        'user_input_clean': ['Valid input', 'OK', 'X'],  # Last one too short
        'requester_email': ['user@company.com', 'invalid', 'test@test.com']
    })
    
    with tempfile.TemporaryDirectory() as temp_dir:
        cleaner = LineItemCleaner(temp_dir, temp_dir)
        result_df = cleaner.validate_data_quality(test_df)
    
    # First row should have highest quality (has SKU, price, clean text, valid email)
    # Second row should have medium quality (no SKU, has price, clean text, invalid email)  
    # Third row should have lower quality (has SKU, no price, short text, valid email)
    
    assert result_df.loc[0, 'quality_score'] == 1.0  # All quality checks pass
    assert result_df.loc[1, 'quality_score'] == 0.5  # 2 out of 4 checks pass
    assert result_df.loc[2, 'quality_score'] == 0.75  # 3 out of 4 checks pass

if __name__ == "__main__":
    pytest.main([__file__])
