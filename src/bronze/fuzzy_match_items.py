"""
Fuzzy Matching Module

Advanced fuzzy matching algorithms for matching messy line items to clean product SKUs.
Uses multiple similarity algorithms and confidence scoring.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import logging
from difflib import SequenceMatcher
import re

logger = logging.getLogger(__name__)

class FuzzyMatcher:
    """Fuzzy matching engine for procurement items."""
    
    def __init__(self, staged_data_path: str, simulated_data_path: str):
        self.staged_data_path = Path(staged_data_path)
        self.simulated_data_path = Path(simulated_data_path)
        self.product_catalog = None
        self.vendor_catalog = None
        
    def load_catalogs(self) -> None:
        """Load product and vendor catalogs for matching."""
        # Load product SKUs
        product_path = self.simulated_data_path / "product_skus.jsonl"
        self.product_catalog = pd.read_json(product_path, lines=True)
        
        # Create searchable product fields
        self.product_catalog['searchable_name'] = (
            self.product_catalog['product_name'].str.lower().str.replace(r'[^\w\s]', '', regex=True)
        )
        self.product_catalog['brand_lower'] = self.product_catalog['brand'].str.lower()
        
        # Load vendor master (if exists)
        vendor_path = self.staged_data_path.parent / "raw" / "vendor_master.csv"
        if vendor_path.exists():
            self.vendor_catalog = pd.read_csv(vendor_path)
            self.vendor_catalog['searchable_vendor'] = (
                self.vendor_catalog['vendor_name'].str.lower().str.replace(r'[^\w\s]', '', regex=True)
            )
        
        logger.info(f"Loaded {len(self.product_catalog)} products and {len(self.vendor_catalog) if self.vendor_catalog is not None else 0} vendors")
    
    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two text strings."""
        if pd.isna(text1) or pd.isna(text2):
            return 0.0
        
        # Use SequenceMatcher for basic similarity
        return SequenceMatcher(None, str(text1).lower(), str(text2).lower()).ratio()
    
    def extract_features(self, user_input: str) -> Dict[str, str]:
        """Extract matching features from user input."""
        features = {}
        
        # Extract potential brand mentions
        known_brands = set(self.product_catalog['brand'].str.lower().unique())
        for brand in known_brands:
            if brand.lower() in user_input.lower():
                features['mentioned_brand'] = brand
                break
        
        # Extract potential category keywords
        category_keywords = {
            'office': ['paper', 'pen', 'staple', 'folder', 'binder'],
            'tech': ['computer', 'laptop', 'mouse', 'keyboard', 'monitor', 'cable'],
            'furniture': ['desk', 'chair', 'table', 'shelf', 'cabinet'],
            'supplies': ['ink', 'toner', 'cartridge', 'ribbon']
        }
        
        for category, keywords in category_keywords.items():
            if any(keyword in user_input.lower() for keyword in keywords):
                features['suggested_category'] = category
                break
        
        # Extract potential SKU
        sku_match = re.search(r'[A-Z]{2,4}-[0-9]{3,6}', user_input)
        if sku_match:
            features['extracted_sku'] = sku_match.group()
        
        return features
    
    def find_best_product_match(self, user_input: str, top_n: int = 5) -> List[Dict]:
        """Find best matching products for user input."""
        if self.product_catalog is None:
            self.load_catalogs()
        
        features = self.extract_features(user_input)
        matches = []
        
        # Direct SKU match (highest priority)
        if 'extracted_sku' in features:
            direct_matches = self.product_catalog[
                self.product_catalog['sku'].str.upper() == features['extracted_sku'].upper()
            ]
            for _, product in direct_matches.iterrows():
                matches.append({
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'brand': product['brand'],
                    'confidence': 0.95,
                    'match_type': 'direct_sku',
                    'reasoning': f"Direct SKU match: {features['extracted_sku']}"
                })
        
        # Brand + text similarity matching
        if 'mentioned_brand' in features:
            brand_products = self.product_catalog[
                self.product_catalog['brand_lower'] == features['mentioned_brand']
            ]
            
            for _, product in brand_products.iterrows():
                similarity = self.calculate_similarity(user_input, product['searchable_name'])
                if similarity > 0.3:  # Minimum threshold
                    matches.append({
                        'sku': product['sku'],
                        'product_name': product['product_name'],
                        'brand': product['brand'],
                        'confidence': min(0.9, similarity + 0.2),  # Brand boost
                        'match_type': 'brand_similarity',
                        'reasoning': f"Brand match ({features['mentioned_brand']}) + text similarity ({similarity:.2f})"
                    })
        
        # General text similarity matching
        for _, product in self.product_catalog.iterrows():
            similarity = self.calculate_similarity(user_input, product['searchable_name'])
            if similarity > 0.5:  # Higher threshold for general matching
                matches.append({
                    'sku': product['sku'],
                    'product_name': product['product_name'],
                    'brand': product['brand'],
                    'confidence': similarity,
                    'match_type': 'text_similarity',
                    'reasoning': f"Text similarity: {similarity:.2f}"
                })
        
        # Sort by confidence and return top matches
        matches = sorted(matches, key=lambda x: x['confidence'], reverse=True)
        return matches[:top_n]
    
    def find_best_vendor_match(self, vendor_hint: str) -> Optional[Dict]:
        """Find best matching vendor."""
        if self.vendor_catalog is None or pd.isna(vendor_hint):
            return None
        
        best_match = None
        best_score = 0.0
        
        for _, vendor in self.vendor_catalog.iterrows():
            similarity = self.calculate_similarity(vendor_hint, vendor['searchable_vendor'])
            if similarity > best_score and similarity > 0.6:  # Minimum threshold
                best_score = similarity
                best_match = {
                    'vendor_id': vendor['vendor_id'],
                    'vendor_name': vendor['vendor_name'],
                    'confidence': similarity,
                    'reasoning': f"Vendor name similarity: {similarity:.2f}"
                }
        
        return best_match
    
    def process_line_items(self, line_items_df: pd.DataFrame) -> pd.DataFrame:
        """Process all line items and add fuzzy matching results."""
        if self.product_catalog is None:
            self.load_catalogs()
        
        results = []
        
        for idx, row in line_items_df.iterrows():
            logger.debug(f"Processing line item {idx+1}/{len(line_items_df)}")
            
            # Find product matches
            product_matches = self.find_best_product_match(row['user_input_clean'])
            best_product = product_matches[0] if product_matches else None
            
            # Find vendor match
            vendor_match = None
            if 'preferred_vendor' in row and pd.notna(row['preferred_vendor']):
                vendor_match = self.find_best_vendor_match(row['preferred_vendor'])
            
            # Compile results
            result_row = row.copy()
            
            if best_product:
                result_row['matched_sku'] = best_product['sku']
                result_row['matched_product_name'] = best_product['product_name']
                result_row['matched_brand'] = best_product['brand']
                result_row['match_confidence'] = best_product['confidence']
                result_row['match_type'] = best_product['match_type']
                result_row['match_reasoning'] = best_product['reasoning']
            else:
                result_row['matched_sku'] = None
                result_row['matched_product_name'] = None
                result_row['matched_brand'] = None
                result_row['match_confidence'] = 0.0
                result_row['match_type'] = 'no_match'
                result_row['match_reasoning'] = 'No suitable product found'
            
            if vendor_match:
                result_row['matched_vendor_id'] = vendor_match['vendor_id']
                result_row['matched_vendor_name'] = vendor_match['vendor_name']
                result_row['vendor_match_confidence'] = vendor_match['confidence']
            else:
                result_row['matched_vendor_id'] = None
                result_row['matched_vendor_name'] = None
                result_row['vendor_match_confidence'] = 0.0
            
            results.append(result_row)
        
        return pd.DataFrame(results)
    
    def save_matched_results(self, df: pd.DataFrame) -> None:
        """Save fuzzy matching results."""
        output_path = self.staged_data_path / "line_items_matched.csv"
        df.to_csv(output_path, index=False)
        
        # Generate matching summary
        summary = {
            'total_items': len(df),
            'successfully_matched': len(df[df['matched_sku'].notna()]),
            'high_confidence_matches': len(df[df['match_confidence'] >= 0.8]),
            'avg_confidence': df['match_confidence'].mean(),
            'match_types': df['match_type'].value_counts().to_dict()
        }
        
        logger.info(f"Fuzzy matching complete: {summary}")
        logger.info(f"Results saved to {output_path}")

def main():
    """Main entry point for fuzzy matching."""
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configure paths
    project_root = Path(__file__).parent.parent.parent
    staged_data_path = project_root / "data" / "staged"
    simulated_data_path = project_root / "data" / "simulated"
    
    # Load cleaned line items
    cleaned_items_path = staged_data_path / "line_items_cleaned.csv"
    if not cleaned_items_path.exists():
        logger.error(f"Cleaned line items not found at {cleaned_items_path}")
        logger.error("Please run clean_lineitems.py first")
        sys.exit(1)
    
    line_items_df = pd.read_csv(cleaned_items_path)
    logger.info(f"Loaded {len(line_items_df)} cleaned line items")
    
    # Run fuzzy matching
    matcher = FuzzyMatcher(str(staged_data_path), str(simulated_data_path))
    matched_df = matcher.process_line_items(line_items_df)
    matcher.save_matched_results(matched_df)
    
    print(f"✅ Processed {len(matched_df)} line items with fuzzy matching")

if __name__ == "__main__":
    main()
