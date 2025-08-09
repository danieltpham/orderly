#!/usr/bin/env python3
"""
SKU Curation CLI Tool

Auto-generate a pre-approved curation table for SKU name candidates using text cleaning pipeline to standardise SKU aliases. This curation table is exported to CSV and will be formalised into a seed table.
"""

import argparse
import pandas as pd
import sys
import duckdb
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Add the tools directory to Python path for imports
sys.path.append(str(Path(__file__).parent))
from alias_cleaning import transform_aliases_with_canonical_tokens


def load_sku_candidates(db_path: Optional[str] = None) -> pd.DataFrame:
    """Load SKU name candidates directly from DuckDB ref schema."""
    try:
        # Use default path if not provided
        if db_path is None:
            db_path = "./warehouse/orderly.duckdb"
        
        # Connect to DuckDB and query the ref table
        conn = duckdb.connect(db_path)
        
        # Determine schema based on environment
        environment = os.getenv('ENVIRONMENT', 'dev').lower()
        schema_prefix = 'dev_' if environment == 'dev' else ''
        schema_name = f"{schema_prefix}ref"
        
        query = f"""
        SELECT 
            sku_id,
            item_description,
            line_order_count
        FROM {schema_name}.cur_sku_name_candidates
        ORDER BY sku_id, line_order_count DESC
        """
        
        df = conn.execute(query).fetchdf()
        conn.close()
        
        required_cols = ['sku_id', 'item_description', 'line_order_count']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Query result must contain columns: {required_cols}")
            
        print(f"✅ Loaded {len(df)} records from DuckDB {schema_name} schema (env: {environment})")
        return df
        
    except Exception as e:
        print(f"❌ Error loading from DuckDB: {e}")
        sys.exit(1)


def process_sku_group(sku_id: str, descriptions: List[str]) -> Dict:
    """Process a single SKU group to generate candidate names and alternatives."""
    
    # Apply alias cleaning pipeline
    result = transform_aliases_with_canonical_tokens(
        descriptions,
        max_edit_distance=2, # After testing, best tuned results
        top_m_canonical_tokens=2, # After testing, best tuned results
        top_k_original=3,
    )
    
    # Extract results
    canonical_tokens = result['canonical_tokens']
    most_freq_tokens = " ".join(w.capitalize() if i == 0 else w for i, w in enumerate(canonical_tokens))
    
    # Get top canonicalized aliases
    top_aliases = result['top_k_canonicalized']
    
    # Get the highest scoring alternative as the candidate score
    top_candidate_score = top_aliases[0]['score'] if top_aliases else 0
    runner_up_score = top_aliases[1]['score'] if len(top_aliases)>1 else 0
    
    # Fill alternative names (use canonicalized versions)
    alt_names = [item['alias_canonicalized'] for item in top_aliases]
    while len(alt_names) < 3:
        alt_names.append('')
    
    return {
        'sku_id': sku_id,
        'raw_names': ','.join(descriptions),
        'match_name': alt_names[0],
        'match_score': top_candidate_score,
        'final_sku_name': (
                alt_names[0] if (top_candidate_score > runner_up_score and top_candidate_score > 80) else ''
        ),
        'decision': (
                'AUTO' if (top_candidate_score > runner_up_score and top_candidate_score > 80) else 'NEED_APPROVAL'
        )
    }


def export_sku_curation_table(db_path: Optional[str] = None, output_dir: str = "data/intermediate/curation_exports") -> str:
    """
    Generate SKU curation table from DuckDB ref schema.
    
    Args:
        db_path: Path to the DuckDB database file (default: ./warehouse/orderly.duckdb)
        output_dir: Directory to save the output CSV (default: data/intermediate/curation_exports)
    
    Returns:
        Path to the generated CSV file
    """
    print(f"Loading SKU candidates from DuckDB...")
    df = load_sku_candidates(db_path)
    
    print(f"Processing {len(df)} candidate records...")
    
    # Group by SKU ID and process each group
    results = []
    sku_groups = df.groupby('sku_id')
    
    for sku_id, group in sku_groups:
        descriptions = group['item_description'].tolist()
        
        result = process_sku_group(str(sku_id), descriptions)
        results.append(result)
    
    # Create output DataFrame
    output_df = pd.DataFrame(results)
    
    # Generate date-tagged filename
    date_tag = datetime.now().strftime('%Y%m%d')
    output_filename = f"sku_name_curation_{date_tag}.csv"
    output_path = Path(output_dir) / output_filename
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    output_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Generated curation table: {output_path}")
    print(f"📊 Processed {len(results)} SKUs")
    
    return str(output_path)


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Generate SKU curation table from DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/transform/sku_curation_cli.py export
  python src/transform/sku_curation_cli.py export --db-path warehouse/custom.duckdb
        """
    )
    
    parser.add_argument(
        'command',
        choices=['export'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--db-path',
        help='Path to DuckDB database file (default: ./warehouse/orderly.duckdb)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='data/intermediate/curation_exports',
        help='Output directory for curation CSV (default: data/intermediate/curation_exports)'
    )
    
    args = parser.parse_args()
    
    if args.command == 'export':
        # Resolve output directory relative to current working directory (top-level project dir)
        output_dir = Path(args.output_dir).resolve()
        
        try:
            export_sku_curation_table(args.db_path, str(output_dir))
        except Exception as e:
            print(f"❌ Export failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
