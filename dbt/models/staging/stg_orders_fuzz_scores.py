import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz import process


def model(dbt, session):
    """
    Compute fuzzy scores between:
    1. Order line item descriptions and seed SKU names
    2. Vendor brands and silver vendor master names
    Only processes lines that have matching SKU IDs in the seed.
    """
    
    # Load data from dbt refs 
    stg_orders_df = dbt.ref("stg_orders").to_df()
    
    # Load seed data directly using session - the seed table is in dev_seeds schema
    ref_sku_names_df = session.execute("SELECT * FROM dev_seeds.ref_sku_names").fetchdf()
    
    # Load vendor master data from bronze layer
    bronze_vendor_master_df = dbt.ref("raw_vendor_master").to_df()
    
    # Keep only rows that have a SKU match to compare against seed
    merged_df = stg_orders_df.merge(
        ref_sku_names_df,
        left_on="sku_id",
        right_on="sku_id",
        how="inner"
    )
    
    def normalize_text(text):
        """Normalize text for comparison."""
        if not isinstance(text, str) or text is None:
            return ""
        return " ".join(text.lower().split())
    
    # Direct abbreviation mapping (case-sensitive)
    ABBREV_VENDOR_MAP = {
        "VM": "ViewMaster Corporation",
        "CP": "ConnectPro Corporation",
        "DV": "DataVault Corporation",
        "TF": "TechFlow Corporation",
        "PC": "ProConnect Corporation",
        "OL": "OfficeLink Corporation",
    }
    # Reverse map for fast lookup of vendor_id
    VENDOR_NAME_TO_ID = {
        normalize_text(row['vendor_name']): row['vendor_id']
        for _, row in bronze_vendor_master_df.iterrows()
    }

    def find_best_vendor_match(vendor_brand, vendor_master_df):
        """Find the best vendor match using abbreviation or fuzzy match."""
        if not isinstance(vendor_brand, str) or not vendor_brand.strip():
            return None, None, 0.0

        # Check for abbreviation direct mapping (case-sensitive)
        if vendor_brand in ABBREV_VENDOR_MAP:
            mapped_name = ABBREV_VENDOR_MAP[vendor_brand]
            normalized_mapped = normalize_text(mapped_name)
            vendor_id = VENDOR_NAME_TO_ID.get(normalized_mapped)
            return vendor_id, mapped_name, 1.0

        normalized_brand = normalize_text(vendor_brand)
        best_score = 0.0
        best_match_id = None
        best_match_name = None

        # Prepare list of normalized vendor names
        vendor_names = vendor_master_df['vendor_name'].fillna("").astype(str).apply(normalize_text).tolist()
        # Use RapidFuzz's extractOne for efficient best match
        match = process.extractOne(
            normalized_brand,
            vendor_names,
            scorer=fuzz.partial_token_set_ratio,
            score_cutoff=0  # We'll check threshold later
        )
        if match:
            best_score = match[1] / 100.0
            best_match_idx = match[2]
            best_match_id = vendor_master_df.iloc[best_match_idx]['vendor_id']
            best_match_name = vendor_master_df.iloc[best_match_idx]['vendor_name']

        # Only return match if score >= 0.6
        if best_score >= 0.6:
            return best_match_id, best_match_name, best_score
        else:
            return None, None, best_score
    
    # Calculate SKU description fuzzy scores using partial_token_ratio
    sku_scores = []
    for order_desc, seed_name in zip(
        merged_df["item_description_cleaned"].fillna(""),
        merged_df["canonical_name"].fillna("")
    ):
        normalized_order = normalize_text(order_desc)
        normalized_seed = normalize_text(seed_name)
        
        # RapidFuzz partial_token_ratio returns 0-100, convert to 0-1
        score = fuzz.partial_token_ratio(normalized_order, normalized_seed) / 100.0
        sku_scores.append(score)
    
    # Calculate vendor brand matches
    vendor_matches = []
    vendor_scores = []
    vendor_names = []
    
    for vendor_brand in merged_df["vendor_brand"].fillna(""):
        vendor_id, vendor_name, score = find_best_vendor_match(vendor_brand, bronze_vendor_master_df)
        vendor_matches.append(vendor_id)
        vendor_names.append(vendor_name)
        vendor_scores.append(score)
    
    # Create output DataFrame
    result_df = pd.DataFrame({
        "line_uid": merged_df["line_uid"],
        "sku_id": merged_df["sku_id"],
        "order_description": merged_df["item_description_cleaned"],
        "seed_name": merged_df["canonical_name"],
        "fuzz_score": sku_scores,
        "vendor_brand_original": merged_df["vendor_brand"],
        "matched_vendor_id": [str(v) if v is not None else None for v in vendor_matches],  # Ensure string type
        "matched_vendor_name": vendor_names,
        "vendor_fuzz_score": vendor_scores
    })
    
    return result_df
