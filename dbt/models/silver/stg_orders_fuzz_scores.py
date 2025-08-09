import pandas as pd
from rapidfuzz import fuzz


def model(dbt, session):
    """
    Compute fuzzy scores between order line item descriptions and seed SKU names.
    Only processes lines that have matching SKU IDs in the seed.
    """
    
    # Load data from dbt refs 
    stg_orders_df = dbt.ref("stg_orders").to_df()
    
    # Load seed data directly using session - the seed table is in dev_seeds schema
    ref_sku_names_df = session.execute("SELECT * FROM dev_seeds.ref_sku_names").fetchdf()
    
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
    
    # Calculate fuzzy scores using partial_token_ratio
    scores = []
    for order_desc, seed_name in zip(
        merged_df["item_description_cleaned"].fillna(""),
        merged_df["canonical_name"].fillna("")
    ):
        normalized_order = normalize_text(order_desc)
        normalized_seed = normalize_text(seed_name)
        
        # RapidFuzz partial_token_ratio returns 0-100, convert to 0-1
        score = fuzz.partial_token_ratio(normalized_order, normalized_seed) / 100.0
        scores.append(score)
    
    # Create output DataFrame
    result_df = pd.DataFrame({
        "line_uid": merged_df["line_uid"],
        "sku_id": merged_df["sku_id"],
        "order_description": merged_df["item_description_cleaned"],
        "seed_name": merged_df["canonical_name"],
        "fuzz_score": scores
    })
    
    return result_df
