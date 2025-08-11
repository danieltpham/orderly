from dash.dependencies import Input, Output, State
from dash import callback, html
import dash_mantine_components as dmc
import pandas as pd
from pathlib import Path

def register_hitl_auto_callbacks(app):
    @app.callback(
        [
            Output('hitl-raw-names', 'children'),
            Output('hitl-canonical-tokens', 'children'),
            Output('hitl-matches', 'children'),
            Output('hitl-match-name', 'children'),
            Output('hitl-decision', 'children'),
            Output('hitl-decision', 'color'),
        ],
        [Input('hitl-sku-dropdown', 'value')]
    )
    def update_hitl_view(sku_id):
        if not sku_id:
            return [], "No SKU selected", [], "", "NO DATA", "gray"
            
        # Load curation data
        exports_dir = Path(__file__).parents[2] / 'data' / 'intermediate' / 'curation_exports'
        latest_file = "sku_name_curation_20250810.csv"
        df = pd.read_csv(exports_dir / latest_file, encoding='utf-8')
        
        # Get row for selected SKU
        sku_data = df[df['sku_id'] == sku_id].iloc[0]
        
        # Get raw names and process them
        raw_name_list = [name.strip() for name in sku_data['raw_names'].split(',')]
        
        # Import and use the alias cleaning function
        from hitl.utils.alias_cleaning import transform_aliases_with_canonical_tokens
        
        # Process the raw names to get canonical tokens
        cleaning_result = transform_aliases_with_canonical_tokens(
            raw_name_list,
            max_edit_distance=2,
            top_m_canonical_tokens=2,
            top_k_original=3,
        )
        
        # Get canonical tokens for highlighting
        canonical_tokens = cleaning_result['canonical_tokens']
        
        # Create raw names list with highlighting
        raw_names = [
            dmc.ListItem(
                dmc.Highlight(
                    name.strip(),
                    highlight=canonical_tokens,
                    color="teal"
                )
            ) for name in raw_name_list
        ]
        
        # Show canonical tokens as a summary
        canonical_summary = dmc.Text(
            f"Identified tokens: {', '.join(canonical_tokens)}",
        )
        
        # Top matches from the cleaning pipeline
        matches = [
            dmc.ListItem([
                dmc.Highlight(
                    item['alias'],
                    highlight=canonical_tokens,
                    color="teal"
                ),
                dmc.Text(f"Score: {item['score']}", size="xs")
            ]) for item in cleaning_result['top_k_canonicalized']
        ]
        
        # Decision badge
        decision = sku_data['decision']
        decision_color = "green" if decision == "AUTO" else "orange"
        
        return raw_names, ' · '.join(canonical_tokens), matches, sku_data['match_name'], decision, decision_color
