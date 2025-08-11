from dash.dependencies import Input, Output, State
from dash import callback, html
import pandas as pd
import duckdb
from pathlib import Path
from components.steps.silver import get_silver_data, create_table_component

def register_silver_callbacks(app):
    @app.callback(
        [Output('silver-table-preview', 'children'),
         Output('silver-table-title', 'children')],
        [Input('silver-valid-card', 'n_clicks'),
         Input('silver-nonproduct-card', 'n_clicks'),
         Input('silver-exception-sku_not_in_seed-card', 'n_clicks'),
         Input('silver-exception-name_mismatch-card', 'n_clicks'),
         Input('silver-exception-vendor_mismatch-card', 'n_clicks')]
    )
    def update_table_view(*args):
        import dash
        
        # Get the ID of the triggered component
        ctx = dash.callback_context
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else 'silver-valid-card'
        
        # Load data
        data = get_silver_data()
        if not data:
            return html.Div("Error loading data"), "Error"
        
        # Map button IDs to table names and titles
        table_map = {
            'silver-valid-card': ('silver_orders_valid', 'dev_silver.silver_orders_valid'),
            'silver-nonproduct-card': ('silver_orders_nonproduct', 'dev_silver.silver_orders_nonproduct'),
            'silver-exception-sku_not_in_seed-card': ('silver_orders_exceptions', 'dev_silver.silver_orders_exceptions'),
            'silver-exception-name_mismatch-card': ('silver_orders_exceptions', 'dev_silver.silver_orders_exceptions'),
            'silver-exception-vendor_mismatch-card': ('silver_orders_exceptions', 'dev_silver.silver_orders_exceptions')
        }
        
        table_name, title = table_map.get(triggered_id, ('silver_orders_valid', 'Valid Orders'))
        df = data['tables'][table_name]
        
        # Filter exceptions if needed
        if table_name == 'silver_orders_exceptions':
            exception_type = triggered_id.split('-')[2].upper()
            df = df[df['exception_type'] == exception_type]
        
        # Create the table component
        table = create_table_component(df, table_name)
        
        return table, title
