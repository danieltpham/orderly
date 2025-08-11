from dash.dependencies import Input, Output, State
from dash import callback, html
import pandas as pd
import json
import duckdb
from pathlib import Path

def register_bronze_callbacks(app):
    @app.callback(
        [
            Output('bronze-raw-content', 'children'),
            Output('bronze-table-preview', 'children'),
            Output('bronze-table-title', 'children'),
        ],
        [Input('bronze-source-file-dropdown', 'value')]
    )
    def update_bronze_view(file_path):
        import dash_mantine_components as dmc
        
        if not file_path:
            return "# Select a file to view its contents...", dmc.Text("No file selected"), "Imported DuckDB Table"
            
        # Read raw file content
        with open(file_path, 'r') as f:
            raw_content = f.read()

        try:
            # Try DuckDB first if warehouse exists
            if Path('warehouse/orderly.duckdb').exists():
                con = duckdb.connect('warehouse/orderly.duckdb')
                
                # Query from bronze tables based on file type and set table name
                filename = Path(file_path).name
                table_info = None
                
                if filename.startswith('orders'):
                    query = "SELECT * FROM dev_bronze.raw_orders LIMIT 10"
                    table_info = "dev_bronze.raw_orders"
                elif filename == 'vendor_master.csv':
                    query = "SELECT * FROM dev_bronze.raw_vendor_master LIMIT 10"
                    table_info = "dev_bronze.raw_vendor_master"
                elif filename == 'cost_centres.csv':
                    query = "SELECT * FROM dev_bronze.raw_cost_centres LIMIT 10"
                    table_info = "dev_bronze.raw_cost_centres"
                elif filename == 'exchange_rates.csv':
                    query = "SELECT * FROM dev_bronze.raw_exchange_rates LIMIT 10"
                    table_info = "dev_bronze.raw_exchange_rates"
                else:
                    raise Exception(f"Unknown file type: {filename}")
                    
                df = con.execute(query).df()
                
            else:
                # Fallback to pandas direct read
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path, nrows=10)
                else:
                    df = pd.read_json(file_path, lines=True, nrows=10)

        except Exception as e:
            raise Exception(f"Failed to load data: {str(e)}")
        
        # Convert DataFrame to DataTable
        from dash import dash_table
        
        # Convert DataFrame to a format compatible with DataTable
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                val = row[col]
                record[str(col)] = str(val) if pd.notnull(val) else ''
            records.append(record)
        
        table = dash_table.DataTable(
            data=records,
            columns=[{"name": i, "id": i} for i in df.columns],
            style_data={
                'backgroundColor': '#0b0f0a',
                'color': '#00a67d',
                'border': '1px solid #00a67d'
            },
            style_header={
                'backgroundColor': '#0b0f0a',
                'color': '#00a67d',
                'fontWeight': 'bold',
                'border': '1px solid #00a67d'
            },
            style_cell={
                'backgroundColor': '#0b0f0a',
                'padding': '10px',
                'fontFamily': '"IBM Plex Mono", monospace',
                'textAlign': 'left'
            },
            style_table={
                'overflowX': 'auto',
                'backgroundColor': '#0b0f0a'
            }
        )
        
        table_title = f"Imported DuckDB Table // {table_info}" if table_info else "Imported DuckDB Table"
        return raw_content, table, table_title
