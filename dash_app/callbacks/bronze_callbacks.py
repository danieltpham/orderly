from dash.dependencies import Input, Output, State
from dash import callback, html
import pandas as pd
import json
import duckdb

def register_bronze_callbacks(app):
    @app.callback(
        [
            Output('bronze-raw-content', 'children'),
            Output('bronze-table-preview', 'children')
        ],
        [Input('bronze-source-file-dropdown', 'value')]
    )
    def update_bronze_view(file_path):
        import dash_mantine_components as dmc
        
        if not file_path:
            return "# Select a file to view its contents...", dmc.Text("No file selected")
            
        try:
            # Read raw file content
            with open(file_path, 'r') as f:
                raw_content = f.read()
                
            # Determine file type and load into DuckDB
            if file_path.endswith('.csv'):
                # Read CSV directly
                query = f"SELECT * FROM read_csv('{file_path}') LIMIT 10"
            else:
                # Handle JSONL files
                query = f"""
                SELECT * FROM read_json_auto('{file_path}',
                    format='auto',
                    ignore_errors=true,
                    maximum_depth=100
                ) LIMIT 10
                """
                
            # Execute query and get preview
            con = duckdb.connect('warehouse/orderly.duckdb')
            df = con.execute(query).df()
            
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
                    'color': '#5ADA8C',
                    'border': '1px solid #00a67d'
                },
                style_header={
                    'backgroundColor': '#0b0f0a',
                    'color': '#00d28b',
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
            
            return raw_content, table
            
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            return error_msg, dmc.Alert(
                error_msg,
                title="Error Loading Data",
                color="red",
                variant="filled"
            )
