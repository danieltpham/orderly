from dash import html, dcc
import dash_bootstrap_components as dbc
from pathlib import Path
import os
import duckdb
import pandas as pd

def get_source_files():
    """Get list of source files from data/source directory"""
    source_dir = Path(__file__).parents[3] / 'data' / 'source'
    # print(f"Looking for files in: {source_dir}")  # Debug print
    files = []
    for f in source_dir.rglob('*'):
        # Skip hidden files/directories and files in hidden directories
        if not any(part.startswith('.') for part in f.parts) and 'hidden' not in f.parts:
            if f.is_file():
                files.append({
                    'label': f.name,
                    'value': str(f.absolute())
                })
                # print(f"Found file: {f.name}")  # Debug print
    
    if not files:
        print("No files found!")  # Debug print
    return sorted(files, key=lambda x: x['label'])

def create_bronze_layout():
    """Create the layout for Bronze stage view"""
    import dash_mantine_components as dmc
    
    layout = dmc.Stack([
        # File selector
        dmc.Select(
            id='bronze-source-file-dropdown',
            data=get_source_files(),
            label="Select Source File",
            placeholder="Choose a file to view",
            style={"maxWidth": "100%"},
        ),
        
        # Split view container
        dmc.Grid([
            # Top half - Raw file content
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Title("Raw File Content", order=2),
                        dmc.ScrollArea(
                            children=dmc.Code(
                                id='bronze-raw-content',
                                block=True,
                                style={"backgroundColor": "#0b0f0a"}
                            ),
                            style={"height": 120},
                        )
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
                )
            ], span=12),
            
            # Bottom half - DuckDB table preview
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Title("DuckDB Table Preview", order=2),
                        dmc.ScrollArea(
                            children=html.Div(
                                id='bronze-table-preview',
                                style={"color": "#5ADA8C"}
                            ),
                            style={"height": 200},
                        )
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
                )
            ], span=12),
        ]),
    ])
    
    return layout
