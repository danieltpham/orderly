from dash import html, dcc, dash_table
import dash_mantine_components as dmc
import pandas as pd
from pathlib import Path
import dash_bootstrap_components as dbc

def get_approved_updates():
    """Get SQL-like commands for approved entries"""
    csv_path = Path(__file__).parents[3] / 'data' / 'intermediate' / 'curation_exports' / 'sku_name_curation_20250810_approved.csv'
    df = pd.read_csv(csv_path)
    
    # Filter for APPROVED entries
    approved_df = df[df['decision'] == 'APPROVED']
    
    # Generate SQL-like commands
    sql_commands = []
    for _, row in approved_df.iterrows():
        sql = f"UPDATE sku_names SET final_sku_name = '{row['final_sku_name']}' WHERE sku_id = '{row['sku_id']}';"
        sql_commands.append(sql)
    
    return '\n'.join([
        "-- Generated SQL-like commands for approved entries:",
        *sql_commands,
        "",
        "# Generate SKU seed command:",
        "python src/transform/generate_sku_seed.py data/intermediate/curation_exports/sku_name_curation_approved.csv"
    ])

def get_seed_data():
    """Get seed data from ref_sku_names.csv"""
    csv_path = Path(__file__).parents[3] / 'dbt' / 'seeds' / 'ref_sku_names.csv'
    return pd.read_csv(csv_path)

def render():
    """Render the HITL review step component."""
    # Load the data
    df = get_seed_data()
    
    # Convert DataFrame to records format for DataTable
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            record[str(col)] = str(val) if pd.notnull(val) else ''
        records.append(record)
    
    return dmc.Stack([
        # Grid container for both sections
        dmc.Grid([
            # SQL-like commands for APPROVED entries
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Group([
                            dmc.Title("Approved Updates", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=300,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "Manually approved name mappings "
                                            "that will be used to generate the dbt seed file. "
                                            "These mappings have been manually verified for accuracy.",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.ScrollArea(
                            children=dmc.Code(
                                get_approved_updates(),
                                block=True,
                                style={"backgroundColor": "#0b0f0a", "color": "rgb(0, 166, 125)"}
                            ),
                            style={"height": 150},
                        )
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
                )
            ], span=12),
            
            # Data table display
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Group([
                            dmc.Title("Current SKU Reference Table", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=300,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "The dbt seed file (ref_sku_names.csv) containing the "
                                            "canonical product names, their sources (approved_seed or auto_ref), "
                                            "effective dates, and version information. This table is the "
                                            "final output of the HITL curation process, and consumed by dbt in the next step.",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.ScrollArea(
                            children=html.Div(
                                id='hitl-review-table',
                                children=dash_table.DataTable(
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
                            ),
                            style={"height": 250},
                        )
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
                )
            ], span=12),
        ]),
    ])
