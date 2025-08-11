from dash import html
import dash_mantine_components as dmc
from pathlib import Path
import pandas as pd
import duckdb

def get_silver_data():
    """Get data from silver tables in DuckDB, fallback to CSVs if needed"""
    try:
        if Path('warehouse/orderly.duckdb').exists():
            con = duckdb.connect('warehouse/orderly.duckdb')
            
            # Get data from each silver table with row counts
            valid_orders = con.execute("SELECT COUNT(*) as count FROM dev_silver.silver_orders_valid").fetchone()[0]
            nonproduct_orders = con.execute("SELECT COUNT(*) as count FROM dev_silver.silver_orders_nonproduct").fetchone()[0]
            exceptions = con.execute("""
                SELECT exception_type, COUNT(*) as count 
                FROM dev_silver.silver_orders_exceptions 
                GROUP BY exception_type
            """).df()
            
            data = {
                'counts': {
                    'valid': valid_orders,
                    'nonproduct': nonproduct_orders,
                    'exceptions': exceptions.to_dict('records')
                },
                'tables': {
                    'silver_orders_valid': con.execute("SELECT * FROM dev_silver.silver_orders_valid LIMIT 100").df(),
                    'silver_orders_nonproduct': con.execute("SELECT * FROM dev_silver.silver_orders_nonproduct LIMIT 100").df(),
                    'silver_orders_exceptions': con.execute("SELECT * FROM dev_silver.silver_orders_exceptions LIMIT 100").df()
                }
            }
            
        else:
            # Fallback to CSVs
            data_dir = Path(__file__).parents[3] / 'data' / 'intermediate'
            
            valid_df = pd.read_csv(data_dir / 'silver_silver_orders_valid.csv')
            nonprod_df = pd.read_csv(data_dir / 'silver_silver_orders_nonproduct.csv')
            except_df = pd.read_csv(data_dir / 'silver_silver_orders_exceptions.csv')
            
            data = {
                'counts': {
                    'valid': len(valid_df),
                    'nonproduct': len(nonprod_df),
                    'exceptions': except_df.groupby('exception_type').size().reset_index(name='count').to_dict('records')
                },
                'tables': {
                    'silver_orders_valid': valid_df.head(100),
                    'silver_orders_nonproduct': nonprod_df.head(100),
                    'silver_orders_exceptions': except_df.head(100)
                }
            }
            
        return data
    except Exception as e:
        print(f"Error loading silver data: {str(e)}")
        return None

def create_silver_layout():
    """Create the layout for Silver stage view"""
    # Load initial data
    data = get_silver_data()
    if not data:
        return dmc.Text("Error loading silver data", color="red")
    
    return dmc.Stack([
        # Top half - KPI Cards in two columns
        dmc.Grid([
            # Left column - Data Quality Summary
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Group([
                            dmc.Title("Data Quality Summary", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=300,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help", "color": "dark"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "Click on each card to view the corresponding Silver table data below.",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.Space(h=20),
                        
                        # Valid Orders Card
                        dmc.Button(
                            children=[
                                dmc.Stack([
                                    dmc.Group([
                                        dmc.Title("Valid Orders", order=3),
                                        dmc.HoverCard(
                                            shadow="md",
                                            width=300,
                                            children=[
                                                dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help", "color": "dark"})),
                                                dmc.HoverCardDropdown([
                                                    dmc.Text(
                                                        "Clean, valid orders with high-confidence SKU, name, "
                                                        "and vendor matches. These records will flow to the Gold layer.",
                                                        size="sm"
                                                    )
                                                ])
                                            ]
                                        )
                                    ]),
                                    dmc.Text(str(data['counts']['valid']), size="xl", fw="bold")
                                ])
                            ],
                            variant="filled",
                            size="xl",
                            fullWidth=True,
                            styles={'root': {'height': 'auto', 'padding': '1rem'}},
                            id="silver-valid-card"
                        ),
                        dmc.Space(h=10),
                        
                        # Non-Product Orders Card
                        dmc.Button(
                            children=[
                                dmc.Stack([
                                    dmc.Group([
                                        dmc.Title("Non-Product Orders", order=3),
                                        dmc.HoverCard(
                                            shadow="md",
                                            width=300,
                                            children=[
                                                dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help", "color": "dark"})),
                                                dmc.HoverCardDropdown([
                                                    dmc.Text(
                                                        "Orders identified as services, fees, taxes, or other "
                                                        "non-product items. These are filtered from product analysis.",
                                                        size="sm"
                                                    )
                                                ])
                                            ]
                                        )
                                    ]),
                                    dmc.Text(str(data['counts']['nonproduct']), size="xl", fw="bold")
                                ])
                            ],
                            variant="filled",
                            size="xl",
                            fullWidth=True,
                            styles={'root': {'height': 'auto', 'padding': '1rem'}},
                            id="silver-nonproduct-card"
                        ),
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d", "height": "100%"}
                ),
            ], span=6),
            
            # Right column - Data Quality Issues
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Group([
                            dmc.Title("Data Quality Issues", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=300,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help", "color": "dark"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "Orders that require attention due to data quality issues. "
                                            "Click on each issue type to view the affected records.",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.Space(h=20),
                        dmc.Stack([
                            *[
                                dmc.Button(
                                    children=[
                                        dmc.Group([
                                            dmc.Group([
                                                dmc.Title(exc['exception_type'].replace('_', ' ').title(), order=4),
                                                dmc.HoverCard(
                                                    shadow="md",
                                                    width=300,
                                                    children=[
                                                        dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help", "color": "dark"})),
                                                        dmc.HoverCardDropdown([
                                                            dmc.Text(
                                                                {
                                                                    'sku_not_found': "SKU not found in the seed file. These items need to be properly mapped to a valid SKU.",
                                                                    'vendor_mapping_failed': "Vendor cannot be mapped to the Vendor Master list. Requires vendor mapping review.",
                                                                    'name_mismatch': "The item description doesn't match the standard SKU name despite having the same SKU_id. Needs name standardization."
                                                                }.get(exc['exception_type'], "Orders with data quality issues that need review."),
                                                                size="sm"
                                                            )
                                                        ])
                                                    ]
                                                )
                                            ]),
                                            dmc.Text(str(exc['count']), size="xl", fw="bold")
                                        ])
                                    ],
                                    variant="filled",
                                    size="xl",
                                    fullWidth=True,
                                    styles={'root': {'height': 'auto', 'padding': '1rem'}},
                                    id=f"silver-exception-{exc['exception_type'].lower()}-card"
                                ) for exc in data['counts']['exceptions']
                            ]
                        ])
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d", "height": "100%"}
                ),
            ], span=6),
        ]),
        
        # Bottom half - Table View
        dmc.Paper(
            children=[
                dmc.Group([
                    dmc.Title(f"dev_silver.silver_orders_valid", id="silver-table-title", order=2),
                    dmc.HoverCard(
                        shadow="md",
                        width=300,
                        children=[
                            dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help", "color": "dark"})),
                            dmc.HoverCardDropdown([
                                dmc.Text(
                                    "The table shows the first 100 rows of data from the selected category.",
                                    size="sm"
                                )
                            ])
                        ]
                    )
                ]),
                dmc.ScrollArea(
                    children=html.Div(
                        id='silver-table-preview',
                        children=create_table_component(data['tables']['silver_orders_valid'], 'silver_orders_valid')
                    ),
                    style={"height": 150},
                )
            ],
            p="md",
            style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
        ),
    ])

def create_table_component(df, table_name):
    """Create a styled DataTable component"""
    from dash import dash_table
    
    # Convert DataFrame to records format
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            record[str(col)] = str(val) if pd.notnull(val) else ''
        records.append(record)
    
    return dash_table.DataTable(
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
