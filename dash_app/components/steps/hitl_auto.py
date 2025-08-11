from dash import html
import dash_mantine_components as dmc
from pathlib import Path
import pandas as pd

def get_sku_list():
    """Get list of SKUs from the latest curation file"""
    exports_dir = Path(__file__).parents[3] / 'data' / 'intermediate' / 'curation_exports'
    latest_file = "sku_name_curation_20250810.csv"  # Hardcoded for demo
    curation_file = exports_dir / latest_file
    
    if not curation_file.exists():
        return []
    
    df = pd.read_csv(curation_file, encoding='utf-8')
    return [{'label': str(sku), 'value': str(sku)} for sku in df['sku_id'].unique()]

def create_hitl_auto_layout():
    """Create the layout for HITL Auto-curation view"""
    layout = dmc.Stack([
        # SKU selector
        dmc.Select(
            id='hitl-sku-dropdown',
            data=get_sku_list(), # type: ignore
            label="Select SKU ID",
            placeholder="Choose a SKU to review",
            style={"maxWidth": "100%"},
        ),
        
        # Results display
        dmc.Grid([
            # Left side - Original names
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Group([
                            dmc.Title("Raw Item Names", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=300,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "Original product descriptions from orders data, "
                                            "before any standardization or cleaning.",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.ScrollArea(
                            children=dmc.List(
                                id='hitl-raw-names',
                                style={"color": "#5ADA8C"}
                            ),
                            style={"height": 350},
                        )
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
                )
            ], span=6),
            
            # Right side - Canonical analysis
            dmc.GridCol([
                dmc.Paper(
                    children=[
                        dmc.Stack([
                            # Canonical tokens with highlighting
                            dmc.Group([
                                dmc.Title("Canonical Tokens", order=3),
                                dmc.HoverCard(
                                    shadow="md",
                                    width=300,
                                    children=[
                                        dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                        dmc.HoverCardDropdown([
                                            dmc.Text(
                                                "Standardized tokens identified from product names after text cleaning and normalization. "
                                                "These are the most representative terms found across all variations.",
                                                size="sm"
                                            )
                                        ])
                                    ]
                                )
                            ]),
                            html.Div(id='hitl-canonical-tokens'),
                            
                            # Top matches
                            dmc.Divider(variant="dashed", my="sm"),
                            dmc.Group([
                                dmc.Title("Top Matches", order=3),
                                dmc.HoverCard(
                                    shadow="md",
                                    width=300,
                                    children=[
                                        dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                        dmc.HoverCardDropdown([
                                            dmc.Text(
                                                "Best matching product names ranked by similarity to canonical tokens. "
                                                "These are the most standardized and representative product descriptions.",
                                                size="sm"
                                            )
                                        ])
                                    ]
                                )
                            ]),
                            dmc.List(
                                id='hitl-matches',
                                style={"color": "#5ADA8C"}
                            ),
                            
                            # Decision
                            dmc.Divider(variant="dashed", my="sm"),
                            dmc.Group([
                                dmc.Title("Auto-decision", order=3),
                                dmc.HoverCard(
                                    shadow="md",
                                    width=300,
                                    children=[
                                        dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                        dmc.HoverCardDropdown([
                                            dmc.Text(
                                                "'AUTO' if a high-confidence match was found (score > 80 and clear winner). "
                                                "'NEED_APPROVAL' if manual review is required for this SKU.",
                                                size="sm"
                                            )
                                        ])
                                    ]
                                ),
                                dmc.Badge(
                                    id='hitl-decision',
                                    variant="filled",
                                )
                            ])
                        ])
                    ],
                    p="md",
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d"}
                )
            ], span=6),
        ]),
    ])
    
    return layout
