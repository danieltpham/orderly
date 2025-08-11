# gold.py
from __future__ import annotations

import dash_mantine_components as dmc

from dash_app.callbacks.gold_callbacks import QUESTIONS

# -------------------------------------------------------------------
# Layout
# -------------------------------------------------------------------

def create_gold_layout():
    """
    UI:
      - Dropdown of business questions (hard-coded above).
      - 50/50 split:
          Left  : SQL text loaded from /sql/<file>.sql
          Right : Summary statements (no plots)
    """
    options = [{"label": v["label"], "value": k} for k, v in QUESTIONS.items()]

    return dmc.Stack([
        # Dropdown
        dmc.Select(
            id="gold-question-dropdown",
            data=options, # type: ignore
            label="Business question",
            placeholder="Choose a question",
            style={"maxWidth": "100%"},
            searchable=True,
        ),

        # Split
        dmc.Grid([
            # LEFT: SQL (read file contents)
            dmc.GridCol([
                dmc.Paper(
                    p="md",
                    children=[
                        dmc.Group([
                            dmc.Title("SQL", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=300,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "Analytical SQL on Gold Star Schema",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.ScrollArea(
                            id="gold-sql-scroll",
                            style={"height": 380},
                            children=dmc.Code(
                                id="gold-sql-code",
                                children="-- Select a question to view SQL",
                                block=True,
                                style={"color": "#5ADA8C"}
                            )
                        )
                    ],
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d", "height": 450}
                )
            ], span=6),

            # RIGHT: Summary statements (no plots)
            dmc.GridCol([
                dmc.Paper(
                    p="md",
                    children=[
                        dmc.Group([
                            dmc.Title("Summary", order=2),
                            dmc.HoverCard(
                                shadow="md",
                                width=320,
                                children=[
                                    dmc.HoverCardTarget(dmc.Text("[?]", style={"cursor": "help"})),
                                    dmc.HoverCardDropdown([
                                        dmc.Text(
                                            "Summary statistics derived from the query results. ",
                                            size="sm"
                                        )
                                    ])
                                ]
                            )
                        ]),
                        dmc.ScrollArea(
                            children=dmc.List(
                                id="gold-summary-list",
                                children=[dmc.ListItem("Select a question to compute summaries.")],
                                size="sm",
                                style={"color": "#5ADA8C"}
                            )
                        )
                    ],
                    style={"backgroundColor": "#0b0f0a", "border": "1px solid #00a67d", "height": 450}
                )
            ], span=6),
        ]),
    ])