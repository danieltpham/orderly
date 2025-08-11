# app.py
from dash import Dash
import dash_mantine_components as dmc
from layout.appshell import build_appshell
from utils.theming import terminal_theme

app = Dash(__name__, suppress_callback_exceptions=True, title="ORDERLY DEMO SYSTEM")

app.layout = dmc.MantineProvider(
    theme=terminal_theme, # type: ignore
    withCssVariables=True,
    forceColorScheme="dark",  # Mantine v8 / DMC 2.x way to force dark
    children=build_appshell(),
)

# Register callbacks
from callbacks.sidebar import register_sidebar_callbacks
from callbacks.navigation import register_navigation_callbacks
from callbacks.bronze_callbacks import register_bronze_callbacks
from callbacks.hitl_auto_callbacks import register_hitl_auto_callbacks
from callbacks.hitl_review_callbacks import register_hitl_review_callbacks
from callbacks.silver_callbacks import register_silver_callbacks
from callbacks.gold_callbacks import register_gold_callbacks

register_sidebar_callbacks(app)
register_navigation_callbacks(app)
register_bronze_callbacks(app)
register_hitl_auto_callbacks(app)
register_hitl_review_callbacks(app)
register_silver_callbacks(app)
register_gold_callbacks(app)

server = app.server
