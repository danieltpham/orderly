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
register_sidebar_callbacks(app)

server = app.server
