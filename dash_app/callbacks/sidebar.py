from dash import Input, Output, State
from components.ui import STEPS, app_main, narrative_box
from layout.appshell import build_appshell

def register_navigation_callbacks(app):
    @app.callback(
        Output("appshell", "navbar"),
        Input("burger", "opened"),
        State("appshell", "navbar"),
        prevent_initial_call=True,
    )
    def toggle_navbar(opened, navbar_cfg):
        # Toggle mobile collapse using the header burger
        navbar_cfg = dict(navbar_cfg or {})
        navbar_cfg["collapsed"] = {"mobile": not opened}
        return navbar_cfg

def register_sidebar_callbacks(app):
    @app.callback(
        Output("stage-content", "children"),
        Input("stage-stepper", "active"),
        prevent_initial_call=True,
    )
    def update_main(active_idx):
        if active_idx is None:
            return narrative_box(STEPS[0][0])
        active_idx = max(0, min(active_idx, len(STEPS)-1))
        return narrative_box(STEPS[active_idx][0])