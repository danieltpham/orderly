from dash.dependencies import Input, Output
from dash import callback, html
from components.steps.bronze import create_bronze_layout
from components.steps.hitl_auto import create_hitl_auto_layout
from components.steps.hitl_review import render as create_hitl_review_layout

def register_navigation_callbacks(app):
    @app.callback(
        Output("main-content", "children"),
        [Input("stage-stepper", "active")]
    )
    def update_main_content(active_step):
        # Mapping of step index to content
        if active_step == 0:  # Bronze stage
            return create_bronze_layout()
        elif active_step == 1:  # HITL Auto stage
            return create_hitl_auto_layout()
        elif active_step == 2:  # HITL Review stage
            return create_hitl_review_layout()
        return html.Div("Content for other stages coming soon...")
