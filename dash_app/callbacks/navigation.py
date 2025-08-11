from dash.dependencies import Input, Output
from dash import callback, html
from components.steps.bronze import create_bronze_layout

def register_navigation_callbacks(app):
    @app.callback(
        Output("main-content", "children"),
        [Input("stage-stepper", "active")]
    )
    def update_main_content(active_step):
        # Mapping of step index to content
        if active_step == 0:  # Bronze stage
            return create_bronze_layout()
        return html.Div("Content for other stages coming soon...")
