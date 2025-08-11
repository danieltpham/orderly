# components/ui.py
from dash import html
import dash_mantine_components as dmc
from dash_app.assets.config import STEP_CONTENT, STEPS

def narrative_box(stage_key: str):
    data = STEP_CONTENT.get(stage_key, {})
    return dmc.Accordion(
        chevronPosition="right",
        variant="separated",
        multiple=True,
        children=[
            dmc.AccordionItem(
                [
                    dmc.AccordionControl(title),
                    dmc.AccordionPanel(
                        dmc.List([dmc.ListItem(item) for item in items], size="sm")
                    ),
                ],
                value=title,
            )
            for title, items in data.items()
        ],
        value = ["Purpose"]
    )

def app_header():
    return dmc.AppShellHeader(
        dmc.Group(
            [dmc.Title("ORDERLY dbt showcase"), dmc.Divider(orientation="vertical"), dmc.Title(">> v2.1.0 // (C) DANIEL PHAM // READY <<")],
            h="100%", px="md",
        )
    )

def app_navbar():
    return dmc.AppShellNavbar(
        p="md",
        style={"overflowY": "auto", "maxHeight": "100vh"},
        children=[
            dmc.Title("dbt stages", order=3, mb="xs"),
            dmc.Stepper(
                id="stage-stepper",
                active=0,
                orientation="vertical",
                allowNextStepsSelect=True,
                size="sm",
                children=[
                        dmc.StepperStep(
                        label=label,
                        description=desc,
                        allowStepClick=True,
                        allowStepSelect=True
                    )
                    for _, label, desc in STEPS
                ] + [dmc.StepperCompleted()],
            ),
            dmc.Divider(my="md"),
            html.Div(id="stage-content", children=narrative_box(STEPS[0][0])),
        ],
    )

def app_aside():
    return dmc.AppShellAside(dmc.Text("Aside"), p="md")

def app_footer():
    return dmc.AppShellFooter(dmc.Text("Footer"), p="md")

def app_main():
    return dmc.AppShellMain(dmc.Text(" "))
