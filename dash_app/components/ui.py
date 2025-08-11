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
        value = ["Aim"]
    )

def app_header():
    return dmc.AppShellHeader(
        dmc.Group(
            [dmc.Title("ORDERLY dbt showcase"), dmc.Divider(orientation="vertical"), dmc.Title(">> v2.1.0 // (C) DANIEL PHAM // READY <<", order=3)],
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
    return dmc.AppShellAside(
        dmc.Stack(
            [
                dmc.Title("About this demo", order=3),
                dmc.Text(
                    "An interactive storytelling dashboard using snapshotted data "
                    "to show the ORDERLY pipeline’s complexity and decisions.\n"
                    "Have fun interacting with the UI ^_^"
                ),
                dmc.Accordion(
                    children=[
                        dmc.AccordionItem(
                            [
                                dmc.AccordionControl("UI Features"),
                                dmc.AccordionPanel(
                                    dmc.List(
                                        [
                                            dmc.ListItem("Data generation (mock)"),
                                            dmc.ListItem("Bronze ingestion (raw → DuckDB)"),
                                            dmc.ListItem("HITL curation (SKU & vendor)"),
                                            dmc.ListItem("Silver QC, exceptions, categorisation"),
                                            dmc.ListItem("Gold star schema for BI"),
                                            dmc.ListItem("Final analytics (text summaries)"),
                                        ],
                                        size="sm",
                                    )
                                ),
                            ],
                            value="shows",
                        ),
                        dmc.AccordionItem(
                            [
                                dmc.AccordionControl("Displaying"),
                                dmc.AccordionPanel(
                                    dmc.List(
                                        [
                                            dmc.ListItem("Technical depth across layers"),
                                            dmc.ListItem("Data governance: versioning & QA"),
                                            dmc.ListItem("UX for data operations"),
                                            dmc.ListItem("Attention to detail end-to-end"),
                                        ],
                                        size="sm",
                                    )
                                ),
                            ],
                            value="proves",
                        ),
                    ],
                    multiple=True,
                    variant="separated",
                ),
            ],
            p="md",
            gap="sm",
        ),
        style={"overflowY": "auto", "maxHeight": "100vh"},
    )


def app_footer():
    return dmc.AppShellFooter(
        dmc.Group(
            [
                dmc.Text("A project by © 2025 Daniel Pham", size="sm", c="#00d28b"),

                dmc.Anchor(
                    dmc.Text("[GITHUB]", size="sm", c="#00d28b"),
                    href="https://github.com/danieltpham/orderly",
                    target="_blank"
                ),

                dmc.Anchor(
                    dmc.Text("[DBT DOCS]", size="sm", c="#00d28b"),
                    href="#",
                    target="_blank"
                ),
            ],
            justify="space-between",
            w="100%",
        ),
        p="md",
    )


def app_main():
    from components.steps.bronze import create_bronze_layout
    
    return dmc.AppShellMain(
        html.Div(id="main-content")
    )
