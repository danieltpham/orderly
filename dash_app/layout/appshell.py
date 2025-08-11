# layout/appshell.py
import dash_mantine_components as dmc
from components.ui import app_header, app_navbar, app_aside, app_footer, app_main

def build_appshell():
    return dmc.AppShell(
        children=[
            app_header(),
            app_navbar(),
            app_main(),
            app_aside(),
            app_footer(),
        ],
        layout="alt",
        header={"height": 60},
        footer={"height": 60},
        navbar={"width": 300, "breakpoint": "sm", "collapsed": {"mobile": True}},
        aside={"width": 300, "breakpoint": "md", "collapsed": {"desktop": False, "mobile": True}},
        padding="md",
        id="appshell",
    )
