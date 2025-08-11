# utils/theming.py

TERMINAL = [
    "#00a67d", "#009673", "#008669", "#00765e", "#006654",
    "#26b893", "#4dc5a6", "#80d6bd", "#b3e6d4", "#d9f2e8",
]

terminal_theme = {
    "colorScheme": "dark",
    "colors": {"terminal": TERMINAL},
    "primaryColor": "terminal",
    "primaryShade": {"light": 6, "dark": 4},
    "fontFamily": "'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
    "defaultRadius": "xs",
    "headings": {
        "fontFamily": "'VT323', monospace",
        "color": "#00d28b",
        "fontFamily": "VT323, monospace",
        "textTransform": "uppercase",
        "fontSize": "1.2rem",
        "fontWeight": 700,
        "letterSpacing": "0.05em",
    },
    "components": {
        "Title": {
            "styles": {"root": {"color": "#00d28b"}}
        },
        "Text": {
            "styles": {"root": {"color": "#00a67d"}}
        },
        "Divider": {
            "styles": {"label": {"color": "#00d28b"}}
        },
        "Button": {
            "styles": {
                "root": {
                    "backgroundColor": "#000000",
                    "color": "#00a67d",
                    "border": "1px solid #00a67d",
                    "fontFamily": "'IBM Plex Mono', monospace",
                    "textTransform": "uppercase",
                    "boxShadow": "0 0 8px rgba(0,166,125,0.25)",
                    "&:hover": {
                        "backgroundColor": "#00a67d",
                        "color": "#000000",
                    },
                }
            }
        },
        "AppShell": {
            "styles": {
                "root": {"backgroundColor": "#000000"},
                "main": {"backgroundColor": "#000000"},
            }
        },
        "Navbar": {"styles": {"root": {"backgroundColor": "#000000"}}},
        "Aside":  {"styles": {"root": {"backgroundColor": "#000000"}}},
        "Header": {"styles": {"root": {"backgroundColor": "#000000"}}},
        "Footer": {"styles": {"root": {"backgroundColor": "#000000"}}},
        "Accordion": {
            "defaultProps": {
                "variant": "default",
                "radius": "xs",
                "chevronPosition": "right"
            },
            "styles": {
                "root": {
                    "--accordion-chevron-size": "18px",
                    "--accordion-radius": "2px",
                    "--accordion-transition-duration": "120ms",
                    "backgroundColor": "transparent",
                },
                "item": {
                    "backgroundColor": "transparent",
                    "border": "1px solid #00a67d",
                    "borderRadius": "2px",
                    "marginBottom": "8px",
                },
                "control": {
                    "backgroundColor": "transparent !important",
                    "padding": "8px 12px",
                    "borderBottom": "1px solid #00a67d",
                    "&[data-active]": {
                        "backgroundColor": "transparent",
                    },
                },
                "label": {
                    "color": "#00d28b",
                    "fontFamily": "VT323, monospace",
                    "textTransform": "uppercase",
                    "fontSize": "1.2rem",
                    "fontWeight": 700,
                    "letterSpacing": "0.05em",
                },
                "chevron": { "color": "#00a67d" },
                "icon":    { "color": "#00a67d" },
                "itemTitle": { "margin": 0 },
                "panel": {
                    "backgroundColor": "transparent",
                    "borderTop": "1px solid #00a67d",
                },
                "content": {
                    "color": "#00a67d",
                    "fontFamily": "'IBM Plex Mono', monospace",
                    "padding": "10px 12px",
                },
            },
        },
        "Stepper": {
            "defaultProps": {
                "size": "sm",
                "radius": "xs",
                "orientation": "vertical",
                "variant": "default",
                "color": "#00a67d",
                "wrap": False
            },
            "styles": {
                "root": {"backgroundColor": "transparent"},
                "separator": {"backgroundColor": "transparent", "borderTop": "1px solid #00a67d", "opacity": 0.8},
                "verticalSeparator": {"backgroundColor": "transparent", "borderLeft": "1px solid #00a67d", "opacity": 0.8},
                "separatorActive": {"borderColor": "#00d28b", "opacity": 1},
                "verticalSeparatorActive": {"borderColor": "#00d28b", "opacity": 1},
                "content": {
                    "backgroundColor": "transparent",
                    "paddingTop": "8px",
                    "color": "#00a67d",
                    "fontFamily": "'IBM Plex Mono', monospace",
                },

                # No box around steps
                "step": {
                    "backgroundColor": "transparent",
                    "padding": 0,
                },

                # Square icon baseline
                "stepIcon": {
                    "backgroundColor": "transparent",
                    "border": "1px solid #00a67d",
                    "color": "#00a67d",
                    "borderRadius": "2px",   # ← square
                    "minWidth": "24px",
                    "height": "24px",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "fontFamily": "'IBM Plex Mono', monospace",
                    "fontWeight": 700,
                },
                "stepCompletedIcon": { "color": "#00a67d" },  # will be overridden to black on completed (above)

                "stepLabel": {
                    "color": "#00d28b",
                    "fontFamily": "VT323, monospace",
                    "textTransform": "uppercase",
                    "fontWeight": 700,
                    "fontSize": "1em",
                    "letterSpacing": "0.02em",
                },
                "stepDescription": {
                    "color": "#00a67d",
                    "fontFamily": "'IBM Plex Mono', monospace",
                    "opacity": 0.95,
                },
            },
        }
    }, # A level inside this
}
