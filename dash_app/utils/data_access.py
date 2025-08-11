from pathlib import Path
import pandas as pd
import duckdb

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
SAMPLES = DATA_DIR / "samples"
DB_PATH = DATA_DIR / "orderly.duckdb"  # change to your path

def _connect():
    return duckdb.connect(str(DB_PATH), read_only=True) if DB_PATH.exists() else duckdb.connect(":memory:")

def load_raw_sample(source: str, fmt: str):
    # Minimal demo read
    p = (SAMPLES / ("raw_source_a.jsonl" if source=="Source A" else "raw_source_b.csv"))
    if fmt == "jsonl":
        if not p.exists():
            text = '{ "order_id": 1, "desc": "wireless keybord black" }\n{ "order_id": 2, "desc": "24in led monitor" }'
        else:
            text = p.read_text(encoding="utf-8")
        return text, "json"
    else:
        if not p.exists():
            text = "order_id,desc\n1,wireless keybord black\n2,24in led monitor"
        else:
            text = p.read_text(encoding="utf-8")
        return text, "markup"

def load_bronze_table(source: str):
    # Replace with real query to your Bronze table
    df = pd.DataFrame({
        "order_id":[1,2,3,4,5],
        "raw_desc":[
            "wireless keybord black","24in led monitor",
            "techflow kb wirelss","viewmaster monitor 24\"",
            "usb-c to hdmi 1m"
        ],
        "source":[source]*5
    })
    return df
