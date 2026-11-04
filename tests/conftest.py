import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CATALOG = [
    line.strip()
    for line in (ROOT / "tests" / "data" / "metaquotes_demo_catalog.txt").read_text().splitlines()
    if line.strip()
]
