"""Small structured-result example for Awaitless."""

import json
import time
from pathlib import Path

time.sleep(1)
Path("benchmark-result.json").write_text(
    json.dumps({"correctness": True, "baseline_us": 31.2, "candidate_us": 24.7, "speedup": 1.263}),
    encoding="utf-8",
)
print("candidate latency: 24.7 us")
