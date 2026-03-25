"""get_session_stats tool: Token savings telemetry."""

import time
from typing import Optional

from ..config import get_index_path
from ..storage.data_store import DataStore
from ..storage.token_tracker import get_total_saved, cost_avoided, PRICING


def get_session_stats(storage_path: Optional[str] = None) -> dict:
    """Return cumulative token savings and cost avoided for this server instance."""
    t0 = time.time()
    store = DataStore(base_path=storage_path or str(get_index_path()))
    total_saved = get_total_saved(str(store.base_path))

    total_costs = {
        model: round(total_saved * rate, 4)
        for model, rate in PRICING.items()
    }

    return {
        "result": {
            "total_tokens_saved": total_saved,
            "total_cost_avoided": total_costs,
            "pricing_reference": {
                model: f"${rate * 1_000_000:.2f}/1M tokens"
                for model, rate in PRICING.items()
            },
        },
        "_meta": {
            "timing_ms": round((time.time() - t0) * 1000, 1),
            "tokens_saved": 0,
            "total_tokens_saved": total_saved,
        },
    }
