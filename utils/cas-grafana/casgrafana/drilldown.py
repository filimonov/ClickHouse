"""CAS / Drilldown: GC, write-path and S3 detail as collapsed rows on one time axis."""
from . import panels as P
from .panels import OVERVIEW_LINK


def build() -> dict:
    return P.dashboard(
        "cas-drilldown", "CAS / Drilldown",
        "Content-addressed (CAS) storage internals for support engineers and CAS developers.",
        [], tags=["cas", "clickhouse"],
        links=[{"title": "CAS / Overview", "type": "link", "url": OVERVIEW_LINK["url"], "keepTime": True, "includeVars": True, "icon": "dashboard"}])
