"""Everpure — Pure Storage Alert and Replication SLA Monitor.

This package replaces the legacy single-file pure_monitor.py. The
public symbol surface is preserved verbatim by re-exporting every name
that used to live at module scope, so external callers and existing
smoke tests that do ``import pure_monitor as pm; pm._foo()`` keep
working unchanged.

Layering (no cycles):

    common  <-- alert_report
            <-- protection_report
            <-- gui                <-- __main__
"""
from .common import *           # noqa: F401, F403
from .alert_report import *     # noqa: F401, F403
from .protection_report import *  # noqa: F401, F403
from .gui import PureMonitorApp  # noqa: F401

# Convenience pass-throughs for module-level GUI constants the legacy
# top-level pure_monitor.py exposed (HAS_CTK, ctk, UI_FONT_FAMILY, ...).
from .gui import (  # noqa: F401
    HAS_CTK, HAS_PIL, HAS_TKSHEET, ctk,
    UI_FONT_FAMILY, SHEET_GRID_FG,
    DEFAULT_FB_ARRAYS, DEFAULT_FA_FILE_ARRAYS, DEFAULT_FA_BLOCK_ARRAYS,
    DEFAULT_FB_LOCATIONS, DEFAULT_FA_FILE_LOCATIONS, DEFAULT_FA_BLOCK_LOCATIONS,
    DEFAULT_EXCLUDED_ALERTS,
)
