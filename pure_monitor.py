"""Backward-compat shim: forwards ``python pure_monitor.py`` to the package.

The implementation has moved to the ``pure_monitor`` package (see
pure_monitor/__init__.py and pure_monitor/__main__.py). Existing
schedules, shortcuts and batch files that still invoke
``python pure_monitor.py [args]`` keep working through this shim;
new callers should use ``python -m pure_monitor [args]`` directly.
"""
import runpy
runpy.run_module("pure_monitor", run_name="__main__")
