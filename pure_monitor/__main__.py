"""Command-line entry point: ``python -m pure_monitor``.

Launches the GUI by default; ``--nogui`` runs run_nogui() to write the
daily report to disk; ``--alert-debug`` and ``--fake-arrays`` swap in
synthetic data; ``--help`` prints the usage block below.
"""
import datetime
import json
import os
import sys

from .common import (
    FAKE_ARRAYS, _fake_arrays_config,
    parse_time_to_seconds, unified_arrays_from_config_full,
)
from .alert_report import (
    run_collection_core, build_nogui_header,
    append_history_csv, send_html_report, build_status_html,
)
from .gui import PureMonitorApp


def run_nogui():
    # --fake-arrays --nogui produces a Daily HTML report from the synthetic
    # 12-array dataset and skips disk I/O for the source config entirely.
    if FAKE_ARRAYS:
        print("Running in --nogui --fake-arrays mode (synthetic 12-array config).")
        raw = _fake_arrays_config()
    else:
        config_path = "monitor_config.json"
        if not os.path.exists(config_path):
            print(f"Error: {config_path} not found. Please run the GUI first to save a configuration.")
            return
        with open(config_path, 'r', encoding='utf-8') as f:
            raw = json.load(f)

    _arrays = unified_arrays_from_config_full(raw)
    # Pass-through the raw arrays list-of-dicts so per-array auth_user
    # entries survive into the SSH detection path below; only fall back to
    # the (name, location, notes) tuples when no list-of-dicts was present.
    _raw_arrays = raw.get('arrays')
    if isinstance(_raw_arrays, list):
        _arrays_cfg = _raw_arrays
    else:
        _arrays_cfg = [{'name': n, 'location': l, 'notes': nt}
                       for n, l, nt in _arrays]
    cfg = {
        'arrays':   _arrays_cfg,
        'sla_fb':   parse_time_to_seconds(raw.get('sla_fb',  '1h 30m')),
        'sla_faf':  parse_time_to_seconds(raw.get('sla_faf', '1h')),
        'sla_fab':  parse_time_to_seconds(raw.get('sla_fab', '1h')),
        'excluded': [x.strip() for x in raw.get('alerts_excluded', '').replace('\n', ',').split(',')
                     if x.strip() and 'e.g.' not in x],
        'ignore_source_lag': raw.get('ignore_source_lag', False),
        'replication_pairs': raw.get('replication_pairs', []),
    }

    print("Running in --nogui mode. Polling arrays...")
    summary, detailed, stats = run_collection_core(cfg, nogui=True)
    header = build_nogui_header(cfg)
    date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dir_summary = os.path.join(script_dir, "reports", "summary")
    dir_logs    = os.path.join(script_dir, "reports", "logs")
    dir_daily   = os.path.join(script_dir, "reports", "daily")
    for _d in (dir_summary, dir_logs, dir_daily):
        os.makedirs(_d, exist_ok=True)

    summary_path  = os.path.join(dir_summary, f"Pure Alert and Replication Lag Summary {date_str}.log")
    detailed_path = os.path.join(dir_logs,    f"Pure Alert and Replication Lag Logs {date_str}.log")
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(header + summary)
    print(f"Summary saved to: {os.path.abspath(summary_path)}")
    with open(detailed_path, 'w', encoding='utf-8') as f:
        f.write(header + detailed)
    print(f"Detailed log saved to: {os.path.abspath(detailed_path)}")

    # ── Append run data to history CSV ───────────────────────────────────────
    try:
        append_history_csv(stats)
        print("History CSV updated.")
    except Exception as e:
        print(f"Warning: could not update history CSV: {e}")

    # ── Save HTML status report ───────────────────────────────────────────────
    try:
        html_status = build_status_html(stats, cfg)
        html_path = os.path.join(dir_daily, f"Pure Array Report {date_str}.html")
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_status)
        print(f"HTML report saved to: {os.path.abspath(html_path)}")
    except Exception as e:
        print(f"Warning: could not save HTML report: {e}")

    # ── Email HTML report if --email is passed ────────────────────────────────
    if '--email' in sys.argv:
        smtp_server = raw.get('smtp_server', '').strip()
        smtp_port   = raw.get('smtp_port',   '587').strip()
        smtp_from   = raw.get('smtp_from',   '').strip()
        smtp_to     = raw.get('smtp_to',     '').strip()
        smtp_pwd    = os.environ.get('EVERPURE_SMTP_PASSWORD', '')
        missing = [n for n, v in [('smtp_server', smtp_server), ('smtp_from', smtp_from),
                                   ('smtp_to', smtp_to)]
                   if not v]
        if missing:
            print(f"Email skipped — missing configuration: {', '.join(missing)}")
        else:
            try:
                with open(html_path, 'r', encoding='utf-8') as f:
                    html_for_email = f.read()
                send_html_report(html_for_email, smtp_server, smtp_port,
                                 smtp_from, smtp_to, smtp_pwd)
                print(f"Email sent to: {smtp_to}")
            except Exception as e:
                print(f"Warning: email failed: {e}")

    # ── Regenerate Array Health History ───────────────────────────────────────
    try:
        PureMonitorApp._health_history_impl(open_browser=False)
        print("Array Health History updated.")
    except Exception as e:
        print(f"Warning: could not update Array Health History: {e}")




if __name__ == "__main__":
    if '-h' in sys.argv or '--help' in sys.argv:
        print("""
Everpure - Pure Storage Alert and Replication SLA Monitor

USAGE:
    python -m pure_monitor [OPTION]

OPTIONS:
    (no option)    Launch the graphical user interface (GUI).
                   Allows you to configure arrays, credentials, SLA thresholds,
                   excluded alert codes, and run or export reports interactively.

    --nogui        Run headlessly without launching the GUI.
                   Reads all settings from monitor_config.json (created by the
                   GUI's "Save Config" button) and automatically saves four output
                   files under a "reports" subdirectory:
                     reports/summary/ - "Pure Alert and Replication Lag Summary <date>.log"
                     reports/logs/    - "Pure Alert and Replication Lag Logs <date>.log"
                     reports/daily/   - "Pure Array Report <date>.html"
                     reports/         - "Pure_Array_History.html" (always updated)
                   If an array requires a password or cannot be reached, it is
                   skipped and noted in the output files rather than prompting.

    --alert-debug  Launch the GUI with synthetic alert and replication data.
                   No SSH connections are made. Every array in your saved
                   configuration receives a set of fake Critical, Warning,
                   and/or Info alerts so you can test the daily HTML report's
                   colored alert columns and detail pop-up without needing
                   live arrays. Each array is assigned a different alert mix
                   and plausible lag values so the history chart is populated.

    --fake-arrays  Launch the GUI loaded with a synthetic 12-array / 5-location
                   demo dataset (New York, Chicago, Dallas, Seattle, London).
                   Implies --alert-debug. monitor_config.json is neither read
                   nor written, and Save Config is disabled, so an existing
                   real configuration is never touched. Useful for previewing
                   the Daily HTML report and Array Health History pages with
                   a fully populated dataset before deploying to real arrays.
                   Combine with --nogui to generate the Daily report headlessly:
                     python -m pure_monitor --fake-arrays --nogui

    --email        (Use with --nogui) Email the daily HTML report after saving it.
                   SMTP settings must be saved in monitor_config.json via the GUI's
                   Email Configuration section. The SMTP password must be supplied
                   through the environment variable EVERPURE_SMTP_PASSWORD — it is
                   never stored on disk.
                   Supports STARTTLS (port 587, default) and SSL (port 465).
                   Example:
                     set EVERPURE_SMTP_PASSWORD=MyP@ssword
                     python -m pure_monitor --nogui --email

    -h, --help     Show this help message and exit.

EXAMPLES:
    python -m pure_monitor
    python -m pure_monitor --nogui
    python -m pure_monitor --nogui --email
    python -m pure_monitor --alert-debug
    python -m pure_monitor --fake-arrays
    python -m pure_monitor --fake-arrays --nogui
    python -m pure_monitor --help

SSH COMMANDS USED
-----------------
All interaction with the arrays is read-only. No configuration changes
are ever issued. The script runs the following commands over SSH:

  Type detection (run against every configured array)
    purearray list --csv              - identify FlashBlade vs FlashArray
    purepod list --csv                - detect FA-File capability
    purepgroup list --csv             - detect FA-Block capability

  Hardware health
    purehw list --csv                 - enumerate hardware components

  Replication partners
    purearray list --connect --csv    - FlashBlade partners
    purearray connection list --csv   - FlashArray partners

  Alerts
    purealert list --filter "state='open'" --csv

  Replication lag
    purefs replica-link list --csv                              (FB)
    purepod replica-link list --historical 24h --lag --csv      (FA-File)
    purearray list --time                                       (FA-Block clock)
    purevol list --snap --transfer --filter "created >= '...'" --csv  (FA-Block)

CONFIGURATION:
    Launch the GUI at least once and click "Save Config" to create
    monitor_config.json before using --nogui mode. The GUI presents a
    single Arrays spreadsheet (Array + Location columns) instead of
    separate per-type lists — the platform of each array is detected
    automatically via the SSH commands listed above. Column widths
    in the sheet can be dragged to resize and are persisted to the
    JSON file under "arrays_col_widths".
""")
    elif '--nogui' in sys.argv:
        run_nogui()
    elif FAKE_ARRAYS:
        # GUI mode loaded with the synthetic 12-array / 5-location dataset.
        # Real monitor_config.json is neither read nor written; saving is
        # blocked from inside _save_config to protect a real configuration.
        PureMonitorApp().mainloop()
    elif '--alert-debug' in sys.argv:
        # GUI mode with synthetic alert data — no live arrays required.
        PureMonitorApp().mainloop()
    else:

        PureMonitorApp().mainloop()
