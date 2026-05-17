"""Smoke test for the Config Drift Exceptions pipeline.

Drives build_protection_html with fake data, verifies that:
  * exceptions.json is created with the expected key shape and fields
  * the rendered HTML carries cde-name / cde-cell markers
  * after editing exceptions.json (color + reason), re-rendering paints
    the row first-column background and Config Drift Exceptions cell
    according to the edits

Run from this directory:
    python -m pure_monitor._cde_smoke_test --fake-arrays
"""
import json
import os
import re
import sys

# Force fake-arrays mode before importing the package
if "--fake-arrays" not in sys.argv:
    sys.argv.append("--fake-arrays")

# Add parent so the package import works when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from pure_monitor import protection_report as pr  # noqa: E402
from pure_monitor.common import parse_unified_arrays  # noqa: E402


def _build_fake_per_array():
    """Mimic run_protection_collection_core enough to feed build_protection_html."""
    arrays = ['nyc-pure-fa-01', 'nyc-pure-fb-01']
    per_array = {}
    for a in arrays:
        platform = 'FB' if 'fb' in a else 'FA'
        info = {'platform': platform, 'user': 'demo', 'error': None}
        if platform == 'FA':
            info.update(pr._fake_protection_data_for(a))
        else:
            info.update(pr._fake_fb_protection_data_for(a))
        per_array[a] = info
    return per_array


def main():
    json_path = pr._exceptions_json_path()
    # Backup any existing exceptions.json so the smoke run is reproducible
    backup = None
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as fh:
            backup = fh.read()
        os.remove(json_path)

    try:
        per_array = _build_fake_per_array()
        cfg = {'sla_snap_days': 1, 'sla_repl_days': 1, 'arrays': []}

        html_v1 = pr.build_protection_html(per_array, cfg)

        assert os.path.exists(json_path), "exceptions.json not created"
        data = pr._load_exceptions()
        assert data, "exceptions.json is empty"
        print(f"PASS: exceptions.json created with {len(data)} entries")

        # Key shape: <array>|<type>|<name> with FA-Volume / FA-Filesystem
        # / FB-Filesystem buckets.
        type_counts = {}
        for k, rec in data.items():
            parts = k.split('|', 2)
            assert len(parts) == 3, f"Bad key shape: {k!r}"
            arr, typ, nm = parts
            assert rec['array_name'] == arr
            assert rec['volume_name'] == nm
            assert rec['array_type'] == typ
            assert rec['exception_reason'] == 'None'
            assert rec['color'] in ('green', 'red', 'grey')
            assert rec['Last_Update'] == ''
            type_counts[typ] = type_counts.get(typ, 0) + 1
        print(f"PASS: key shape and schema valid; "
              f"buckets={type_counts}")

        # HTML markers must be present for each row
        name_keys = set(re.findall(
            r'<td class="cde-name" data-ckey="([^"]+)"[^>]*>', html_v1))
        cell_keys = set(re.findall(
            r'<td class="cde-cell" data-ckey="([^"]+)"', html_v1))
        assert name_keys == set(data.keys()), (
            f"cde-name keys mismatch JSON: "
            f"only_html={name_keys-set(data.keys())}, "
            f"only_json={set(data.keys())-name_keys}")
        assert cell_keys == set(data.keys()), \
            "cde-cell keys mismatch JSON"
        print(f"PASS: HTML row markers present for all "
              f"{len(name_keys)} rows")

        # Mutate one entry and verify the overlay paints it
        sample_key = sorted(data.keys())[0]
        data[sample_key]['exception_reason'] = 'Dev/Test - No SLA'
        data[sample_key]['color'] = 'grey'
        data[sample_key]['Last_Update'] = '05-17-2026-09:15:00'
        pr._save_exceptions(data)

        html_v2 = pr.update_html_with_exceptions(html_v1)
        # The mutated row's cde-name should now carry the grey bg
        grey_bg = '#e9ecef'
        m = re.search(
            r'<td class="cde-name" data-ckey="' + re.escape(sample_key)
            + r'"[^>]*style="background:([^;"]+)',
            html_v2)
        assert m and m.group(1) == grey_bg, \
            f"cde-name background not painted; saw: {m.group(1) if m else 'no match'}"
        assert 'Dev/Test - No SLA' in html_v2, \
            "exception_reason not injected into HTML"
        assert '05-17-2026-09:15:00' in html_v2, \
            "Last_Update not injected into HTML"
        print("PASS: post-process overlay applied color + reason + "
              "timestamp for edited row")

        print("\nALL SMOKE CHECKS PASSED")
        return 0
    finally:
        # Restore prior state
        if backup is not None:
            with open(json_path, 'w', encoding='utf-8') as fh:
                fh.write(backup)
        elif os.path.exists(json_path):
            os.remove(json_path)


if __name__ == '__main__':
    raise SystemExit(main())
