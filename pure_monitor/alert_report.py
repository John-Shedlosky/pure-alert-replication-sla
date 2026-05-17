"""Alert and Replication SLA report.

Owns:
* hardware-health and replication-relationship collectors
* run_collection_core (the alert/SLA orchestrator)
* the daily HTML report (build_status_html) and its CLI-mode header
* history CSV append and SMTP email helpers
"""
import datetime
import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from .common import (
    HAS_PARAMIKO,
    FAKE_ARRAYS, ALERT_DEBUG,
    credentials_cache, _alert_collection_lock,
    ask_password_in_main, run_ssh_command,
    parse_time_to_seconds, format_seconds_human,
    _parse_sla_days, _fmt_alert_str,
    _fake_arrays_config, _get_debug_alerts,
    parse_pure_date, parse_arr_loc, align_rel_pairs_by_location,
    _parse_csv_text, _classify_array_output, detect_array_type,
    parse_unified_arrays, unified_arrays_from_config,
    parse_unified_arrays_full, unified_arrays_from_config_full,
    auth_user_for_array,
)


# Platforms share Status-column semantics but use different unhealthy keywords.
_HW_FB_BAD = {'critical', 'unhealthy', 'unknown', 'unrecognized'}
_HW_FA_BAD = {'critical', 'degraded', 'unknown'}


def collect_hw_health(array, user, platform, detailed_logs, nogui=False, idx=0):
    """Run 'purehw list --csv' on *array* as *user* and classify its hardware.

    *platform* is 'FB' or 'FA' (FA-File and FA-Block share the same FlashArray
    hardware columns so they are grouped together). Returns a dict with keys:
      name, platform, header, rows, unhealthy_rows, status_idx, healthy, error.
    """
    import csv as _csv
    import io  as _io
    bad = _HW_FB_BAD if platform == 'FB' else _HW_FA_BAD
    result = {'name': array, 'platform': platform, 'header': [], 'rows': [],
              'unhealthy_rows': [], 'status_idx': -1,
              'healthy': None, 'error': None}

    def _finalize():
        for i, h in enumerate(result['header']):
            if h.strip().lower() == 'status':
                result['status_idx'] = i
                break
        si = result['status_idx']
        if si >= 0:
            for r in result['rows']:
                if si < len(r) and r[si].strip().lower() in bad:
                    result['unhealthy_rows'].append(r)
            result['healthy'] = (len(result['unhealthy_rows']) == 0)

    if ALERT_DEBUG:
        if platform == 'FB':
            result['header'] = ['Name', 'Type', 'Status', 'Speed', 'Details', 'Identify']
            result['rows'] = [
                ['CH1.FM1', 'fm',  'healthy', '-', '-', 'off'],
                ['CH1.FB1', 'fb',  'healthy', '-', '-', 'off'],
                ['CH1.PSU0','psu', 'healthy', '-', '-', 'off'],
            ]
            if idx % 3 == 1:
                result['rows'].append(['CH1.PSU1', 'psu', 'critical', '-', 'Power supply failed', 'off'])
            if idx % 4 == 2:
                result['rows'].append(['CH2.FB3', 'fb', 'unknown', '-', 'Blade unresponsive', 'off'])
        else:
            result['header'] = ['Name', 'Status', 'Identify', 'Slot', 'Index', 'Speed', 'Temperature', 'Voltage', 'Details']
            result['rows'] = [
                ['CH0.BAY0', 'ok',   'off', '0', '0', '-',    '-',   '-', ''],
                ['CH0.BAY1', 'ok',   'off', '0', '1', '-',    '-',   '-', ''],
                ['CT0',      'ok',   'off', '-', '-', '-',    '-',   '-', ''],
            ]
            if idx % 4 == 2:
                result['rows'].append(['CT0.FAN0', 'critical', 'off', '-', '-', '-', '-',   '-', 'Fan failure'])
            if idx % 5 == 3:
                result['rows'].append(['CT1.TMP0', 'degraded', 'off', '-', '-', '-', '72C', '-', 'Temp above threshold'])
        _finalize()
        _title = ",".join(result['header'])
        _body  = "\n".join(",".join(r) for r in result['rows'])
        detailed_logs.append(
            f"=== Command Log: {user}@{array} ===\n> purehw list --csv\n[OUTPUT-DEBUG]\n{_title}\n{_body}\n")
        return result

    try:
        out = run_ssh_command(array, user, "purehw list --csv",
                              log_list=detailed_logs, nogui=nogui)
        reader = _csv.reader(_io.StringIO(out.strip()))
        rows = list(reader)
        if not rows:
            result['error'] = "Empty response"
            return result
        result['header'] = rows[0]
        result['rows']   = rows[1:]
        _finalize()
    except Exception as e:
        result['error'] = str(e)
    return result


def collect_replication_relationships(array, user, platform, detailed_logs,
                                      nogui=False, idx=0, peers=None):
    """Run the array's connection-list command and parse partner arrays.

    FB platform issues 'purearray list --connect --csv'.
    FA platform (FA-File and FA-Block share the same connection schema)
    issues 'purearray connection list --csv'.

    Returns dict with keys: name, platform, header, rows, partners, error.
    Each partner is {'remote', 'status', 'type', 'mgmt_addr'} (extra fields
    blank when the source CSV omits them).
    """
    import csv as _csv
    import io  as _io
    cmd = ("purearray list --connect --csv" if platform == 'FB'
           else "purearray connection list --csv")
    result = {'name': array, 'platform': platform, 'header': [], 'rows': [],
              'partners': [], 'error': None}

    def _idx(name):
        for i, h in enumerate(result['header']):
            if h.strip().lower() == name.lower():
                return i
        return -1

    def _finalize():
        ni = _idx('name')
        si = _idx('status')
        ti = _idx('type')
        mi = _idx('management address')
        if mi < 0:
            mi = _idx('mgmt address')
        for r in result['rows']:
            if not r or ni < 0 or ni >= len(r):
                continue
            remote = r[ni].strip()
            if not remote or remote == array:
                continue
            result['partners'].append({
                'remote':    remote,
                'status':    r[si].strip() if 0 <= si < len(r) else '',
                'type':      r[ti].strip() if 0 <= ti < len(r) else '',
                'mgmt_addr': r[mi].strip() if 0 <= mi < len(r) else '',
            })

    if ALERT_DEBUG:
        candidates = [p for p in (peers or []) if p != array]
        peer = candidates[idx % len(candidates)] if candidates else (array + '_dr')
        if platform == 'FB':
            result['header'] = ['Name', 'ID', 'Status', 'Throttle', 'Type']
            result['rows']   = [[peer, 'aaaa-bbbb-cccc-dddd', 'connected', '-', 'replication']]
        else:
            result['header'] = ['Name', 'Type', 'Throttled', 'Status',
                                'Management Address', 'Replication Address', 'Version']
            result['rows']   = [[peer, 'replication', 'false', 'connected',
                                 '10.10.10.10', '10.20.20.20', '6.5.0']]
        _finalize()
        _title = ",".join(result['header'])
        _body  = "\n".join(",".join(r) for r in result['rows'])
        detailed_logs.append(
            f"=== Command Log: {user}@{array} ===\n> {cmd}\n[OUTPUT-DEBUG]\n{_title}\n{_body}\n")
        return result

    try:
        out = run_ssh_command(array, user, cmd,
                              log_list=detailed_logs, nogui=nogui)
        reader = _csv.reader(_io.StringIO(out.strip()))
        rows = list(reader)
        if not rows:
            result['error'] = "Empty response"
            return result
        result['header'] = rows[0]
        result['rows']   = rows[1:]
        _finalize()
    except Exception as e:
        result['error'] = str(e)
    return result


def run_collection_core(config, nogui=False, progress_cb=None):
    import csv, io
    alert_lines = []
    repl_lines = []
    detailed_logs = []
    alerted_arrays = set()
    alert_counts = {}   # array -> {'info': n, 'warning': n, 'critical': n, 'error': bool}
    array_stats  = []   # list of per-array dicts for Word report
    hw_by_array  = {}   # array_name -> hardware-health dict (from collect_hw_health)
    hw_lines     = []   # lines for the top-of-report Hardware Health summary
    rel_by_array = {}   # array_name -> replication-relationship dict
    rel_lines    = []   # lines for the bottom-of-report Replication Relationships summary

    # Optional progress hook. The GUI passes a thread-safe callback that
    # posts to the main-thread status label under the busy spinner; the
    # nogui path leaves it None and the helper becomes a no-op.
    def _p(msg):
        if progress_cb is None:
            return
        try:
            progress_cb(msg)
        except Exception:
            pass

    # ── Unified array list → per-type buckets ────────────────────────────────
    # When the config supplies a single ``arrays`` list (new-style, from the
    # consolidated tksheet), probe each array once via SSH to classify it and
    # populate arr_fb / arr_faf / arr_fab / loc_fb / loc_faf / loc_fab. Arrays
    # that turn out to be "No Replication FA" roll into arr_fab so existing
    # FA-Block-style alert + hardware-health checks still cover them.
    _unified = config.get('arrays')
    if _unified is not None:
        _bfb_a, _bfb_l   = [], []
        _bfaf_a, _bfaf_l = [], []
        _bfab_a, _bfab_l = [], []
        # Run up to 4 array-type detections concurrently. Detection is
        # I/O-bound on paramiko socket reads (purearray/purepod/purepgroup),
        # so threads give real wall-clock parallelism. Output bucket order
        # is preserved by indexing results by input position and assembling
        # the buckets after all workers finish.
        _arrays_in_order = list(parse_unified_arrays(_unified))
        _results = [None] * len(_arrays_in_order)

        def _detect_one(_idx_pair):
            _idx, (_name, _loc) = _idx_pair
            _p(f"Detecting array {_name} type...")
            _per_users = [('Array', auth_user_for_array(_name, config))]
            try:
                info = detect_array_type(_name, _per_users,
                                         detailed_logs=detailed_logs,
                                         nogui=nogui)
            except Exception as e:
                info = {'is_fb': False, 'is_faf': False, 'is_fab': False,
                        'is_nrp': False, 'user': None, 'error': str(e)}
            return (_idx, _name, _loc, info)

        if _arrays_in_order:
            _workers = min(4, len(_arrays_in_order))
            with ThreadPoolExecutor(max_workers=_workers) as _ex:
                for _idx, _name, _loc, info in _ex.map(
                        _detect_one, list(enumerate(_arrays_in_order))):
                    _results[_idx] = (_name, _loc, info)

        for _entry in _results:
            if _entry is None:
                continue
            _name, _loc, info = _entry
            if info.get('error') and not any((info['is_fb'], info['is_faf'],
                                              info['is_fab'], info['is_nrp'])):
                detailed_logs.append(
                    f"[DETECT] {_name} - classification failed: {info['error']}\n")
                continue
            if info['is_fb']:
                _bfb_a.append(_name);  _bfb_l.append(_loc)
            if info['is_faf']:
                _bfaf_a.append(_name); _bfaf_l.append(_loc)
            if info['is_fab'] or info['is_nrp']:
                _bfab_a.append(_name); _bfab_l.append(_loc)
        config['arr_fb']  = _bfb_a;  config['loc_fb']  = _bfb_l
        config['arr_faf'] = _bfaf_a; config['loc_faf'] = _bfaf_l
        config['arr_fab'] = _bfab_a; config['loc_fab'] = _bfab_l

    # Array -> location map. Locations are line-aligned with their array lists.
    # If an FA array appears in both FA-File and FA-Block lists, the first
    # non-empty entry wins so we don't drop a location because the second list
    # left that slot blank.
    def _zip_loc(_arrs, _locs):
        _out = {}
        for _idx, _name in enumerate(_arrs):
            if not _name:
                continue
            _loc = _locs[_idx].strip() if _idx < len(_locs) and _locs[_idx] else ''
            _out[_name] = _loc
        return _out
    _loc_by_array = {}
    for _d in (_zip_loc(config.get('arr_fb',  []), config.get('loc_fb',  [])),
               _zip_loc(config.get('arr_faf', []), config.get('loc_faf', [])),
               _zip_loc(config.get('arr_fab', []), config.get('loc_fab', []))):
        for _k, _v in _d.items():
            if _k not in _loc_by_array or (not _loc_by_array[_k] and _v):
                _loc_by_array[_k] = _v

    # Array -> notes map. Sourced from the unified ``arrays`` list-of-dicts
    # (the per-type arr_fb/arr_faf/arr_fab buckets do not carry notes).
    _notes_by_array = {}
    for _item in (config.get('arrays') or []):
        if isinstance(_item, dict):
            _n = str(_item.get('name', '') or '').strip()
            _nt = str(_item.get('notes', '') or '').strip()
            if _n and _nt:
                _notes_by_array[_n] = _nt

    # ── Hardware health (purehw list) — run once per unique array ────────────
    # FA-File and FA-Block share the same FlashArray hardware columns, so an
    # array appearing in both lists is probed just once as platform 'FA'.
    _hw_targets = []  # ordered list of (array, user, platform)
    _hw_seen    = set()
    for _a in config.get('arr_fb', []):
        if _a and _a not in _hw_seen:
            _hw_targets.append((_a, auth_user_for_array(_a, config), 'FB'))
            _hw_seen.add(_a)
    for _a in config.get('arr_faf', []):
        if _a and _a not in _hw_seen:
            _hw_targets.append((_a, auth_user_for_array(_a, config), 'FA'))
            _hw_seen.add(_a)
    for _a in config.get('arr_fab', []):
        if _a and _a not in _hw_seen:
            _hw_targets.append((_a, auth_user_for_array(_a, config), 'FA'))
            _hw_seen.add(_a)
    # Up to 4 hardware-health probes run concurrently. Each worker only
    # touches its own array_name key in hw_by_array, and the per-array
    # summary line is returned so the caller can extend hw_lines in the
    # same order as _hw_targets (preserving report layout).
    def _hw_one(_arg):
        _i, (_a, _u, _plat) = _arg
        _p(f"Collecting array {_a} Hardware Health...")
        info = collect_hw_health(_a, _u, _plat, detailed_logs, nogui=nogui, idx=_i)
        if info.get('error'):
            line = f"{_a} - Hardware Health: Error ({info['error']})"
        elif info.get('healthy') is True:
            line = f"{_a} - Hardware Health: Healthy"
        elif info.get('healthy') is False:
            _names = [r[0] for r in info['unhealthy_rows'] if r]
            line = (f"{_a} - Hardware Health: Unhealthy "
                    f"({len(info['unhealthy_rows'])} issue(s): {', '.join(_names)})")
        else:
            line = f"{_a} - Hardware Health: Unknown (no Status column)"
        return _a, info, line

    if _hw_targets:
        _workers = min(4, len(_hw_targets))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _a, info, line in _ex.map(_hw_one, list(enumerate(_hw_targets))):
                hw_by_array[_a] = info
                hw_lines.append(line)

    # ── Replication relationships — run once per unique array ─────────────────
    # FB arrays use 'purearray list --connect'; FA arrays (File and Block share
    # the same connection schema) use 'purearray connection list'. An FA array
    # appearing in both lists is probed exactly once as platform 'FA'.
    _fb_arrs = [a for a in config.get('arr_fb', []) if a]
    _faf_arrs = [a for a in config.get('arr_faf', []) if a]
    _fab_arrs = [a for a in config.get('arr_fab', []) if a]
    _fa_arrs  = list(dict.fromkeys(_faf_arrs + _fab_arrs))
    _rel_targets = []   # ordered list of (array, user, platform, peers)
    _rel_seen    = set()
    for _a in _fb_arrs:
        if _a not in _rel_seen:
            _rel_targets.append((_a, auth_user_for_array(_a, config), 'FB', _fb_arrs))
            _rel_seen.add(_a)
    for _a in _fa_arrs:
        if _a not in _rel_seen:
            _rel_targets.append((_a, auth_user_for_array(_a, config), 'FA', _fa_arrs))
            _rel_seen.add(_a)
    # Up to 4 partner-list probes run concurrently. Each worker only
    # writes its own array_name key in rel_by_array, so no lock is
    # needed for the merge.
    def _rel_one(_arg):
        _i, (_a, _u, _plat, _peers) = _arg
        _p(f"Collecting array {_a} Partners...")
        info = collect_replication_relationships(
            _a, _u, _plat, detailed_logs, nogui=nogui, idx=_i, peers=_peers)
        return _a, info

    if _rel_targets:
        _workers = min(4, len(_rel_targets))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _a, info in _ex.map(_rel_one, list(enumerate(_rel_targets))):
                rel_by_array[_a] = info

    def _alert_dict(array):
        """Return alert severity fields ready to unpack into array_stats entries."""
        ac = alert_counts.get(array, {})
        return {
            'info_alerts':    ac.get('info',     0),
            'warning_alerts': ac.get('warning',  0),
            'critical_alerts':ac.get('critical', 0),
            'alert_error':    ac.get('error',    False),
            'alert_details':  ac.get('alerts',   []),
        }

    def is_ignored(line, ignored):
        for ex in ignored:
            if '-' in ex:
                parts = ex.split('-')
                if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
                    low, high = int(parts[0]), int(parts[1])
                    for n in re.findall(r'\b\d+\b', line):
                        if low <= int(n) <= high: return True
            elif ex in line: return True
        return False

    def format_csv(csv_lines, prefixes):
        reader = csv.reader(csv_lines)
        rows = list(reader)
        if not rows: return []
        widths = []
        for row in rows:
            for j, cell in enumerate(row):
                if j >= len(widths): widths.append(len(cell.strip()))
                else: widths[j] = max(widths[j], len(cell.strip()))
        widths = [w + 2 for w in widths]
        res = []
        for i, row in enumerate(rows):
            pref = prefixes[i] if i < len(prefixes) else prefixes[-1]
            line = "".join([cell.strip().ljust(widths[j]) for j, cell in enumerate(row)])
            res.append(f"{pref.ljust(50)} {line}")
        return res

    # Sentinel used inside the replication loops to bypass SSH in debug mode.
    class _AlertDebugSkip(Exception): pass

    # Stable per-array debug index. With concurrent execution the previous
    # running counter (and list(alert_counts.keys()).index(array)) would
    # produce non-deterministic indices; precomputing once over the union
    # of arr_fb / arr_faf / arr_fab guarantees each array always picks
    # the same synthetic alert pattern regardless of completion order.
    _debug_idx_by_array = {}
    for _a in (list(config.get('arr_fb', []))
               + list(config.get('arr_faf', []))
               + list(config.get('arr_fab', []))):
        if _a and _a not in _debug_idx_by_array:
            _debug_idx_by_array[_a] = len(_debug_idx_by_array)

    def check_alert(array, user, local_alert_lines):
        """Thread-safe alert collection.

        Appends report lines to *local_alert_lines* (a per-worker buffer)
        and writes the counts dict into the shared alert_counts under
        _alert_collection_lock. Cross-loop dedup via alerted_arrays so an
        array that appears in multiple type buckets only runs alerts once.
        """
        with _alert_collection_lock:
            if array in alerted_arrays:
                return
            alerted_arrays.add(array)
        _p(f"Collecting array {array} Alerts...")

        if ALERT_DEBUG:
            counts, log_lines, _avg, _max = _get_debug_alerts(
                array, _debug_idx_by_array.get(array, 0))
            with _alert_collection_lock:
                alert_counts[array] = counts
            local_alert_lines.append(f"[ALERT-DEBUG] {array} - "
                                f"{counts['critical']} Critical, "
                                f"{counts['warning']} Warning, "
                                f"{counts['info']} Info (synthetic data)")
            local_alert_lines.extend(log_lines)
            local_alert_lines.append("")
            return

        try:
            out = run_ssh_command(array, user, "purealert list --filter \"state='open'\" --csv", log_list=detailed_logs, nogui=nogui)
            lines = out.splitlines()
            valid = []
            header = None
            sev_idx = -1
            for l in lines:
                if "ID" in l and "Code" in l:
                    header = l
                    hdr_fields = list(csv.reader([l]))[0]
                    for hi, h in enumerate(hdr_fields):
                        if 'sever' in h.lower():
                            sev_idx = hi
                            break
                    continue
                if is_ignored(l, config['excluded']): continue
                valid.append(l)
            counts = {'info': 0, 'warning': 0, 'critical': 0, 'error': False, 'alerts': []}
            hdr_fields = list(csv.reader([header]))[0] if header else []
            if valid:
                for row_str in valid:
                    fields = list(csv.reader([row_str]))[0]
                    sev = fields[sev_idx].strip().lower() if 0 <= sev_idx < len(fields) else ''
                    if 'info' in sev:
                        counts['info'] += 1
                        sev_label = 'info'
                    elif 'warn' in sev:
                        counts['warning'] += 1
                        sev_label = 'warning'
                    else:
                        counts['critical'] += 1
                        sev_label = 'critical'
                    detail = {'_sev': sev_label}
                    for i, hf in enumerate(hdr_fields):
                        detail[hf.strip()] = fields[i].strip() if i < len(fields) else ''
                    counts['alerts'].append(detail)
                with _alert_collection_lock:
                    alert_counts[array] = counts
                block = ([header] if header else []) + valid
                prefs = ([f"{array} - Alert Header:"] if header else []) + [f"{array} - Alert:"] * len(valid)
                local_alert_lines.extend(format_csv(block, prefs))
            else:
                with _alert_collection_lock:
                    alert_counts[array] = counts
                local_alert_lines.append(f"{array} - Alerts: Healthy")
        except Exception as e:
            with _alert_collection_lock:
                alert_counts[array] = {'info': 0, 'warning': 0, 'critical': 0, 'error': True}
            local_alert_lines.append(f"{array} - Alerts Error: {str(e)}")
        local_alert_lines.append("")

    # FB Loop -- up to 4 arrays processed concurrently. Each worker writes
    # to its own buffers and returns them so the main thread can extend the
    # shared alert_lines/repl_lines/array_stats in input order.
    def _fb_one(array):
        local_alert_lines = []
        local_repl_lines = []
        check_alert(array, auth_user_for_array(array, config), local_alert_lines)
        _p(f"Collecting array {array} Replication...")
        all_lags = []
        repl_rows = []
        stat = None
        try:
            if ALERT_DEBUG:
                _, _, avg_s, max_s = _get_debug_alerts(array, _debug_idx_by_array.get(array, 0))
                all_lags = [avg_s, max_s]
                _rp_time = (datetime.datetime.now() - datetime.timedelta(seconds=int(avg_s))).strftime('%Y-%m-%d %H:%M:%S')
                repl_rows = [
                    {'Name': 'pod1::fs-prod',   'Direction': 'inbound',  'Remote Array': 'remote-fb-01',
                     'Policy': 'auto',           'Status': 'replicating', 'Recovery Point': _rp_time,
                     'Lag': f'{avg_s/60:.1f}m',  'Link Type': 'asynchronous',
                     'SLA Status': 'Exceeded' if avg_s > config['sla_fb'] else 'OK'},
                    {'Name': 'pod1::fs-backup', 'Direction': 'outbound', 'Remote Array': 'remote-fb-02',
                     'Policy': 'auto',           'Status': 'replicating', 'Recovery Point': _rp_time,
                     'Lag': f'{max_s/60:.1f}m',  'Link Type': 'asynchronous',
                     'SLA Status': 'Exceeded' if max_s > config['sla_fb'] else 'OK'},
                ]
                if max_s > config['sla_fb']:
                    local_repl_lines.append(f"[ALERT-DEBUG] {array} - FB Replication SLA exceeded "
                                      f"(simulated max lag {format_seconds_human(max_s)} vs SLA {format_seconds_human(config['sla_fb'])})")
                else:
                    local_repl_lines.append(f"[ALERT-DEBUG] {array} - FB Replication: Healthy (synthetic data)")
                raise _AlertDebugSkip()
            out = run_ssh_command(array, auth_user_for_array(array, config), "purefs replica-link list --csv", log_list=detailed_logs, nogui=nogui)
            rows = list(csv.reader(io.StringIO(out)))
            lag_idx, header = -1, None
            bad = []
            if rows:
                header = ",".join(rows[0])
                for i, h in enumerate(rows[0]):
                    if "Lag" in h: lag_idx = i
                for r in rows[1:]:
                    # Always capture every row so all columns appear in the detail view
                    row_dict = {rows[0][i].strip(): r[i].strip() if i < len(r) else ''
                                for i in range(len(rows[0]))}
                    if lag_idx != -1 and lag_idx < len(r):
                        try:
                            tmin = float(r[lag_idx].strip().replace("m", ""))
                            act = tmin * 60
                            all_lags.append(act)
                            if act > config['sla_fb']: bad.append((",".join(r), act, config['sla_fb']))
                            row_dict['SLA Status'] = 'Exceeded' if act > config['sla_fb'] else 'OK'
                        except:
                            row_dict['SLA Status'] = '—'
                    else:
                        row_dict['SLA Status'] = '—'
                    repl_rows.append(row_dict)
            if bad:
                block, prefs = [], []
                if header: block.append(header); prefs.append(f"{array} - Repl Header:")
                for line, act, req in bad:
                    block.extend([line, f"SLA = {format_seconds_human(req)} vs Actual = {format_seconds_human(act)} --- A SLA violation of {format_seconds_human(act-req)}"])
                    prefs.extend([f"{array} - Repl Exceeded:", f"{array} - SLA Status:"])
                local_repl_lines.extend(format_csv(block, prefs))
            else: local_repl_lines.append(f"{array} - FB Replication: Healthy")
            stat = {'name': array, 'type': 'FB',
                    **_alert_dict(array),
                    'sla_target': config['sla_fb'],
                    'avg_lag': sum(all_lags)/len(all_lags) if all_lags else None,
                    'max_lag': max(all_lags) if all_lags else None,
                    'repl_details': repl_rows}
        except _AlertDebugSkip:
            stat = {'name': array, 'type': 'FB',
                    **_alert_dict(array),
                    'sla_target': config['sla_fb'],
                    'avg_lag': sum(all_lags)/len(all_lags) if all_lags else None,
                    'max_lag': max(all_lags) if all_lags else None,
                    'repl_details': repl_rows}
        except Exception as e:
            local_repl_lines.append(f"{array} - Repl Error: {str(e)}")
            stat = {'name': array, 'type': 'FB',
                    **_alert_dict(array),
                    'sla_target': config['sla_fb'],
                    'avg_lag': None, 'max_lag': None,
                    'repl_details': []}
        local_repl_lines.append("")
        return local_alert_lines, local_repl_lines, stat

    if config['arr_fb']:
        _workers = min(4, len(config['arr_fb']))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _la, _lr, _st in _ex.map(_fb_one, list(config['arr_fb'])):
                alert_lines.extend(_la)
                repl_lines.extend(_lr)
                if _st is not None:
                    array_stats.append(_st)

    # FA-File Loop -- up to 4 arrays concurrent; same buffer-and-merge
    # pattern as FB so the merged output preserves arr_faf input order.
    def _faf_one(array):
        local_alert_lines = []
        local_repl_lines = []
        check_alert(array, auth_user_for_array(array, config), local_alert_lines)
        _p(f"Collecting array {array} Replication...")
        all_avgs, all_maxes = [], []
        repl_rows = []
        stat = None
        try:
            if ALERT_DEBUG:
                _, _, avg_s, max_s = _get_debug_alerts(array, _debug_idx_by_array.get(array, 0))
                all_avgs  = [avg_s]
                all_maxes = [max_s]
                repl_rows = [
                    {'Pod Name': 'pod-prod',   'Direction': 'inbound',
                     'Average Lag': f'{avg_s/60:.1f}m', 'Maximum Lag': f'{max_s/60:.1f}m',
                     'SLA Status': 'Exceeded' if max_s > config['sla_faf'] else 'OK'},
                    {'Pod Name': 'pod-backup', 'Direction': 'inbound',
                     'Average Lag': f'{avg_s/60:.1f}m', 'Maximum Lag': f'{avg_s/60:.1f}m',
                     'SLA Status': 'Exceeded' if avg_s > config['sla_faf'] else 'OK'},
                ]
                if max_s > config['sla_faf']:
                    local_repl_lines.append(f"[ALERT-DEBUG] {array} - FA File Replication SLA exceeded "
                                      f"(simulated max lag {format_seconds_human(max_s)} vs SLA {format_seconds_human(config['sla_faf'])})")
                else:
                    local_repl_lines.append(f"[ALERT-DEBUG] {array} - FA File Replication: Healthy (synthetic data)")
                raise _AlertDebugSkip()
            out = run_ssh_command(array, auth_user_for_array(array, config), "purepod replica-link list --historical 24h --lag --csv", log_list=detailed_logs, nogui=nogui)
            rows = list(csv.reader(io.StringIO(out)))
            avg_idx, max_idx, header = -1, -1, None
            bad = []
            if rows:
                header = ",".join(rows[0])
                for i, h in enumerate(rows[0]):
                    if "Average Lag" in h: avg_idx = i
                    elif "Maximum Lag" in h: max_idx = i
                for r in rows[1:]:
                    try:
                        v1 = float(r[avg_idx].replace("m","")) if avg_idx!=-1 else 0
                        v2 = float(r[max_idx].replace("m","")) if max_idx!=-1 else 0
                        all_avgs.append(v1 * 60)
                        all_maxes.append(v2 * 60)
                        act = max(v1, v2) * 60
                        if act > config['sla_faf']: bad.append((",".join(r), act, config['sla_faf']))
                        row_dict = {rows[0][i].strip(): r[i].strip() if i < len(r) else ''
                                    for i in range(len(rows[0]))}
                        row_dict['SLA Status'] = 'Exceeded' if act > config['sla_faf'] else 'OK'
                        repl_rows.append(row_dict)
                    except: pass
            if bad:
                block, prefs = [], []
                if header: block.append(header); prefs.append(f"{array} - Repl Header:")
                for line, act, req in bad:
                    block.extend([line, f"SLA = {format_seconds_human(req)} vs Actual = {format_seconds_human(act)} --- A SLA violation of {format_seconds_human(act-req)}"])
                    prefs.extend([f"{array} - Repl Exceeded:", f"{array} - SLA Status:"])
                local_repl_lines.extend(format_csv(block, prefs))
            else: local_repl_lines.append(f"{array} - FA File Replication: Healthy")
            stat = {'name': array, 'type': 'FA-File',
                    **_alert_dict(array),
                    'sla_target': config['sla_faf'],
                    'avg_lag': sum(all_avgs)/len(all_avgs) if all_avgs else None,
                    'max_lag': max(all_maxes) if all_maxes else None,
                    'repl_details': repl_rows}
        except _AlertDebugSkip:
            stat = {'name': array, 'type': 'FA-File',
                    **_alert_dict(array),
                    'sla_target': config['sla_faf'],
                    'avg_lag': sum(all_avgs)/len(all_avgs) if all_avgs else None,
                    'max_lag': max(all_maxes) if all_maxes else None,
                    'repl_details': repl_rows}
        except Exception as e:
            local_repl_lines.append(f"{array} - Repl Error: {str(e)}")
            stat = {'name': array, 'type': 'FA-File',
                    **_alert_dict(array),
                    'sla_target': config['sla_faf'],
                    'avg_lag': None, 'max_lag': None,
                    'repl_details': []}
        local_repl_lines.append("")
        return local_alert_lines, local_repl_lines, stat

    if config['arr_faf']:
        _workers = min(4, len(config['arr_faf']))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _la, _lr, _st in _ex.map(_faf_one, list(config['arr_faf'])):
                alert_lines.extend(_la)
                repl_lines.extend(_lr)
                if _st is not None:
                    array_stats.append(_st)

    # FA-Block Loop -- up to 4 arrays concurrent; same pattern as FB/FA-File.
    def _fab_one(array):
        local_alert_lines = []
        local_repl_lines = []
        check_alert(array, auth_user_for_array(array, config), local_alert_lines)
        _p(f"Collecting array {array} Replication...")
        all_diffs = []
        repl_rows = []
        stat = None
        try:
            if ALERT_DEBUG:
                _, _, avg_s, max_s = _get_debug_alerts(array, _debug_idx_by_array.get(array, 0))
                all_diffs = [avg_s, max_s]
                _now_dbg = datetime.datetime.now()
                repl_rows = [
                    {'Name': 'vol-prod-001.snap1',
                     'Created':   (_now_dbg - datetime.timedelta(seconds=int(max_s) + 300)).strftime('%Y-%m-%d %H:%M:%S'),
                     'Completed': (_now_dbg - datetime.timedelta(seconds=300)).strftime('%Y-%m-%d %H:%M:%S'),
                     'Progress': '100%',
                     'Transfer Time': format_seconds_human(int(max_s)),
                     'SLA Status': 'Exceeded' if max_s > config['sla_fab'] else 'OK'},
                    {'Name': 'vol-backup-007.snap2',
                     'Created':   (_now_dbg - datetime.timedelta(seconds=int(avg_s) + 180)).strftime('%Y-%m-%d %H:%M:%S'),
                     'Completed': (_now_dbg - datetime.timedelta(seconds=180)).strftime('%Y-%m-%d %H:%M:%S'),
                     'Progress': '100%',
                     'Transfer Time': format_seconds_human(int(avg_s)),
                     'SLA Status': 'Exceeded' if avg_s > config['sla_fab'] else 'OK'},
                ]
                if max_s > config['sla_fab']:
                    local_repl_lines.append(f"[ALERT-DEBUG] {array} - FA Block Replication SLA exceeded "
                                      f"(simulated max lag {format_seconds_human(max_s)} vs SLA {format_seconds_human(config['sla_fab'])})")
                else:
                    local_repl_lines.append(f"[ALERT-DEBUG] {array} - FA Block Replication: Healthy (synthetic data)")
                raise _AlertDebugSkip()
            time_out = run_ssh_command(array, auth_user_for_array(array, config), "purearray list --time", log_list=detailed_logs, nogui=nogui)
            tm = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', time_out)
            if tm:
                now_dt = parse_pure_date(tm.group(0))
                target = now_dt - datetime.timedelta(days=1)
                cmd = f"purevol list --snap --transfer --filter \"created >= '{target.strftime('%Y-%m-%d %H:%M:%S')}'\" --csv"
                vol_out = run_ssh_command(array, auth_user_for_array(array, config), cmd, log_list=detailed_logs, nogui=nogui)
                rows = list(csv.reader(io.StringIO(vol_out)))
                c_idx, comp_idx, p_idx, header = -1, -1, -1, None
                bad = []
                if rows:
                    header = ",".join(rows[0])
                    for i, h in enumerate(rows[0]):
                        if "Created" in h: c_idx = i
                        elif "Completed" in h: comp_idx = i
                        elif "Progress" in h: p_idx = i
                    for r in rows[1:]:
                        prog = r[p_idx].strip() if p_idx!=-1 else ""
                        is_in_prog = prog not in ["-", "1.0", "100%", ""]
                        if config['ignore_source_lag'] and is_in_prog:
                            continue  # skip source-side (still-in-progress) entries
                        c_dt = parse_pure_date(r[c_idx]) if c_idx!=-1 else None
                        diff = None
                        c_type = ""
                        if is_in_prog and c_dt:
                            diff = (now_dt - c_dt).total_seconds()
                            c_type = f"(at {prog} progress)"
                        elif not is_in_prog:
                            comp_dt = parse_pure_date(r[comp_idx]) if comp_idx!=-1 else None
                            if c_dt and comp_dt:
                                diff = (comp_dt - c_dt).total_seconds()
                                c_type = "(Completed)"
                        if diff is not None:
                            all_diffs.append(diff)
                            if diff > config['sla_fab']: bad.append((",".join(r), diff, config['sla_fab'], c_type))
                            row_dict = {rows[0][i].strip(): r[i].strip() if i < len(r) else ''
                                        for i in range(len(rows[0]))}
                            row_dict['Transfer Time'] = format_seconds_human(int(diff))
                            row_dict['SLA Status'] = 'Exceeded' if diff > config['sla_fab'] else 'OK'
                            # Convert 0-to-1 progress values to percentages for display
                            for _k in list(row_dict.keys()):
                                if 'progress' in _k.lower():
                                    try:
                                        _v = float(row_dict[_k])
                                        if 0.0 <= _v <= 1.0:
                                            row_dict[_k] = f'{_v * 100:.0f}%'
                                    except (ValueError, TypeError):
                                        pass
                            repl_rows.append(row_dict)
                if bad:
                    block, prefs = [], []
                    if header: block.append(header); prefs.append(f"{array} - Block Repl Header:")
                    for line, act, req, ct in bad:
                        block.extend([line, f"SLA = {format_seconds_human(req)} vs Actual = {format_seconds_human(act)} {ct} --- A SLA violation of {format_seconds_human(act-req)}"])
                        prefs.extend([f"{array} - Block Repl SLA Exceeded:", f"{array} - SLA Status:"])
                    local_repl_lines.extend(format_csv(block, prefs))
                else: local_repl_lines.append(f"{array} - FA Block Replication: Healthy")
            stat = {'name': array, 'type': 'FA-Block',
                    **_alert_dict(array),
                    'sla_target': config['sla_fab'],
                    'avg_lag': sum(all_diffs)/len(all_diffs) if all_diffs else None,
                    'max_lag': max(all_diffs) if all_diffs else None,
                    'repl_details': repl_rows}
        except _AlertDebugSkip:
            stat = {'name': array, 'type': 'FA-Block',
                    **_alert_dict(array),
                    'sla_target': config['sla_fab'],
                    'avg_lag': sum(all_diffs)/len(all_diffs) if all_diffs else None,
                    'max_lag': max(all_diffs) if all_diffs else None,
                    'repl_details': repl_rows}
        except Exception as e:
            local_repl_lines.append(f"{array} - Repl Error: {str(e)}")
            stat = {'name': array, 'type': 'FA-Block',
                    **_alert_dict(array),
                    'sla_target': config['sla_fab'],
                    'avg_lag': None, 'max_lag': None,
                    'repl_details': []}
        local_repl_lines.append("")
        return local_alert_lines, local_repl_lines, stat

    if config['arr_fab']:
        _workers = min(4, len(config['arr_fab']))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _la, _lr, _st in _ex.map(_fab_one, list(config['arr_fab'])):
                alert_lines.extend(_la)
                repl_lines.extend(_lr)
                if _st is not None:
                    array_stats.append(_st)

    # Attach hardware-health, replication-relationship, and location metadata
    # to every array_stats entry (an FA array that appears in both FA-File and
    # FA-Block lists gets the same info on both of its stat entries — each
    # probe only ran once).
    for _s in array_stats:
        _s['hw']  = hw_by_array.get(_s['name'])
        _s['rel'] = rel_by_array.get(_s['name'])
        _s['location'] = _loc_by_array.get(_s['name'], '')
        _s['notes']    = _notes_by_array.get(_s['name'], '')

    # Build a deduplicated pair list for the Replication Relationships section.
    def _loc_suffix(_name):
        _loc = _loc_by_array.get(_name, '')
        return f", {_loc}" if _loc else ''

    _pair_seen = set()
    for _a, _info in rel_by_array.items():
        if _info.get('error'):
            rel_lines.append(
                f"{_a} ({_info.get('platform','')}{_loc_suffix(_a)}) "
                f"- Error: {_info['error']}")
            continue
        _plat_a = _info.get('platform', '')
        # NOTE: loop variable is _part (not _p) because _p is the progress
        # callback in this function's scope; binding it to a partner dict
        # here would break the _p("Compiling Reports...") call below.
        for _part in _info.get('partners', []):
            _b      = _part['remote']
            _plat_b = rel_by_array.get(_b, {}).get('platform', _plat_a)
            _key    = tuple(sorted((_a, _b)))
            if _key in _pair_seen:
                continue
            _pair_seen.add(_key)
            _pa = _plat_a if _key[0] == _a else _plat_b
            _pb = _plat_b if _key[1] == _b else _plat_a
            _suffix = ''
            _st = (_part.get('status') or '').strip()
            if _st and _st.lower() != 'connected':
                _suffix = f"  [{_st}]"
            rel_lines.append(
                f"{_key[0]} ({_pa}{_loc_suffix(_key[0])}) <-> "
                f"{_key[1]} ({_pb}{_loc_suffix(_key[1])}){_suffix}")
    for _a, _info in rel_by_array.items():
        if _info.get('error') is None and not _info.get('partners'):
            rel_lines.append(
                f"{_a} ({_info.get('platform','')}{_loc_suffix(_a)}) "
                f"- No replication relationships configured")

    _p("Compiling Reports...")
    final  = "=== HARDWARE HEALTH SECTION ===\n" + "\n".join(hw_lines) + "\n\n"
    final += "=== ALERTS SECTION ===\n" + "\n".join(alert_lines)
    final += "\n=== REPLICATION SECTION ===\n" + "\n".join(repl_lines)
    final += "\n\n=== REPLICATION RELATIONSHIPS SECTION ===\n" + "\n".join(rel_lines)
    return final, "\n".join(detailed_logs), array_stats


def build_nogui_header(config):
    import time
    tz = time.tzname[time.daylight]
    now = datetime.datetime.now().strftime("%A, %B %d, %Y at %I:%M:%S %p")
    header = f"Output from Report run on {now} {tz}\n"
    header += f"Defined Replication SLA for SLA FB: {format_seconds_human(config['sla_fb'])}\n"
    header += f"Defined Replication SLA for SLA FA-File: {format_seconds_human(config['sla_faf'])}\n"
    header += f"Defined Replication SLA for SLA FA-Block: {format_seconds_human(config['sla_fab'])}\n"
    header += f"Alert Codes Ignored: {', '.join(config['excluded']) if config['excluded'] else 'None'}\n"
    ignore_source = "Checked" if config['ignore_source_lag'] else "Unchecked"
    header += f"Ignore Source Side Replica Reporting setting: {ignore_source}\n\n"
    # Prefer the unified ``arrays`` list (name, location). When the detection
    # pass has already populated the per-type buckets, list them by detected
    # type instead so the summary reflects the post-classification picture.
    _unified = config.get('arrays')
    if _unified is not None and not config.get('arr_fb') and not config.get('arr_faf') and not config.get('arr_fab'):
        for _n, _l in parse_unified_arrays(_unified):
            header += f"Array - {_n}" + (f"  ({_l})" if _l else "") + "\n"
    else:
        for a in config.get('arr_fb', []):
            header += f"FB Array - {a}\n"
        header += "\n"
        for a in config.get('arr_faf', []):
            header += f"FA-File Array - {a}\n"
        header += "\n"
        for a in config.get('arr_fab', []):
            header += f"FA-Block Array - {a}\n"
    pairs = config.get('replication_pairs', [])
    if pairs:
        header += "\nReplication Pairs:\n"
        for p in pairs:
            header += (f"  [{p.get('type', '?')}]  "
                       f"{p.get('source', '')}  \u2192  {p.get('destination', '')}    "
                       f"({p.get('name', '')})\n")
    return header + "\n"


def append_history_csv(stats):
    """Append per-array stats from the current run to Pure Array History.csv."""
    import csv as _csv
    fieldnames = ['timestamp', 'array_name', 'type',
                  'info_alerts', 'warning_alerts', 'critical_alerts',
                  'sla_target_sec', 'avg_lag_sec', 'max_lag_sec', 'sla_violated']
    csv_path  = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "Pure Array History.csv")
    ts        = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    write_hdr = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    try:
        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            w = _csv.DictWriter(f, fieldnames=fieldnames)
            if write_hdr:
                w.writeheader()
            for stat in stats:
                sla      = stat.get('sla_target', 0) or 0
                avg      = stat.get('avg_lag')
                mx       = stat.get('max_lag')
                violated = bool(mx is not None and sla and mx > sla)
                w.writerow({
                    'timestamp':       ts,
                    'array_name':      stat['name'],
                    'type':            stat['type'],
                    'info_alerts':     stat.get('info_alerts',     0),
                    'warning_alerts':  stat.get('warning_alerts',  0),
                    'critical_alerts': stat.get('critical_alerts', 0),
                    'sla_target_sec':  int(sla),
                    'avg_lag_sec':     int(avg) if avg is not None else '',
                    'max_lag_sec':     int(mx)  if mx  is not None else '',
                    'sla_violated':    violated,
                })
    except Exception:
        pass   # never block the caller for a write error


def send_html_report(html_content, smtp_server, smtp_port, from_addr, to_str, password):
    """Email *html_content* as an HTML message.

    Automatically selects SSL (port 465) or STARTTLS (all other ports, default 587).
    *to_str* accepts comma- or semicolon-separated recipient addresses.
    Raises an exception on any connection or authentication failure so the caller
    can surface the error to the user.
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    recipients = [a.strip() for a in to_str.replace(';', ',').split(',') if a.strip()]
    if not recipients:
        raise ValueError("No recipient email addresses provided.")

    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"Everpure (Pure Storage) - Evergreen//One - Alert and Replication Status Report - {date_str}"
    msg['From']    = from_addr
    msg['To']      = ', '.join(recipients)
    msg.attach(MIMEText(html_content, 'html'))

    port = int(smtp_port)
    if port == 465:
        with smtplib.SMTP_SSL(smtp_server, port) as s:
            s.login(from_addr, password)
            s.sendmail(from_addr, recipients, msg.as_string())
    else:
        with smtplib.SMTP(smtp_server, port) as s:
            s.ehlo()
            try:
                s.starttls()
            except smtplib.SMTPNotSupportedError:
                pass  # server does not advertise STARTTLS; continue unencrypted
            try:
                s.login(from_addr, password)
            except smtplib.SMTPNotSupportedError:
                pass  # server does not require authentication; continue without it
            s.sendmail(from_addr, recipients, msg.as_string())


def build_status_html(stats, config):
    """Generate and return the HTML status report string from array stats + config."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import io as _io, base64, os, time as _time, html as _html

    tz         = _time.tzname[_time.daylight]
    now_str    = datetime.datetime.now().strftime("%A, %B %d, %Y at %I:%M:%S %p")
    fb_sec     = config['sla_fb']
    faf_sec    = config['sla_faf']
    fab_sec    = config['sla_fab']
    excluded   = config.get('excluded', [])
    ignore_lbl = "Checked" if config.get('ignore_source_lag') else "Unchecked"
    fb_list    = config.get('arr_fb',  [])
    faf_list   = config.get('arr_faf', [])
    fab_list   = config.get('arr_fab', [])

    _img_dir     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
    _status_imgs = ['FB-Green.png', 'FB-Red.png', 'FA-Green.png', 'FA-Red.png']
    _imgs_ok     = all(os.path.exists(os.path.join(_img_dir, n)) for n in _status_imgs)
    _img_cache   = {}
    if _imgs_ok:
        for n in _status_imgs:
            with open(os.path.join(_img_dir, n), 'rb') as f:
                _img_cache[n] = base64.b64encode(f.read()).decode('ascii')

    def _status_cell_html(stat):
        if not _imgs_ok:
            return ''
        total_alerts = (stat.get('critical_alerts', 0) + stat.get('warning_alerts', 0) +
                        stat.get('info_alerts', 0))
        if stat.get('alert_error'):
            total_alerts = 1
        sla    = stat.get('sla_target', 0)
        is_red = (total_alerts != 0 or
                  (stat.get('max_lag') is not None and stat['max_lag'] > sla) or
                  (stat.get('avg_lag') is not None and stat['avg_lag'] > sla) or
                  (stat.get('avg_lag') is None and stat.get('max_lag') is None))
        key = f"{'FB' if stat['type'] == 'FB' else 'FA'}-{'Red' if is_red else 'Green'}.png"
        b64 = _img_cache.get(key, '')
        if not b64:
            return ''
        safe = stat['name'].replace("'", "\\'")
        return (f'<div style="cursor:pointer;display:inline-block;" '
                f'onclick="showArrRel(\'{safe}\')" '
                f'title="Click to view replication relationships">'
                f'<img src="data:image/png;base64,{b64}" '
                f'style="width:100%;max-width:96px;display:block;"></div>')

    def _make_chart_b64(stat):
        sla_min = (stat['sla_target'] or 0) / 60.0
        values  = [sla_min,
                   stat['avg_lag'] / 60.0 if stat['avg_lag'] is not None else 0,
                   stat['max_lag'] / 60.0 if stat['max_lag'] is not None else 0]
        labels  = ['SLA Target', 'Avg Lag', 'Max Lag']
        colors  = ['#5B9BD5' if l == 'SLA Target' else ('#C00000' if v > sla_min else '#70AD47')
                   for l, v in zip(labels, values)]
        fig, ax = plt.subplots(figsize=(2.64, 1.14))
        bars = ax.bar(labels, values, color=colors, width=0.5)
        ax.set_ylabel("min", fontsize=5, labelpad=2)
        ax.tick_params(axis='x', labelsize=5, pad=1)
        ax.tick_params(axis='y', labelsize=5)
        ax.set_ylim(0, max(values) * 1.35 + 0.1)
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.03,
                    f"{val:.1f}", ha='center', va='bottom', fontsize=4.5)
        fig.tight_layout(pad=0.3)
        buf = _io.BytesIO()
        fig.savefig(buf, format='png', dpi=130)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode('ascii')

    array_items  = ''.join(f'      <li>FB: {a}</li>\n'      for a in fb_list)
    array_items += ''.join(f'      <li>FA-File: {a}</li>\n' for a in faf_list)
    array_items += ''.join(f'      <li>FA-Block: {a}</li>\n' for a in fab_list)

    import json as _json

    # Build per-array alert details for the JS modal
    _alert_js = {}
    for stat in stats:
        _alert_js[stat['name']] = stat.get('alert_details', [])
    _alert_js_str = _json.dumps(_alert_js, ensure_ascii=False).replace('</script>', '<\\/script>')

    # Build per-array replication detail data for the replication modal.
    # Key by "name (type)" so an array that appears in both FA-File and FA-Block
    # gets two independent entries rather than the second overwriting the first.
    _repl_js = {}
    for stat in stats:
        _key = f"{stat['name']} ({stat['type']})"
        _repl_js[_key] = {
            'type':    stat['type'],
            'sla':     format_seconds_human(stat.get('sla_target', 0)),
            'avg_lag': format_seconds_human(int(stat['avg_lag'])) if stat.get('avg_lag') is not None else None,
            'max_lag': format_seconds_human(int(stat['max_lag'])) if stat.get('max_lag') is not None else None,
            'rows':    stat.get('repl_details', []),
        }
    _repl_js_str = _json.dumps(_repl_js, ensure_ascii=False).replace('</script>', '<\\/script>')

    # Build per-array hardware-health data for the HW cell modal and the
    # "All Hardware Issues" panel. Keyed by array name (platform is carried
    # inside the object so FA-File and FA-Block rows collapse to one entry).
    _hw_js = {}
    for stat in stats:
        _h = stat.get('hw')
        if not _h:
            continue
        _name = _h.get('name') or stat['name']
        if _name in _hw_js:
            continue
        _hw_js[_name] = {
            'platform':       _h.get('platform', ''),
            'healthy':        _h.get('healthy'),
            'error':          _h.get('error'),
            'header':         _h.get('header', []),
            'rows':           _h.get('rows', []),
            'unhealthy_rows': _h.get('unhealthy_rows', []),
        }
    _hw_js_str = _json.dumps(_hw_js, ensure_ascii=False).replace('</script>', '<\\/script>')

    # Build the per-array replication-relationship dict for the JS payload.
    # FA-File and FA-Block stat entries that share an array name collapse to
    # one entry. Each partner is enriched with the peer's platform + location
    # when known.
    _rel_js = {}
    _platform_lookup = {}
    _location_lookup = {}
    for stat in stats:
        _loc = stat.get('location', '') or ''
        if stat.get('name') and (_loc or stat['name'] not in _location_lookup):
            _location_lookup[stat['name']] = _loc
        _r = stat.get('rel')
        if _r and _r.get('name'):
            _platform_lookup[_r['name']] = _r.get('platform', '')
    for stat in stats:
        _r = stat.get('rel')
        if not _r:
            continue
        _name = _r.get('name') or stat['name']
        if _name in _rel_js:
            continue
        _parts = []
        for _p in _r.get('partners', []):
            _remote = _p.get('remote', '')
            _parts.append({
                'remote':    _remote,
                'platform':  _platform_lookup.get(_remote, _r.get('platform', '')),
                'location':  _location_lookup.get(_remote, ''),
                'status':    _p.get('status', ''),
                'type':      _p.get('type', ''),
                'mgmt_addr': _p.get('mgmt_addr', ''),
            })
        _rel_js[_name] = {
            'platform': _r.get('platform', ''),
            'location': _location_lookup.get(_name, ''),
            'header':   _r.get('header', []),
            'rows':     _r.get('rows', []),
            'partners': _parts,
            'error':    _r.get('error'),
        }
    _rel_js_str = _json.dumps(_rel_js, ensure_ascii=False).replace('</script>', '<\\/script>')

    # Deduplicated pair list for the "Show Replication Relationships" panel.
    # Each entry carries both sides' platform, location, and connection status
    # so the UI can pick the correct FB/FA green/red image and group by
    # location without consulting REL_DATA.
    def _status_of(src_info, tgt_name):
        for _q in (src_info.get('partners') or []):
            if _q.get('remote') == tgt_name:
                return (_q.get('status') or '').strip()
        return ''

    _raw_pairs = []
    _pair_seen_ui = set()
    for _aname, _ainfo in _rel_js.items():
        for _p in _ainfo.get('partners', []):
            _bname = _p.get('remote', '')
            if not _bname:
                continue
            _key = tuple(sorted([_aname, _bname]))
            if _key in _pair_seen_ui:
                continue
            _pair_seen_ui.add(_key)
            _a, _b = _key
            _a_info = _rel_js.get(_a, {})
            _b_info = _rel_js.get(_b, {})
            _raw_pairs.append({
                'a_name': _a,
                'a_plat': _a_info.get('platform') or _b_info.get('platform') or 'FA',
                'a_loc':  _location_lookup.get(_a, ''),
                'a_status': _status_of(_a_info, _b) or _status_of(_b_info, _a),
                'b_name': _b,
                'b_plat': _b_info.get('platform') or _a_info.get('platform') or 'FA',
                'b_loc':  _location_lookup.get(_b, ''),
                'b_status': _status_of(_b_info, _a) or _status_of(_a_info, _b),
            })
    _raw_pairs.sort(key=lambda _pp: (_pp['a_name'].lower(), _pp['b_name'].lower()))
    _rel_pairs = align_rel_pairs_by_location(_raw_pairs)
    _rel_pairs_str = _json.dumps(_rel_pairs, ensure_ascii=False).replace('</script>', '<\\/script>')

    # Status-image base64 map for the JS side (keys without the .png extension).
    _img_b64_js  = {k.replace('.png', ''): v for k, v in _img_cache.items()}
    _img_b64_str = _json.dumps(_img_b64_js, ensure_ascii=False).replace('</script>', '<\\/script>')

    def _alert_cell(count, sev, array_name):
        """Return a <td> for one severity column. Clickable if count > 0."""
        colors = {'critical': ('#c00000', '#ffd6d6'),
                  'warning':  ('#c07000', '#fff4d6'),
                  'info':     ('#004490', '#d6eaff')}
        fg, bg = colors.get(sev, ('#333', '#eee'))
        if count == 0:
            return f'<td style="text-align:center;color:#888;">0</td>'
        safe = array_name.replace("'", "\\'")
        return (f'<td style="text-align:center;background:{bg};color:{fg};'
                f'font-weight:bold;cursor:pointer;" '
                f'onclick="showAlerts(\'{safe}\',\'{sev}\')" '
                f'title="Click to view {sev} alerts">{count}</td>')

    def _hw_cell(hw, array_name):
        """Return a <td> for the Hardware Health column."""
        if not hw:
            return '<td style="text-align:center;color:#888;">\u2014</td>'
        if hw.get('error'):
            return ('<td style="text-align:center;background:#f5f5f5;color:#888;" '
                    f'title="{hw["error"]}">Error</td>')
        if hw.get('healthy') is None:
            return '<td style="text-align:center;color:#888;">\u2014</td>'
        safe = array_name.replace("'", "\\'")
        if hw.get('healthy'):
            return ('<td style="text-align:center;background:#d4edda;'
                    'color:#155724;font-weight:bold;cursor:pointer;" '
                    f'onclick="showHw(\'{safe}\')" '
                    'title="Click to view full hardware list">Healthy</td>')
        return ('<td style="text-align:center;background:#ffd6d6;color:#c00000;'
                'font-weight:bold;cursor:pointer;" '
                f'onclick="showHw(\'{safe}\')" '
                'title="Click to view full hardware list">Unhealthy</td>')

    # Pre-compute per-stat SLA success counts and overall totals
    _sla_counts = []
    for _st in stats:
        _det   = _st.get('repl_details', [])
        _ok    = sum(1 for _r in _det if _r.get('SLA Status', '') == 'OK')
        _sla_counts.append((_ok, len(_det)))
    _total_ok   = sum(c[0] for c in _sla_counts)
    _total_jobs = sum(c[1] for c in _sla_counts)
    if _total_jobs > 0:
        _overall_pct   = _total_ok / _total_jobs * 100
        _overall_rate  = f'{_overall_pct:.0f}%'
        _sum_color     = '#206020' if _overall_pct >= 90 else ('#c07000' if _overall_pct >= 80 else '#c00000')
    else:
        _overall_rate  = '\u2014'
        _sum_color     = '#333'
    summary_html = (
        f'<div class="sla-summary">Overall Replication SLA Success: '
        f'<strong>{_total_ok}/{_total_jobs}</strong>'
        f'&nbsp;&nbsp;|&nbsp;&nbsp;Success Rate: '
        f'<strong style="color:{_sum_color};">{_overall_rate}</strong></div>'
    )

    rows_html = ''
    for stat, (ok, total) in zip(stats, _sla_counts):
        status_td = _status_cell_html(stat)
        if stat['avg_lag'] is None and stat['max_lag'] is None:
            chart_td = '<em>No data collected</em>'
        else:
            # Key must match the composite key used in REPL_DATA
            _safe = f"{stat['name']} ({stat['type']})".replace("'", "\\'")
            chart_td = (f'<div style="cursor:pointer;" onclick="showRepl(\'{_safe}\')" '
                        f'title="Click for 24h replication detail">'
                        f'<img src="data:image/png;base64,{_make_chart_b64(stat)}" style="display:block;">'
                        f'</div>')
        c_td  = _alert_cell(stat.get('critical_alerts', 0), 'critical', stat['name'])
        w_td  = _alert_cell(stat.get('warning_alerts',  0), 'warning',  stat['name'])
        i_td  = _alert_cell(stat.get('info_alerts',     0), 'info',     stat['name'])
        hw_td = _hw_cell(stat.get('hw'), stat['name'])
        if total > 0:
            rate_pct = ok / total * 100
            r_color  = '#206020' if rate_pct >= 90 else ('#c07000' if rate_pct >= 80 else '#c00000')
            r_bg     = '#e8f5e9' if rate_pct >= 90 else ('#fff4d6' if rate_pct >= 80 else '#ffd6d6')
            sla_td   = f'<td style="text-align:center;">{ok}/{total}</td>'
            rate_td  = (f'<td style="text-align:center;color:{r_color};'
                        f'background:{r_bg};font-weight:bold;">{rate_pct:.0f}%</td>')
        else:
            sla_td  = '<td style="text-align:center;color:#888;">\u2014</td>'
            rate_td = '<td style="text-align:center;color:#888;">\u2014</td>'
        _loc_txt = stat.get('location', '') or ''
        _loc_td = (f'<td>{_loc_txt}</td>' if _loc_txt
                   else '<td style="text-align:center;color:#888;">\u2014</td>')
        _notes_txt = stat.get('notes', '') or ''
        _notes_td = (f'<td>{_html.escape(_notes_txt)}</td>' if _notes_txt
                     else '<td style="text-align:center;color:#888;">\u2014</td>')
        rows_html += (f'      <tr>'
                      f'<td style="text-align:center;">{status_td}</td>'
                      f'<td>{stat["name"]}</td>'
                      f'{_loc_td}'
                      f'{_notes_td}'
                      f'<td>{stat["type"]}</td>'
                      f'{c_td}{w_td}{i_td}'
                      f'{hw_td}'
                      f'<td>{chart_td}</td>'
                      f'{sla_td}{rate_td}'
                      f'</tr>\n')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Everpure Array Report</title>
  <style>
    body  {{ font-family: Calibri, Arial, sans-serif; margin: 10px; font-size: 11pt; }}
    h1    {{ font-size: 16pt; margin-bottom: 6px; }}
    p     {{ margin: 2px 0; }}
    .arrays-heading {{ font-weight: bold; margin-top: 10px; }}
    ul    {{ margin: 4px 0 10px 24px; }}
    table {{ border-collapse: collapse; width: auto; margin-top: 12px; table-layout: auto; }}
    th, td {{ border: 1px solid #999; padding: 4px 6px; vertical-align: middle; word-wrap: break-word; }}
    th    {{ background: #dce6f1; font-weight: bold; font-size: 10pt; }}
    col.c0  {{ width: 96px; }}  col.c1  {{ width: 144px; }} col.c1a {{ width: 110px; }}
    col.c1b {{ width: 180px; }}
    col.c2  {{ width: 67px; }}
    col.c3  {{ width: 52px; }}  col.c4  {{ width: 52px; }}  col.c5  {{ width: 52px; }}
    col.c6  {{ width: 80px; }}  col.c8  {{ width: 80px; }}  col.c9  {{ width: 80px; }}
    /* SLA summary bar */
    .sla-summary {{ margin: 8px 0 4px; padding: 6px 12px; background: #f0f4fa;
      border: 1px solid #b8cfe8; border-radius: 4px; font-size: 10pt; }}
    /* Severity colour classes (shared by modal rows and panel rows) */
    .sev-critical {{ background:#ffd6d6; color:#c00000; font-weight:bold; }}
    .sev-warning  {{ background:#fff4d6; color:#c07000; font-weight:bold; }}
    .sev-info     {{ background:#d6eaff; color:#004490; font-weight:bold; }}
    /* Toggle buttons */
    .filter-bar {{ margin:14px 0 8px; display:flex; align-items:center; gap:8px; }}
    .filter-bar span {{ font-weight:bold; margin-right:4px; }}
    .sev-btn {{ padding:5px 16px; border-radius:4px; border:2px solid; cursor:pointer;
      font-size:10pt; font-weight:bold; opacity:0.4; transition:opacity 0.15s; background:#fff; }}
    .sev-btn.active {{ opacity:1; }}
    .info-btn     {{ border-color:#004490; color:#004490; }}
    .warning-btn  {{ border-color:#c07000; color:#c07000; }}
    .critical-btn {{ border-color:#c00000; color:#c00000; }}
    .repl-all-btn {{ border-color:#2e6da4; color:#2e6da4; }}
    .hw-all-btn   {{ border-color:#8a2a2a; color:#8a2a2a; }}
    .rel-pairs-btn {{ border-color:#2a7a2a; color:#2a7a2a; }}
    .rel-pairs-bar {{ margin-bottom:4px; }}
    /* Replication-relationships pair panel */
    .rel-panel-section {{ margin:12px 0 8px; }}
    .rel-panel-section h3 {{ margin:0 0 6px; font-size:12pt; font-weight:bold;
      padding:5px 10px; border-radius:3px; background:#dff0d8; color:#2a7a2a; }}
    .rel-panel-section table {{ border-collapse:collapse; width:auto; margin-top:4px;
      table-layout:auto; }}
    .rel-panel-section td {{ border:1px solid #bbb; padding:6px 10px; vertical-align:middle; }}
    .rel-pair-row {{ cursor:pointer; transition:background 0.15s; }}
    .rel-pair-row:hover {{ background:#eef5ff; }}
    .rel-array-cell {{ text-align:center; min-width:160px; }}
    .rel-array-cell img {{ display:block; width:64px; height:auto; margin:0 auto 4px; }}
    .rel-array-loc  {{ font-size:9pt; font-weight:bold; color:#2a5a7a;
      margin-bottom:3px; text-transform:uppercase; letter-spacing:0.3px; }}
    .rel-array-loc.empty {{ color:#999; font-weight:normal;
      font-style:italic; text-transform:none; letter-spacing:0; }}
    .rel-array-name {{ font-weight:bold; font-size:10pt; }}
    .rel-array-stat {{ font-size:9pt; color:#555; }}
    .rel-arrow {{ font-size:22pt; color:#444; text-align:center; min-width:50px; }}
    .rel-group-hdr td {{ background:#eaf2f8; font-weight:bold; color:#24527a;
      font-size:10pt; padding:4px 10px; letter-spacing:0.3px; }}
    /* Alert panel (bottom of page) */
    .alert-panel-section {{ margin-top:20px; }}
    .alert-panel-section h3 {{ margin:0 0 6px; font-size:12pt; font-weight:bold;
      padding:5px 10px; border-radius:3px; }}
    .panel-critical h3 {{ background:#ffd6d6; color:#c00000; }}
    .panel-warning  h3 {{ background:#fff4d6; color:#c07000; }}
    .panel-info     h3 {{ background:#d6eaff; color:#004490; }}
    .panel-hw       h3 {{ background:#ffd6d6; color:#8a2a2a; }}
    .alert-panel-section table {{ width:100%; }}
    .hw-panel-section {{ margin-top:20px; }}
    .hw-panel-section h3 {{ margin:0 0 6px; font-size:12pt; font-weight:bold;
      padding:5px 10px; border-radius:3px; background:#ffd6d6; color:#8a2a2a; }}
    .hw-panel-section h4 {{ margin:8px 0 4px; font-size:11pt; }}
    .hw-panel-section table {{ width:100%; margin-bottom:10px; }}
    .hw-panel-section td.sev-critical,
    .hw-panel-section td.sev-warning {{ text-align:center; }}
    /* HW modal (reuses overlay) */
    #hw-overlay {{ display:none; position:fixed; top:0; left:0; width:100%; height:100%;
      background:rgba(0,0,0,0.5); z-index:1000; }}
    #hw-modal {{ position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
      background:#fff; border-radius:6px; padding:18px 22px; max-width:95vw; max-height:85vh;
      overflow:auto; box-shadow:0 4px 24px rgba(0,0,0,0.4); min-width:500px; }}
    #hw-modal h2 {{ margin:0 0 12px; font-size:13pt; color:#8a2a2a; }}
    #hw-modal table {{ width:100%; margin-top:0; }}
    #hw-modal th {{ background:#dce6f1; }}
    #close-hw-modal {{ float:right; cursor:pointer; font-size:16pt; line-height:1;
      border:none; background:none; color:#555; margin-top:-4px; }}
    /* Alert modal */
    #alert-overlay {{ display:none; position:fixed; top:0; left:0; width:100%; height:100%;
      background:rgba(0,0,0,0.5); z-index:1000; }}
    #alert-modal {{ position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
      background:#fff; border-radius:6px; padding:18px 22px; max-width:90vw; max-height:85vh;
      overflow:auto; box-shadow:0 4px 24px rgba(0,0,0,0.4); min-width:420px; }}
    #alert-modal h2 {{ margin:0 0 12px; font-size:13pt; }}
    #alert-modal table {{ width:100%; margin-top:0; }}
    #alert-modal th {{ background:#dce6f1; }}
    #close-modal {{ float:right; cursor:pointer; font-size:16pt; line-height:1;
      border:none; background:none; color:#555; margin-top:-4px; }}
    /* Replication detail modal */
    #repl-overlay {{ display:none; position:fixed; top:0; left:0; width:100%; height:100%;
      background:rgba(0,0,0,0.5); z-index:1000; }}
    #repl-modal {{ position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
      background:#fff; border-radius:6px; padding:18px 22px; max-width:92vw; max-height:85vh;
      overflow:auto; box-shadow:0 4px 24px rgba(0,0,0,0.4); min-width:520px; }}
    #repl-modal h2 {{ margin:0 0 8px; font-size:13pt; }}
    #repl-modal p.repl-meta {{ margin:0 0 10px; font-size:10pt; color:#444; }}
    #repl-modal table {{ width:100%; margin-top:0; }}
    #repl-modal th {{ background:#dce6f1; }}
    #close-repl-modal {{ float:right; cursor:pointer; font-size:16pt; line-height:1;
      border:none; background:none; color:#555; margin-top:-4px; }}
    .repl-ok       {{ background:#e8f5e9; }}
    .repl-exceeded {{ background:#ffd6d6; color:#c00000; font-weight:bold; }}
    /* Replication-relationships modal */
    #rel-overlay {{ display:none; position:fixed; top:0; left:0; width:100%; height:100%;
      background:rgba(0,0,0,0.5); z-index:1000; }}
    #rel-modal {{ position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
      background:#fff; border-radius:6px; padding:18px 22px; max-width:92vw; max-height:85vh;
      overflow:auto; box-shadow:0 4px 24px rgba(0,0,0,0.4); min-width:520px; }}
    #rel-modal h2 {{ margin:0 0 12px; font-size:13pt; color:#2a7a2a; }}
    #rel-modal h3 {{ margin:12px 0 6px; font-size:11pt; background:#dff0d8;
      color:#2a7a2a; padding:4px 8px; border-radius:3px; }}
    #rel-modal table {{ width:100%; margin-top:0; }}
    #rel-modal th {{ background:#dce6f1; }}
    #close-rel-modal {{ float:right; cursor:pointer; font-size:16pt; line-height:1;
      border:none; background:none; color:#555; margin-top:-4px; }}
  </style>
</head>
<body>
  <h1>Everpure &#8211; Pure Storage Array Report</h1>
  <p>Output from Report run on {now_str} {tz}</p>
  <p>Defined Replication SLA for SLA FB: {format_seconds_human(fb_sec)}</p>
  <p>Defined Replication SLA for SLA FA-File: {format_seconds_human(faf_sec)}</p>
  <p>Defined Replication SLA for SLA FA-Block: {format_seconds_human(fab_sec)}</p>
  <p>Alert Codes Ignored: {', '.join(excluded) if excluded else 'None'}</p>
  <p>Ignore Source Side Replica Reporting: {ignore_lbl}</p>
  <p class="arrays-heading">Arrays Checked:</p>
  <ul>
{array_items}  </ul>
  <div class="filter-bar rel-pairs-bar">
    <button id="btn-rel-pairs" class="sev-btn rel-pairs-btn" onclick="toggleRelPairs()">Show Replication Relationships</button>
  </div>
  <!-- Replication-relationships pair panel (populated by JS when toggled) -->
  <div id="rel-pairs-panel"></div>
  <div class="filter-bar">
    <span>Alert View:</span>
    <button id="btn-critical" class="sev-btn critical-btn" onclick="toggleSev('critical')">Show Critical</button>
    <button id="btn-warning"  class="sev-btn warning-btn"  onclick="toggleSev('warning')">Show Warning</button>
    <button id="btn-info"     class="sev-btn info-btn"     onclick="toggleSev('info')">Show Info</button>
    <span style="border-left:1px solid #ccc;height:20px;margin:0 6px;"></span>
    <button id="btn-repl-FB"      class="sev-btn repl-all-btn" onclick="toggleReplGroup('FB')">FlashBlade Replication Detail</button>
    <button id="btn-repl-FAFile"  class="sev-btn repl-all-btn" onclick="toggleReplGroup('FA-File')">FlashArray Pod Replication Detail - File</button>
    <button id="btn-repl-FABlock" class="sev-btn repl-all-btn" onclick="toggleReplGroup('FA-Block')">FlashArray Snapshot Replication Detail</button>
    <span style="border-left:1px solid #ccc;height:20px;margin:0 6px;"></span>
    <button id="btn-hw-all" class="sev-btn hw-all-btn" onclick="toggleHwAll()">All Hardware Issues</button>
  </div>
{summary_html}  <table>
    <colgroup>
      <col class="c0"><col class="c1"><col class="c1a"><col class="c1b"><col class="c2">
      <col class="c3"><col class="c4"><col class="c5"><col class="c6">
      <col class="c7"><col class="c8"><col class="c9">
    </colgroup>
    <thead>
      <tr>
        <th>Array Status</th><th>Array Name</th><th>Location</th><th>Notes</th><th>Type</th>
        <th style="color:#c00000;">Critical</th>
        <th style="color:#c07000;">Warning</th>
        <th style="color:#004490;">Info</th>
        <th style="color:#8a2a2a;">Hardware Health</th>
        <th>Replication Lag vs SLA</th>
        <th>Repl SLA Success</th>
        <th>Repl SLA Success Rate</th>
      </tr>
    </thead>
    <tbody>
{rows_html}    </tbody>
  </table>

  <!-- Hardware-issues panel (populated by JS when "All Hardware Issues" is active) -->
  <div id="hw-panel"></div>

  <!-- Severity alert panel (populated by JS when toggle buttons are active) -->
  <div id="alert-panel"></div>

  <!-- Per-type replication detail panels -->
  <div id="repl-panel-FB"></div>
  <div id="repl-panel-FA-File"></div>
  <div id="repl-panel-FA-Block"></div>

  <!-- Replication detail modal -->
  <div id="repl-overlay" onclick="closeRepl()">
    <div id="repl-modal" onclick="event.stopPropagation()">
      <button id="close-repl-modal" onclick="closeRepl()" title="Close">&times;</button>
      <h2 id="repl-modal-title">Replication Detail</h2>
      <div id="repl-modal-body"></div>
    </div>
  </div>

  <!-- Alert detail modal -->
  <div id="alert-overlay" onclick="closeAlerts()">
    <div id="alert-modal" onclick="event.stopPropagation()">
      <button id="close-modal" onclick="closeAlerts()" title="Close">&times;</button>
      <h2 id="modal-title">Alerts</h2>
      <div id="modal-body"></div>
    </div>
  </div>

  <!-- Hardware-health detail modal -->
  <div id="hw-overlay" onclick="closeHw()">
    <div id="hw-modal" onclick="event.stopPropagation()">
      <button id="close-hw-modal" onclick="closeHw()" title="Close">&times;</button>
      <h2 id="hw-modal-title">Hardware Issues</h2>
      <div id="hw-modal-body"></div>
    </div>
  </div>

  <!-- Replication-relationships detail modal -->
  <div id="rel-overlay" onclick="closeRelModal()">
    <div id="rel-modal" onclick="event.stopPropagation()">
      <button id="close-rel-modal" onclick="closeRelModal()" title="Close">&times;</button>
      <h2 id="rel-modal-title">Replication Relationship</h2>
      <div id="rel-modal-body"></div>
    </div>
  </div>

  <script>
    var ALERT_DATA = {_alert_js_str};
    var REPL_DATA  = {_repl_js_str};
    var HW_DATA    = {_hw_js_str};
    var REL_DATA   = {_rel_js_str};
    var REL_PAIRS  = {_rel_pairs_str};
    var IMG_B64    = {_img_b64_str};

    /* ── Per-type replication detail panels ────────────────────────────── */
    var _replGroupActive = {{ 'FB': false, 'FA-File': false, 'FA-Block': false }};
    var _replGroupBtnIds   = {{ 'FB': 'btn-repl-FB', 'FA-File': 'btn-repl-FAFile', 'FA-Block': 'btn-repl-FABlock' }};
    var _replGroupPanelIds = {{ 'FB': 'repl-panel-FB', 'FA-File': 'repl-panel-FA-File', 'FA-Block': 'repl-panel-FA-Block' }};
    var _replGroupLabels   = {{
      'FB':       'FlashBlade Replication Detail',
      'FA-File':  'FlashArray Pod Replication Detail \u2013 File',
      'FA-Block': 'FlashArray Snapshot Replication Detail'
    }};

    function toggleReplGroup(type) {{
      _replGroupActive[type] = !_replGroupActive[type];
      var btn = document.getElementById(_replGroupBtnIds[type]);
      if (_replGroupActive[type]) {{
        btn.classList.add('active');
      }} else {{
        btn.classList.remove('active');
      }}
      buildReplGroupPanel(type);
    }}

    function buildReplGroupPanel(type) {{
      var panel = document.getElementById(_replGroupPanelIds[type]);
      if (!_replGroupActive[type]) {{ panel.innerHTML = ''; return; }}

      /* Gather rows for this type only; strip " (type)" suffix from key */
      var rows = [];
      Object.keys(REPL_DATA).forEach(function(arrKey) {{
        var d = REPL_DATA[arrKey];
        if ((d.type || '') !== type) return;
        var displayArr = arrKey.replace(/ \\([^)]+\\)$/, '');
        (d.rows || []).forEach(function(r) {{
          rows.push({{ _array: displayArr, _row: r }});
        }});
      }});

      var headStyle = 'margin:0 0 6px;font-size:12pt;font-weight:bold;'
                    + 'padding:5px 10px;border-radius:3px;background:#dce6f1;color:#1a3d6e;';
      var label = _replGroupLabels[type] || type;

      if (rows.length === 0) {{
        panel.innerHTML = '<div style="margin-top:20px;"><h3 style="' + headStyle
                        + '">' + label + ' \u2014 No data available.</h3></div>';
        return;
      }}

      /* Build column list from this group's rows only */
      var cols = [];
      rows.forEach(function(item) {{
        Object.keys(item._row).forEach(function(k) {{
          if (cols.indexOf(k) === -1) cols.push(k);
        }});
      }});

      var tbl = '<table><thead><tr><th>Array Name</th>';
      cols.forEach(function(c) {{ tbl += '<th>' + escHtml(c) + '</th>'; }});
      tbl += '</tr></thead><tbody>';
      rows.forEach(function(item) {{
        var exceeded = (item._row['SLA Status'] || '').toLowerCase() === 'exceeded';
        tbl += '<tr class="' + (exceeded ? 'repl-exceeded' : 'repl-ok') + '">';
        tbl += '<td>' + escHtml(item._array) + '</td>';
        cols.forEach(function(c) {{ tbl += '<td>' + escHtml(item._row[c] || '') + '</td>'; }});
        tbl += '</tr>';
      }});
      tbl += '</tbody></table>';

      var disclaimer = '';
      if (type === 'FA-Block') {{
        disclaimer = '<p style="margin:2px 0 6px;font-size:9pt;color:#555;font-style:italic;">'
                   + '<strong>Note:</strong> For FA-Block Replication, the Source Side array will not report '
                   + 'Start Time, Progress, Data Transferred, or Bytes Written. '
                   + 'That information is reported by the Destination array.</p>';
      }}

      panel.innerHTML = '<div style="margin-top:20px;">'
        + '<h3 style="' + headStyle + '">' + label
        + ' <span style="font-weight:normal;font-size:9pt;color:#555;">(' + rows.length + ' jobs)</span></h3>'
        + disclaimer + tbl + '</div>';
    }}

    /* ── Replication detail modal ───────────────────────────────────────── */
    function showRepl(arrayName) {{
      var d = REPL_DATA[arrayName];
      if (!d) return;
      /* Strip the " (type)" suffix for a cleaner title — type is shown in the meta line */
      var displayName = arrayName.replace(/ \\([^)]+\\)$/, '');
      document.getElementById('repl-modal-title').textContent =
        displayName + ' \u2013 Replication Detail (24h)';
      var meta = '<p class="repl-meta">'
        + '<strong>Type:</strong> '        + escHtml(d.type    || '\u2014') + ' \u00a0|\u00a0 '
        + '<strong>SLA Target:</strong> '  + escHtml(d.sla     || '\u2014') + ' \u00a0|\u00a0 '
        + '<strong>Avg Lag:</strong> '     + escHtml(d.avg_lag || '\u2014') + ' \u00a0|\u00a0 '
        + '<strong>Max Lag:</strong> '     + escHtml(d.max_lag || '\u2014')
        + '</p>';
      var body = meta;
      if (!d.rows || d.rows.length === 0) {{
        body += '<p>No replication detail data available.</p>';
      }} else {{
        var cols = [];
        d.rows.forEach(function(r) {{
          Object.keys(r).forEach(function(k) {{
            if (cols.indexOf(k) === -1) cols.push(k);
          }});
        }});
        var tbl = '<table><thead><tr>';
        cols.forEach(function(c) {{ tbl += '<th>' + escHtml(c) + '</th>'; }});
        tbl += '</tr></thead><tbody>';
        d.rows.forEach(function(r) {{
          var exceeded = (r['SLA Status'] || '').toLowerCase() === 'exceeded';
          tbl += '<tr class="' + (exceeded ? 'repl-exceeded' : 'repl-ok') + '">';
          cols.forEach(function(c) {{ tbl += '<td>' + escHtml(r[c] || '') + '</td>'; }});
          tbl += '</tr>';
        }});
        tbl += '</tbody></table>';
        body += tbl;
      }}
      document.getElementById('repl-modal-body').innerHTML = body;
      document.getElementById('repl-overlay').style.display = 'block';
    }}

    function closeRepl() {{
      document.getElementById('repl-overlay').style.display = 'none';
    }}

    /* ── Toggle-panel logic ─────────────────────────────────────────────── */
    var _activeSevs = {{}};

    function toggleSev(sev) {{
      _activeSevs[sev] = !_activeSevs[sev];
      var btn = document.getElementById('btn-' + sev);
      if (_activeSevs[sev]) {{
        btn.classList.add('active');
      }} else {{
        btn.classList.remove('active');
      }}
      buildAlertPanel();
    }}

    function buildAlertPanel() {{
      var panel = document.getElementById('alert-panel');
      var html  = '';
      var order = ['critical', 'warning', 'info'];
      order.forEach(function(sev) {{
        if (!_activeSevs[sev]) return;
        /* Gather all alerts of this severity across every array */
        var rows = [];
        Object.keys(ALERT_DATA).forEach(function(arrName) {{
          (ALERT_DATA[arrName] || []).forEach(function(a) {{
            if (a._sev === sev) rows.push({{ _array: arrName, _alert: a }});
          }});
        }});
        var label = sev.charAt(0).toUpperCase() + sev.slice(1);
        var heading = label + ' Alerts' + (rows.length ? ' (' + rows.length + ')' : ' — None');
        if (rows.length === 0) {{
          html += '<div class="alert-panel-section panel-' + sev + '"><h3>' + heading + '</h3></div>';
          return;
        }}
        /* Build unified column list (skip internal keys) */
        var cols = [];
        rows.forEach(function(r) {{
          Object.keys(r._alert).forEach(function(k) {{
            if (k !== '_sev' && k !== 'Component Type' && k !== 'Component Name' && cols.indexOf(k) === -1) cols.push(k);
          }});
        }});
        var tbl = '<table><thead><tr><th>Array Name</th>';
        cols.forEach(function(c) {{ tbl += '<th>' + escHtml(c) + '</th>'; }});
        tbl += '</tr></thead><tbody>';
        rows.forEach(function(r) {{
          tbl += '<tr class="sev-' + sev + '"><td>' + escHtml(r._array) + '</td>';
          cols.forEach(function(c) {{ tbl += '<td>' + escHtml(r._alert[c] || '') + '</td>'; }});
          tbl += '</tr>';
        }});
        tbl += '</tbody></table>';
        html += '<div class="alert-panel-section panel-' + sev + '"><h3>' + heading + '</h3>' + tbl + '</div>';
      }});
      panel.innerHTML = html;
    }}

    /* ── Per-array modal logic ──────────────────────────────────────────── */
    function showAlerts(arrayName, sev) {{
      var all = ALERT_DATA[arrayName] || [];
      var filtered = all.filter(function(a) {{ return a._sev === sev; }});
      var sevLabel = sev.charAt(0).toUpperCase() + sev.slice(1);
      document.getElementById('modal-title').textContent =
        arrayName + ' \u2013 ' + sevLabel + ' Alerts (' + filtered.length + ')';

      if (filtered.length === 0) {{
        document.getElementById('modal-body').innerHTML = '<p>No alerts found.</p>';
      }} else {{
        // Collect column names (skip internal _sev key)
        var cols = [];
        filtered.forEach(function(a) {{
          Object.keys(a).forEach(function(k) {{
            if (k !== '_sev' && k !== 'Component Type' && k !== 'Component Name' && cols.indexOf(k) === -1) cols.push(k);
          }});
        }});
        var html = '<table><thead><tr>';
        cols.forEach(function(c) {{ html += '<th>' + escHtml(c) + '</th>'; }});
        html += '</tr></thead><tbody>';
        filtered.forEach(function(a) {{
          html += '<tr class="sev-' + sev + '">';
          cols.forEach(function(c) {{ html += '<td>' + escHtml(a[c] || '') + '</td>'; }});
          html += '</tr>';
        }});
        html += '</tbody></table>';
        document.getElementById('modal-body').innerHTML = html;
      }}
      document.getElementById('alert-overlay').style.display = 'block';
    }}

    function closeAlerts() {{
      document.getElementById('alert-overlay').style.display = 'none';
    }}

    function escHtml(s) {{
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }}

    /* ── Hardware-health modal and "All Hardware Issues" panel ─────────── */
    function _buildHwTable(header, rows, highlightIdx, unhealthyKeys) {{
      var h = '<table><thead><tr>';
      for (var i = 0; i < header.length; i++) h += '<th>' + escHtml(header[i]) + '</th>';
      h += '</tr></thead><tbody>';
      for (var r = 0; r < rows.length; r++) {{
        var isBad = unhealthyKeys && unhealthyKeys[JSON.stringify(rows[r])];
        h += isBad ? '<tr style="background:#ffecec;">' : '<tr>';
        for (var c = 0; c < rows[r].length; c++) {{
          var cell = escHtml(rows[r][c]);
          if (c === highlightIdx && isBad) {{
            h += '<td class="sev-critical">' + cell + '</td>';
          }} else {{
            h += '<td>' + cell + '</td>';
          }}
        }}
        h += '</tr>';
      }}
      h += '</tbody></table>';
      return h;
    }}

    function showHw(array) {{
      var info = HW_DATA[array];
      if (!info) return;
      var bad = (info.unhealthy_rows && info.unhealthy_rows.length) || 0;
      var title = (bad ? 'Hardware Issues \u2014 ' : 'Hardware Status \u2014 ')
                + array + ' (' + info.platform + ')';
      if (bad) title += '  \u2014  ' + bad + ' issue' + (bad === 1 ? '' : 's');
      document.getElementById('hw-modal-title').textContent = title;
      var body = document.getElementById('hw-modal-body');
      if (info.error) {{
        body.innerHTML = '<p><em>Error collecting hardware data: ' + escHtml(info.error) + '</em></p>';
      }} else if (!info.rows || info.rows.length === 0) {{
        body.innerHTML = '<p><em>No hardware components reported.</em></p>';
      }} else {{
        // Find the Status column index so unhealthy cells are flagged.
        var si = -1;
        for (var i = 0; i < info.header.length; i++) {{
          if (String(info.header[i]).trim().toLowerCase() === 'status') {{ si = i; break; }}
        }}
        // Build a lookup of unhealthy rows (JSON-keyed) so the full list can
        // highlight them while still showing every component from purehw list.
        var keys = {{}};
        if (info.unhealthy_rows) {{
          for (var u = 0; u < info.unhealthy_rows.length; u++) {{
            keys[JSON.stringify(info.unhealthy_rows[u])] = true;
          }}
        }}
        var intro = bad
          ? '<p style="margin:0 0 8px;color:#8a2a2a;">'
            + bad + ' component' + (bad === 1 ? '' : 's') + ' flagged as unhealthy (highlighted below). '
            + 'Full <code>purehw list</code> output:</p>'
          : '<p style="margin:0 0 8px;color:#155724;">All components healthy. '
            + 'Full <code>purehw list</code> output:</p>';
        body.innerHTML = intro + _buildHwTable(info.header, info.rows, si, keys);
      }}
      document.getElementById('hw-overlay').style.display = 'block';
    }}

    function closeHw() {{
      document.getElementById('hw-overlay').style.display = 'none';
    }}

    /* ── Replication-relationships panel + modal ───────────────────────── */
    var _relPairsActive = false;
    function toggleRelPairs() {{
      _relPairsActive = !_relPairsActive;
      var btn = document.getElementById('btn-rel-pairs');
      if (_relPairsActive) btn.classList.add('active'); else btn.classList.remove('active');
      buildRelPanel();
    }}

    function _relImgTag(plat, status) {{
      var p = (plat === 'FB') ? 'FB' : 'FA';
      var s = (String(status || '').trim().toLowerCase() === 'connected') ? 'Green' : 'Red';
      var b64 = IMG_B64[p + '-' + s] || '';
      if (!b64) return '';
      return '<img src="data:image/png;base64,' + b64 + '" alt="' + p + ' ' + s + '">';
    }}

    function _relLocDiv(loc) {{
      var t = String(loc || '').trim();
      if (!t) return '<div class="rel-array-loc empty">(no location)</div>';
      return '<div class="rel-array-loc">' + escHtml(t) + '</div>';
    }}

    function _relPairCell(name, plat, status, loc) {{
      return '<td class="rel-array-cell">'
        + _relLocDiv(loc)
        + _relImgTag(plat, status)
        + '<div class="rel-array-name">' + escHtml(name) + '</div>'
        + '<div class="rel-array-stat">' + escHtml(status || '(unknown)') + '</div>'
        + '</td>';
    }}

    function buildRelPanel() {{
      var panel = document.getElementById('rel-pairs-panel');
      if (!_relPairsActive) {{ panel.innerHTML = ''; return; }}
      if (!REL_PAIRS || REL_PAIRS.length === 0) {{
        panel.innerHTML = '<div class="rel-panel-section">'
          + '<h3>Replication Relationships \u2014 None discovered.</h3></div>';
        return;
      }}
      // Group pairs by (a_loc, b_loc) preserving first-seen order so the
      // alignment seeded server-side is reflected visually.
      var groupOrder = [];
      var groups = {{}};
      for (var i = 0; i < REL_PAIRS.length; i++) {{
        var p = REL_PAIRS[i];
        var la = String(p.a_loc || '').trim();
        var lb = String(p.b_loc || '').trim();
        var key = la + '\u241F' + lb;
        if (!groups[key]) {{ groups[key] = {{la: la, lb: lb, items: []}}; groupOrder.push(key); }}
        groups[key].items.push(i);
      }}
      var html = '<div class="rel-panel-section">'
               + '<h3>Replication Relationships</h3>';
      for (var g = 0; g < groupOrder.length; g++) {{
        var grp = groups[groupOrder[g]];
        var lbl = (grp.la || '(no location)') + '  \u2194  ' + (grp.lb || '(no location)');
        html += '<table><tbody>'
             +  '<tr class="rel-group-hdr"><td colspan="3">' + escHtml(lbl) + '</td></tr>';
        for (var j = 0; j < grp.items.length; j++) {{
          var idx = grp.items[j];
          var pp = REL_PAIRS[idx];
          html += '<tr class="rel-pair-row" onclick="showRelRow(' + idx + ')" '
               +  'title="Click to view connection-list row detail">'
               +  _relPairCell(pp.a_name, pp.a_plat, pp.a_status, pp.a_loc)
               +  '<td class="rel-arrow">\u2194</td>'
               +  _relPairCell(pp.b_name, pp.b_plat, pp.b_status, pp.b_loc)
               +  '</tr>';
        }}
        html += '</tbody></table>';
      }}
      html += '</div>';
      panel.innerHTML = html;
    }}

    function _relMatchingRows(arr, remote) {{
      var info = REL_DATA[arr];
      if (!info) return {{header: [], rows: []}};
      var header = info.header || [];
      var ni = -1;
      for (var i = 0; i < header.length; i++) {{
        if (String(header[i]).trim().toLowerCase() === 'name') {{ ni = i; break; }}
      }}
      var rows = info.rows || [];
      if (ni < 0) return {{header: header, rows: rows}};
      var out = [];
      for (var r = 0; r < rows.length; r++) {{
        if (ni < rows[r].length && String(rows[r][ni]).trim() === remote) out.push(rows[r]);
      }}
      return {{header: header, rows: out}};
    }}

    function _relSideTable(title, data) {{
      var h = '<h3>' + escHtml(title) + '</h3>';
      if (!data.header.length || !data.rows.length) {{
        h += '<p><em>No matching row in the connection-list output.</em></p>';
        return h;
      }}
      h += '<table><thead><tr>';
      for (var i = 0; i < data.header.length; i++) h += '<th>' + escHtml(data.header[i]) + '</th>';
      h += '</tr></thead><tbody>';
      for (var r = 0; r < data.rows.length; r++) {{
        h += '<tr>';
        for (var c = 0; c < data.rows[r].length; c++) h += '<td>' + escHtml(data.rows[r][c]) + '</td>';
        h += '</tr>';
      }}
      h += '</tbody></table>';
      return h;
    }}

    function _relTitleName(name, loc) {{
      var t = String(loc || '').trim();
      return t ? (name + ' (' + t + ')') : name;
    }}

    function showRelRow(idx) {{
      var p = REL_PAIRS[idx];
      if (!p) return;
      document.getElementById('rel-modal-title').textContent =
        'Replication Relationship \u2014 '
        + _relTitleName(p.a_name, p.a_loc) + ' \u2194 '
        + _relTitleName(p.b_name, p.b_loc);
      var body = _relSideTable(
        p.a_name + ' (' + p.a_plat + ') \u2014 connection to ' + p.b_name,
        _relMatchingRows(p.a_name, p.b_name));
      body += _relSideTable(
        p.b_name + ' (' + p.b_plat + ') \u2014 connection to ' + p.a_name,
        _relMatchingRows(p.b_name, p.a_name));
      document.getElementById('rel-modal-body').innerHTML = body;
      document.getElementById('rel-overlay').style.display = 'block';
    }}

    function showArrRel(arr) {{
      var matches = [];
      for (var i = 0; i < REL_PAIRS.length; i++) {{
        if (REL_PAIRS[i].a_name === arr || REL_PAIRS[i].b_name === arr) matches.push(i);
      }}
      document.getElementById('rel-modal-title').textContent =
        'Replication Partners \u2014 ' + arr;
      var body = document.getElementById('rel-modal-body');
      if (matches.length === 0) {{
        body.innerHTML = '<p><em>' + escHtml(arr)
          + ' has no replication relationships configured.</em></p>';
        document.getElementById('rel-overlay').style.display = 'block';
        return;
      }}
      var html = '<table><tbody>';
      for (var m = 0; m < matches.length; m++) {{
        var p = REL_PAIRS[matches[m]];
        var local = (p.a_name === arr) ? p
          : {{a_name: p.b_name, a_plat: p.b_plat, a_status: p.b_status, a_loc: p.b_loc,
              b_name: p.a_name, b_plat: p.a_plat, b_status: p.a_status, b_loc: p.a_loc}};
        html += '<tr class="rel-pair-row" onclick="showRelRow(' + matches[m] + ')" '
             +  'title="Click for connection-list row detail">'
             +  _relPairCell(local.a_name, local.a_plat, local.a_status, local.a_loc)
             +  '<td class="rel-arrow">\u2194</td>'
             +  _relPairCell(local.b_name, local.b_plat, local.b_status, local.b_loc)
             +  '</tr>';
      }}
      html += '</tbody></table>';
      body.innerHTML = html;
      document.getElementById('rel-overlay').style.display = 'block';
    }}

    function closeRelModal() {{
      document.getElementById('rel-overlay').style.display = 'none';
    }}

    var _hwAllActive = false;
    function toggleHwAll() {{
      _hwAllActive = !_hwAllActive;
      var btn = document.getElementById('btn-hw-all');
      if (_hwAllActive) btn.classList.add('active'); else btn.classList.remove('active');
      buildHwPanel();
    }}

    function buildHwPanel() {{
      var panel = document.getElementById('hw-panel');
      if (!_hwAllActive) {{ panel.innerHTML = ''; return; }}
      // Group arrays by platform. FA-File and FA-Block share columns so they
      // go in a single FA table (the HW data itself is de-duped per array).
      var fb = [], fa = [];
      var fbHeader = null, faHeader = null;
      var names = Object.keys(HW_DATA).sort();
      for (var i = 0; i < names.length; i++) {{
        var n = names[i];
        var info = HW_DATA[n];
        if (!info || !info.unhealthy_rows || info.unhealthy_rows.length === 0) continue;
        var dest = (info.platform === 'FB') ? fb : fa;
        if (info.platform === 'FB' && !fbHeader) fbHeader = info.header;
        if (info.platform !== 'FB' && !faHeader) faHeader = info.header;
        for (var r = 0; r < info.unhealthy_rows.length; r++) {{
          // Prepend array name so the combined table shows which array each row came from
          dest.push([n].concat(info.unhealthy_rows[r]));
        }}
      }}
      var html = '<div class="hw-panel-section"><h3>All Hardware Issues</h3>';
      if (fbHeader && fb.length > 0) {{
        // Find Status column index after prepending 'Array' header
        var si = -1;
        for (var i = 0; i < fbHeader.length; i++) {{
          if (String(fbHeader[i]).trim().toLowerCase() === 'status') {{ si = i + 1; break; }}
        }}
        html += '<h4>FlashBlade</h4>' + _buildHwTable(['Array'].concat(fbHeader), fb, si);
      }}
      if (faHeader && fa.length > 0) {{
        var si = -1;
        for (var i = 0; i < faHeader.length; i++) {{
          if (String(faHeader[i]).trim().toLowerCase() === 'status') {{ si = i + 1; break; }}
        }}
        html += '<h4>FlashArray (File &amp; Block)</h4>' + _buildHwTable(['Array'].concat(faHeader), fa, si);
      }}
      if ((!fbHeader || fb.length === 0) && (!faHeader || fa.length === 0)) {{
        html += '<p><em>No unhealthy hardware components reported across any array.</em></p>';
      }}
      html += '</div>';
      panel.innerHTML = html;
    }}

    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape') {{ closeAlerts(); closeRepl(); closeHw(); }}
    }});
  </script>
</body>
</html>"""


# =========================================================
# VOLUME & SNAPSHOT PROTECTION REPORT
# =========================================================
# Independent collection path used by the "Volume & Snapshot Protection"
# button. Issues purevol list / purepod replica-link list / purevol list
# --snap on each FlashArray, aggregates pod stretch + snapshot counts,
# and emits a separate HTML page (Tables 2 and 3 are placeholders).



__all__ = ['collect_hw_health', 'collect_replication_relationships', 'run_collection_core', 'build_nogui_header', 'append_history_csv', 'send_html_report', 'build_status_html']
