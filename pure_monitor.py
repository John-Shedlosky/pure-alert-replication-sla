import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog, filedialog
import threading
import queue
import re
import datetime
import json
import os
import sys

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

try:
    from PIL import Image, ImageTk
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# =========================================================
# CONFIGURABLE DEFAULTS
# =========================================================
DEFAULT_FB_ARRAYS = "flashblade1, flashblade2"
DEFAULT_FA_FILE_ARRAYS = "flasharray2, flasharray1"
DEFAULT_FA_BLOCK_ARRAYS = "flasharray2, flasharray1"
DEFAULT_EXCLUDED_ALERTS = "9999, 9998"

# We use events/queues to prompt for passwords in the main thread
password_request_event = threading.Event()
password_response_event = threading.Event()
global_password_request_msg = ""
global_password_response = None
credentials_cache = {}

# Set to True when --alert-debug is passed on the command line.
# In this mode SSH calls are bypassed and synthetic alert / lag data are injected
# so that the daily HTML report and history CSV can be tested without live arrays.
ALERT_DEBUG = '--alert-debug' in sys.argv

def ask_password_in_main(msg):
    global global_password_request_msg
    global_password_request_msg = msg
    password_request_event.set()
    password_response_event.wait()
    password_response_event.clear()
    return global_password_response

def run_ssh_command(array, user, command, log_list=None, nogui=False):
    if not HAS_PARAMIKO:
        raise Exception("paramiko library is not installed. Run 'pip install paramiko' to use SSH.")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(array, username=user, password=credentials_cache.get(array), timeout=10)
    except Exception as e:
        if isinstance(e, paramiko.ssh_exception.AuthenticationException) or "No authentication methods available" in str(e):
            if nogui:
                raise Exception(f"Skipped - password required for {user}@{array}")
            pwd = ask_password_in_main(f"SSH authentication failed. Enter password for {user}@{array}:")
            if pwd is None: raise Exception(f"Authentication cancelled for {array}")
            credentials_cache[array] = pwd
            client.connect(array, username=user, password=pwd, timeout=10)
        else:
            if nogui:
                raise Exception(f"Skipped - array not reachable: {array} ({str(e)})")
            raise e

    stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read().decode('utf-8', errors='replace')
    err = stderr.read().decode('utf-8', errors='replace')
    client.close()
    
    if err and not out.strip():
        if log_list is not None:
            log_list.append(f"=== Command Log: {user}@{array} ===\n> {command}\n[ERROR]\n{err}\n")
        raise Exception(f"SSH Error: {err}")
    
    if "--csv" in command and out.strip():
        import csv
        import io
        reader = csv.reader(io.StringIO(out.strip()))
        rows = list(reader)
        if rows:
            lag_idx = -1
            avg_lag_idx = -1
            max_lag_idx = -1
            if "purefs" in command:
                for i, cell in enumerate(rows[0]):
                    if "Lag" in cell: lag_idx = i
            elif "purepod" in command:
                for i, cell in enumerate(rows[0]):
                    if "Average Lag" in cell: avg_lag_idx = i
                    if "Maximum Lag" in cell: max_lag_idx = i
            
            if lag_idx != -1 or avg_lag_idx != -1 or max_lag_idx != -1:
                for r_idx, row in enumerate(rows):
                    if r_idx == 0: continue
                    if lag_idx != -1 and lag_idx < len(row):
                        try: row[lag_idx] = f"{int(int(row[lag_idx].strip()) / 60000)}m"
                        except: pass
                    if avg_lag_idx != -1 and avg_lag_idx < len(row):
                        try: row[avg_lag_idx] = f"{int(int(row[avg_lag_idx].strip()) / 60000)}m"
                        except: pass
                    if max_lag_idx != -1 and max_lag_idx < len(row):
                        try: row[max_lag_idx] = f"{int(int(row[max_lag_idx].strip()) / 60000)}m"
                        except: pass
                si = io.StringIO()
                writer = csv.writer(si)
                writer.writerows(rows)
                out = si.getvalue()
                
    if log_list is not None:
        log_list.append(f"=== Command Log: {user}@{array} ===\n> {command}\n[OUTPUT]\n{out}\n")
    return out

def parse_time_to_seconds(time_str):
    if not time_str or time_str == "-": return 0
    total_seconds = 0
    matches = re.finditer(r'(\d+)([smhd])', time_str.lower())
    found_any = False
    for match in matches:
        found_any = True
        val = int(match.group(1))
        unit = match.group(2)
        if unit == 's': total_seconds += val
        elif unit == 'm': total_seconds += val * 60
        elif unit == 'h': total_seconds += val * 3600
        elif unit == 'd': total_seconds += val * 86400
    if not found_any and time_str.isdigit(): return int(time_str)
    return total_seconds

def format_seconds_human(seconds):
    if seconds == 0: return "0s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h {m}m {s}s"

def _fmt_alert_str(stat):
    """Human-readable alert severity summary: '1 Critical, 2 Warning, 3 Info', 'None', or 'Error'."""
    if stat.get('alert_error'):
        return "Error"
    c_c = stat.get('critical_alerts', 0)
    w_c = stat.get('warning_alerts',  0)
    i_c = stat.get('info_alerts',     0)
    if c_c == 0 and w_c == 0 and i_c == 0:
        return "None"
    parts = []
    if c_c: parts.append(f"{c_c} Critical")
    if w_c: parts.append(f"{w_c} Warning")
    if i_c: parts.append(f"{i_c} Info")
    return ", ".join(parts)

def _get_debug_alerts(array, idx):
    """Return (counts_dict, log_lines) with synthetic alert data for --alert-debug mode.

    The mix of severities rotates across six patterns so that different arrays show
    different combinations of Critical / Warning / Info counts in the daily report.
    Fake lag values (avg_sec, max_sec) are also returned so the replication chart
    has meaningful data even without a live array.
    """
    now = datetime.datetime.now()

    # Pool of fake alert templates: (severity, code, component_type, summary)
    _POOL = [
        ('critical', 'S-CON-1001', 'Replication link disconnected — no progress for 2 hours'),
        ('critical', 'S-CON-1002', 'Drive failure detected; array is degraded (CH0.BAY3)'),
        ('critical', 'S-CON-1003', 'Network interface link is down (eth0)'),
        ('warning',  'S-WRN-2001', 'Array capacity utilization exceeds 80%'),
        ('warning',  'S-WRN-2002', 'Replication lag exceeds configured warning threshold'),
        ('warning',  'S-WRN-2003', 'Controller temperature elevated — check airflow'),
        ('warning',  'S-WRN-2004', 'Volume approaching its configured size limit (vol-prod-001)'),
        ('info',     'S-INF-3001', 'Array software version update is available'),
        ('info',     'S-INF-3002', 'SSL certificate will expire within 30 days'),
        ('info',     'S-INF-3003', 'Replication resync completed successfully'),
        ('info',     'S-INF-3004', 'Volume snapshot count approaching retention limit'),
        ('info',     'S-INF-3005', 'NFS export access list updated by administrator'),
    ]

    # Pattern: indices into _POOL each array slot uses.
    # Six patterns give varied mixes of Critical / Warning / Info counts.
    _PATTERNS = [
        [0, 1, 3, 4, 7, 8, 9],    # 2 crit · 2 warn · 3 info
        [3, 5, 7, 10],             # 0 crit · 2 warn · 2 info
        [2, 7, 11],                # 1 crit · 0 warn · 2 info
        [0, 4, 6, 9, 10, 11],     # 1 crit · 2 warn · 3 info
        [5, 7, 8],                 # 0 crit · 1 warn · 2 info
        [1, 2, 3],                 # 2 crit · 1 warn · 0 info
    ]

    # Paired lag values (avg_sec, max_sec) — some healthy, some over a typical SLA.
    _LAGS = [
        (3000, 4500),   # 50 min avg / 75 min max  — likely violated
        ( 300,  600),   #  5 min avg / 10 min max  — healthy
        (3600, 5400),   # 60 min avg / 90 min max  — violated
        ( 600, 1200),   # 10 min avg / 20 min max  — healthy
        (2400, 3600),   # 40 min avg / 60 min max  — borderline
        ( 900, 1800),   # 15 min avg / 30 min max  — healthy
    ]

    slot    = idx % len(_PATTERNS)
    chosen  = [_POOL[i] for i in _PATTERNS[slot]]
    avg_sec, max_sec = _LAGS[slot]

    counts = {'info': 0, 'warning': 0, 'critical': 0, 'error': False, 'alerts': []}
    log_lines = []

    for offset, (sev, code, summary) in enumerate(chosen):
        alert_id = 1000 + idx * 20 + offset
        counts[sev] += 1
        detail = {
            '_sev':           sev,
            'ID':             str(alert_id),
            'Code':           code,
            'Severity':       sev.capitalize(),
            'Summary':        summary,
            'Created':        (now - datetime.timedelta(minutes=60 + offset * 7))
                              .strftime('%Y-%m-%d %H:%M:%S'),
            'Updated':        now.strftime('%Y-%m-%d %H:%M:%S'),
        }
        counts['alerts'].append(detail)
        log_lines.append(f"[ALERT-DEBUG] {array} - {sev.upper():8s} | {code} | {summary}")

    return counts, log_lines, avg_sec, max_sec


def parse_pure_date(date_str):
    if not date_str: return None
    clean = date_str.strip()
    clean = re.sub(r' [A-Z]{2,4}$', '', clean)   # strip trailing timezone abbrev (UTC, EST…)
    clean = re.sub(r'[+-]\d{2}:\d{2}$', '', clean) # strip +00:00 offset
    clean = clean.replace('T', ' ')                 # normalise ISO-8601 T separator
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H-%M-%S"):
        try:
            return datetime.datetime.strptime(clean, fmt)
        except ValueError:
            continue
    return None

def run_collection_core(config, nogui=False):
    import csv, io
    alert_lines = []
    repl_lines = []
    detailed_logs = []
    alerted_arrays = set()
    alert_counts = {}   # array -> {'info': n, 'warning': n, 'critical': n, 'error': bool}
    array_stats  = []   # list of per-array dicts for Word report

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

    # Running index across all arrays so each gets a different alert pattern.
    _debug_array_idx = [0]

    def check_alert(array, user):
        if array in alerted_arrays:
            return
        alerted_arrays.add(array)

        if ALERT_DEBUG:
            counts, log_lines, _avg, _max = _get_debug_alerts(array, _debug_array_idx[0])
            _debug_array_idx[0] += 1
            alert_counts[array] = counts
            alert_lines.append(f"[ALERT-DEBUG] {array} - "
                                f"{counts['critical']} Critical, "
                                f"{counts['warning']} Warning, "
                                f"{counts['info']} Info (synthetic data)")
            alert_lines.extend(log_lines)
            alert_lines.append("")
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
                alert_counts[array] = counts
                block = ([header] if header else []) + valid
                prefs = ([f"{array} - Alert Header:"] if header else []) + [f"{array} - Alert:"] * len(valid)
                alert_lines.extend(format_csv(block, prefs))
            else:
                alert_counts[array] = counts
                alert_lines.append(f"{array} - Alerts: Healthy")
        except Exception as e:
            alert_counts[array] = {'info': 0, 'warning': 0, 'critical': 0, 'error': True}
            alert_lines.append(f"{array} - Alerts Error: {str(e)}")
        alert_lines.append("")

    # FB Loop
    for array in config['arr_fb']:
        check_alert(array, config['user_fb'])
        all_lags = []
        repl_rows = []
        try:
            if ALERT_DEBUG:
                _, _, avg_s, max_s = _get_debug_alerts(array, list(alert_counts.keys()).index(array))
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
                    repl_lines.append(f"[ALERT-DEBUG] {array} - FB Replication SLA exceeded "
                                      f"(simulated max lag {format_seconds_human(max_s)} vs SLA {format_seconds_human(config['sla_fb'])})")
                else:
                    repl_lines.append(f"[ALERT-DEBUG] {array} - FB Replication: Healthy (synthetic data)")
                raise _AlertDebugSkip()
            out = run_ssh_command(array, config['user_fb'], "purefs replica-link list --csv", log_list=detailed_logs, nogui=nogui)
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
                repl_lines.extend(format_csv(block, prefs))
            else: repl_lines.append(f"{array} - FB Replication: Healthy")
            array_stats.append({'name': array, 'type': 'FB',
                                **_alert_dict(array),
                                'sla_target': config['sla_fb'],
                                'avg_lag': sum(all_lags)/len(all_lags) if all_lags else None,
                                'max_lag': max(all_lags) if all_lags else None,
                                'repl_details': repl_rows})
        except _AlertDebugSkip:
            array_stats.append({'name': array, 'type': 'FB',
                                **_alert_dict(array),
                                'sla_target': config['sla_fb'],
                                'avg_lag': sum(all_lags)/len(all_lags) if all_lags else None,
                                'max_lag': max(all_lags) if all_lags else None,
                                'repl_details': repl_rows})
        except Exception as e:
            repl_lines.append(f"{array} - Repl Error: {str(e)}")
            array_stats.append({'name': array, 'type': 'FB',
                                **_alert_dict(array),
                                'sla_target': config['sla_fb'],
                                'avg_lag': None, 'max_lag': None,
                                'repl_details': []})
        repl_lines.append("")

    # FA-File Loop
    for array in config['arr_faf']:
        check_alert(array, config['user_faf'])
        all_avgs, all_maxes = [], []
        repl_rows = []
        try:
            if ALERT_DEBUG:
                _, _, avg_s, max_s = _get_debug_alerts(array, list(alert_counts.keys()).index(array))
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
                    repl_lines.append(f"[ALERT-DEBUG] {array} - FA File Replication SLA exceeded "
                                      f"(simulated max lag {format_seconds_human(max_s)} vs SLA {format_seconds_human(config['sla_faf'])})")
                else:
                    repl_lines.append(f"[ALERT-DEBUG] {array} - FA File Replication: Healthy (synthetic data)")
                raise _AlertDebugSkip()
            out = run_ssh_command(array, config['user_faf'], "purepod replica-link list --historical 24h --lag --csv", log_list=detailed_logs, nogui=nogui)
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
                repl_lines.extend(format_csv(block, prefs))
            else: repl_lines.append(f"{array} - FA File Replication: Healthy")
            array_stats.append({'name': array, 'type': 'FA-File',
                                **_alert_dict(array),
                                'sla_target': config['sla_faf'],
                                'avg_lag': sum(all_avgs)/len(all_avgs) if all_avgs else None,
                                'max_lag': max(all_maxes) if all_maxes else None,
                                'repl_details': repl_rows})
        except _AlertDebugSkip:
            array_stats.append({'name': array, 'type': 'FA-File',
                                **_alert_dict(array),
                                'sla_target': config['sla_faf'],
                                'avg_lag': sum(all_avgs)/len(all_avgs) if all_avgs else None,
                                'max_lag': max(all_maxes) if all_maxes else None,
                                'repl_details': repl_rows})
        except Exception as e:
            repl_lines.append(f"{array} - Repl Error: {str(e)}")
            array_stats.append({'name': array, 'type': 'FA-File',
                                **_alert_dict(array),
                                'sla_target': config['sla_faf'],
                                'avg_lag': None, 'max_lag': None,
                                'repl_details': []})
        repl_lines.append("")

    # FA-Block Loop
    for array in config['arr_fab']:
        check_alert(array, config['user_fab'])
        all_diffs = []
        repl_rows = []
        try:
            if ALERT_DEBUG:
                _, _, avg_s, max_s = _get_debug_alerts(array, list(alert_counts.keys()).index(array))
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
                    repl_lines.append(f"[ALERT-DEBUG] {array} - FA Block Replication SLA exceeded "
                                      f"(simulated max lag {format_seconds_human(max_s)} vs SLA {format_seconds_human(config['sla_fab'])})")
                else:
                    repl_lines.append(f"[ALERT-DEBUG] {array} - FA Block Replication: Healthy (synthetic data)")
                raise _AlertDebugSkip()
            time_out = run_ssh_command(array, config['user_fab'], "purearray list --time", log_list=detailed_logs, nogui=nogui)
            tm = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', time_out)
            if tm:
                now_dt = parse_pure_date(tm.group(0))
                target = now_dt - datetime.timedelta(days=1)
                cmd = f"purevol list --snap --transfer --filter \"created >= '{target.strftime('%Y-%m-%d %H:%M:%S')}'\" --csv"
                vol_out = run_ssh_command(array, config['user_fab'], cmd, log_list=detailed_logs, nogui=nogui)
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
                    repl_lines.extend(format_csv(block, prefs))
                else: repl_lines.append(f"{array} - FA Block Replication: Healthy")
            array_stats.append({'name': array, 'type': 'FA-Block',
                                **_alert_dict(array),
                                'sla_target': config['sla_fab'],
                                'avg_lag': sum(all_diffs)/len(all_diffs) if all_diffs else None,
                                'max_lag': max(all_diffs) if all_diffs else None,
                                'repl_details': repl_rows})
        except _AlertDebugSkip:
            array_stats.append({'name': array, 'type': 'FA-Block',
                                **_alert_dict(array),
                                'sla_target': config['sla_fab'],
                                'avg_lag': sum(all_diffs)/len(all_diffs) if all_diffs else None,
                                'max_lag': max(all_diffs) if all_diffs else None,
                                'repl_details': repl_rows})
        except Exception as e:
            repl_lines.append(f"{array} - Repl Error: {str(e)}")
            array_stats.append({'name': array, 'type': 'FA-Block',
                                **_alert_dict(array),
                                'sla_target': config['sla_fab'],
                                'avg_lag': None, 'max_lag': None,
                                'repl_details': []})
        repl_lines.append("")
    final = "=== ALERTS SECTION ===\n" + "\n".join(alert_lines)
    final += "\n=== REPLICATION SECTION ===\n" + "\n".join(repl_lines)
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
    for a in config['arr_fb']:
        header += f"FB Array - {a}\n"
    header += "\n"
    for a in config['arr_faf']:
        header += f"FA-File Array - {a}\n"
    header += "\n"
    for a in config['arr_fab']:
        header += f"FA-Block Array - {a}\n"
    pairs = config.get('replication_pairs', [])
    if pairs:
        header += "\nReplication Pairs:\n"
        for p in pairs:
            header += (f"  [{p.get('type', '?')}]  "
                       f"{p.get('source', '')}  \u2192  {p.get('destination', '')}    "
                       f"({p.get('name', '')})\n")
    return header + "\n"


def run_nogui():
    config_path = "monitor_config.json"
    if not os.path.exists(config_path):
        print(f"Error: {config_path} not found. Please run the GUI first to save a configuration.")
        return
    with open(config_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    def parse_arr(val):
        return [x.strip() for x in val.replace('\n', ',').split(',') if x.strip()]

    cfg = {
        'user_fb':  raw.get('user_fb',  'pureuser'),
        'user_faf': raw.get('user_faf', 'pureuser'),
        'user_fab': raw.get('user_fab', 'pureuser'),
        'arr_fb':   parse_arr(raw.get('fb_arrays',  '')),
        'arr_faf':  parse_arr(raw.get('faf_arrays', '')),
        'arr_fab':  parse_arr(raw.get('fab_arrays', '')),
        'sla_fb':   parse_time_to_seconds(raw.get('sla_fb',  '1h 30m')),
        'sla_faf':  parse_time_to_seconds(raw.get('sla_faf', '1h')),
        'sla_fab':  parse_time_to_seconds(raw.get('sla_fab', '1h')),
        'excluded': [x.strip() for x in raw.get('alerts_excluded', '').replace('\n', ',').split(',')
                     if x.strip() and 'e.g.' not in x],
        'ignore_source_lag': raw.get('ignore_source_lag', False),
        'replication_pairs': raw.get('replication_pairs', []),
    }

    lag_yellow_min = parse_time_to_seconds(raw.get('lag_yellow', '10m')) / 60.0
    lag_orange_min = parse_time_to_seconds(raw.get('lag_orange', '30m')) / 60.0

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
        PureMonitorApp._health_history_impl(lag_yellow_min, lag_orange_min,
                                            open_browser=False)
        print("Array Health History updated.")
    except Exception as e:
        print(f"Warning: could not update Array Health History: {e}")


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
    import io as _io, base64, os, time as _time

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
        return f'<img src="data:image/png;base64,{b64}" style="width:100%;max-width:96px;">' if b64 else ''

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
        c_td = _alert_cell(stat.get('critical_alerts', 0), 'critical', stat['name'])
        w_td = _alert_cell(stat.get('warning_alerts',  0), 'warning',  stat['name'])
        i_td = _alert_cell(stat.get('info_alerts',     0), 'info',     stat['name'])
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
        rows_html += (f'      <tr>'
                      f'<td style="text-align:center;">{status_td}</td>'
                      f'<td>{stat["name"]}</td>'
                      f'<td>{stat["type"]}</td>'
                      f'{c_td}{w_td}{i_td}'
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
    col.c0 {{ width: 96px; }} col.c1 {{ width: 144px; }} col.c2 {{ width: 67px; }}
    col.c3 {{ width: 52px; }} col.c4 {{ width: 52px; }} col.c5 {{ width: 52px; }}
    col.c7 {{ width: 80px; }} col.c8 {{ width: 80px; }}
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
    /* Alert panel (bottom of page) */
    .alert-panel-section {{ margin-top:20px; }}
    .alert-panel-section h3 {{ margin:0 0 6px; font-size:12pt; font-weight:bold;
      padding:5px 10px; border-radius:3px; }}
    .panel-critical h3 {{ background:#ffd6d6; color:#c00000; }}
    .panel-warning  h3 {{ background:#fff4d6; color:#c07000; }}
    .panel-info     h3 {{ background:#d6eaff; color:#004490; }}
    .alert-panel-section table {{ width:100%; }}
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
  <div class="filter-bar">
    <span>Alert View:</span>
    <button id="btn-critical" class="sev-btn critical-btn" onclick="toggleSev('critical')">Show Critical</button>
    <button id="btn-warning"  class="sev-btn warning-btn"  onclick="toggleSev('warning')">Show Warning</button>
    <button id="btn-info"     class="sev-btn info-btn"     onclick="toggleSev('info')">Show Info</button>
    <span style="border-left:1px solid #ccc;height:20px;margin:0 6px;"></span>
    <button id="btn-repl-FB"      class="sev-btn repl-all-btn" onclick="toggleReplGroup('FB')">FlashBlade Replication Detail</button>
    <button id="btn-repl-FAFile"  class="sev-btn repl-all-btn" onclick="toggleReplGroup('FA-File')">FlashArray Pod Replication Detail - File</button>
    <button id="btn-repl-FABlock" class="sev-btn repl-all-btn" onclick="toggleReplGroup('FA-Block')">FlashArray Snapshot Replication Detail</button>
  </div>
{summary_html}  <table>
    <colgroup>
      <col class="c0"><col class="c1"><col class="c2">
      <col class="c3"><col class="c4"><col class="c5"><col class="c6">
      <col class="c7"><col class="c8">
    </colgroup>
    <thead>
      <tr>
        <th>Array Status</th><th>Array Name</th><th>Type</th>
        <th style="color:#c00000;">Critical</th>
        <th style="color:#c07000;">Warning</th>
        <th style="color:#004490;">Info</th>
        <th>Replication Lag vs SLA</th>
        <th>Repl SLA Success</th>
        <th>Repl SLA Success Rate</th>
      </tr>
    </thead>
    <tbody>
{rows_html}    </tbody>
  </table>

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

  <script>
    var ALERT_DATA = {_alert_js_str};
    var REPL_DATA  = {_repl_js_str};

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

    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape') {{ closeAlerts(); closeRepl(); }}
    }});
  </script>
</body>
</html>"""


class PureMonitorApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Everpure (Pure Storage) - Alert and Replication SLA Status Report")
        self.geometry("1100x800")
        
        _icon = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images", "pure_logo.png")
        if os.path.exists(_icon):
            try:
                icon_img = tk.PhotoImage(file=_icon)
                self.iconphoto(False, icon_img)
            except: pass
            
        self.detailed_log_data  = ""
        self.array_stats        = []
        self.last_summary_path  = None
        self.last_log_path      = None
        self.last_html_path     = None
        self._setup_ui()
        self.after(100, self.check_queue)
        
    @staticmethod
    def _add_context_menu(widget):
        """Attach right-click Cut/Copy/Paste/Select-All and Ctrl+A to a Text widget."""
        def _select_all():
            widget.tag_add(tk.SEL, "1.0", tk.END)
            widget.mark_set(tk.INSERT, "1.0")
            widget.see(tk.INSERT)

        menu = tk.Menu(widget, tearoff=0)
        menu.add_command(label="Cut",        command=lambda: widget.event_generate("<<Cut>>"))
        menu.add_command(label="Copy",       command=lambda: widget.event_generate("<<Copy>>"))
        menu.add_command(label="Paste",      command=lambda: widget.event_generate("<<Paste>>"))
        menu.add_separator()
        menu.add_command(label="Select All", command=_select_all)

        def _show_menu(event):
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()

        def _ctrl_a(event):
            _select_all()
            return "break"

        widget.bind("<Button-3>", _show_menu)
        widget.bind("<Control-a>", _ctrl_a)

    def _setup_ui(self):
        config = self._load_config()
        self.config_data = config

        # Menu bar
        menubar = tk.Menu(self)
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="Usage / Help...", command=self._show_help)
        menubar.add_command(label="Email / SMTP", command=self._show_email_config)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.config(menu=menubar)

        main_frame = ttk.Frame(self, padding=5)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding=5)
        config_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Label wraplength roughly 100px.
        WL = 100
        
        # Row 0: FB User
        ttk.Label(config_frame, text="FB User:", wraplength=WL, justify=tk.LEFT).grid(row=0, column=0, sticky=tk.W, pady=2)
        self.user_fb_entry = ttk.Entry(config_frame, width=20)
        self.user_fb_entry.insert(0, config.get("user_fb", config.get("user", "pureuser")))
        self.user_fb_entry.grid(row=0, column=1, sticky=tk.W, pady=2)

        # Logo Inside Configuration Frame (Upper Right)
        _img_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
        if HAS_PIL and os.path.exists(os.path.join(_img_dir, "Everpure_logo.jpg")):
            try:
                pil_img = Image.open(os.path.join(_img_dir, "Everpure_logo.jpg"))
                # Resize keeping aspect ratio
                base_w = 200 # Slightly smaller to fit better inside frame
                w_percent = (base_w / float(pil_img.size[0]))
                h_size = int((float(pil_img.size[1]) * float(w_percent)))
                pil_img = pil_img.resize((base_w, h_size), Image.Resampling.LANCZOS)
                
                self.logo_img = ImageTk.PhotoImage(pil_img)
                logo_label = ttk.Label(config_frame, image=self.logo_img)
                logo_label.grid(row=0, column=5, rowspan=3, sticky=tk.NE, padx=10, pady=5)
            except Exception as e:
                print(f"Error loading logo: {e}")
        elif os.path.exists(os.path.join(_img_dir, "everpure_logo.png")):
             try:
                self.logo_img = tk.PhotoImage(file=os.path.join(_img_dir, "everpure_logo.png"))
                if self.logo_img.width() > 200:
                    self.logo_img = self.logo_img.subsample(3, 3)
                logo_label = ttk.Label(config_frame, image=self.logo_img)
                logo_label.grid(row=0, column=5, rowspan=3, sticky=tk.NE, padx=10, pady=5)
             except: pass
        
        # Row 1: FA-Files User
        ttk.Label(config_frame, text="FA-Files User:", wraplength=WL, justify=tk.LEFT).grid(row=1, column=0, sticky=tk.W, pady=2)
        self.user_faf_entry = ttk.Entry(config_frame, width=20)
        self.user_faf_entry.insert(0, config.get("user_faf", config.get("user", "pureuser")))
        self.user_faf_entry.grid(row=1, column=1, sticky=tk.W, pady=2)
        
        # Row 2: FA-Block User
        ttk.Label(config_frame, text="FA-Block User:", wraplength=WL, justify=tk.LEFT).grid(row=2, column=0, sticky=tk.W, pady=2)
        self.user_fab_entry = ttk.Entry(config_frame, width=20)
        self.user_fab_entry.insert(0, config.get("user_fab", config.get("user", "pureuser")))
        self.user_fab_entry.grid(row=2, column=1, sticky=tk.W, pady=2)

        # Row 3: Excluded Alerts
        ttk.Label(config_frame, text="Excluded Alerts (Partial Match or ID Range):", wraplength=120, justify=tk.LEFT).grid(row=3, column=0, sticky=tk.W, pady=2)
        self.alerts_entry = scrolledtext.ScrolledText(config_frame, width=40, height=3)
        self.alerts_entry.insert(tk.END, config.get("alerts_excluded", DEFAULT_EXCLUDED_ALERTS))
        # Reduced columnspan from 5 to 2 to make room for SLA header on the right
        self.alerts_entry.grid(row=3, column=1, columnspan=2, sticky=tk.W, pady=2)

        # SLA Section Header
        ttk.Label(config_frame, text="Replication Lag Thresholds", font=("Segoe UI", 9, "bold")).grid(row=3, column=3, columnspan=2, sticky=tk.S, pady=(0, 2))

        # HTML Report - Replication Lag Threshold Section Header
        ttk.Label(config_frame, text="HTML Report - Replication Lag Thresholds:", font=("Segoe UI", 9, "bold")).grid(row=3, column=5, columnspan=2, sticky=tk.S, pady=(0, 2))

        # Row 4: FB Arrays
        ttk.Label(config_frame, text="FB Arrays:", wraplength=WL, justify=tk.LEFT).grid(row=4, column=0, sticky=tk.NW, pady=2)
        self.fb_arr_entry = scrolledtext.ScrolledText(config_frame, width=40, height=4)
        self.fb_arr_entry.insert(tk.END, config.get("fb_arrays", DEFAULT_FB_ARRAYS))
        self.fb_arr_entry.grid(row=4, column=1, columnspan=2, sticky=tk.W, pady=2)
        self._add_context_menu(self.fb_arr_entry)
        
        # SLA labels in column 3, entry boxes in column 4 to prevent overlap
        ttk.Label(config_frame, text="SLA FB:", justify=tk.LEFT).grid(row=4, column=3, sticky=tk.W, padx=(10, 5), pady=2)
        self.sla_fb_entry = ttk.Entry(config_frame, width=10)
        self.sla_fb_entry.insert(0, config.get("sla_fb", "1h 30m"))
        self.sla_fb_entry.grid(row=4, column=4, sticky=tk.W, padx=(5, 10), pady=2)

        # Lag threshold: Yellow
        ttk.Label(config_frame, text="Yellow above:", justify=tk.LEFT).grid(row=4, column=5, sticky=tk.W, padx=(10, 5), pady=2)
        self.lag_yellow_entry = ttk.Entry(config_frame, width=10)
        self.lag_yellow_entry.insert(0, config.get("lag_yellow", "10m"))
        self.lag_yellow_entry.grid(row=4, column=6, sticky=tk.W, padx=(5, 10), pady=2)

        # Row 5: FA-File Arrays
        ttk.Label(config_frame, text="FA-File Arrays:", wraplength=WL, justify=tk.LEFT).grid(row=5, column=0, sticky=tk.NW, pady=2)
        self.faf_arr_entry = scrolledtext.ScrolledText(config_frame, width=40, height=4)
        self.faf_arr_entry.insert(tk.END, config.get("faf_arrays", DEFAULT_FA_FILE_ARRAYS))
        self.faf_arr_entry.grid(row=5, column=1, columnspan=2, sticky=tk.W, pady=2)
        self._add_context_menu(self.faf_arr_entry)
        
        ttk.Label(config_frame, text="SLA FA-File:", justify=tk.LEFT).grid(row=5, column=3, sticky=tk.W, padx=(10, 5), pady=2)
        self.sla_faf_entry = ttk.Entry(config_frame, width=10)
        self.sla_faf_entry.insert(0, config.get("sla_faf", "1h"))
        self.sla_faf_entry.grid(row=5, column=4, sticky=tk.W, padx=(5, 10), pady=2)

        # Lag threshold: Orange
        ttk.Label(config_frame, text="Orange above:", justify=tk.LEFT).grid(row=5, column=5, sticky=tk.W, padx=(10, 5), pady=2)
        self.lag_orange_entry = ttk.Entry(config_frame, width=10)
        self.lag_orange_entry.insert(0, config.get("lag_orange", "30m"))
        self.lag_orange_entry.grid(row=5, column=6, sticky=tk.W, padx=(5, 10), pady=2)

        # Row 6: FA-Block Arrays
        ttk.Label(config_frame, text="FA-Block Arrays:", wraplength=WL, justify=tk.LEFT).grid(row=6, column=0, sticky=tk.NW, pady=2)
        self.fab_arr_entry = scrolledtext.ScrolledText(config_frame, width=40, height=4)
        self.fab_arr_entry.insert(tk.END, config.get("fab_arrays", DEFAULT_FA_BLOCK_ARRAYS))
        self.fab_arr_entry.grid(row=6, column=1, columnspan=2, sticky=tk.W, pady=2)
        self._add_context_menu(self.fab_arr_entry)
        
        ttk.Label(config_frame, text="SLA FA-Block:", justify=tk.LEFT).grid(row=6, column=3, sticky=tk.W, padx=(10, 5), pady=2)
        self.sla_fab_entry = ttk.Entry(config_frame, width=10)
        self.sla_fab_entry.insert(0, config.get("sla_fab", "1h"))
        self.sla_fab_entry.grid(row=6, column=4, sticky=tk.W, padx=(5, 10), pady=2)
        
        # Row 7: Ignore Source Lag Checkbox for FA-Block
        self.ignore_source_lag_var = tk.BooleanVar(value=config.get("ignore_source_lag", False))
        fab_note_frame = ttk.Frame(config_frame)
        fab_note_frame.grid(row=7, column=0, columnspan=6, sticky=tk.W, padx=5, pady=(5, 2)) # Removed large padx
        
        ttk.Checkbutton(fab_note_frame, text="Ignore Source Side Replica Reporting.      ", 
                        variable=self.ignore_source_lag_var).pack(side=tk.LEFT)
        note_text_frame = ttk.Frame(fab_note_frame)
        note_text_frame.pack(side=tk.LEFT)
        sub_line_frame = ttk.Frame(note_text_frame)
        sub_line_frame.pack(side=tk.TOP, anchor=tk.W)
        ttk.Label(sub_line_frame, text="Use only ").pack(side=tk.LEFT)
        ttk.Label(sub_line_frame, text="Destination", font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT)
        ttk.Label(sub_line_frame, text=" array for FA-Block snapshot replication reporting.").pack(side=tk.LEFT)

        # Email config stored as plain attrs; edited via Email/SMTP menu dialog
        self._smtp_server = config.get("smtp_server", "")
        self._smtp_port   = config.get("smtp_port",   "587")
        self._smtp_from   = config.get("smtp_from",   "")
        self._smtp_to     = config.get("smtp_to",     "")

        # ── Button bar ────────────────────────────────────────────────────────
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=5)

        # Email button packed RIGHT first so it anchors to the far edge
        self.email_btn = tk.Button(btn_frame, text="Email Daily Report",
                                   command=self._email_daily_report, state=tk.DISABLED,
                                   relief=tk.RAISED, padx=6, pady=3)
        self.email_btn.pack(side=tk.RIGHT, padx=5)

        _orange_font = ("Segoe UI", 9, "bold")
        ttk.Button(btn_frame, text="Save Config", command=self._save_config).pack(side=tk.LEFT, padx=5)
        self.run_btn = tk.Button(btn_frame, text="Run Report", command=self.run_report,
                                 font=_orange_font, fg="#d4600a",
                                 relief=tk.RAISED, padx=6, pady=3)
        self.run_btn.pack(side=tk.LEFT, padx=5)
        self.open_summary_btn = ttk.Button(btn_frame, text="Open Summary",
                                           command=self._open_summary, state=tk.DISABLED)
        self.open_summary_btn.pack(side=tk.LEFT, padx=5)
        self.open_logs_btn = ttk.Button(btn_frame, text="Open Logs",
                                        command=self._open_logs, state=tk.DISABLED)
        self.open_logs_btn.pack(side=tk.LEFT, padx=5)
        self.open_daily_btn = tk.Button(btn_frame, text="Open Daily Report",
                                        command=self._open_daily_report, state=tk.DISABLED,
                                        font=_orange_font, fg="#d4600a",
                                        relief=tk.RAISED, padx=6, pady=3)
        self.open_daily_btn.pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Open History Report",
                   command=self._show_health_history).pack(side=tk.LEFT, padx=5)
        
        self.text_out = scrolledtext.ScrolledText(main_frame, wrap=tk.NONE)
        self.text_out.pack(fill=tk.BOTH, expand=True)

    def check_queue(self):
        if password_request_event.is_set():
            password_request_event.clear()
            pwd = simpledialog.askstring("Password Required", global_password_request_msg, show='*', parent=self)
            global global_password_response
            global_password_response = pwd
            password_response_event.set()
        self.after(200, self.check_queue)
        
    def _load_config(self):
        if os.path.exists("monitor_config.json"):
            try:
                with open("monitor_config.json", "r", encoding="utf-8") as f:
                    return json.load(f)
            except: pass
        return {}

    def _save_config(self):
        data = {
            "user_fb": self.user_fb_entry.get().strip(),
            "user_faf": self.user_faf_entry.get().strip(),
            "user_fab": self.user_fab_entry.get().strip(),
            "alerts_excluded": self.alerts_entry.get("1.0", tk.END).strip(),
            "fb_arrays": self.fb_arr_entry.get("1.0", tk.END).strip(),
            "faf_arrays": self.faf_arr_entry.get("1.0", tk.END).strip(),
            "fab_arrays": self.fab_arr_entry.get("1.0", tk.END).strip(),
            "sla_fb": self.sla_fb_entry.get().strip(),
            "sla_faf": self.sla_faf_entry.get().strip(),
            "sla_fab": self.sla_fab_entry.get().strip(),
            "lag_yellow": self.lag_yellow_entry.get().strip(),
            "lag_orange": self.lag_orange_entry.get().strip(),
            "ignore_source_lag": self.ignore_source_lag_var.get(),
            "smtp_server": self._smtp_server,
            "smtp_port":   self._smtp_port,
            "smtp_from":   self._smtp_from,
            "smtp_to":     self._smtp_to,
        }
        # Preserve any existing replication pairs; seed example entries if none defined yet
        default_pairs = [
            {"name": "Example Pair 1", "source": "source-array1",
             "destination": "dest-array1", "type": "FA-Block"},
            {"name": "Example Pair 2", "source": "source-array2",
             "destination": "dest-array2", "type": "FB"}
        ]
        data['replication_pairs'] = self.config_data.get('replication_pairs', default_pairs)
        with open("monitor_config.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        messagebox.showinfo("Saved", "Configuration saved!")

    def get_export_header(self):
        import time
        tz = time.tzname[time.daylight]
        now = datetime.datetime.now().strftime("%A, %B %d, %Y at %I:%M:%S %p")
        
        fb_sec = parse_time_to_seconds(self.sla_fb_entry.get())
        faf_sec = parse_time_to_seconds(self.sla_faf_entry.get())
        fab_sec = parse_time_to_seconds(self.sla_fab_entry.get())
        
        header = f"Output from Report run on {now} {tz}\n"
        header += f"Defined Replication SLA for SLA FB: {format_seconds_human(fb_sec)}\n"
        header += f"Defined Replication SLA for SLA FA-File: {format_seconds_human(faf_sec)}\n"
        header += f"Defined Replication SLA for SLA FA-Block: {format_seconds_human(fab_sec)}\n"

        excluded_codes = [x.strip() for x in self.alerts_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip() and "e.g." not in x]
        header += f"Alert Codes Ignored: {', '.join(excluded_codes) if excluded_codes else 'None'}\n"

        ignore_source = "Checked" if self.ignore_source_lag_var.get() else "Unchecked"
        header += f"Ignore Source Side Replica Reporting setting: {ignore_source}\n\n"
        
        # Add array lists
        fb_list = [x.strip() for x in self.fb_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()]
        for a in fb_list:
            header += f"FB Array - {a}\n"
        header += "\n"
            
        faf_list = [x.strip() for x in self.faf_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()]
        for a in faf_list:
            header += f"FA-File Array - {a}\n"
        header += "\n"
            
        fab_list = [x.strip() for x in self.fab_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()]
        for a in fab_list:
            header += f"FA-Block Array - {a}\n"

        pairs = self.config_data.get('replication_pairs', [])
        if pairs:
            header += "\nReplication Pairs:\n"
            for p in pairs:
                header += (f"  [{p.get('type', '?')}]  "
                           f"{p.get('source', '')}  \u2192  {p.get('destination', '')}    "
                           f"({p.get('name', '')})\n")

        return header + "\n"

    def export_report(self):
        txt = self.text_out.get("1.0", tk.END).strip()
        if not txt: return
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        default_name = f"Pure Alert and Replication Lag Summary {date_str}.log"
        path = filedialog.asksaveasfilename(defaultextension=".log", initialfile=default_name)
        if path:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(self.get_export_header() + txt)
            os.startfile(os.path.abspath(path))

    def export_detailed_report(self):
        if not self.detailed_log_data:
            messagebox.showwarning("Warning", "No detailed logs available. Please run a report first.")
            return
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        default_name = f"Pure Alert and Replication Lag Logs {date_str}.log"
        path = filedialog.asksaveasfilename(defaultextension=".log", initialfile=default_name)
        if path:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(self.get_export_header() + self.detailed_log_data)
                os.startfile(os.path.abspath(path))
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file: {e}")

    def run_report(self):
        self.run_btn.config(state=tk.NORMAL) # Reset in thread
        self.run_btn.config(state=tk.DISABLED)
        self.text_out.delete("1.0", tk.END)
        self.text_out.insert(tk.END, "Polling arrays... Please wait.\n\n")
        cfg = {
            'user_fb': self.user_fb_entry.get().strip(),
            'user_faf': self.user_faf_entry.get().strip(),
            'user_fab': self.user_fab_entry.get().strip(),
            'arr_fb': [x.strip() for x in self.fb_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()],
            'arr_faf': [x.strip() for x in self.faf_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()],
            'arr_fab': [x.strip() for x in self.fab_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()],
            'sla_fb': parse_time_to_seconds(self.sla_fb_entry.get()),
            'sla_faf': parse_time_to_seconds(self.sla_faf_entry.get()),
            'sla_fab': parse_time_to_seconds(self.sla_fab_entry.get()),
            'excluded': [x.strip() for x in self.alerts_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip() and "e.g." not in x],
            'ignore_source_lag': self.ignore_source_lag_var.get()
        }
        threading.Thread(target=self._run_collection, args=(cfg,), daemon=True).start()

    def _run_collection(self, config):
        final, detailed, stats = run_collection_core(config, nogui=False)
        self.after(0, lambda: self._update_gui(final, detailed, stats))


    # ── Open-file helpers (files are auto-saved after every run) ─────────────

    def _open_summary(self):
        if self.last_summary_path and os.path.exists(self.last_summary_path):
            os.startfile(os.path.abspath(self.last_summary_path))

    def _open_logs(self):
        if self.last_log_path and os.path.exists(self.last_log_path):
            os.startfile(os.path.abspath(self.last_log_path))

    def _open_daily_report(self):
        if self.last_html_path and os.path.exists(self.last_html_path):
            os.startfile(os.path.abspath(self.last_html_path))

    def _auto_save_reports(self, text, detailed, stats):
        """Auto-save summary log, detailed log, and HTML report after each run."""
        date_str    = datetime.datetime.now().strftime("%Y-%m-%d")
        script_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_summary = os.path.join(script_dir, "reports", "summary")
        dir_logs    = os.path.join(script_dir, "reports", "logs")
        dir_daily   = os.path.join(script_dir, "reports", "daily")
        for _d in (dir_summary, dir_logs, dir_daily):
            os.makedirs(_d, exist_ok=True)
        import time as _time
        tz      = _time.tzname[_time.daylight]
        now_str = datetime.datetime.now().strftime("%A, %B %d, %Y at %I:%M:%S %p")
        fb_sec  = parse_time_to_seconds(self.sla_fb_entry.get())
        faf_sec = parse_time_to_seconds(self.sla_faf_entry.get())
        fab_sec = parse_time_to_seconds(self.sla_fab_entry.get())
        excluded = [x.strip() for x in self.alerts_entry.get("1.0", tk.END)
                    .replace('\n', ',').split(',') if x.strip() and "e.g." not in x]
        ignore_lbl = "Checked" if self.ignore_source_lag_var.get() else "Unchecked"
        header = (f"Output from Report run on {now_str} {tz}\n"
                  f"Defined Replication SLA for SLA FB: {format_seconds_human(fb_sec)}\n"
                  f"Defined Replication SLA for SLA FA-File: {format_seconds_human(faf_sec)}\n"
                  f"Defined Replication SLA for SLA FA-Block: {format_seconds_human(fab_sec)}\n"
                  f"Alert Codes Ignored: {', '.join(excluded) if excluded else 'None'}\n"
                  f"Ignore Source Side Replica Reporting setting: {ignore_lbl}\n\n")

        # Summary log
        try:
            path = os.path.join(dir_summary, f"Pure Alert and Replication Lag Summary {date_str}.log")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(header + text)
            self.last_summary_path = path
            self.open_summary_btn.config(state=tk.NORMAL)
        except Exception:
            pass

        # Detailed log
        try:
            path = os.path.join(dir_logs, f"Pure Alert and Replication Lag Logs {date_str}.log")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(header + detailed)
            self.last_log_path = path
            self.open_logs_btn.config(state=tk.NORMAL)
        except Exception:
            pass

        # HTML daily report
        try:
            cfg = {
                'sla_fb':          fb_sec,
                'sla_faf':         faf_sec,
                'sla_fab':         fab_sec,
                'excluded':        excluded,
                'ignore_source_lag': self.ignore_source_lag_var.get(),
                'arr_fb':  [x.strip() for x in self.fb_arr_entry.get("1.0",  tk.END).replace('\n', ',').split(',') if x.strip()],
                'arr_faf': [x.strip() for x in self.faf_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()],
                'arr_fab': [x.strip() for x in self.fab_arr_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip()],
            }
            html = build_status_html(stats, cfg)
            path = os.path.join(dir_daily, f"Pure Array Report {date_str}.html")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(html)
            self.last_html_path = path
            self.open_daily_btn.config(state=tk.NORMAL)
            # Enable email button only when email config has at minimum a server and recipient
            if (self._smtp_server and self._smtp_to):
                self.email_btn.config(state=tk.NORMAL)
        except Exception:
            pass

    def _email_daily_report(self):
        """Prompt for SMTP password then email the saved daily HTML report."""
        server = self._smtp_server
        port   = self._smtp_port or "587"
        from_a = self._smtp_from
        to_a   = self._smtp_to

        if not all([server, from_a, to_a]):
            messagebox.showerror(
                "Email Configuration Incomplete",
                "Please fill in SMTP Server, From, and To fields in the\n"
                "Email Configuration section, then click Save Config.",
                parent=self)
            return

        if not self.last_html_path or not os.path.exists(self.last_html_path):
            messagebox.showerror("No Report", "No daily report has been generated yet.\n"
                                 "Run a report first.", parent=self)
            return

        pwd = simpledialog.askstring(
            "SMTP Password",
            f"Enter password for {from_a}\non {server}:{port}:",
            show='*', parent=self)
        if pwd is None:
            return  # user cancelled

        try:
            with open(self.last_html_path, 'r', encoding='utf-8') as f:
                html = f.read()
        except Exception as e:
            messagebox.showerror("Error", f"Could not read report file:\n{e}", parent=self)
            return

        self.email_btn.config(state=tk.DISABLED, text="Sending…")

        def _send():
            try:
                send_html_report(html, server, port, from_a, to_a, pwd)
                self.after(0, lambda: messagebox.showinfo(
                    "Email Sent",
                    f"Daily report sent successfully to:\n{to_a}", parent=self))
            except Exception as e:
                self.after(0, lambda msg=str(e): messagebox.showerror(
                    "Email Failed",
                    f"Failed to send report:\n{msg}", parent=self))
            finally:
                self.after(0, lambda: self.email_btn.config(
                    state=tk.NORMAL, text="Email Daily Report"))

        threading.Thread(target=_send, daemon=True).start()

    def _show_email_config(self):
        """Open the Email / SMTP configuration dialog."""
        dlg = tk.Toplevel(self)
        dlg.title("Email / SMTP Configuration")
        dlg.resizable(False, False)
        dlg.grab_set()  # modal

        frm = ttk.Frame(dlg, padding=14)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frm, text="SMTP Server:").grid(row=0, column=0, sticky=tk.W, pady=4)
        e_server = ttk.Entry(frm, width=34)
        e_server.insert(0, self._smtp_server)
        e_server.grid(row=0, column=1, columnspan=3, sticky=tk.W, pady=4)

        ttk.Label(frm, text="Port:").grid(row=1, column=0, sticky=tk.W, pady=4)
        e_port = ttk.Entry(frm, width=7)
        e_port.insert(0, self._smtp_port)
        e_port.grid(row=1, column=1, sticky=tk.W, pady=4)
        ttk.Label(frm, text="(587 = STARTTLS · 465 = SSL · 25 = plain)",
                  foreground="#666").grid(row=1, column=2, columnspan=2, sticky=tk.W, padx=(8, 0), pady=4)

        ttk.Label(frm, text="From:").grid(row=2, column=0, sticky=tk.W, pady=4)
        e_from = ttk.Entry(frm, width=34)
        e_from.insert(0, self._smtp_from)
        e_from.grid(row=2, column=1, columnspan=3, sticky=tk.W, pady=4)

        ttk.Label(frm, text="To:").grid(row=3, column=0, sticky=tk.W, pady=4)
        e_to = ttk.Entry(frm, width=50)
        e_to.insert(0, self._smtp_to)
        e_to.grid(row=3, column=1, columnspan=3, sticky=tk.W, pady=4)
        ttk.Label(frm, text="Comma-separated for multiple recipients.",
                  foreground="#666").grid(row=4, column=1, columnspan=3, sticky=tk.W)

        def _save():
            self._smtp_server = e_server.get().strip()
            self._smtp_port   = e_port.get().strip() or "587"
            self._smtp_from   = e_from.get().strip()
            self._smtp_to     = e_to.get().strip()
            # Persist alongside the rest of the configuration
            try:
                cfg_path = "monitor_config.json"
                data = {}
                if os.path.exists(cfg_path):
                    with open(cfg_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                data["smtp_server"] = self._smtp_server
                data["smtp_port"]   = self._smtp_port
                data["smtp_from"]   = self._smtp_from
                data["smtp_to"]     = self._smtp_to
                with open(cfg_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            except Exception as ex:
                messagebox.showwarning("Save Warning",
                                       f"Email settings updated in memory but could not be "
                                       f"written to monitor_config.json:\n{ex}", parent=dlg)
            dlg.destroy()

        btn_row = ttk.Frame(frm)
        btn_row.grid(row=5, column=0, columnspan=4, pady=(14, 0))
        ttk.Button(btn_row, text="Save",   command=_save).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row, text="Cancel", command=dlg.destroy).pack(side=tk.LEFT, padx=6)

        # Centre the dialog over the main window
        self.update_idletasks()
        dlg.update_idletasks()
        x = self.winfo_x() + (self.winfo_width()  - dlg.winfo_width())  // 2
        y = self.winfo_y() + (self.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")

    def _placeholder_removed(self):
        pass

    def _placeholder_removed2(self):
        pass

    def _export_html_report(self):
        # Reports are now auto-saved; this method retained for any legacy callers.
        self._open_daily_report()

    def _show_health_history(self):
        lag_yellow_min = parse_time_to_seconds(self.lag_yellow_entry.get().strip() or "10m") / 60.0
        lag_orange_min = parse_time_to_seconds(self.lag_orange_entry.get().strip() or "30m") / 60.0
        self._health_history_impl(lag_yellow_min, lag_orange_min,
                                  open_browser=True,
                                  _warn=messagebox.showwarning,
                                  _error=messagebox.showerror)

    @staticmethod
    def _health_history_impl(lag_yellow_min, lag_orange_min, open_browser=True,
                             _warn=None, _error=None):
        if _warn  is None: _warn  = lambda t, m: print(f"Warning: {m}")
        if _error is None: _error = lambda t, m: print(f"Error: {m}")
        import csv as _csv
        import base64, io, os, calendar, json

        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Pure Array History.csv")
        if not os.path.exists(csv_path):
            _warn("No History", f"No history file found:\n{csv_path}")
            return

        # ── Read CSV ──────────────────────────────────────────────────────────
        rows = []
        try:
            with open(csv_path, newline='', encoding='utf-8') as f:
                for r in _csv.DictReader(f):
                    rows.append(r)
        except Exception as e:
            _error("Error", f"Failed to read history CSV:\n{e}")
            return

        if not rows:
            _warn("No Data", "The history CSV file is empty.")
            return

        # ── Build daily aggregates (date × array) ────────────────────────────
        dates_set  = sorted({r['timestamp'][:10] for r in rows})
        arrays_set = sorted({r['array_name'] for r in rows})
        use_months = len(dates_set) > 30

        daily_sla  = {d: {a: 0    for a in arrays_set} for d in dates_set}
        daily_alrt = {d: {a: {'i': 0, 'w': 0, 'c': 0} for a in arrays_set} for d in dates_set}
        daily_lag  = {d: {a: None for a in arrays_set} for d in dates_set}
        for r in rows:
            d, a = r['timestamp'][:10], r['array_name']
            if r.get('sla_violated', '').strip().lower() == 'true':
                daily_sla[d][a] = 1
            try:
                daily_alrt[d][a]['i'] += int(r.get('info_alerts',     0))
                daily_alrt[d][a]['w'] += int(r.get('warning_alerts',  0))
                daily_alrt[d][a]['c'] += int(r.get('critical_alerts', 0))
            except ValueError:
                pass
            lag_str = r.get('avg_lag_sec', '').strip()
            if lag_str:
                try:
                    daily_lag[d][a] = float(lag_str) / 60.0   # store as minutes
                except ValueError:
                    pass

        # ── Group into periods ────────────────────────────────────────────────
        # Each period: (label, x_labels, sla_data, alert_data)
        # sla_data / alert_data are dicts  x_label -> {array -> value}
        if use_months:
            from collections import defaultdict
            month_keys = sorted({d[:7] for d in dates_set})   # 'YYYY-MM'
            periods = []
            for mk in month_keys:
                yr, mo = int(mk[:4]), int(mk[5:])
                label    = f"{calendar.month_name[mo]} {yr}"
                mo_dates = [d for d in dates_set if d[:7] == mk]
                sla_agg  = {d: daily_sla[d]  for d in mo_dates}
                alrt_agg = {d: daily_alrt[d] for d in mo_dates}
                # x-label: just the day number  "01", "02" …
                x_labels = [d[8:] for d in mo_dates]
                periods.append((label, mo_dates, x_labels, sla_agg, alrt_agg))
        else:
            x_labels = [d[5:] for d in dates_set]   # 'MM-DD'
            periods = [("All Days", dates_set, x_labels, daily_sla, daily_alrt)]

        # ── Chart helpers ─────────────────────────────────────────────────────
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.ticker
        import numpy as np

        cmap    = plt.get_cmap('tab10')
        colours = {a: cmap(i % 10) for i, a in enumerate(arrays_set)}

        def _sla_bar_b64(period_dates, x_labels, sla_agg, title):
            """SLA chart – stacked by array (unchanged)."""
            n = len(period_dates)
            fig, ax = plt.subplots(figsize=(max(7, n * 0.55), 4.2))
            x = np.arange(n); bottom = np.zeros(n); any_bar = False
            for a in arrays_set:
                vals = np.array([sla_agg[d][a] for d in period_dates], dtype=float)
                if vals.sum() > 0:
                    ax.bar(x, vals, 0.65, bottom=bottom, label=a, color=colours[a])
                    bottom += vals; any_bar = True
            ax.set_xticks(x); ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=8)
            ax.set_ylabel("# Violations", fontsize=9)
            ax.set_title(title, fontsize=11, fontweight='bold')
            ax.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True))
            if any_bar: ax.legend(loc='upper right', fontsize=8, framealpha=0.7)
            ax.grid(axis='y', linestyle='--', alpha=0.4)
            fig.tight_layout()
            buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=130); plt.close(fig); buf.seek(0)
            return base64.b64encode(buf.read()).decode('ascii')

        def _alrt_bar_b64(period_dates, x_labels, alrt_agg, title, show_info, show_warn):
            """Alert chart – stacked by severity (Info / Warning / Critical)."""
            n = len(period_dates)
            fig, ax = plt.subplots(figsize=(max(7, n * 0.55), 4.2))
            x = np.arange(n); bottom = np.zeros(n); any_bar = False
            c_vals = np.array([sum(alrt_agg[d][a]['c'] for a in arrays_set)
                               for d in period_dates], dtype=float)
            w_vals = np.array([sum(alrt_agg[d][a]['w'] for a in arrays_set)
                               for d in period_dates], dtype=float) if show_warn else np.zeros(n)
            i_vals = np.array([sum(alrt_agg[d][a]['i'] for a in arrays_set)
                               for d in period_dates], dtype=float) if show_info else np.zeros(n)
            for vals, label_s, colour in [
                (i_vals, 'Info',     '#5B9BD5'),
                (w_vals, 'Warning',  '#FFC000'),
                (c_vals, 'Critical', '#C00000'),
            ]:
                if vals.sum() > 0:
                    ax.bar(x, vals, 0.65, bottom=bottom, label=label_s, color=colour)
                    bottom += vals; any_bar = True
            ax.set_xticks(x); ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=8)
            ax.set_ylabel("Alert Count", fontsize=9)
            ax.set_title(title, fontsize=11, fontweight='bold')
            ax.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True))
            if any_bar: ax.legend(loc='upper right', fontsize=8, framealpha=0.7)
            ax.grid(axis='y', linestyle='--', alpha=0.4)
            fig.tight_layout()
            buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=130); plt.close(fig); buf.seek(0)
            return base64.b64encode(buf.read()).decode('ascii')

        def _lag_line_b64(period_dates, x_labels, arr_daily_lag, title,
                          yellow_min=10.0, orange_min=30.0):
            """Line chart of avg lag in minutes for one array over a period."""
            n     = len(period_dates)
            y_raw = [arr_daily_lag.get(d) for d in period_dates]
            y     = [v if v is not None else float('nan') for v in y_raw]
            y_fin = [v for v in y_raw if v is not None]
            y_max = max(max(y_fin) * 1.15, orange_min * 1.2) if y_fin else orange_min * 1.2

            fig, ax = plt.subplots(figsize=(max(8, n * 0.6), 4.5))
            x = np.arange(n)

            # Colour-banded background
            ax.axhspan(0,          yellow_min, alpha=0.10, color='#28a745', zorder=0)
            ax.axhspan(yellow_min, orange_min, alpha=0.10, color='#ffc107', zorder=0)
            ax.axhspan(orange_min, y_max,      alpha=0.10, color='#fd7e14', zorder=0)
            ax.set_ylim(0, y_max)

            # Threshold dashed lines
            ax.axhline(yellow_min, color='#856404', linestyle='--', linewidth=1,
                       alpha=0.75, label=f'{yellow_min:g} min (yellow)')
            ax.axhline(orange_min, color='#7a3500', linestyle='--', linewidth=1,
                       alpha=0.75, label=f'{orange_min:g} min (orange)')

            # Data line
            ax.plot(x, y, color='#2E4D8C', linewidth=2,
                    marker='o', markersize=4, zorder=3, label='Avg Lag')

            ax.set_xticks(x)
            ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=8)
            ax.set_ylabel('Average Lag (minutes)', fontsize=9)
            ax.set_title(title, fontsize=11, fontweight='bold')
            ax.legend(fontsize=8, loc='upper right', framealpha=0.7)
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            fig.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=130)
            plt.close(fig)
            buf.seek(0)
            return base64.b64encode(buf.read()).decode('ascii')

        # ── Chart cache: load and prepare ─────────────────────────────────────
        import hashlib
        _cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "Pure_Array_History_cache.json")
        _chart_cache = {}
        try:
            if os.path.exists(_cache_path):
                with open(_cache_path, 'r', encoding='utf-8') as _cf:
                    _chart_cache = json.load(_cf)
        except Exception:
            _chart_cache = {}

        # Group raw CSV rows by YYYY-MM for hashing
        _rows_by_month = {}
        for _r in rows:
            _mk = _r['timestamp'][:7]
            _rows_by_month.setdefault(_mk, []).append(_r)

        def _period_hash(period_dates):
            """Hash the CSV data + thresholds for a period to detect changes."""
            _mk  = period_dates[0][:7] if period_dates else ''
            _mrs = sorted(_rows_by_month.get(_mk, []),
                          key=lambda r: (r['timestamp'], r['array_name']))
            _raw = json.dumps(_mrs, sort_keys=True) + f"|{lag_yellow_min}|{lag_orange_min}"
            return hashlib.md5(_raw.encode()).hexdigest()

        # ── Generate one chart-set per period (SLA + 4 alert severity combos) ─
        period_labels = []
        sla_charts    = []
        alrt_ii = []; alrt_ic = []; alrt_wc = []; alrt_c = []   # ii=Info+Warn, ic=Info, wc=Warn, c=Critical-only
        lag_charts = {}   # {label: {array: b64_line_chart}}
        for label, period_dates, x_labels, sla_agg, alrt_agg in periods:
            period_labels.append(label)
            pt = label if use_months else "Daily"

            # Check chart cache (monthly mode only)
            _ph     = _period_hash(period_dates) if use_months else None
            _cached = _chart_cache.get(label, {}) if _ph else {}
            _hit    = bool(_ph and _cached.get('hash') == _ph)

            if _hit:
                sla_charts.append(_cached['sla'])
                alrt_ii.append(_cached['alrt_ii'])
                alrt_ic.append(_cached['alrt_ic'])
                alrt_wc.append(_cached['alrt_wc'])
                alrt_c.append(_cached['alrt_c'])
                lag_charts[label] = _cached['lag']
            else:
                _sla = _sla_bar_b64(period_dates, x_labels, sla_agg,
                                    f"SLA Violations – {pt}")
                _aii = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                     f"Alerts – {pt}", True,  True)
                _aic = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                     f"Alerts – {pt}", True,  False)
                _awc = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                     f"Alerts – {pt}", False, True)
                _ac  = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                     f"Alerts – {pt}", False, False)
                _arr = {}
                for a in arrays_set:
                    _arr[a] = _lag_line_b64(
                        period_dates, x_labels, {d: daily_lag[d][a] for d in period_dates},
                        f"{a}  \u2013  {pt}  Avg Replication Lag",
                        lag_yellow_min, lag_orange_min)
                sla_charts.append(_sla)
                alrt_ii.append(_aii); alrt_ic.append(_aic)
                alrt_wc.append(_awc); alrt_c.append(_ac)
                lag_charts[label] = _arr
                if _ph:
                    _chart_cache[label] = {
                        'hash': _ph, 'sla': _sla,
                        'alrt_ii': _aii, 'alrt_ic': _aic,
                        'alrt_wc': _awc, 'alrt_c': _ac,
                        'lag': _arr,
                    }

        # ── Save updated chart cache ──────────────────────────────────────────
        try:
            with open(_cache_path, 'w', encoding='utf-8') as _cf:
                json.dump(_chart_cache, _cf)
        except Exception:
            pass   # cache write failure is non-fatal

        # ── Calendar day-status data (monthly mode only) ─────────────────────
        cal_data     = {}
        lag_cal_data = {}   # {label: {array: {day_key: minutes_float}}}
        if use_months:
            for label, period_dates, x_labels, sla_agg, alrt_agg in periods:
                day_map = {}
                for d, xl in zip(period_dates, x_labels):
                    any_viol = any(sla_agg[d][a] for a in arrays_set)
                    ti = sum(alrt_agg[d][a]['i'] for a in arrays_set)
                    tw = sum(alrt_agg[d][a]['w'] for a in arrays_set)
                    tc = sum(alrt_agg[d][a]['c'] for a in arrays_set)
                    day_map[xl] = {"v": 1 if any_viol else 0,
                                   "i": ti, "w": tw, "c": tc}
                cal_data[label] = day_map

                arr_lag = {}
                for a in arrays_set:
                    day_lag = {}
                    for d, xl in zip(period_dates, x_labels):
                        v = daily_lag[d][a]
                        if v is not None:
                            day_lag[xl] = round(v, 1)  # minutes, 1 decimal
                    arr_lag[a] = day_lag
                lag_cal_data[label] = arr_lag

        # ── Serialise chart arrays for JS ──────────────────────────────────────
        import json
        js_labels      = json.dumps(period_labels)
        js_sla         = json.dumps(sla_charts)
        js_alrt_ii     = json.dumps(alrt_ii)   # Info + Warning + Critical
        js_alrt_ic     = json.dumps(alrt_ic)   # Info + Critical
        js_alrt_wc     = json.dumps(alrt_wc)   # Warning + Critical
        js_alrt_c      = json.dumps(alrt_c)    # Critical only
        js_cal         = json.dumps(cal_data)
        js_lag_cal     = json.dumps(lag_cal_data)
        js_lag_charts  = json.dumps(lag_charts)
        js_array_names = json.dumps(sorted(arrays_set))
        js_lag_yellow  = round(lag_yellow_min, 4)
        js_lag_orange  = round(lag_orange_min, 4)
        nav_note       = ("Grouped by month &mdash; use arrows to navigate"
                          if use_months else "Showing all days")

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Array Health History</title>
  <style>
    body           {{ font-family: Segoe UI, Arial, sans-serif; font-size: 10pt;
                     margin: 0; padding: 20px 28px; background: #f5f7fa; }}
    h1             {{ font-size: 16pt; margin: 0 0 4px 0; color: #1a2d5a; }}
    .meta          {{ color: #666; font-size: 9pt; margin-bottom: 14px; }}
    .nav-bar       {{ display: flex; align-items: center; gap: 14px;
                     flex-wrap: wrap; margin-bottom: 14px; }}
    .nav-btn       {{ font-size: 18pt; background: #2E4D8C; color: #fff;
                     border: none; border-radius: 6px; padding: 2px 14px;
                     cursor: pointer; line-height: 1.4; }}
    .nav-btn:disabled             {{ background: #aab; cursor: default; }}
    .nav-btn:hover:not(:disabled) {{ background: #3a63b8; }}
    #period-label  {{ font-size: 13pt; font-weight: bold; color: #2E4D8C;
                     min-width: 160px; text-align: center; }}
    .counter       {{ font-size: 9pt; color: #888; }}
    .filter-sep    {{ color: #ccc; font-size: 14pt; }}
    .chk-label     {{ font-size: 9pt; color: #333; display: flex;
                     align-items: center; gap: 5px; cursor: pointer; }}
    .chk-label input {{ cursor: pointer; }}
    h2             {{ font-size: 11pt; margin: 18px 0 6px 0; color: #2E4D8C; }}
    .chart-wrap    {{ background: #fff; border-radius: 8px;
                     box-shadow: 0 1px 4px rgba(0,0,0,.12);
                     padding: 10px; display: inline-block; }}
    img            {{ display: block; }}
    /* ── calendars ─────────────────────────────────────────────────────── */
    .cal-row       {{ display: flex; flex-wrap: wrap; gap: 24px; margin-bottom: 18px; }}
    .cal-block     {{ background: #fff; border-radius: 8px;
                     box-shadow: 0 1px 4px rgba(0,0,0,.12); padding: 12px 16px 10px; }}
    .cal-title     {{ font-size: 10pt; font-weight: bold; color: #2E4D8C; margin-bottom: 8px; }}
    .cal-table     {{ border-collapse: collapse; font-size: 9pt; }}
    .cal-table th  {{ background: #2E4D8C; color: #fff; padding: 5px 10px;
                     text-align: center; font-weight: bold; }}
    .cal-day       {{ text-align: center; padding: 5px 9px;
                     border: 1px solid #ddd; min-width: 30px; cursor: default; }}
    .cal-red       {{ background: #ffcccc; color: #800000; font-weight: bold; }}
    .cal-amber     {{ background: #fff3cc; color: #7a5000; font-weight: bold; }}
    .cal-green     {{ background: #d4edda; color: #155724; }}
    .cal-nodata    {{ background: #f5f5f5; color: #bbb; }}
    .cal-empty     {{ border-color: transparent; }}
    .cal-legend    {{ display: flex; gap: 14px; margin-top: 9px;
                     font-size: 8pt; color: #555; flex-wrap: wrap; }}
    .leg-swatch    {{ display: inline-block; width: 11px; height: 11px;
                     border-radius: 2px; margin-right: 3px; vertical-align: middle; }}
    /* ── lag calendar colours ─────────────────────────────────────────── */
    .cal-lag-green  {{ background: #d4edda; color: #155724; }}
    .cal-lag-yellow {{ background: #fff9c4; color: #856404; }}
    .cal-lag-orange {{ background: #ffe0b2; color: #7a3500; font-weight: bold; }}
    /* ── lag section title ───────────────────────────────────────────── */
    .lag-array-name {{ font-size: 9.5pt; font-weight: bold; color: #2E4D8C;
                      margin-bottom: 6px; }}
    /* ── clickable lag cards ─────────────────────────────────────────── */
    .cal-lag-clickable          {{ cursor: pointer; transition: box-shadow .15s; }}
    .cal-lag-clickable:hover    {{ box-shadow: 0 4px 16px rgba(0,0,0,.22); }}
    /* ── lag detail modal ────────────────────────────────────────────── */
    #lag-modal      {{ display:none; position:fixed; top:0; left:0; width:100%;
                      height:100%; background:rgba(0,0,0,.55); z-index:1000;
                      align-items:center; justify-content:center; }}
    #lag-modal-box  {{ background:#fff; border-radius:10px; padding:22px 24px 16px;
                      max-width:93%; position:relative;
                      box-shadow:0 8px 32px rgba(0,0,0,.3); }}
    #lag-modal-close {{ position:absolute; top:10px; right:14px; font-size:16pt;
                       line-height:1; border:none; background:none;
                       cursor:pointer; color:#666; }}
    #lag-modal-close:hover {{ color:#000; }}
    #lag-modal-title {{ margin:0 0 12px 0; color:#1a2d5a;
                       font-size:12pt; font-weight:bold; }}
  </style>
</head>
<body>
  <h1>Everpure &ndash; Array Health History</h1>
  <p class="meta">
    Source: {os.path.basename(csv_path)} &nbsp;&bull;&nbsp;
    {len(dates_set)} day(s) &nbsp;&bull;&nbsp;
    {len(arrays_set)} array(s): {', '.join(arrays_set)}<br>
    {nav_note}
  </p>

  <div class="nav-bar">
    <button class="nav-btn" id="btn-prev" onclick="navigate(-1)">&#8592;</button>
    <span id="period-label"></span>
    <button class="nav-btn" id="btn-next" onclick="navigate(1)">&#8594;</button>
    <span class="counter" id="counter"></span>
    <span class="filter-sep">|</span>
    <label class="chk-label">
      <input type="checkbox" id="chk-info" checked onchange="onFilterChange()">
      Show Informational
    </label>
    <label class="chk-label">
      <input type="checkbox" id="chk-warn" checked onchange="onFilterChange()">
      Show Warning
    </label>
    <span style="font-size:8pt;color:#888;">(Critical always shown)</span>
  </div>

  <div class="cal-row">
    <div class="cal-block" id="cal-sla-wrap">
      <div class="cal-title">SLA Violations</div>
      <div id="cal-sla-body"></div>
    </div>
    <div class="cal-block" id="cal-alrt-wrap">
      <div class="cal-title">Support Alerts</div>
      <div id="cal-alrt-body"></div>
    </div>
  </div>

  <h2>SLA Violations</h2>
  <div class="chart-wrap">
    <img id="img-sla" src="" alt="SLA violations chart">
  </div>

  <h2>Support Alerts</h2>
  <div class="chart-wrap">
    <img id="img-alrt" src="" alt="Alert count chart">
  </div>

  <h2>Array Replication Lag</h2>
  <p style="font-size:8.5pt;color:#666;margin:-4px 0 10px 0;">
    Click any array calendar to see its lag trend for the month.
  </p>
  <div id="lag-row" class="cal-row" style="margin-top:8px;"></div>

  <!-- Lag detail modal -->
  <div id="lag-modal" onclick="closeLagModal(event)">
    <div id="lag-modal-box">
      <button id="lag-modal-close" onclick="closeLagModal(event)">&#x2715;</button>
      <p id="lag-modal-title"></p>
      <img id="lag-modal-img" src="" style="display:block;max-width:100%;">
    </div>
  </div>

  <script>
    var LABELS       = {js_labels};
    var SLA          = {js_sla};
    var ALRT_II      = {js_alrt_ii};
    var ALRT_IC      = {js_alrt_ic};
    var ALRT_WC      = {js_alrt_wc};
    var ALRT_C       = {js_alrt_c};
    var CAL_DATA     = {js_cal};
    var LAG_CAL_DATA = {js_lag_cal};
    var LAG_CHARTS      = {js_lag_charts};
    var ARRAY_NAMES     = {js_array_names};
    var LAG_YELLOW_MIN  = {js_lag_yellow};
    var LAG_ORANGE_MIN  = {js_lag_orange};
    var idx          = 0;

    var MONTH_NAMES = ['January','February','March','April','May','June',
                       'July','August','September','October','November','December'];
    var DAY_NAMES   = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

    function getAlrtCharts() {{
      var si = document.getElementById('chk-info').checked;
      var sw = document.getElementById('chk-warn').checked;
      if  (si &&  sw) return ALRT_II;
      if  (si && !sw) return ALRT_IC;
      if (!si &&  sw) return ALRT_WC;
      return ALRT_C;
    }}

    /* Visible alert total for a day given current checkbox state. */
    function visibleAlerts(info) {{
      if (!info) return 0;
      var si = document.getElementById('chk-info').checked;
      var sw = document.getElementById('chk-warn').checked;
      return info.c + (sw ? info.w : 0) + (si ? info.i : 0);
    }}

    function buildCal(firstDay, daysInMonth, dayData, getCls, getTip, legend) {{
      var h = '<table class="cal-table"><tr>';
      for (var i = 0; i < 7; i++) h += '<th>' + DAY_NAMES[i] + '</th>';
      h += '</tr><tr>';
      var col = 0;
      for (var i = 0; i < firstDay; i++) {{ h += '<td class="cal-empty"></td>'; col++; }}
      for (var day = 1; day <= daysInMonth; day++) {{
        var key  = day < 10 ? '0' + day : '' + day;
        var info = dayData[key];
        h += '<td class="cal-day ' + getCls(info) + '" title="' + getTip(info) + '">' + day + '</td>';
        col++;
        if (col % 7 === 0 && day < daysInMonth) h += '</tr><tr>';
      }}
      while (col % 7 !== 0) {{ h += '<td class="cal-empty"></td>'; col++; }}
      h += '</tr></table><div class="cal-legend">' + legend + '</div>';
      return h;
    }}

    function renderCalendars(label) {{
      var parts = label.split(' ');
      var mi    = MONTH_NAMES.indexOf(parts[0]);
      var show  = (mi !== -1 && parts[1]);
      document.getElementById('cal-sla-wrap').style.display  = show ? '' : 'none';
      document.getElementById('cal-alrt-wrap').style.display = show ? '' : 'none';
      if (!show) return;
      var yr          = parseInt(parts[1], 10);
      var firstDay    = new Date(yr, mi, 1).getDay();
      var daysInMonth = new Date(yr, mi + 1, 0).getDate();
      var dayData     = CAL_DATA[label] || {{}};

      document.getElementById('cal-sla-body').innerHTML = buildCal(
        firstDay, daysInMonth, dayData,
        function(info) {{ return !info ? 'cal-nodata' : (info.v ? 'cal-red' : 'cal-green'); }},
        function(info) {{ return !info ? 'No data'   : (info.v ? 'SLA Violated' : 'No Violation'); }},
        '<span><span class="leg-swatch" style="background:#ffcccc;"></span>Violated</span>'
      + '<span><span class="leg-swatch" style="background:#d4edda;"></span>No Violation</span>'
      + '<span><span class="leg-swatch" style="background:#f5f5f5;border:1px solid #ccc;"></span>No Data</span>'
      );

      document.getElementById('cal-alrt-body').innerHTML = buildCal(
        firstDay, daysInMonth, dayData,
        function(info) {{
          if (!info) return 'cal-nodata';
          return visibleAlerts(info) > 0 ? 'cal-amber' : 'cal-green';
        }},
        function(info) {{
          if (!info) return 'No data';
          var n = visibleAlerts(info);
          if (n === 0) return 'No Alerts';
          var parts = [];
          if (info.c)                                          parts.push(info.c + ' Critical');
          if (info.w && document.getElementById('chk-warn').checked) parts.push(info.w + ' Warning');
          if (info.i && document.getElementById('chk-info').checked) parts.push(info.i + ' Info');
          return parts.join(', ');
        }},
        '<span><span class="leg-swatch" style="background:#fff3cc;"></span>Has Alerts</span>'
      + '<span><span class="leg-swatch" style="background:#d4edda;"></span>No Alerts</span>'
      + '<span><span class="leg-swatch" style="background:#f5f5f5;border:1px solid #ccc;"></span>No Data</span>'
      );
    }}

    function onFilterChange() {{ render(); }}

    function openLagChart(label, arr) {{
      var charts = LAG_CHARTS[label];
      if (!charts || !charts[arr]) return;
      document.getElementById('lag-modal-title').textContent = arr + '  \u2013  ' + label;
      document.getElementById('lag-modal-img').src = 'data:image/png;base64,' + charts[arr];
      document.getElementById('lag-modal').style.display = 'flex';
    }}

    function closeLagModal(e) {{
      var modal = document.getElementById('lag-modal');
      var box   = document.getElementById('lag-modal-box');
      if (e.target === modal || !box.contains(e.target) ||
          e.currentTarget.id === 'lag-modal-close') {{
        modal.style.display = 'none';
      }}
    }}

    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape') document.getElementById('lag-modal').style.display = 'none';
    }});

    function renderLagCalendars(label) {{
      var row   = document.getElementById('lag-row');
      var parts = label.split(' ');
      var mi    = MONTH_NAMES.indexOf(parts[0]);
      var show  = (mi !== -1 && parts[1]);
      if (!show) {{ row.innerHTML = ''; return; }}
      var yr          = parseInt(parts[1], 10);
      var firstDay    = new Date(yr, mi, 1).getDay();
      var daysInMonth = new Date(yr, mi + 1, 0).getDate();
      var monthData   = LAG_CAL_DATA[label] || {{}};
      var lagLegend   =
          '<span><span class="leg-swatch" style="background:#d4edda;"></span>&lt;' + LAG_YELLOW_MIN + ' min</span>'
        + '<span><span class="leg-swatch" style="background:#fff9c4;"></span>' + LAG_YELLOW_MIN + '&ndash;' + LAG_ORANGE_MIN + ' min</span>'
        + '<span><span class="leg-swatch" style="background:#ffe0b2;"></span>&gt;' + LAG_ORANGE_MIN + ' min</span>'
        + '<span><span class="leg-swatch" style="background:#f5f5f5;border:1px solid #ccc;"></span>No Data</span>';
      var html = '';
      for (var ai = 0; ai < ARRAY_NAMES.length; ai++) {{
        var arr     = ARRAY_NAMES[ai];
        var lagData = monthData[arr] || {{}};
        var calHtml = buildCal(
          firstDay, daysInMonth, lagData,
          function(mins) {{
            if (mins === undefined || mins === null) return 'cal-nodata';
            if (mins < LAG_YELLOW_MIN)  return 'cal-lag-green';
            if (mins < LAG_ORANGE_MIN)  return 'cal-lag-yellow';
            return 'cal-lag-orange';
          }},
          function(mins) {{
            if (mins === undefined || mins === null) return 'No data';
            return mins.toFixed(1) + ' min avg lag';
          }},
          lagLegend
        );
        html += '<div class="cal-block cal-lag-clickable"'
             +       ' data-label="' + label + '" data-arr="' + arr + '">'
             +   '<div class="lag-array-name">' + arr
             +     ' <span style="font-size:8pt;color:#999;font-weight:normal;">'
             +     '&#x1F4C8; click for trend</span></div>'
             +   calHtml
             + '</div>';
      }}
      row.innerHTML = html;
      // Remove any previous listener before adding a new one (avoids duplicates
      // when the user navigates between months), and do NOT use {{ once: true }}
      // so the listener stays active for all subsequent clicks.
      if (row._lagClickHandler) {{
        row.removeEventListener('click', row._lagClickHandler);
      }}
      row._lagClickHandler = function(e) {{
        var card = e.target.closest('.cal-lag-clickable');
        if (card) openLagChart(card.dataset.label, card.dataset.arr);
      }};
      row.addEventListener('click', row._lagClickHandler);
    }}

    function navigate(dir) {{
      idx = Math.max(0, Math.min(LABELS.length - 1, idx + dir));
      render();
    }}

    function render() {{
      document.getElementById('period-label').textContent = LABELS[idx];
      document.getElementById('img-sla').src   = 'data:image/png;base64,' + SLA[idx];
      document.getElementById('img-alrt').src  = 'data:image/png;base64,' + getAlrtCharts()[idx];
      document.getElementById('btn-prev').disabled = (idx === 0);
      document.getElementById('btn-next').disabled = (idx === LABELS.length - 1);
      document.getElementById('counter').textContent = (idx + 1) + ' / ' + LABELS.length;
      renderCalendars(LABELS[idx]);
      renderLagCalendars(LABELS[idx]);
    }}

    idx = LABELS.length - 1;
    render();
  </script>
</body>
</html>"""

        reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        out_path = os.path.join(reports_dir, "Pure_Array_History.html")
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(html)
            if open_browser and os.name == 'nt':
                os.startfile(os.path.abspath(out_path))
        except Exception as e:
            _error("Error", f"Failed to save history HTML:\n{e}")

    def _show_help(self):
        win = tk.Toplevel(self)
        win.title("Everpure - Help")
        win.geometry("680x580")
        win.resizable(True, True)
        _icon = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images", "pure_logo.png")
        if os.path.exists(_icon):
            try:
                win.iconphoto(False, tk.PhotoImage(file=_icon))
            except: pass

        # Header frame: logo upper-left
        header_frame = ttk.Frame(win)
        header_frame.pack(fill=tk.X, padx=10, pady=(8, 4))
        if hasattr(self, 'logo_img'):
            ttk.Label(header_frame, image=self.logo_img).pack(side=tk.LEFT)

        text = scrolledtext.ScrolledText(win, wrap=tk.WORD, padx=12, pady=10,
                                         font=("Segoe UI", 9))
        text.pack(fill=tk.BOTH, expand=True, padx=5)

        help_text = """\
EVERPURE - Pure Storage Alert and Replication SLA Monitor
==========================================================

OVERVIEW
--------
Everpure connects to Pure Storage arrays via SSH and checks two things:

  1. ALERTS  - Queries each array for open alerts, filtering out any
               alert codes you have configured to ignore.

  2. REPLICATION SLA  - Compares replication lag against your defined
               thresholds and flags links that exceed the SLA.

Three array types are supported:
  * FB (FlashBlade)        - file replication via 'purefs replica-link'
  * FA-File (FlashArray)   - file replication via 'purepod replica-link'
  * FA-Block (FlashArray)  - block snapshot replication via 'purevol'


CONFIGURATION FIELDS
--------------------
  FB / FA-Files / FA-Block User
      SSH username used to connect to each array type.
      The ideal method is to use SSH keys, from the user/computer running
      the script to each array.  But if some/all of the arrays do not have
      this setup, the user will be prompted to enter the password for each 
      array not using keys.

  Excluded Alerts
      Comma-separated list of alert codes or partial strings to ignore.
      Ranges are supported (e.g. "2000-3000"). Any alert line containing
      a matching value will be suppressed from the output.  These should be
      used sparingly as they will suppress any alert in the GUI or Report.

  FB / FA-File / FA-Block Arrays
      Comma- or newline-separated list of array hostnames or IP addresses.
      If the same array name appears in both FA-File and FA-Block, its
      alerts are checked only once.

  SLA FB / SLA FA-File / SLA FA-Block
      Maximum acceptable replication lag. Accepts values like:
        30m   1h   1h 30m   2h 45m   90m

  Ignore Source Side Replica Reporting (FA-Block)
      When checked, only destination-side snapshot transfers are evaluated.
      Source-side entries (those still showing a numeric progress value)
      are excluded from the FA-Block SLA check.

  Replication Pairs
      A list of source → destination array relationships stored in
      monitor_config.json. These are displayed in the "Replication Pairs"
      panel and included in exported report headers for reference.

      To add or edit pairs, open monitor_config.json and update the
      "replication_pairs" section. Each pair has four fields:

        "name"        - A friendly label for the relationship
        "source"      - Hostname or IP of the source array
        "destination" - Hostname or IP of the destination array
        "type"        - One of: "FB", "FA-File", or "FA-Block"

      Example:
        "replication_pairs": [
          {
            "name": "Site A to Site B",
            "source": "flasharray-prod",
            "destination": "flasharray-dr",
            "type": "FA-Block"
          },
          {
            "name": "FlashBlade DR",
            "source": "fb-site-a",
            "destination": "fb-site-b",
            "type": "FB"
          }
        ]

      You may define as many pairs as needed. The list is preserved
      when you click "Save Config" in the GUI.


BUTTONS
-------
  Save Config          Saves all current settings to monitor_config.json
                       in the same directory as the script.

  Run Report           Polls all configured arrays and displays results
                       in the output panel below.

  Save Report Summary  Saves the summary output (Alerts + Replication
                       sections) to a dated .log file of your choice.

  Save All Logs        Saves the full SSH command log (raw output from
                       every command sent to every array) to a dated
                       .log file of your choice.

  Save Word Report     Exports a Word-compatible (.docx) summary report
                       after a Run Report has been completed. The document
                       contains a table with one row per array (FB,
                       FA-File, and FA-Block) and four columns:

                         Array Name   - Hostname or IP of the array
                         Type         - FB, FA-File, or FA-Block
                         Alert Count  - Number of active alerts found
                                        (-1 or "Error" if SSH failed)
                         Lag vs SLA   - A mini bar chart with three bars:
                                          SLA Target (blue)
                                          Avg Lag    (green = OK, red = exceeded)
                                          Max Lag    (green = OK, red = exceeded)
                                        Values are shown in minutes.
                                        If no replication data was collected
                                        (e.g. SSH error) the cell shows
                                        "No data collected" instead.

                       The file is opened automatically in Word after
                       saving. Requires python-docx and matplotlib
                       (pip install python-docx matplotlib).


RUNNING WITHOUT THE GUI (--nogui MODE)
---------------------------------------
The script can be run unattended from the command line, for example
as a scheduled task or cron job:

    python pure_monitor.py --nogui

In this mode:
  - Settings are read from monitor_config.json (use "Save Config" in
    the GUI first to create this file).
  - Both output files are saved automatically to the current directory
    using the default dated filenames.
  - If an array requires a password or cannot be reached, it is skipped
    and the reason is noted in the output files. No prompts are shown.

SSH COMMANDS USED
-----------------
This script interacts with the arrays via three remote SSH commands only.
These are:

    purepod replica-link list --historical 24h --lag
    purefs replica-link list
    purealert list --filter "state='open'"

EMAILING REPORTS
----------------
Fill in the Email Configuration section of the GUI and click Save Config.
After running a report, click "Email Daily Report" — you will be prompted
for your SMTP password (never stored on disk).

For headless / scheduled use, set the environment variable
EVERPURE_SMTP_PASSWORD and pass --email alongside --nogui:

    set EVERPURE_SMTP_PASSWORD=MyP@ssword
    python pure_monitor.py --nogui --email

Supports STARTTLS (port 587, default) and SSL (port 465).

COMMAND-LINE OPTIONS
--------------------
    python pure_monitor.py               Launch the GUI (default)
    python pure_monitor.py --nogui       Run headlessly
    python pure_monitor.py --nogui --email  Run headlessly and email the report
    python pure_monitor.py --alert-debug Launch GUI with synthetic alert data
                                         (no live arrays needed — tests the
                                         daily report alert columns & modal)
    python pure_monitor.py --help        Show command-line help
"""
        text.insert(tk.END, help_text)
        text.config(state=tk.DISABLED)

        ttk.Button(win, text="Close", command=win.destroy).pack(pady=8)

    def _append_history_csv(self, stats):
        """Append per-array stats from the current run to Pure Array History.csv."""
        append_history_csv(stats)

    def _update_gui(self, text, detailed, stats):
        self.detailed_log_data = detailed
        self.array_stats = stats
        self.text_out.delete("1.0", tk.END)
        self.text_out.insert(tk.END, text)
        self.run_btn.config(state=tk.NORMAL)
        self._append_history_csv(stats)
        self._auto_save_reports(text, detailed, stats)

if __name__ == "__main__":
    if '-h' in sys.argv or '--help' in sys.argv:
        print("""
Everpure - Pure Storage Alert and Replication SLA Monitor

USAGE:
    python pure_monitor.py [OPTION]

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

    --email        (Use with --nogui) Email the daily HTML report after saving it.
                   SMTP settings must be saved in monitor_config.json via the GUI's
                   Email Configuration section. The SMTP password must be supplied
                   through the environment variable EVERPURE_SMTP_PASSWORD — it is
                   never stored on disk.
                   Supports STARTTLS (port 587, default) and SSL (port 465).
                   Example:
                     set EVERPURE_SMTP_PASSWORD=MyP@ssword
                     python pure_monitor.py --nogui --email

    -h, --help     Show this help message and exit.

EXAMPLES:
    python pure_monitor.py
    python pure_monitor.py --nogui
    python pure_monitor.py --nogui --email
    python pure_monitor.py --alert-debug
    python pure_monitor.py --help

SSH COMMANDS USED
-----------------
This script interacts with the arrays via three remote SSH commands only.
These are:

    purepod replica-link list --historical 24h --lag
    purefs replica-link list
    purealert list --filter "state='open'"

CONFIGURATION:
    Launch the GUI at least once and click "Save Config" to create
    monitor_config.json before using --nogui mode.
""")
    elif '--nogui' in sys.argv:
        run_nogui()
    elif '--alert-debug' in sys.argv:
        # GUI mode with synthetic alert data — no live arrays required.
        PureMonitorApp().mainloop()
    else:
        PureMonitorApp().mainloop()
