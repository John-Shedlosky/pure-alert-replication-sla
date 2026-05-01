import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog, filedialog
import threading
import queue
import re
import datetime
import json
import os
import sys
import webbrowser
from concurrent.futures import ThreadPoolExecutor

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

try:
    from tksheet import Sheet
    HAS_TKSHEET = True
except ImportError:
    HAS_TKSHEET = False

# =========================================================
# CONFIGURABLE DEFAULTS
# =========================================================
DEFAULT_FB_ARRAYS = "flashblade1\nflashblade2"
DEFAULT_FA_FILE_ARRAYS = "flasharray2\nflasharray1"
DEFAULT_FA_BLOCK_ARRAYS = "flasharray2\nflasharray1"
DEFAULT_FB_LOCATIONS = ""
DEFAULT_FA_FILE_LOCATIONS = ""
DEFAULT_FA_BLOCK_LOCATIONS = ""
DEFAULT_EXCLUDED_ALERTS = "9999, 9998"

# We use events/queues to prompt for passwords in the main thread
password_request_event = threading.Event()
password_response_event = threading.Event()
global_password_request_msg = ""
global_password_response = None
credentials_cache = {}
# Serializes the request/response transaction in ask_password_in_main so
# that concurrent workers (e.g. parallel array detection) don't clobber
# global_password_request_msg or both consume the same response.
_password_prompt_lock = threading.Lock()
# Guards alerted_arrays, alert_counts and detailed_logs writes when the
# replication loops process up to 4 arrays concurrently. Workers also use
# their own thread-local buffers for alert_lines / repl_lines so report
# ordering follows array input order rather than worker completion order.
_alert_collection_lock = threading.Lock()

# Set to True when --alert-debug is passed on the command line.
# In this mode SSH calls are bypassed and synthetic alert / lag data are injected
# so that the daily HTML report and history CSV can be tested without live arrays.
# --fake-arrays additionally injects a synthetic 12-array / 5-location configuration
# (see _fake_arrays_config) so the GUI / report can be exercised without any real
# monitor_config.json. Fake-arrays mode implies alert-debug because the synthetic
# arrays cannot be reached over SSH.
FAKE_ARRAYS = '--fake-arrays' in sys.argv
ALERT_DEBUG = ('--alert-debug' in sys.argv) or FAKE_ARRAYS

def ask_password_in_main(msg):
    global global_password_request_msg
    with _password_prompt_lock:
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

def _fake_arrays_config():
    """Return a synthetic config dict with 12 arrays across 5 locations.

    Used by --fake-arrays mode so the GUI / Daily HTML report / History
    page can be demonstrated without any saved monitor_config.json or
    reachable live arrays. Names follow a city-prefix convention so the
    grouped sections in the daily report visibly cluster by location.

    Returned shape matches what _load_config / run_nogui consume: the
    new-style "arrays" list-of-dicts (with name + location + notes), plus
    SLA targets, default usernames, and a few replication pairs that
    cross sites so the Replication Pairs panel is non-empty.
    """
    arrays = [
        # New York (3)
        {'name': 'nyc-pure-fa-01', 'location': 'New York, NY',
         'notes': 'Primary VMware datastores'},
        {'name': 'nyc-pure-fa-02', 'location': 'New York, NY',
         'notes': 'SQL prod cluster'},
        {'name': 'nyc-pure-fb-01', 'location': 'New York, NY',
         'notes': 'Analytics + nightly backup target'},
        # Chicago (3)
        {'name': 'chi-pure-fa-01', 'location': 'Chicago, IL',
         'notes': 'DR target for NYC FA-Block'},
        {'name': 'chi-pure-fa-02', 'location': 'Chicago, IL',
         'notes': 'Oracle prod'},
        {'name': 'chi-pure-fb-01', 'location': 'Chicago, IL',
         'notes': 'NFS shares + DR for NYC FB'},
        # Dallas (2)
        {'name': 'dal-pure-fa-01', 'location': 'Dallas, TX',
         'notes': 'Mixed workload, Tier-2'},
        {'name': 'dal-pure-fb-01', 'location': 'Dallas, TX',
         'notes': 'S3 object storage'},
        # Seattle (2)
        {'name': 'sea-pure-fa-01', 'location': 'Seattle, WA',
         'notes': 'West-coast primary'},
        {'name': 'sea-pure-fa-02', 'location': 'Seattle, WA',
         'notes': 'Dev / test'},
        # London (2)
        {'name': 'lon-pure-fa-01', 'location': 'London, UK',
         'notes': 'EMEA primary'},
        {'name': 'lon-pure-fb-01', 'location': 'London, UK',
         'notes': 'EMEA backup target'},
    ]
    return {
        'user_fb':  'pureuser',
        'user_faf': 'pureuser',
        'user_fab': 'pureuser',
        'sla_fb':   '1h 30m',
        'sla_faf':  '1h',
        'sla_fab':  '1h',
        'arrays':   arrays,
        'alerts_excluded': '',
        'ignore_source_lag': False,
        'replication_pairs': [
            {'name': 'NYC FA \u2192 Chi FA',
             'source': 'nyc-pure-fa-01', 'destination': 'chi-pure-fa-01',
             'type': 'FA-Block'},
            {'name': 'NYC FB \u2192 Chi FB',
             'source': 'nyc-pure-fb-01', 'destination': 'chi-pure-fb-01',
             'type': 'FB'},
            {'name': 'Sea FA \u2192 Dal FA',
             'source': 'sea-pure-fa-01', 'destination': 'dal-pure-fa-01',
             'type': 'FA-Block'},
        ],
    }


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


def parse_arr_loc(arr_val, loc_val):
    """Parse parallel newline-delimited Array and Location text blocks.

    Returns (arrays, locations) lists with a strict 1:1 index relationship.
    Rows whose array name is blank are dropped (and their paired location
    discarded). When the locations block has fewer lines than the arrays
    block, missing entries become empty strings.
    """
    _arrs = (arr_val or '').splitlines()
    _locs = (loc_val or '').splitlines()
    out_arr, out_loc = [], []
    for i, a in enumerate(_arrs):
        a = a.strip()
        if not a:
            continue
        l = _locs[i].strip() if i < len(_locs) else ''
        out_arr.append(a)
        out_loc.append(l)
    return out_arr, out_loc


def align_rel_pairs_by_location(raw_pairs):
    """Return *raw_pairs* reordered so arrays at the same location land in
    the same column across all rows.

    The first encountered pair seeds the left (``a_loc``) and right
    (``b_loc``) location columns; subsequent pairs are swapped when
    necessary so any location consistently sits on the same side.
    Missing / blank locations are tracked under the sentinel
    ``"(no location)"``. Pairs whose two locations have both already been
    mapped to the same column are kept as-is (left-side preference).

    Each input pair must be a dict with keys
    ``a_name`` ``a_plat`` ``a_loc`` ``a_status``
    ``b_name`` ``b_plat`` ``b_loc`` ``b_status``.
    The returned list contains new dicts with the same keys.
    """
    LOC_EMPTY = '(no location)'
    def _lkey(v):
        return v if v else LOC_EMPTY

    col_of_loc = {}   # location -> 'L' or 'R'
    out = []
    for pp in raw_pairs:
        la, lb = _lkey(pp.get('a_loc', '')), _lkey(pp.get('b_loc', ''))
        # Seed mappings for any locations not yet placed.
        if la not in col_of_loc and lb not in col_of_loc:
            col_of_loc[la] = 'L'
            if lb != la:
                col_of_loc[lb] = 'R'
        elif la in col_of_loc and lb not in col_of_loc:
            col_of_loc[lb] = 'R' if col_of_loc[la] == 'L' else 'L'
        elif lb in col_of_loc and la not in col_of_loc:
            col_of_loc[la] = 'R' if col_of_loc[lb] == 'L' else 'L'

        swap = False
        if col_of_loc.get(la) == 'L' or col_of_loc.get(lb) == 'R':
            swap = False
        elif col_of_loc.get(la) == 'R' or col_of_loc.get(lb) == 'L':
            swap = True
        # else: same-column conflict -> keep as-is (prefer left-side alignment)

        if swap:
            out.append({
                'a_name': pp['b_name'], 'a_plat': pp['b_plat'],
                'a_loc':  pp['b_loc'],  'a_status': pp['b_status'],
                'b_name': pp['a_name'], 'b_plat': pp['a_plat'],
                'b_loc':  pp['a_loc'],  'b_status': pp['a_status'],
            })
        else:
            out.append(dict(pp))
    return out


def _parse_csv_text(text):
    """Return [[row cells ...], ...] from a CSV blob, or [] if empty/unparsable."""
    import csv as _csv
    import io as _io
    if not text or not text.strip():
        return []
    try:
        return list(_csv.reader(_io.StringIO(text.strip())))
    except Exception:
        return []


def _classify_array_output(purearray_csv, purepod_csv, purepgroup_csv):
    """Classify an array into (is_fb, is_faf, is_fab, is_nrp) from three CSV blobs.

    Rules (per request):
      * purearray list has a 'Product Type' column containing 'FlashBlade' -> FB.
      * Otherwise the array is a FlashArray; it may be FA-File, FA-Block, both,
        or neither ('No Replication FA'):
          - purepod list with any data rows  -> FA-File
          - purepgroup list 'Targets' column with any non-empty, non '-' cell
            -> FA-Block
    """
    fb = faf = fab = False

    pa_rows = _parse_csv_text(purearray_csv)
    if pa_rows:
        header = [c.strip() for c in pa_rows[0]]
        try:
            pt_idx = next(i for i, c in enumerate(header) if c.lower() == 'product type')
        except StopIteration:
            pt_idx = -1
        if pt_idx >= 0:
            for row in pa_rows[1:]:
                if pt_idx < len(row) and 'flashblade' in row[pt_idx].strip().lower():
                    fb = True
                    break
    if fb:
        return True, False, False, False

    pod_rows = _parse_csv_text(purepod_csv)
    if len(pod_rows) > 1:
        faf = True

    pg_rows = _parse_csv_text(purepgroup_csv)
    if pg_rows:
        pg_hdr = [c.strip() for c in pg_rows[0]]
        try:
            tgt_idx = next(i for i, c in enumerate(pg_hdr) if c.lower() == 'targets')
        except StopIteration:
            tgt_idx = -1
        if tgt_idx >= 0:
            for row in pg_rows[1:]:
                if tgt_idx < len(row):
                    val = row[tgt_idx].strip()
                    if val and val != '-':
                        fab = True
                        break

    nrp = not (faf or fab)
    return False, faf, fab, nrp


def detect_array_type(array, users, detailed_logs=None, nogui=False):
    """Detect an array's platform and replication capabilities via SSH.

    *users* is an ordered iterable of (label, username) tuples (e.g. the three
    configured users in FB, FA-File, FA-Block order). Detection tries each
    until one connects, then issues ``purearray list --csv`` followed by
    ``purepod list --csv`` and ``purepgroup list --csv`` (the latter two only
    when the array is not a FlashBlade).

    Returns a dict with keys:
        is_fb, is_faf, is_fab, is_nrp  - booleans
        user   - username that succeeded (or None if all failed)
        error  - last error string, or None on success

    When ALERT_DEBUG is set, SSH is bypassed and the array is reported as a
    FA-Block so the rest of the pipeline has something to work with.
    """
    result = {'is_fb': False, 'is_faf': False, 'is_fab': False, 'is_nrp': False,
              'user': None, 'error': None}
    if ALERT_DEBUG:
        result['is_fab'] = True
        result['user']   = (users[0][1] if users else None)
        return result

    pa_out = None
    last_err = None
    used_user = None
    for _label, _u in users:
        if not _u:
            continue
        try:
            pa_out = run_ssh_command(array, _u, "purearray list --csv",
                                     log_list=detailed_logs, nogui=nogui)
            used_user = _u
            break
        except Exception as e:
            last_err = str(e)
            continue
    if pa_out is None:
        result['error'] = last_err or "No SSH user could connect"
        return result

    result['user'] = used_user
    pod_out = pg_out = ''
    # Only need pod / pgroup output when we might be a FlashArray. A cheap way
    # is to peek for 'FlashBlade' before issuing them.
    if 'flashblade' not in pa_out.lower():
        try:
            pod_out = run_ssh_command(array, used_user, "purepod list --csv",
                                      log_list=detailed_logs, nogui=nogui)
        except Exception as e:
            last_err = str(e)
        try:
            pg_out = run_ssh_command(array, used_user, "purepgroup list --csv",
                                     log_list=detailed_logs, nogui=nogui)
        except Exception as e:
            last_err = str(e)

    fb, faf, fab, nrp = _classify_array_output(pa_out, pod_out, pg_out)
    result['is_fb']  = fb
    result['is_faf'] = faf
    result['is_fab'] = fab
    result['is_nrp'] = nrp
    if not any((fb, faf, fab, nrp)):
        result['error'] = last_err or "Could not classify array"
    return result


def parse_unified_arrays(val):
    """Parse the unified ``arrays`` config value into [(name, location), ...].

    Accepts either a list of ``{"name": ..., "location": ...}`` dicts (the
    preferred new form) or a newline/semicolon-delimited string where each
    row is ``"name<TAB or comma>location"``. Blank name rows are dropped.
    """
    out = []
    if isinstance(val, list):
        for item in val:
            if not isinstance(item, dict):
                continue
            name = str(item.get('name', '') or '').strip()
            if not name:
                continue
            loc = str(item.get('location', '') or '').strip()
            out.append((name, loc))
        return out
    if isinstance(val, str):
        for line in val.splitlines():
            parts = re.split(r'[\t,]', line, maxsplit=1)
            name = parts[0].strip()
            if not name:
                continue
            loc = parts[1].strip() if len(parts) > 1 else ''
            out.append((name, loc))
    return out


def unified_arrays_from_config(raw):
    """Return [(name, location), ...] from a raw config dict.

    Prefers the new-style ``arrays`` list-of-dicts key. When absent, falls
    back to the legacy ``fb_arrays`` / ``faf_arrays`` / ``fab_arrays`` and
    paired ``*_locations`` newline-delimited strings; names that appear in
    more than one legacy bucket are deduplicated (first occurrence wins).
    """
    if 'arrays' in raw:
        return parse_unified_arrays(raw.get('arrays'))
    seen = set()
    out = []
    for arr_key, loc_key in (('fb_arrays',  'fb_locations'),
                             ('faf_arrays', 'faf_locations'),
                             ('fab_arrays', 'fab_locations')):
        names, locs = parse_arr_loc(raw.get(arr_key, ''), raw.get(loc_key, ''))
        for n, l in zip(names, locs):
            if n in seen:
                continue
            seen.add(n)
            out.append((n, l))
    return out


def parse_unified_arrays_full(val):
    """Parse the unified ``arrays`` config value into [(name, location, notes), ...].

    Same input shapes as :func:`parse_unified_arrays` but also extracts the
    optional ``notes`` field. The 2-tuple variant remains the canonical form
    for the SSH/report pipeline; this 3-tuple variant is used by the GUI
    sheet so the user-entered notes survive a save/reload cycle.
    """
    out = []
    if isinstance(val, list):
        for item in val:
            if not isinstance(item, dict):
                continue
            name = str(item.get('name', '') or '').strip()
            if not name:
                continue
            loc = str(item.get('location', '') or '').strip()
            notes = str(item.get('notes', '') or '').strip()
            out.append((name, loc, notes))
        return out
    if isinstance(val, str):
        for line in val.splitlines():
            parts = re.split(r'[\t,]', line, maxsplit=2)
            name = parts[0].strip()
            if not name:
                continue
            loc   = parts[1].strip() if len(parts) > 1 else ''
            notes = parts[2].strip() if len(parts) > 2 else ''
            out.append((name, loc, notes))
    return out


def unified_arrays_from_config_full(raw):
    """Return [(name, location, notes), ...] from a raw config dict.

    Mirrors :func:`unified_arrays_from_config` but preserves notes when the
    new-style ``arrays`` list-of-dicts key is present. Legacy fall-back
    rows always have empty notes.
    """
    if 'arrays' in raw:
        return parse_unified_arrays_full(raw.get('arrays'))
    return [(n, l, '') for n, l in unified_arrays_from_config(raw)]


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
        _users = [
            ('FB',      config.get('user_fb',  'pureuser')),
            ('FA-File', config.get('user_faf', 'pureuser')),
            ('FA-Block',config.get('user_fab', 'pureuser')),
        ]
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
            try:
                info = detect_array_type(_name, _users,
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
            _hw_targets.append((_a, config.get('user_fb',  'pureuser'), 'FB'))
            _hw_seen.add(_a)
    for _a in config.get('arr_faf', []):
        if _a and _a not in _hw_seen:
            _hw_targets.append((_a, config.get('user_faf', 'pureuser'), 'FA'))
            _hw_seen.add(_a)
    for _a in config.get('arr_fab', []):
        if _a and _a not in _hw_seen:
            _hw_targets.append((_a, config.get('user_fab', 'pureuser'), 'FA'))
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
            _rel_targets.append((_a, config.get('user_fb', 'pureuser'), 'FB', _fb_arrs))
            _rel_seen.add(_a)
    for _a in _fa_arrs:
        if _a not in _rel_seen:
            _user = (config.get('user_faf', 'pureuser') if _a in _faf_arrs
                     else config.get('user_fab', 'pureuser'))
            _rel_targets.append((_a, _user, 'FA', _fa_arrs))
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
        check_alert(array, config['user_fb'], local_alert_lines)
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
        check_alert(array, config['user_faf'], local_alert_lines)
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
        check_alert(array, config['user_fab'], local_alert_lines)
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
    cfg = {
        'user_fb':  raw.get('user_fb',  'pureuser'),
        'user_faf': raw.get('user_faf', 'pureuser'),
        'user_fab': raw.get('user_fab', 'pureuser'),
        'arrays':   [{'name': n, 'location': l, 'notes': nt} for n, l, nt in _arrays],
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

def _csv_to_dicts(text):
    """Parse CSV blob into [{header: cell}, ...]; tolerant of empty input."""
    rows = _parse_csv_text(text)
    if not rows:
        return []
    header = [c.strip() for c in rows[0]]
    out = []
    for r in rows[1:]:
        if not r:
            continue
        d = {}
        for i, h in enumerate(header):
            d[h] = (r[i].strip() if i < len(r) else '')
        out.append(d)
    return out


def _fake_protection_data_for(array):
    """Synthetic per-array protection data used when ALERT_DEBUG is set.
    Generates a small but representative mix of plain volumes, pod
    volumes (with a stretched pair), and three snapshot kinds so the
    aggregator and HTML can be exercised without live arrays.
    """
    # Two stretched pod pairs across the 12 fake arrays:
    #   nyc-pure-fa-01 <-> chi-pure-fa-01  (pod 'vmware_pod')
    #   dal-pure-fa-01 <-> sea-pure-fa-01  (pod 'oracle_pod')
    pod_pairs = {
        'nyc-pure-fa-01': ('chi-pure-fa-01', 'vmware_pod', 'vmware_pod', '-->'),
        'chi-pure-fa-01': ('nyc-pure-fa-01', 'vmware_pod', 'vmware_pod', '<--'),
        'dal-pure-fa-01': ('sea-pure-fa-01', 'oracle_pod', 'oracle_pod', '-->'),
        'sea-pure-fa-01': ('dal-pure-fa-01', 'oracle_pod', 'oracle_pod', '<--'),
    }
    volumes  = [{'name': f'{array}_vol01', 'pod': None, 'volume': f'{array}_vol01'},
                {'name': f'{array}_vol02', 'pod': None, 'volume': f'{array}_vol02'}]
    pod_links = []
    snapshots = []
    pgroups   = []
    if array in pod_pairs:
        rem, lp, rp, direction = pod_pairs[array]
        for vname in ('db_data', 'db_log'):
            volumes.append({'name': f'{lp}::{vname}', 'pod': lp, 'volume': vname})
            for _i in range(2):
                snapshots.append({'source': f'{lp}::{vname}', 'name': f'{lp}::{vname}.snap{_i}'})
        pod_links.append({'local_pod': lp, 'direction': direction,
                          'remote_pod': rp, 'remote_array': rem, 'status': 'replicating'})
        # Pod-scoped pgroup; identical name on both sides of the stretch.
        pgroups.append({'name': f'{lp}::pg_{lp}', 'pod': lp,
                        'pgname': f'pg_{lp}',
                        'volumes': [f'{lp}::db_data', f'{lp}::db_log']})
    # Two local snapshots of vol01, one replicated snapshot from a peer.
    snapshots.append({'source': f'{array}_vol01', 'name': f'{array}_vol01.snap1'})
    snapshots.append({'source': f'{array}_vol01', 'name': f'{array}_vol01.snap2'})
    # Each FA receives a replicated snapshot from the next FA in the list
    # (forms a ring) so Replicated Snapshots / Destinations get populated.
    _peers = ['nyc-pure-fa-01', 'nyc-pure-fa-02', 'chi-pure-fa-01', 'chi-pure-fa-02',
              'dal-pure-fa-01', 'sea-pure-fa-01', 'sea-pure-fa-02', 'lon-pure-fa-01']
    if array in _peers:
        _idx  = _peers.index(array)
        _peer = _peers[(_idx + 1) % len(_peers)]
        snapshots.append({'source': f'{_peer}:{_peer}_vol02',
                          'name': f'{_peer}:{_peer}_vol02.snap1'})
    # Local pgroup covering vol01 (and vol02 on every other array, to vary
    # multi-pgroup membership in the demo).
    _local_pg_vols = [f'{array}_vol01']
    if array in _peers and _peers.index(array) % 2 == 0:
        _local_pg_vols.append(f'{array}_vol02')
    pgroups.append({'name': f'pg_{array}_daily', 'pod': None,
                    'pgname': f'pg_{array}_daily',
                    'volumes': _local_pg_vols})
    # A second local pgroup on vol01 only, so a few volumes show two
    # pgroups in the rendered table.
    if array in _peers and _peers.index(array) % 3 == 0:
        pgroups.append({'name': f'pg_{array}_hourly', 'pod': None,
                        'pgname': f'pg_{array}_hourly',
                        'volumes': [f'{array}_vol01']})
    # Retention-lock map: pod-scoped pgroups are 'ratcheted' so the
    # stretched-pod source rows demo Safemode=Enabled. Local _hourly
    # pgroups are also ratcheted on every other array that has them.
    pgroup_locks = {}
    for pg in pgroups:
        if pg['pod'] is not None:
            pgroup_locks[pg['name']] = 'ratcheted'
        elif pg['name'].endswith('_hourly') and \
             array in _peers and _peers.index(array) % 2 == 0:
            pgroup_locks[pg['name']] = 'ratcheted'
        else:
            pgroup_locks[pg['name']] = 'unlocked'
    # Synthetic host connections: vol01 connected to a single host on
    # every array; vol02 connected to two hosts on every other array;
    # pod-resident db_data connected to a cluster host on the source side.
    connections = []
    connections.append({'name': f'{array}_vol01', 'host': f'{array}-host01'})
    if array in _peers and _peers.index(array) % 2 == 0:
        connections.append({'name': f'{array}_vol02', 'host': f'{array}-host01'})
        connections.append({'name': f'{array}_vol02', 'host': f'{array}-host02'})
    if array in pod_pairs:
        _lp = pod_pairs[array][1]
        _direction = pod_pairs[array][3]
        if _direction == '-->':
            connections.append({'name': f'{_lp}::db_data',
                                'host': f'{_lp}-cluster'})
    # Schedule + retention profiles for each pgroup. Pod-scoped pgroups
    # carry a 2-target retention (this array + peer) so the / split is
    # exercised in the demo. Local pgroups have a single retention row.
    pgroup_schedules = {}
    pgroup_retentions = {}
    for pg in pgroups:
        pgname = pg['name']
        if pg['name'].endswith('_hourly'):
            pgroup_schedules[pgname] = {
                'Schedule': ['snap', 'replicate'],
                'Enabled':  ['True', 'True'],
                'Frequency': ['3600', '3600'],
                'At':        ['', ''],
                'Blackout':  ['', '']}
        else:
            pgroup_schedules[pgname] = {
                'Schedule': ['snap', 'replicate'],
                'Enabled':  ['True', 'True'],
                'Frequency': ['86400', '86400'],
                'At':        ['09:00:00', '09:30:00'],
                'Blackout':  ['', '']}
        if pg['pod'] is not None and array in pod_pairs:
            _peer = pod_pairs[array][0]
            pgroup_retentions[pgname] = {
                'Array':         [array, _peer],
                'All For':       ['1d', '1d'],
                'Per Period':    ['4', '4'],
                'Period Length': ['1d', '1d'],
                'Days':          ['7', '7']}
        else:
            pgroup_retentions[pgname] = {
                'Array':         [array],
                'All For':       ['1d'],
                'Per Period':    ['4'],
                'Period Length': ['1d'],
                'Days':          ['7']}
    return {'volumes': volumes, 'pod_links': pod_links, 'snapshots': snapshots,
            'pgroups': pgroups, 'pgroup_locks': pgroup_locks,
            'pgroup_schedules': pgroup_schedules,
            'pgroup_retentions': pgroup_retentions,
            'connections': connections, 'error': None}


def _parse_purevol_list_csv(text):
    """Parse `purevol list --csv` -> [{'name','pod','volume'}, ...].
    Pod separator is `::`; absent -> pod=None, volume=name.
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        if '::' in name:
            pod, vol = name.split('::', 1)
            out.append({'name': name, 'pod': pod, 'volume': vol})
        else:
            out.append({'name': name, 'pod': None, 'volume': name})
    return out


def _parse_purepod_replica_link_csv(text):
    """Parse `purepod replica-link list --csv` rows.
    Returns list of {'local_pod','direction','remote_pod','remote_array','status'}.
    Column names vary slightly across Purity versions; this matches loosely.
    """
    out = []
    for d in _csv_to_dicts(text):
        local_pod = (d.get('Name') or d.get('Local Pod') or '').strip()
        direction = (d.get('Direction') or '').strip()
        remote_pod = (d.get('Remote Pod') or '').strip()
        remote_array = (d.get('Remote') or d.get('Remote Array') or '').strip()
        status = (d.get('Status') or '').strip()
        if not local_pod:
            continue
        out.append({'local_pod': local_pod, 'direction': direction,
                    'remote_pod': remote_pod, 'remote_array': remote_array,
                    'status': status})
    return out


def _parse_purevol_snap_csv(text):
    """Parse `purevol list --snap --csv` rows -> [{'name','source'}, ...]."""
    out = []
    for d in _csv_to_dicts(text):
        out.append({'name': (d.get('Name') or '').strip(),
                    'source': (d.get('Source') or '').strip()})
    return out


def _parse_purepgroup_list_csv(text):
    """Parse `purepgroup list --csv` rows.
    Returns [{'name','pod','pgname','volumes'}, ...] where 'volumes' is a
    list of volume entries (split on '/'). If Name contains '::', the
    first part is the pod name; the full Name string is preserved as
    'name' (used as the protection group identifier).
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        if '::' in name:
            pod, pgname = name.split('::', 1)
        else:
            pod, pgname = None, name
        vols_raw = (d.get('Volumes') or '').strip()
        vols = [v.strip() for v in vols_raw.split('/') if v.strip()]
        out.append({'name': name, 'pod': pod, 'pgname': pgname,
                    'volumes': vols})
    return out


def _parse_purepgroup_retention_csv(text):
    """Parse `purepgroup list --retention-lock --csv` rows.
    Returns {pg_full_name: retention_lock_value_lowercase}. The 'Name'
    column is preserved verbatim (including any `pod::` prefix).
    """
    out = {}
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        rl = (d.get('Retention Lock') or '').strip().lower()
        out[name] = rl
    return out


def _parse_purevol_connect_csv(text):
    """Parse `purevol list --connect --csv` rows -> [{'name','host'}, ...].
    A volume connected to multiple hosts appears on multiple rows. Names
    containing '::' are pod-qualified (`pod::vol`) and preserved verbatim.
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        host = (d.get('Host') or '').strip()
        if not name or not host:
            continue
        out.append({'name': name, 'host': host})
    return out


def _parse_purepgroup_schedule_csv(text):
    """Parse `purepgroup list --schedule --csv` rows.
    Returns {pg_full_name: {col: [vals]}}. Every column other than 'Name'
    is split on '/' so each parallel index represents one schedule entry.
    """
    cols = ('Schedule', 'Enabled', 'Frequency', 'At', 'Blackout')
    out = {}
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        out[name] = {c: [v.strip() for v in (d.get(c) or '').split('/')]
                     for c in cols}
    return out


def _parse_purepgroup_retention_full_csv(text):
    """Parse `purepgroup list --retention --csv` rows.
    Returns {pg_full_name: {col: [vals]}}. Every column other than 'Name'
    is split on '/' so each parallel index represents one retention entry.
    """
    cols = ('Array', 'All For', 'Per Period', 'Period Length', 'Days')
    out = {}
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        out[name] = {c: [v.strip() for v in (d.get(c) or '').split('/')]
                     for c in cols}
    return out


def _collect_one_fa_protection(array, user, detailed_logs, nogui=False):
    """Issue the three FlashArray protection commands and parse their output.
    Returns {'volumes', 'pod_links', 'snapshots', 'error'}. Errors on a
    single command are non-fatal: that section returns [] and the error
    text is recorded in 'error'.
    """
    if ALERT_DEBUG:
        return _fake_protection_data_for(array)
    out = {'volumes': [], 'pod_links': [], 'snapshots': [],
           'pgroups': [], 'pgroup_locks': {}, 'pgroup_schedules': {},
           'pgroup_retentions': {}, 'connections': [], 'error': None}
    _errs = []
    try:
        out['volumes'] = _parse_purevol_list_csv(run_ssh_command(
            array, user, "purevol list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purevol list: {e}")
    try:
        out['pod_links'] = _parse_purepod_replica_link_csv(run_ssh_command(
            array, user, "purepod replica-link list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepod replica-link list: {e}")
    try:
        out['snapshots'] = _parse_purevol_snap_csv(run_ssh_command(
            array, user, "purevol list --snap --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purevol list --snap: {e}")
    # Only fetch pgroups if we managed to discover any volumes; pgroups are
    # purely a per-volume annotation and the call is otherwise pointless.
    if out['volumes']:
        try:
            out['pgroups'] = _parse_purepgroup_list_csv(run_ssh_command(
                array, user, "purepgroup list --csv",
                log_list=detailed_logs, nogui=nogui))
        except Exception as e:
            _errs.append(f"purepgroup list: {e}")
        try:
            out['pgroup_locks'] = _parse_purepgroup_retention_csv(run_ssh_command(
                array, user, "purepgroup list --retention-lock --csv",
                log_list=detailed_logs, nogui=nogui))
        except Exception as e:
            _errs.append(f"purepgroup list --retention-lock: {e}")
        try:
            out['pgroup_schedules'] = _parse_purepgroup_schedule_csv(run_ssh_command(
                array, user, "purepgroup list --schedule --csv",
                log_list=detailed_logs, nogui=nogui))
        except Exception as e:
            _errs.append(f"purepgroup list --schedule: {e}")
        try:
            out['pgroup_retentions'] = _parse_purepgroup_retention_full_csv(run_ssh_command(
                array, user, "purepgroup list --retention --csv",
                log_list=detailed_logs, nogui=nogui))
        except Exception as e:
            _errs.append(f"purepgroup list --retention: {e}")
        try:
            out['connections'] = _parse_purevol_connect_csv(run_ssh_command(
                array, user, "purevol list --connect --csv",
                log_list=detailed_logs, nogui=nogui))
        except Exception as e:
            _errs.append(f"purevol list --connect: {e}")
    if _errs:
        out['error'] = "; ".join(_errs)
    return out


def run_protection_collection_core(config, nogui=False, progress_cb=None):
    """Detect array types, then collect protection data for each FA.
    Returns (per_array, detailed_logs):
        per_array : {array_name: {platform, user, error, volumes, pod_links, snapshots}}
        detailed_logs : list of SSH command-log strings (matches Daily Report log style)
    """
    detailed_logs = []
    def _p(msg):
        if progress_cb:
            try: progress_cb(msg)
            except Exception: pass

    _unified = config.get('arrays', [])
    _arrays_in_order = list(parse_unified_arrays(_unified)) if _unified else []
    if not _arrays_in_order:
        _seen = set(); _arrays_in_order = []
        for _a in (list(config.get('arr_fb',  []))
                  + list(config.get('arr_faf', []))
                  + list(config.get('arr_fab', []))):
            if _a and _a not in _seen:
                _seen.add(_a); _arrays_in_order.append((_a, ''))

    _users = [('FlashBlade',         config.get('user_fb',  'pureuser')),
              ('FlashArray Pod',     config.get('user_faf', 'pureuser')),
              ('FlashArray Async',   config.get('user_fab', 'pureuser'))]

    # ── Step 1: Array-type detection (4 workers) ─────────────────────────
    detect_results = [None] * len(_arrays_in_order)
    def _detect_one(_idx_pair):
        _idx, (_name, _loc) = _idx_pair
        _p(f"Detecting array {_name} type...")
        try:
            info = detect_array_type(_name, _users,
                                     detailed_logs=detailed_logs, nogui=nogui)
        except Exception as e:
            info = {'is_fb': False, 'is_faf': False, 'is_fab': False,
                    'is_nrp': False, 'user': None, 'error': str(e)}
        return _idx, _name, info

    if _arrays_in_order:
        _workers = min(4, len(_arrays_in_order))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _idx, _name, info in _ex.map(_detect_one,
                                             list(enumerate(_arrays_in_order))):
                detect_results[_idx] = (_name, info)

    # ── Step 2: Collect FlashArray protection (4 workers) ────────────────
    per_array = {}
    fa_targets = []
    for _entry in detect_results:
        if not _entry: continue
        _name, info = _entry
        platform = ('FB' if info.get('is_fb') else
                    ('FA' if (info.get('is_faf') or info.get('is_fab')
                              or info.get('is_nrp')) else None))
        per_array[_name] = {'platform': platform, 'user': info.get('user'),
                            'error': info.get('error'), 'volumes': [],
                            'pod_links': [], 'snapshots': [], 'pgroups': [],
                            'pgroup_locks': {}, 'pgroup_schedules': {},
                            'pgroup_retentions': {}, 'connections': []}
        if platform == 'FA':
            fa_targets.append((_name, info.get('user')
                                      or config.get('user_fab', 'pureuser')))

    def _collect_one(_arg):
        _idx, (_name, _u) = _arg
        _p(f"Collecting array {_name} volumes & snapshots...")
        return _name, _collect_one_fa_protection(
            _name, _u, detailed_logs, nogui=nogui)

    if fa_targets:
        _workers = min(4, len(fa_targets))
        with ThreadPoolExecutor(max_workers=_workers) as _ex:
            for _name, data in _ex.map(_collect_one,
                                       list(enumerate(fa_targets))):
                per_array[_name].update(data)

    return per_array, detailed_logs


def aggregate_fa_volume_rows(per_array):
    """Build Table 1 rows from per-array protection data.

    Returns list of dicts:
        array, volume, in_pod, pod_direction, remote_pod,
        local_snaps, pod_snaps, replicated_snaps, replication_destinations
    """
    # ── Tally snapshot counts keyed by (array, pod_or_None, volume) ─────
    local_snaps = {}      # (arr, None, vol) -> int (no-colon source)
    pod_snaps   = {}      # (arr, pod,  vol) -> int (pod::vol source)
    repl_snaps  = {}      # (src_arr, None, vol) -> int (one-colon source)
    repl_dests  = {}      # (src_arr, None, vol) -> set of dest arrays

    for arr, data in per_array.items():
        for snap in data.get('snapshots', []):
            src = snap.get('source', '')
            if not src:
                continue
            if '::' in src:
                pod, vol = src.split('::', 1)
                pod_snaps[(arr, pod, vol)] = pod_snaps.get((arr, pod, vol), 0) + 1
            elif ':' in src:
                src_arr, vol = src.split(':', 1)
                k = (src_arr, None, vol)
                repl_snaps[k] = repl_snaps.get(k, 0) + 1
                repl_dests.setdefault(k, set()).add(arr)
            else:
                k = (arr, None, src)
                local_snaps[k] = local_snaps.get(k, 0) + 1

    # ── Build pod-stretch map. Only 'replicating' links count; the
    # destination side will be folded into the source side so each
    # stretched pod-volume appears in the table only once. ──────────────
    # stretch[(arr, pod)] = (peer_arr, peer_pod, role) where role is
    # 'source' if direction is '-->' (this side replicates outward) and
    # 'dest' if direction is '<--' (this side receives the replica).
    stretch = {}
    for arr, data in per_array.items():
        for link in data.get('pod_links', []):
            if (link.get('status') or '').lower() != 'replicating':
                continue
            local_pod    = link.get('local_pod', '')
            remote_pod   = link.get('remote_pod', '')
            remote_array = link.get('remote_array', '')
            direction    = link.get('direction', '')
            if not (local_pod and remote_array):
                continue
            role = 'source' if direction == '-->' else (
                   'dest' if direction == '<--' else '')
            stretch[(arr, local_pod)] = (remote_array, remote_pod, role)

    # ── Build (array, pod_or_None, volume) -> set(pgroup_name) map from
    # purepgroup list output. Each pgroup's Volumes column lists volume
    # entries that may themselves contain '::' (for pod-scoped pgroups).
    pg_membership = {}
    for arr, data in per_array.items():
        for pg in data.get('pgroups', []):
            pg_full = pg.get('name') or ''
            if not pg_full:
                continue
            for vent in pg.get('volumes', []):
                if '::' in vent:
                    vp, vv = vent.split('::', 1)
                    vkey = (arr, vp, vv)
                else:
                    vkey = (arr, None, vent)
                pg_membership.setdefault(vkey, set()).add(pg_full)

    # ── Set of (array, pgroup_full_name) where Retention Lock is
    # 'ratcheted'. A volume's Safemode is Enabled when any of its
    # member pgroups is ratcheted on the array(s) it lives on
    # (including the stretched-pod peer for pod volumes).
    pg_ratcheted = set()
    for arr, data in per_array.items():
        for pg_name, lock in (data.get('pgroup_locks') or {}).items():
            if (lock or '').lower() == 'ratcheted':
                pg_ratcheted.add((arr, pg_name))

    # ── Map (array, pod, volume) -> set of connected host names. The
    # `purevol list --connect --csv` output emits one row per host so a
    # multi-host volume contributes multiple entries.
    vol_to_hosts = {}
    for arr, data in per_array.items():
        for c in (data.get('connections') or []):
            cname, host = c.get('name', ''), c.get('host', '')
            if not cname or not host:
                continue
            if '::' in cname:
                cp, cv = cname.split('::', 1)
            else:
                cp, cv = None, cname
            vol_to_hosts.setdefault((arr, cp, cv), set()).add(host)

    # ── Assemble rows. Track which (arr, pod, vol) keys to skip because
    # they belong to the dest side of a stretched pod. ──────────────────
    drop = set()
    for arr, data in per_array.items():
        for v in data.get('volumes', []):
            if v.get('pod') is None:
                continue
            key = (arr, v['pod'])
            if key in stretch and stretch[key][2] == 'dest':
                drop.add((arr, v['pod'], v['volume']))

    rows = []
    seen_pod_volume_pairs = set()  # (source_arr, pod, vol) — dedup source side
    for arr, data in per_array.items():
        for v in data.get('volumes', []):
            pod, vol = v.get('pod'), v['volume']
            key3 = (arr, pod, vol)
            if key3 in drop:
                continue

            # `source_array` is the array on which the volume physically
            # resides (and therefore where its pgroup profile lives). For
            # plain volumes this is `arr`; for stretched-pod source-side
            # rows it stays `arr`. The displayed 'array' column may show
            # 'src --> dest' but `source_array` always points at src.
            row = {'array': arr, 'volume': vol, 'pod_name': pod or '',
                   'source_array': arr,
                   'in_pod': bool(pod),
                   'pod_direction': '', 'remote_pod': '',
                   'local_snaps': 0, 'pod_snaps': 0,
                   'replicated_snaps': 0, 'replication_destinations': [],
                   'protection_groups': [], 'safemode': False,
                   'connected_hosts': []}

            if pod is None:
                row['local_snaps']      = local_snaps.get((arr, None, vol), 0)
                row['replicated_snaps'] = repl_snaps.get((arr, None, vol), 0)
                row['replication_destinations'] = sorted(
                    repl_dests.get((arr, None, vol), set()))
                pgs = pg_membership.get((arr, None, vol), set())
                row['protection_groups'] = sorted(pgs)
                row['safemode'] = any((arr, p) in pg_ratcheted for p in pgs)
                row['connected_hosts'] = sorted(
                    vol_to_hosts.get((arr, None, vol), set()))
            else:
                # Pod volume. Pod snapshots on this side count as both
                # 'pod' and 'local' snapshots; pod snapshots on the
                # stretched peer (if any) count as 'pod' and 'replicated'
                # snapshots, and the peer array is recorded as a
                # replication destination.
                local_pod_snaps = pod_snaps.get((arr, pod, vol), 0)
                peer_pod_snaps  = 0
                dests           = set()
                pgs             = set(pg_membership.get((arr, pod, vol), set()))
                stretch_info    = stretch.get((arr, pod))
                if stretch_info:
                    peer_arr, peer_pod, role = stretch_info
                    peer_pod_snaps = pod_snaps.get((peer_arr, peer_pod, vol), 0)
                    if peer_arr:
                        dests.add(peer_arr)
                    # Pod-scoped pgroups replicate with the pod, so the
                    # same pgroup may appear on both sides. Union them.
                    pgs |= pg_membership.get((peer_arr, peer_pod, vol), set())
                    # Show "source --> dest" in the Array column. If our
                    # role is 'dest' this row should already be dropped
                    # above; defensive in case direction was odd.
                    if role == 'source':
                        row['array']         = f'{arr} \u2192 {peer_arr}'
                        row['pod_direction'] = f'{arr} \u2192 {peer_arr}'
                        row['remote_pod']    = peer_pod
                    elif role == 'dest':
                        row['array']         = f'{peer_arr} \u2192 {arr}'
                        row['pod_direction'] = f'{peer_arr} \u2192 {arr}'
                        row['remote_pod']    = peer_pod
                # Dedup pod-volume rows that may appear because both sides
                # exposed identical (pod, volume) names but only one had a
                # 'replicating' link recorded (rare but possible).
                src_key = (row['array'], pod, vol)
                if src_key in seen_pod_volume_pairs:
                    continue
                seen_pod_volume_pairs.add(src_key)
                row['pod_snaps']        = local_pod_snaps + peer_pod_snaps
                row['local_snaps']      = local_pod_snaps
                row['replicated_snaps'] = peer_pod_snaps
                row['replication_destinations'] = sorted(dests)
                row['protection_groups'] = sorted(pgs)
                # Safemode is enabled if any member pgroup is ratcheted
                # on either side of the stretched pod.
                _arrs_to_check = {arr}
                if stretch_info and stretch_info[0]:
                    _arrs_to_check.add(stretch_info[0])
                row['safemode'] = any((a, p) in pg_ratcheted
                                      for a in _arrs_to_check for p in pgs)
                # Pod-resident volumes can be host-connected on either
                # side of the stretch; union both sides' host sets.
                hosts = set(vol_to_hosts.get((arr, pod, vol), set()))
                if stretch_info:
                    hosts |= vol_to_hosts.get(
                        (stretch_info[0], stretch_info[1], vol), set())
                row['connected_hosts'] = sorted(hosts)
            rows.append(row)

    rows.sort(key=lambda r: (r['array'].lower(), (0 if r['in_pod'] else 1),
                             r['volume'].lower()))
    return rows


def build_protection_html(per_array, config):
    """Generate the Volume & Snapshot Protection HTML report.

    Three sections:
      1. FlashArray Volumes  (fully populated)
      2. FlashArray Filesystems  (placeholder, spec pending)
      3. FlashBlade Filesystems  (placeholder, spec pending)
    """
    import html as _html
    import time as _time
    import json as _json

    tz      = _time.tzname[_time.daylight]
    now_str = datetime.datetime.now().strftime("%A, %B %d, %Y at %I:%M:%S %p")
    rows    = aggregate_fa_volume_rows(per_array)

    # Per-array error banner contents.
    err_lines = []
    for arr in sorted(per_array.keys()):
        info = per_array[arr]
        if info.get('error'):
            err_lines.append(f"{_html.escape(arr)}: {_html.escape(str(info['error']))}")

    # Per-(array, pgroup) Schedule + Retention profile — drives the modal
    # popup that opens when a Protection Group name is clicked. Only the
    # source array's profile is published for each row; identical pgroup
    # names on other arrays are not exposed via this row's link.
    pg_profiles = {}
    for arr, info in per_array.items():
        sched = info.get('pgroup_schedules') or {}
        rete  = info.get('pgroup_retentions') or {}
        keys  = set(sched.keys()) | set(rete.keys())
        if not keys:
            continue
        pg_profiles[arr] = {
            k: {'schedule': sched.get(k, {}), 'retention': rete.get(k, {})}
            for k in keys}
    profiles_json = _json.dumps(pg_profiles)

    # ── Build Table 1 rows ────────────────────────────────────────────────
    tr_html = ""
    if not rows:
        tr_html = ('<tr><td colspan="11" style="text-align:center;color:#888;">'
                   'No FlashArray volumes discovered.</td></tr>')
    else:
        # Cells for the seven protection-related columns are shaded based
        # on whether they carry meaningful content. Numeric snapshot
        # cells use the "ok" colour when the count is > 0.
        _OK   = 'background:#d4edda;'  # light green
        _BAD  = 'background:#f8d7da;'  # light red
        _DASH = '<span style="color:#888;">&mdash;</span>'
        for r in rows:
            dests_ok = bool(r['replication_destinations'])
            dests = (', '.join(_html.escape(d) for d in r['replication_destinations'])
                     if dests_ok else _DASH)
            pgs_list = r.get('protection_groups', [])
            pgs_ok = bool(pgs_list)
            # Each pgroup name renders as a clickable link bound to the
            # row's source array; the modal popup pulls schedule and
            # retention from PG_PROFILES[source][pgname].
            _src = r.get('source_array', r['array'])
            pgs = (', '.join(
                f'<a href="#" class="pg-link" '
                f'data-arr="{_html.escape(_src, quote=True)}" '
                f'data-pg="{_html.escape(p, quote=True)}" '
                f'onclick="showPg(this);return false;">{_html.escape(p)}</a>'
                for p in pgs_list) if pgs_ok else _DASH)
            pod_ok = bool(r['in_pod'] and r.get('pod_name'))
            pod_cell = _html.escape(r['pod_name']) if pod_ok else _DASH
            remote_ok = bool(r['remote_pod'])
            remote_pod = _html.escape(r['remote_pod']) if remote_ok else _DASH
            # Pod volumes are displayed with their fully-qualified name
            # (pod::vol) so the pod scope is visible in the Volume Name
            # column even on stretched-pod source-side rows.
            vol_disp = (f'{r["pod_name"]}::{r["volume"]}'
                        if r['in_pod'] and r.get('pod_name') else r['volume'])
            sm_on = bool(r.get('safemode'))
            sm_text = 'Enabled' if sm_on else 'Disabled'
            sm_style = (_OK if sm_on else _BAD) + 'text-align:center;font-weight:bold;'
            hosts_list = r.get('connected_hosts', [])
            hosts_ok = bool(hosts_list)
            hosts = (', '.join(_html.escape(h) for h in hosts_list)
                     if hosts_ok else _DASH)
            local_n = int(r['local_snaps'])
            pod_n   = int(r['pod_snaps'])
            rep_n   = int(r['replicated_snaps'])
            tr_html += (
                '<tr>'
                f'<td>{_html.escape(vol_disp)}</td>'
                f'<td>{_html.escape(r["array"])}</td>'
                f'<td style="{_OK if hosts_ok else _BAD}">{hosts}</td>'
                f'<td style="{_OK if dests_ok else _BAD}">{dests}</td>'
                f'<td style="{_OK if pod_ok else _BAD}text-align:center;">{pod_cell}</td>'
                f'<td style="{_OK if remote_ok else _BAD}text-align:center;">{remote_pod}</td>'
                f'<td style="{_OK if local_n > 0 else _BAD}text-align:right;">{local_n}</td>'
                f'<td style="{_OK if pod_n > 0 else _BAD}text-align:right;">{pod_n}</td>'
                f'<td style="{_OK if rep_n > 0 else _BAD}text-align:right;">{rep_n}</td>'
                f'<td style="{sm_style}">{sm_text}</td>'
                f'<td style="{_OK if pgs_ok else _BAD}">{pgs}</td>'
                '</tr>\n')

    err_banner = ''
    if err_lines:
        err_banner = ('<div class="err-banner"><strong>Collection errors:</strong><br>'
                      + '<br>'.join(err_lines) + '</div>')

    placeholder = ('<p style="color:#666;font-style:italic;">'
                   'Specification pending &mdash; coming in next update.</p>')

    # Modal markup + JS for the Protection Group detail popup. Rendered
    # only when at least one array has profile data so empty datasets
    # don't ship dead JS.
    if pg_profiles:
        modal_block = (
            '<div id="pg-modal" class="modal" '
            'onclick="closeModalIfBg(event)">'
            '<div class="modal-content">'
            '<span class="modal-close" onclick="closeModal()">&times;</span>'
            '<h3 id="pg-title"></h3>'
            '<p class="meta" id="pg-array"></p>'
            '<h4>Schedule</h4><div id="pg-schedule"></div>'
            '<h4>Retention</h4><div id="pg-retention"></div>'
            '</div></div>')
        script_block = (
            '<script>\n'
            'const PG_PROFILES = ' + profiles_json + ';\n'
            'function escHtml(s){'
            'return String(s).replace(/[&<>\"\\\']/g,'
            'ch=>({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","\\\'":"&#39;"})[ch]);'
            '}\n'
            'function renderTable(o){'
            'if(!o)return \'<p style="color:#888;">No data.</p>\';'
            'const cols=Object.keys(o);'
            'if(cols.length===0)return \'<p style="color:#888;">No data.</p>\';'
            'const n=Math.max(0,...cols.map(c=>(o[c]||[]).length));'
            'if(n===0)return \'<p style="color:#888;">No data.</p>\';'
            'let h=\'<table class="pg-detail"><thead><tr>\';'
            'cols.forEach(c=>{h+=\'<th>\'+escHtml(c)+\'</th>\';});'
            'h+=\'</tr></thead><tbody>\';'
            'for(let i=0;i<n;i++){'
            'h+=\'<tr>\';'
            'cols.forEach(c=>{const v=(o[c]||[])[i];'
            'h+=\'<td>\'+(v?escHtml(v):\'<span style="color:#888;">&mdash;</span>\')+\'</td>\';});'
            'h+=\'</tr>\';}'
            'return h+\'</tbody></table>\';}\n'
            'function showPg(el){'
            'const arr=el.getAttribute("data-arr"),pg=el.getAttribute("data-pg");'
            'const data=(PG_PROFILES[arr]||{})[pg]||null;'
            'document.getElementById("pg-title").textContent=pg;'
            'document.getElementById("pg-array").textContent="Source array: "+arr;'
            'document.getElementById("pg-schedule").innerHTML=renderTable(data&&data.schedule);'
            'document.getElementById("pg-retention").innerHTML=renderTable(data&&data.retention);'
            'document.getElementById("pg-modal").style.display="flex";}\n'
            'function closeModal(){document.getElementById("pg-modal").style.display="none";}\n'
            'function closeModalIfBg(e){if(e.target.id==="pg-modal")closeModal();}\n'
            'document.addEventListener("keydown",e=>{if(e.key==="Escape")closeModal();});\n'
            '</script>')
    else:
        modal_block = ''
        script_block = ''

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Volume &amp; Snapshot Protection</title>
  <style>
    body {{ font-family: 'Segoe UI', Arial, sans-serif; font-size: 10pt; margin: 16px; }}
    h1   {{ color: #1f3a5c; margin: 0 0 4px 0; }}
    h2   {{ color: #1f3a5c; margin: 24px 0 8px 0; border-bottom: 1px solid #b8cfe8;
           padding-bottom: 4px; }}
    p.meta {{ margin: 2px 0; color: #444; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 6px; }}
    th, td {{ border: 1px solid #b8cfe8; padding: 4px 8px; vertical-align: top; }}
    th    {{ background: #dce6f1; font-weight: bold; }}
    tr:nth-child(even) td {{ background: #f7faff; }}
    .err-banner {{ background: #fff4f4; border: 1px solid #e0a0a0;
                  padding: 6px 10px; margin: 8px 0; border-radius: 4px;
                  color: #802020; }}
    .pg-link {{ color: #1a5fb4; cursor: pointer; text-decoration: underline; }}
    .pg-link:hover {{ color: #0b3d8c; }}
    .modal {{ display: none; position: fixed; inset: 0;
             background: rgba(0,0,0,0.45);
             align-items: flex-start; justify-content: center;
             z-index: 1000; overflow: auto; padding-top: 60px; }}
    .modal-content {{ background: white; padding: 16px 20px;
                     border-radius: 6px; width: min(90%, 760px);
                     box-shadow: 0 6px 24px rgba(0,0,0,0.25);
                     position: relative; }}
    .modal-close {{ position: absolute; top: 6px; right: 12px;
                   font-size: 20px; cursor: pointer; color: #666; }}
    .modal-close:hover {{ color: #000; }}
    .modal-content h3 {{ margin: 0 0 4px 0; color: #1f3a5c; }}
    .modal-content h4 {{ margin: 14px 0 4px 0; color: #1f3a5c; }}
    .pg-detail {{ width: 100%; border-collapse: collapse; margin-top: 0; }}
    .pg-detail th, .pg-detail td {{ border: 1px solid #b8cfe8;
                                   padding: 4px 8px; font-size: 9pt; }}
    .pg-detail th {{ background: #dce6f1; }}
  </style>
</head>
<body>
  <h1>Everpure &mdash; Volume &amp; Snapshot Protection</h1>
  <p class="meta">Generated {now_str} {tz}</p>
  <p class="meta">Arrays inventoried: {len(per_array)} &middot;
                  FlashArray volume rows: {len(rows)}</p>
  {err_banner}

  <h2>1. FlashArray Volumes</h2>
  <table>
    <thead>
      <tr>
        <th>Volume Name</th>
        <th>Array Name</th>
        <th>Connected Hosts</th>
        <th>Replication Destinations</th>
        <th>Pod</th>
        <th>Remote Pod</th>
        <th>Local Snapshots</th>
        <th>Pod Snapshots</th>
        <th>Replicated Snapshots</th>
        <th>Safemode</th>
        <th>Protection Groups</th>
      </tr>
    </thead>
    <tbody>
{tr_html}    </tbody>
  </table>

  <h2>2. FlashArray Filesystems</h2>
  {placeholder}

  <h2>3. FlashBlade Filesystems</h2>
  {placeholder}

  {modal_block}
  {script_block}
</body>
</html>
"""


class PureMonitorApp(tk.Tk):
    def __init__(self):
        super().__init__()
        _title = "Everpure (Pure Storage) - Alert and Replication SLA Status Report"
        if FAKE_ARRAYS:
            _title += "  [DEMO: 12 fake arrays, 5 locations]"
        self.title(_title)
        # 1000 px tall fits the 660 px Configuration pane (3 user rows +
        # alerts + 15-row Arrays sheet + checkbox + LabelFrame chrome) and
        # leaves room for the button bar plus an 8-row output log without
        # the sheet getting squeezed by the PanedWindow's initial layout.
        self.geometry("1100x1000")

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

    def _build_arrays_sheet(self, parent, config):
        """Build the unified Arrays/Location editor, spanning rows 4-6, cols 1-2.

        Uses ``tksheet`` when available; otherwise falls back to a simple two-
        column Treeview-based editor so the app still runs without tksheet.
        """
        rows = [[n, l, nt] for n, l, nt in unified_arrays_from_config_full(config)]
        # Pad with 50 blank rows at the end on startup so the user has ample
        # scratch space to paste into without having to insert rows first.
        # _ensure_trailing_blank_rows then appends another 50 in one batch
        # whenever the user fills the last visible blank row, so the sheet
        # auto-grows in 50-row chunks rather than one row at a time.
        # NOTE: must construct each row as its own list \u2014 [['','',''] ] * 50 would
        # create 50 references to the same inner list, so writing one cell
        # would propagate the value into every padding row.
        rows.extend([['', '', ''] for _ in range(50)])

        sheet_frame = ttk.Frame(parent)
        sheet_frame.grid(row=4, column=1, columnspan=2, rowspan=3,
                         sticky=tk.NSEW, padx=(0, 0), pady=2)
        # Kept on self so the <Configure> handler in _on_sheet_resize can
        # query the live frame width and redistribute column widths.
        self._sheet_frame = sheet_frame
        # Let the sheet grow when the user resizes the window or drags the
        # main vertical PanedWindow sash to give Configuration more height.
        parent.rowconfigure(4, weight=1)
        parent.rowconfigure(5, weight=1)
        parent.rowconfigure(6, weight=1)
        # weight=1 lets the grid cell expand horizontally when the parent
        # frame is wider than the sheet's reserved minsize, so the sheet's
        # drawing area tracks the visible width rather than being clipped.
        parent.columnconfigure(1, minsize=290, weight=1)
        parent.columnconfigure(2, minsize=295, weight=1)

        if HAS_TKSHEET:
            # Initial width/height are just a starting size; fill=BOTH +
            # expand=True below makes the sheet's drawing area track the
            # frame size, so the columns stay inside the visible widget
            # rather than being clipped, and the user can grow the sheet
            # by dragging the main vertical PanedWindow sash downward.
            # height=466 px targets ~15 visible data rows on first paint
            # (header ~28 + 15 rows x ~28 + horizontal scrollbar ~18); the
            # ttk.PanedWindow uses each pane's requested height to seed the
            # initial sash position, so this is what determines how many
            # rows the user sees before they touch the sash.
            # show_row_index=True enables a non-editable gutter to the left
            # of "Array" that _refresh_arrays_row_index populates with a
            # 1-based count of rows that actually have a name filled in.
            self.arrays_sheet = Sheet(
                sheet_frame,
                headers=["Array", "Location", "Notes"],
                data=rows,
                width=585, height=466,
                show_row_index=True,
                show_top_left=False,
                # Always render the horizontal scrollbar so when the user
                # widens either column past the widget's visible width the
                # extra content is reachable by sliding the thumb at the
                # bottom.
                show_x_scrollbar=True,
            )
            self.arrays_sheet.enable_bindings((
                "single_select", "drag_select", "arrowkeys", "edit_cell",
                "copy", "paste", "delete", "undo",
                "right_click_popup_menu", "rc_insert_row", "rc_delete_row",
                "column_width_resize", "double_click_column_resize",
            ))
            # Force single-cell paste semantics. tksheet's default Excel-like
            # behavior tiles the clipboard across the current selection box
            # when that box is larger than the clipboard data (and the row
            # count is a multiple of it). That surprised the user when a
            # stray multi-row selection was active, so shrink the selection
            # to just the caret cell before delegating to tksheet's ctrl_v.
            try:
                self._install_single_cell_paste(self.arrays_sheet)
            except Exception:
                pass
            # Auto-grow: whenever the last two rows are no longer both blank
            # (e.g. the user typed into the last empty row, or pasted a block
            # that filled past the end), append fresh blank rows so there is
            # always room to keep going without manually inserting rows.
            self._blank_row_guard = False
            try:
                self.arrays_sheet.bind("<<SheetModified>>",
                                       self._ensure_trailing_blank_rows)
            except Exception:
                pass
            # Column widths: use values saved in the config when present
            # (the user can drag column separators to resize; those widths
            # are persisted back by _save_config under 'arrays_col_widths').
            _saved_w = config.get('arrays_col_widths') or []
            try:
                w0 = int(_saved_w[0]) if len(_saved_w) > 0 and _saved_w[0] else 200
            except Exception:
                w0 = 200
            try:
                w1 = int(_saved_w[1]) if len(_saved_w) > 1 and _saved_w[1] else 165
            except Exception:
                w1 = 165
            try:
                w2 = int(_saved_w[2]) if len(_saved_w) > 2 and _saved_w[2] else 200
            except Exception:
                w2 = 200
            try:
                self.arrays_sheet.column_width(column=0, width=w0)
                self.arrays_sheet.column_width(column=1, width=w1)
                self.arrays_sheet.column_width(column=2, width=w2)
            except Exception:
                pass
            # Narrow non-editable row-number gutter; populated with a
            # 1-based running count of rows that have an Array name.
            try:
                self.arrays_sheet.set_index_width(40)
            except Exception:
                pass
            # fill=tk.BOTH + expand=True lets the sheet's drawing area
            # track the frame's actual size, so when the user enlarges the
            # window or drags the vertical PanedWindow sash downward the
            # sheet absorbs the extra space (more visible rows / wider
            # columns) instead of staying pinned to its initial 585x466.
            self.arrays_sheet.pack(fill=tk.BOTH, expand=True)
            # Bind on the frame (not the sheet) so we get exactly one
            # <Configure> per geometry change and can read the authoritative
            # available width before redistributing column widths.
            sheet_frame.bind('<Configure>', self._on_sheet_resize)
            try:
                self._refresh_arrays_row_index()
            except Exception:
                pass
        else:
            # Fallback: two synced text boxes. Keeps the app usable without
            # tksheet (diagnostics prompt shown at run time).
            self.arrays_sheet = None
            self._fallback_arr_txt   = scrolledtext.ScrolledText(sheet_frame, width=30, height=6)
            self._fallback_loc_txt   = scrolledtext.ScrolledText(sheet_frame, width=20, height=6)
            self._fallback_notes_txt = scrolledtext.ScrolledText(sheet_frame, width=24, height=6)
            self._fallback_arr_txt.pack(side=tk.LEFT, fill=tk.Y)
            self._fallback_loc_txt.pack(side=tk.LEFT, fill=tk.Y, padx=(4, 0))
            self._fallback_notes_txt.pack(side=tk.LEFT, fill=tk.Y, padx=(4, 0))
            self._fallback_arr_txt.insert(tk.END,   "\n".join(r[0] for r in rows))
            self._fallback_loc_txt.insert(tk.END,   "\n".join(r[1] for r in rows))
            self._fallback_notes_txt.insert(tk.END, "\n".join((r[2] if len(r) > 2 else '') for r in rows))
            self._add_context_menu(self._fallback_arr_txt)
            self._add_context_menu(self._fallback_loc_txt)
            self._add_context_menu(self._fallback_notes_txt)

    def _get_arrays_from_sheet(self):
        """Return [(name, location, notes), ...] from the unified sheet editor.

        Drops rows whose name is blank after whitespace trimming.
        """
        out = []
        if getattr(self, 'arrays_sheet', None) is not None:
            try:
                data = self.arrays_sheet.get_sheet_data() or []
            except Exception:
                data = []
            for row in data:
                if not row:
                    continue
                name = str(row[0] if len(row) > 0 else '').strip()
                if not name:
                    continue
                loc   = str(row[1] if len(row) > 1 else '').strip()
                notes = str(row[2] if len(row) > 2 else '').strip()
                out.append((name, loc, notes))
            return out
        # Fallback path: three synced ScrolledText boxes (Array / Location / Notes).
        arr_lines = (self._fallback_arr_txt.get("1.0", tk.END)
                     if hasattr(self, '_fallback_arr_txt') else '').splitlines()
        loc_lines = (self._fallback_loc_txt.get("1.0", tk.END)
                     if hasattr(self, '_fallback_loc_txt') else '').splitlines()
        notes_lines = (self._fallback_notes_txt.get("1.0", tk.END)
                       if hasattr(self, '_fallback_notes_txt') else '').splitlines()
        out = []
        for i, name in enumerate(arr_lines):
            name = name.strip()
            if not name:
                continue
            loc   = loc_lines[i].strip()   if i < len(loc_lines)   else ''
            notes = notes_lines[i].strip() if i < len(notes_lines) else ''
            out.append((name, loc, notes))
        return out

    def _install_single_cell_paste(self, sheet):
        """Rebind Ctrl-V on the sheet so pasting always targets a single cell.

        tksheet's native ctrl_v will tile the clipboard across the current
        selection box when it spans more cells than the clipboard contains.
        That surprises users who expect plain single-cell paste. This wrapper
        deselects any wider selection, then re-selects just the caret cell
        before invoking the built-in ctrl_v so it can only ever paste into
        that one cell (plus whatever expansion the clipboard data itself
        contributes when it has multiple rows/cols).
        """
        mt = getattr(sheet, 'MT', None)
        if mt is None or not hasattr(mt, 'ctrl_v'):
            return
        orig_ctrl_v = mt.ctrl_v

        def _single_cell_paste(event=None):
            try:
                sel = sheet.get_currently_selected()
                if sel:
                    r = getattr(sel, 'row', None)
                    c = getattr(sel, 'column', None)
                    if r is not None and c is not None:
                        try:
                            sheet.deselect("all", redraw=False)
                        except Exception:
                            pass
                        try:
                            sheet.select_cell(r, c, redraw=True,
                                              run_binding_func=False)
                        except Exception:
                            pass
            except Exception:
                pass
            return orig_ctrl_v(event)

        for w in (mt, getattr(sheet, 'RI', None),
                  getattr(sheet, 'CH', None), getattr(sheet, 'TL', None)):
            if w is None:
                continue
            for seq in ("<Control-v>", "<Control-V>"):
                try:
                    w.bind(seq, _single_cell_paste)
                except Exception:
                    pass

    def _on_sheet_resize(self, event=None):
        """Redistribute Array/Location/Notes column widths when the arrays
        sheet frame is resized (window resize, PanedWindow sash drag, etc.).

        The three columns are stretched proportionally to their current
        width ratio so any user-driven column resize is preserved across
        subsequent frame resizes; columns are floored at 60 px each so
        they cannot collapse below a usable minimum.
        """
        sheet = getattr(self, 'arrays_sheet', None)
        sf = getattr(self, '_sheet_frame', None)
        if sheet is None or sf is None:
            return
        # Re-entrancy guard: column_width() writes can theoretically
        # bounce a <Configure> back through tksheet's internal layout.
        if getattr(self, '_sheet_resize_guard', False):
            return
        try:
            # Available drawing width = frame width - row-index gutter (40)
            # - vertical scrollbar (~18) - widget borders (~4).
            avail = sf.winfo_width() - 62
        except Exception:
            return
        # Skip until the frame has a real geometry; the first <Configure>
        # often fires with width=1 before the layout has settled.
        if avail < 200:
            return
        try:
            w0 = int(sheet.column_width(column=0))
            w1 = int(sheet.column_width(column=1))
            w2 = int(sheet.column_width(column=2))
        except Exception:
            return
        total = max(w0 + w1 + w2, 1)
        r0 = w0 / total
        r1 = w1 / total
        new_w0 = max(60, int(avail * r0))
        new_w1 = max(60, int(avail * r1))
        new_w2 = max(60, avail - new_w0 - new_w1)
        if new_w0 == w0 and new_w1 == w1 and new_w2 == w2:
            return
        self._sheet_resize_guard = True
        try:
            try:
                sheet.column_width(column=0, width=new_w0)
                sheet.column_width(column=1, width=new_w1)
                sheet.column_width(column=2, width=new_w2)
                # refresh() forces tksheet to repaint with the new widths
                # without waiting for the next user interaction.
                try:
                    sheet.refresh()
                except Exception:
                    pass
            except Exception:
                pass
        finally:
            self._sheet_resize_guard = False

    def _ensure_trailing_blank_rows(self, event=None,
                                    min_trailing=1, grow_chunk=50):
        """Auto-grow the arrays sheet in 50-row chunks.

        Triggered by ``<<SheetModified>>`` (and explicit calls) so that
        whenever the user fills in the last blank row \u2014 or pastes a block
        that consumes all of the trailing blanks \u2014 a fresh batch of
        *grow_chunk* empty rows is appended in one go. Keeping the
        threshold at *min_trailing=1* means the user always has at least
        one ready-to-edit blank row at the bottom; growth happens in
        50-row increments rather than topping up one row at a time.
        """
        sheet = getattr(self, 'arrays_sheet', None)
        if sheet is None:
            return
        # Guard against the <<SheetModified>> event firing recursively when
        # insert_rows itself triggers another modification.
        if getattr(self, '_blank_row_guard', False):
            return
        try:
            data = sheet.get_sheet_data() or []
        except Exception:
            return

        def _row_blank(r):
            return all(not str(c if c is not None else '').strip() for c in r)

        trailing = 0
        for r in reversed(data):
            if _row_blank(r):
                trailing += 1
            else:
                break

        # Add a full grow_chunk batch (50 rows) when the trailing blank
        # count drops below the threshold, instead of just topping up to
        # the threshold one row at a time.
        needed = grow_chunk if trailing < min_trailing else 0
        if needed > 0:
            self._blank_row_guard = True
            try:
                try:
                    # create_selections=False prevents tksheet from leaving a
                    # selection box spanning the newly-added rows, which would
                    # otherwise cause the next paste to tile the clipboard across
                    # all of them.
                    sheet.insert_rows(rows=needed, idx="end",
                                      emit_event=False,
                                      create_selections=False,
                                      redraw=True)
                except TypeError:
                    # Older tksheet signatures that don't accept these kwargs.
                    try:
                        sheet.insert_rows(rows=needed, idx="end", redraw=True)
                    except Exception:
                        new_data = list(data) + [['', '', ''] for _ in range(needed)]
                        sheet.set_sheet_data(new_data, redraw=True)
                except Exception:
                    # Fallback: rebuild the data in one shot.
                    new_data = list(data) + [['', '', ''] for _ in range(needed)]
                    sheet.set_sheet_data(new_data, redraw=True)
            finally:
                self._blank_row_guard = False
        # Always refresh the row-index labels: edits that don't add rows
        # (e.g. the user typed a name into a blank row) still change the
        # running count of non-empty arrays.
        self._refresh_arrays_row_index()

    def _refresh_arrays_row_index(self):
        """Populate the row-index gutter with a 1-based count of rows that
        have a non-blank Array name; blank rows get an empty label.
        """
        sheet = getattr(self, 'arrays_sheet', None)
        if sheet is None:
            return
        try:
            data = sheet.get_sheet_data() or []
        except Exception:
            return
        labels = []
        n = 0
        for r in data:
            name = str((r[0] if r else '') or '').strip()
            if name:
                n += 1
                labels.append(str(n))
            else:
                labels.append('')
        try:
            sheet.row_index(newindex=labels, redraw=True)
        except Exception:
            pass

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

        # Vertical PanedWindow lets the user redistribute height between the
        # Configuration pane (which hosts the Arrays sheet) and the lower
        # pane (button bar + run-output log). Dragging the sash downward
        # grows the sheet; dragging it upward grows the output log.
        # sashrelief=RAISED makes the divider visually grabbable.
        main_paned = ttk.PanedWindow(main_frame, orient=tk.VERTICAL)
        main_paned.pack(fill=tk.BOTH, expand=True)
        self._main_paned = main_paned

        config_frame = ttk.LabelFrame(main_paned, text="Configuration", padding=5)
        # weight=3 biases initial sash placement toward the Configuration
        # pane so the sheet has room for ~8-10 visible rows on first launch.
        main_paned.add(config_frame, weight=3)
        # Kept on self so _show_busy_spinner can grid an inline spinner
        # widget directly under the Everpure logo (column 5).
        self._config_frame = config_frame
        
        # Label wraplength roughly 100px.
        WL = 100
        
        # Row 0: FlashBlade User
        ttk.Label(config_frame, text="FlashBlade User:", wraplength=WL, justify=tk.LEFT).grid(row=0, column=0, sticky=tk.W, pady=2)
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
        
        # Row 1: FlashArray Pod User
        ttk.Label(config_frame, text="FlashArray Pod User:", wraplength=WL, justify=tk.LEFT).grid(row=1, column=0, sticky=tk.W, pady=2)
        self.user_faf_entry = ttk.Entry(config_frame, width=20)
        self.user_faf_entry.insert(0, config.get("user_faf", config.get("user", "pureuser")))
        self.user_faf_entry.grid(row=1, column=1, sticky=tk.W, pady=2)

        # Row 2: FlashArray Async User
        ttk.Label(config_frame, text="FlashArray Async User:", wraplength=WL, justify=tk.LEFT).grid(row=2, column=0, sticky=tk.W, pady=2)
        self.user_fab_entry = ttk.Entry(config_frame, width=20)
        self.user_fab_entry.insert(0, config.get("user_fab", config.get("user", "pureuser")))
        self.user_fab_entry.grid(row=2, column=1, sticky=tk.W, pady=2)

        # Row 3: Excluded Alerts (column 1)
        ttk.Label(config_frame, text="Excluded Alerts (Partial Match or ID Range):", wraplength=120, justify=tk.LEFT).grid(row=3, column=0, sticky=tk.W, pady=2)
        self.alerts_entry = scrolledtext.ScrolledText(config_frame, width=30, height=3)
        self.alerts_entry.insert(tk.END, config.get("alerts_excluded", DEFAULT_EXCLUDED_ALERTS))
        self.alerts_entry.grid(row=3, column=1, sticky=tk.W, pady=2)

        # Quick-reference links to Pure Storage alert catalogues, placed to the
        # right of the Excluded Alerts box so the user can look up alert IDs.
        alerts_links_frame = ttk.Frame(config_frame)
        alerts_links_frame.grid(row=3, column=2, columnspan=3, sticky=tk.NW,
                                padx=(10, 5), pady=2)
        def _make_link(parent, text, url):
            lbl = tk.Label(parent, text=text, fg="#1a5fb4", cursor="hand2",
                           font=("TkDefaultFont", 9, "underline"))
            lbl.bind("<Button-1>", lambda _e, u=url: webbrowser.open_new_tab(u))
            return lbl
        _fb_alerts_url = ("https://support.purestorage.com/bundle/m_purityfb_alerts/"
                          "page/FlashBlade/Purity_FB/topics/concept/c_purityfb_alerts.html")
        _fa_alerts_url = ("https://support.purestorage.com/bundle/m_purityfa_alerts/"
                          "page/FlashArray/PurityFA/topics/concept/c_purityfa_alerts.html")
        _make_link(alerts_links_frame, "FlashBlade Alert Code Reference", _fb_alerts_url).pack(side=tk.TOP, anchor=tk.W)
        _make_link(alerts_links_frame, "FlashArray Alert Code Reference", _fa_alerts_url).pack(side=tk.TOP, anchor=tk.W, pady=(2, 0))

        # Rows 4-6: unified Arrays / Location sheet on the left, SLA entries on the right
        ttk.Label(config_frame, text="Arrays:", wraplength=WL, justify=tk.LEFT).grid(row=4, column=0, sticky=tk.NW, pady=2)
        self._build_arrays_sheet(config_frame, config)

        # SLA inputs: label stacked above the entry so longer label text fits
        # without forcing the configuration column to grow. Each row reuses a
        # small frame at column 3 spanning the previous label+entry footprint.
        SLA_WL = 140
        sla_fb_frame = ttk.Frame(config_frame)
        sla_fb_frame.grid(row=4, column=3, columnspan=2, sticky=tk.W, padx=(10, 10), pady=2)
        ttk.Label(sla_fb_frame, text="FlashBlade Target SLA:", wraplength=SLA_WL, justify=tk.LEFT).pack(side=tk.TOP, anchor=tk.W)
        self.sla_fb_entry = ttk.Entry(sla_fb_frame, width=10)
        self.sla_fb_entry.insert(0, config.get("sla_fb", "1h 30m"))
        self.sla_fb_entry.pack(side=tk.TOP, anchor=tk.W)

        sla_faf_frame = ttk.Frame(config_frame)
        sla_faf_frame.grid(row=5, column=3, columnspan=2, sticky=tk.W, padx=(10, 10), pady=2)
        ttk.Label(sla_faf_frame, text="FlashArray Pod Target SLA:", wraplength=SLA_WL, justify=tk.LEFT).pack(side=tk.TOP, anchor=tk.W)
        self.sla_faf_entry = ttk.Entry(sla_faf_frame, width=10)
        self.sla_faf_entry.insert(0, config.get("sla_faf", "1h"))
        self.sla_faf_entry.pack(side=tk.TOP, anchor=tk.W)

        sla_fab_frame = ttk.Frame(config_frame)
        sla_fab_frame.grid(row=6, column=3, columnspan=2, sticky=tk.W, padx=(10, 10), pady=2)
        ttk.Label(sla_fab_frame, text="FlashArray Async Target SLA:", wraplength=SLA_WL, justify=tk.LEFT).pack(side=tk.TOP, anchor=tk.W)
        self.sla_fab_entry = ttk.Entry(sla_fab_frame, width=10)
        self.sla_fab_entry.insert(0, config.get("sla_fab", "1h"))
        self.sla_fab_entry.pack(side=tk.TOP, anchor=tk.W)
        
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

        # ── Lower pane: button bar + run-output log ──────────────────────────
        # Wrapping btn_frame and text_out in a single Frame lets us add them
        # to the PanedWindow as one pane; the user-draggable sash above it
        # grows or shrinks the Configuration pane (and the Arrays sheet
        # inside it) at the expense of this lower pane.
        lower_pane = ttk.Frame(main_paned)
        # weight=2 leaves a usable initial slice of the window for the
        # output log while still favoring the Configuration pane on top.
        main_paned.add(lower_pane, weight=2)

        # ttk.PanedWindow seeds its sash position from each pane's
        # *requested* size; under tight initial geometry the lower pane's
        # ScrolledText can out-bid the Configuration pane and squeeze the
        # Arrays sheet down to a few visible rows. Force the sash to the
        # target Configuration height after the first idle pass so 15
        # data rows are visible regardless of initial window height.
        # Target: 3 user rows + alerts + 15-row sheet + checkbox + label
        # frame chrome ~= 660 px. update_idletasks forces the geometry
        # manager to compute sizes before we read/set the sash.
        def _set_initial_sash():
            try:
                main_paned.update_idletasks()
                main_paned.sashpos(0, 660)
            except Exception:
                pass
        self.after_idle(_set_initial_sash)

        # ── Button bar ────────────────────────────────────────────────────────
        btn_frame = ttk.Frame(lower_pane)
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
        # Independent of Run Report; does its own type detection so users
        # can jump straight to volume/snapshot protection without first
        # running the daily collection.
        self.protect_btn = tk.Button(btn_frame, text="Volume & Snapshot Protection",
                                     command=self._run_protection_report,
                                     relief=tk.RAISED, padx=6, pady=3)
        self.protect_btn.pack(side=tk.LEFT, padx=5)
        self.last_protection_path = None
        
        # height=8 (rows) caps the ScrolledText's natural requested height
        # so it doesn't out-bid the Configuration pane for vertical space
        # in the PanedWindow on first paint. fill=BOTH + expand=True still
        # lets it grow when the user drags the sash upward.
        self.text_out = scrolledtext.ScrolledText(lower_pane, wrap=tk.NONE, height=8)
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
        # --fake-arrays bypasses monitor_config.json entirely so a real saved
        # config is never read or overwritten when previewing the synthetic
        # 12-array / 5-location dataset.
        if FAKE_ARRAYS:
            return _fake_arrays_config()
        if os.path.exists("monitor_config.json"):
            try:
                with open("monitor_config.json", "r", encoding="utf-8") as f:
                    return json.load(f)
            except: pass
        return {}

    def _save_config(self):
        # Hard guard against clobbering the real monitor_config.json with the
        # synthetic dataset when the GUI was launched in --fake-arrays mode.
        if FAKE_ARRAYS:
            messagebox.showwarning(
                "Save disabled in --fake-arrays mode",
                "The GUI is running with the synthetic 12-array dataset.\n"
                "Saving is disabled to protect your real monitor_config.json.\n\n"
                "Restart without --fake-arrays to edit and save your config.")
            return
        arrays = [{"name": n, "location": l, "notes": nt}
                  for n, l, nt in self._get_arrays_from_sheet()]
        # Capture the current Arrays-sheet column widths so any user-driven
        # resize (drag separator / double-click auto-fit) survives restart.
        col_widths = []
        try:
            _sheet = getattr(self, 'arrays_sheet', None)
            if _sheet is not None:
                col_widths = [int(_sheet.column_width(column=0)),
                              int(_sheet.column_width(column=1)),
                              int(_sheet.column_width(column=2))]
        except Exception:
            col_widths = []
        data = {
            "user_fb": self.user_fb_entry.get().strip(),
            "user_faf": self.user_faf_entry.get().strip(),
            "user_fab": self.user_fab_entry.get().strip(),
            "alerts_excluded": self.alerts_entry.get("1.0", tk.END).strip(),
            "arrays": arrays,
            "arrays_col_widths": col_widths,
            "sla_fb": self.sla_fb_entry.get().strip(),
            "sla_faf": self.sla_faf_entry.get().strip(),
            "sla_fab": self.sla_fab_entry.get().strip(),
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

        # Array list: if a run has completed, list by detected type buckets;
        # otherwise fall back to the unified sheet entries (type unknown).
        _last = getattr(self, '_last_cfg', None) or {}
        if _last.get('arr_fb') or _last.get('arr_faf') or _last.get('arr_fab'):
            for a in _last.get('arr_fb', []):
                header += f"FB Array - {a}\n"
            header += "\n"
            for a in _last.get('arr_faf', []):
                header += f"FA-File Array - {a}\n"
            header += "\n"
            for a in _last.get('arr_fab', []):
                header += f"FA-Block Array - {a}\n"
        else:
            for n, l, _nt in self._get_arrays_from_sheet():
                header += f"Array - {n}" + (f"  ({l})" if l else "") + "\n"

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

    def _show_busy_spinner(self, message="Running report..."):
        """Embed a spinning logo directly in the Configuration frame,
        immediately under the Everpure logo (column 5, row 3+).

        Cycles through FB-Green.png \u2192 FA-Green.png \u2192 everpure_logo.png
        (and wraps) on successive invocations so each logo gets equal
        screen time. Silent no-op when Pillow isn't available or none
        of the candidate images exist. Safe to call repeatedly \u2014
        subsequent calls while a spinner is already visible are no-ops.
        The *message* argument is currently unused (kept for call-site
        compatibility) since the main text output already announces
        "Polling arrays...".
        """
        try:
            if getattr(self, '_busy_spinner_win', None) is not None:
                return
            parent = getattr(self, '_config_frame', None)
            if parent is None or not HAS_PIL:
                return

            import math
            _img_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
            _candidates = ["FB-Green.png", "FA-Green.png", "everpure_logo.png"]
            _existing = [os.path.join(_img_dir, n)
                         for n in _candidates
                         if os.path.exists(os.path.join(_img_dir, n))]
            if not _existing:
                return
            # Round-robin selection: advance an instance counter each call
            # so the three logos rotate 1 \u2192 2 \u2192 3 \u2192 1 \u2026.
            idx = getattr(self, '_busy_img_idx', -1) + 1
            idx %= len(_existing)
            self._busy_img_idx = idx
            img_path = _existing[idx]

            self._busy_pil_img   = None
            self._busy_tk_img    = None
            self._busy_angle     = 0
            self._busy_stop      = False
            self._busy_img_label = None
            self._busy_canvas_sz = 0

            # Per-image target widths \u2014 the logos are all wider than tall
            # (e.g. everpure_logo is ~997x183, ~5.4:1), so forcing them
            # all to 96x96 squished them. Preserve aspect ratio and give
            # the very-wide everpure logo double the nominal width.
            _target_widths = {
                "everpure_logo.png": 192,
                "FB-Green.png":       96,
                "FA-Green.png":       96,
            }
            orig = Image.open(img_path).convert("RGBA")
            tw = _target_widths.get(os.path.basename(img_path), 96)
            th = max(1, int(round(tw * orig.height / orig.width)))
            pil = orig.resize((tw, th), Image.Resampling.LANCZOS)
            self._busy_pil_img = pil
            # Fixed canvas >= image diagonal so rotation never clips the
            # corners or resizes the Label as the angle changes.
            diag = int(math.ceil(math.sqrt(tw * tw + th * th)))
            self._busy_canvas_sz = diag + 4

            sz = self._busy_canvas_sz
            initial = Image.new("RGBA", (sz, sz), (0, 0, 0, 0))
            ox = (sz - tw) // 2
            oy = (sz - th) // 2
            initial.paste(pil, (ox, oy), pil)
            self._busy_tk_img = ImageTk.PhotoImage(initial)

            # Match the Configuration LabelFrame's background so the
            # square bounding box of the rotation canvas blends in
            # with the rest of the frame.
            try:
                bg = ttk.Style().lookup("TLabelframe", "background") or ""
            except Exception:
                bg = ""
            # Wrap the spinning image and the per-phase status text in a
            # single container so both live as one grid cell directly
            # under the Everpure logo (column 5, rows 3\u20137).
            container = tk.Frame(parent)
            if bg:
                try:
                    container.configure(bg=bg)
                except Exception:
                    pass
            container.grid(row=3, column=5, rowspan=5,
                           sticky=tk.NE, padx=10, pady=(2, 5))
            lbl = tk.Label(container, image=self._busy_tk_img,
                           borderwidth=0, highlightthickness=0)
            if bg:
                try:
                    lbl.configure(bg=bg)
                except Exception:
                    pass
            lbl.pack(side=tk.TOP)
            # Status line printed underneath the spinner by
            # _update_busy_status() as collection progresses.
            status = tk.Label(container, text="",
                              font=("Segoe UI", 10, "bold"),
                              wraplength=max(self._busy_canvas_sz, 180),
                              justify=tk.CENTER)
            if bg:
                try:
                    status.configure(bg=bg)
                except Exception:
                    pass
            status.pack(side=tk.TOP, pady=(2, 0))
            self._busy_img_label    = lbl
            self._busy_status_label = status
            self._busy_spinner_win  = container
            self._spin_busy_tick()
        except Exception:
            pass

    def _spin_busy_tick(self):
        """Advance the spinning-logo animation by one frame."""
        if getattr(self, '_busy_stop', True):
            return
        win = getattr(self, '_busy_spinner_win', None)
        pil = getattr(self, '_busy_pil_img',   None)
        lbl = getattr(self, '_busy_img_label', None)
        sz  = getattr(self, '_busy_canvas_sz', 0)
        if win is None or pil is None or lbl is None or not sz:
            return
        try:
            self._busy_angle = (self._busy_angle + 15) % 360
            rot = pil.rotate(-self._busy_angle,
                             resample=Image.Resampling.BILINEAR,
                             expand=True)
            canvas = Image.new("RGBA", (sz, sz), (0, 0, 0, 0))
            x = (sz - rot.width)  // 2
            y = (sz - rot.height) // 2
            canvas.paste(rot, (x, y), rot)
            self._busy_tk_img = ImageTk.PhotoImage(canvas)
            lbl.configure(image=self._busy_tk_img)
            self.after(120, self._spin_busy_tick)
        except Exception:
            return

    def _hide_busy_spinner(self):
        """Tear down the inline busy spinner if it's currently shown.
        Destroying the container Frame also removes its spinner and
        status-text children from the Configuration grid, so the slot
        under the Everpure logo is freed until the next Run Report
        click creates a fresh spinner.
        """
        self._busy_stop = True
        win = getattr(self, '_busy_spinner_win', None)
        if win is not None:
            try:
                win.destroy()
            except Exception:
                pass
        self._busy_spinner_win  = None
        self._busy_pil_img      = None
        self._busy_tk_img       = None
        self._busy_img_label    = None
        self._busy_status_label = None

    def _update_busy_status(self, text):
        """Thread-safe update of the status line under the busy spinner.
        Called indirectly from the worker thread via the ``progress_cb``
        passed into ``run_collection_core`` \u2014 that callback uses
        self.after(0, ...) so all Tk widget writes happen on the main
        thread. Silent no-op when the spinner isn't currently shown.
        Also mirrors the message into the main output text box as a
        running log of phases so the user can scroll back through the
        sequence after the run completes (_update_gui replaces the
        contents with the finished report text on completion).
        """
        lbl = getattr(self, '_busy_status_label', None)
        if lbl is not None:
            try:
                lbl.configure(text=text)
            except Exception:
                pass
        out = getattr(self, 'text_out', None)
        if out is not None:
            try:
                out.insert(tk.END, text + "\n")
                out.see(tk.END)
            except Exception:
                pass

    def run_report(self):
        self.run_btn.config(state=tk.NORMAL) # Reset in thread
        self.run_btn.config(state=tk.DISABLED)
        self.text_out.delete("1.0", tk.END)
        self.text_out.insert(tk.END, "Polling arrays... Please wait.\n\n")
        self._show_busy_spinner("Polling arrays... Please wait.")
        # Unified arrays list (name, location). SSH-based classification happens
        # inside run_collection_core, which fans out arr_fb/arr_faf/arr_fab.
        _arrays = [{'name': n, 'location': l, 'notes': nt}
                   for n, l, nt in self._get_arrays_from_sheet()]
        cfg = {
            'user_fb': self.user_fb_entry.get().strip(),
            'user_faf': self.user_faf_entry.get().strip(),
            'user_fab': self.user_fab_entry.get().strip(),
            'arrays': _arrays,
            'sla_fb': parse_time_to_seconds(self.sla_fb_entry.get()),
            'sla_faf': parse_time_to_seconds(self.sla_faf_entry.get()),
            'sla_fab': parse_time_to_seconds(self.sla_fab_entry.get()),
            'excluded': [x.strip() for x in self.alerts_entry.get("1.0", tk.END).replace('\n', ',').split(',') if x.strip() and "e.g." not in x],
            'ignore_source_lag': self.ignore_source_lag_var.get()
        }
        threading.Thread(target=self._run_collection, args=(cfg,), daemon=True).start()

    def _run_collection(self, config):
        try:
            # Progress callback: invoked from the worker thread, marshals
            # the status update to the main thread via self.after(0, ...)
            # so Tk widget writes stay on the UI thread.
            def _progress(msg):
                try:
                    self.after(0, lambda m=msg: self._update_busy_status(m))
                except Exception:
                    pass
            final, detailed, stats = run_collection_core(
                config, nogui=False, progress_cb=_progress)
            # Stash the post-classification config so _auto_save_reports can reuse
            # the populated arr_fb / arr_faf / arr_fab buckets when building HTML.
            self._last_cfg = config
            self.after(0, lambda: self._update_gui(final, detailed, stats))
        finally:
            # Always tear down the busy spinner on the main thread, even if
            # run_collection_core raised \u2014 otherwise the spinner window
            # would linger after an error.
            self.after(0, self._hide_busy_spinner)


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

    # ── Volume & Snapshot Protection ────────────────────────────────────
    def _run_protection_report(self):
        """Kick off the independent protection-report collection in a
        worker thread so the GUI stays responsive."""
        self.protect_btn.config(state=tk.DISABLED)
        self.text_out.insert(tk.END,
            "\nCollecting volume & snapshot protection data... Please wait.\n")
        self.text_out.see(tk.END)
        self._show_busy_spinner("Collecting volume & snapshot protection data...")
        _arrays = [{'name': n, 'location': l, 'notes': nt}
                   for n, l, nt in self._get_arrays_from_sheet()]
        cfg = {
            'user_fb':  self.user_fb_entry.get().strip(),
            'user_faf': self.user_faf_entry.get().strip(),
            'user_fab': self.user_fab_entry.get().strip(),
            'arrays':   _arrays,
        }
        threading.Thread(target=self._run_protection_collection,
                         args=(cfg,), daemon=True).start()

    def _run_protection_collection(self, config):
        try:
            def _progress(msg):
                try:
                    self.after(0, lambda m=msg: self._update_busy_status(m))
                except Exception:
                    pass
            per_array, _logs = run_protection_collection_core(
                config, nogui=False, progress_cb=_progress)
            html = build_protection_html(per_array, config)
            date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
            script_dir = os.path.dirname(os.path.abspath(__file__))
            out_dir    = os.path.join(script_dir, "reports", "protection")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(
                out_dir, f"Pure_Volume_Snapshot_Protection_{date_str}.html")
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(html)
            self.last_protection_path = out_path
            def _done():
                self.text_out.insert(tk.END,
                    f"Protection report saved to: {os.path.abspath(out_path)}\n")
                self.text_out.see(tk.END)
                try: os.startfile(os.path.abspath(out_path))
                except Exception: pass
            self.after(0, _done)
        except Exception as e:
            self.after(0, lambda err=e: self.text_out.insert(
                tk.END, f"Protection report failed: {err}\n"))
        finally:
            self.after(0, self._hide_busy_spinner)
            self.after(0, lambda: self.protect_btn.config(state=tk.NORMAL))

    def _open_protection_report(self):
        if self.last_protection_path and os.path.exists(self.last_protection_path):
            os.startfile(os.path.abspath(self.last_protection_path))

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
            _last = getattr(self, '_last_cfg', None) or {}
            cfg = {
                'sla_fb':          fb_sec,
                'sla_faf':         faf_sec,
                'sla_fab':         fab_sec,
                'excluded':        excluded,
                'ignore_source_lag': self.ignore_source_lag_var.get(),
                'arr_fb':  list(_last.get('arr_fb',  [])),
                'arr_faf': list(_last.get('arr_faf', [])),
                'arr_fab': list(_last.get('arr_fab', [])),
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
        self._health_history_impl(open_browser=True,
                                  _warn=messagebox.showwarning,
                                  _error=messagebox.showerror)

    @staticmethod
    def _health_history_impl(open_browser=True, _warn=None, _error=None):
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
        # SLA target (minutes) recorded per day/array — may vary over time as
        # the user updates SLA values in the config.
        daily_sla_target = {d: {a: None for a in arrays_set} for d in dates_set}
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
            sla_str = r.get('sla_target_sec', '').strip()
            if sla_str:
                try:
                    daily_sla_target[d][a] = float(sla_str) / 60.0   # minutes
                except ValueError:
                    pass

        # Fill forward missing per-array SLA targets so every (d, a) cell
        # has a threshold to compare against (uses the most recent value
        # seen for that array; arrays with no SLA anywhere get None).
        for a in arrays_set:
            last = None
            for d in dates_set:
                v = daily_sla_target[d][a]
                if v is not None:
                    last = v
                elif last is not None:
                    daily_sla_target[d][a] = last
            # Backfill leading gaps with the first non-None value.
            first = next((daily_sla_target[d][a] for d in dates_set
                          if daily_sla_target[d][a] is not None), None)
            if first is not None:
                for d in dates_set:
                    if daily_sla_target[d][a] is None:
                        daily_sla_target[d][a] = first

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

        def _axes_x_frac(fig, ax):
            """Return (left, right, xmin, xmax) for the axes bbox after
            tight_layout(): left/right are figure-width fractions of the
            data axes' edges; xmin/xmax are the data-coord x-axis limits.
            JS uses these to map a mouse-x fraction to a bar index.
            """
            fig.canvas.draw()
            pos = ax.get_position()
            xmin, xmax = ax.get_xlim()
            return float(pos.x0), float(pos.x1), float(xmin), float(xmax)

        def _sla_bar_b64(period_dates, x_labels, sla_agg, title):
            """SLA chart – stacked by array. Returns (b64, meta) where meta
            carries per-bar array contributions and axes geometry so the
            HTML report can render hover tooltips listing the arrays that
            contributed violations on each day."""
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
            left, right, xmin, xmax = _axes_x_frac(fig, ax)
            buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=130); plt.close(fig); buf.seek(0)
            bars = []
            for i, d in enumerate(period_dates):
                contribs = [{"name": a, "value": int(sla_agg[d][a])}
                            for a in arrays_set if sla_agg[d][a]]
                bars.append({"label": x_labels[i], "date": d, "arrays": contribs})
            meta = {"v": 2, "left": left, "right": right,
                    "xlim": [xmin, xmax], "bars": bars}
            return base64.b64encode(buf.read()).decode('ascii'), meta

        def _alrt_bar_b64(period_dates, x_labels, alrt_agg, title, show_info, show_warn):
            """Alert chart – stacked by severity. Returns (b64, meta) where
            meta lists the arrays contributing to each day's visible alerts
            (filtered by the show_info / show_warn flags) so hover tooltips
            in the HTML report can name them."""
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
            left, right, xmin, xmax = _axes_x_frac(fig, ax)
            buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=130); plt.close(fig); buf.seek(0)
            bars = []
            for i, d in enumerate(period_dates):
                contribs = []
                for a in arrays_set:
                    cnt = int(alrt_agg[d][a]['c'])
                    if show_warn: cnt += int(alrt_agg[d][a]['w'])
                    if show_info: cnt += int(alrt_agg[d][a]['i'])
                    if cnt:
                        contribs.append({"name": a, "value": cnt})
                bars.append({"label": x_labels[i], "date": d, "arrays": contribs})
            meta = {"v": 2, "left": left, "right": right,
                    "xlim": [xmin, xmax], "bars": bars}
            return base64.b64encode(buf.read()).decode('ascii'), meta

        def _lag_line_b64(period_dates, x_labels, arr_daily_lag, title,
                          sla_min=None):
            """Line chart of avg lag in minutes for one array over a period.

            Bands and threshold lines are drawn relative to the array's own
            SLA target, which may vary day-to-day: green 0–50% of SLA,
            yellow 50–100%, orange/red above SLA. *sla_min* accepts either a
            scalar (legacy, constant SLA) or a list of per-x-point values in
            minutes (None entries are forward/backward filled from nearest
            known day). If no SLA data is available the chart is rendered
            without bands.
            """
            n     = len(period_dates)
            y_raw = [arr_daily_lag.get(d) for d in period_dates]
            y     = [v if v is not None else float('nan') for v in y_raw]
            y_fin = [v for v in y_raw if v is not None]

            # Normalise sla_min into a per-point list of length n.
            if isinstance(sla_min, (list, tuple)):
                sla_list = list(sla_min) + [None] * max(0, n - len(sla_min))
                sla_list = sla_list[:n]
            elif sla_min is None:
                sla_list = [None] * n
            else:
                sla_list = [sla_min] * n
            # Forward-fill then backward-fill Nones so every point has a value
            # if at least one day carries an SLA. This lets the bands step on
            # SLA-change days without opening gaps before the first sample.
            _last = None
            for _i in range(n):
                if sla_list[_i] is not None and sla_list[_i] > 0:
                    _last = sla_list[_i]
                else:
                    sla_list[_i] = _last
            _last = None
            for _i in range(n - 1, -1, -1):
                if sla_list[_i] is not None and sla_list[_i] > 0:
                    _last = sla_list[_i]
                else:
                    sla_list[_i] = _last
            have_sla = any(s is not None and s > 0 for s in sla_list)
            sla_valid = [s for s in sla_list if s is not None and s > 0]
            ref   = max(sla_valid) if have_sla else (max(y_fin) if y_fin else 1.0)
            y_max = max((max(y_fin) * 1.15) if y_fin else 0.0, ref * 1.2)

            fig, ax = plt.subplots(figsize=(max(8, n * 0.6), 4.5))
            x = np.arange(n)

            # Colour-banded background — steps per-day so historical points
            # are judged against the SLA that was in effect that day.
            if have_sla:
                sla_arr    = np.array([s if s is not None else 0.0 for s in sla_list], dtype=float)
                yellow_arr = sla_arr * 0.5
                orange_arr = sla_arr
                top_arr    = np.full_like(sla_arr, y_max)
                ax.fill_between(x, 0,          yellow_arr, step='mid',
                                alpha=0.10, color='#28a745', zorder=0, linewidth=0)
                ax.fill_between(x, yellow_arr, orange_arr, step='mid',
                                alpha=0.10, color='#ffc107', zorder=0, linewidth=0)
                ax.fill_between(x, orange_arr, top_arr,    step='mid',
                                alpha=0.10, color='#fd7e14', zorder=0, linewidth=0)
            ax.set_ylim(0, y_max)

            # Threshold step-lines (dashed). Labels describe the relative
            # thresholds; the actual values move with the SLA each day.
            if have_sla:
                _legend_suffix = ''
                if len(set(sla_valid)) == 1:
                    _legend_suffix = f' ({sla_valid[0]:g} min)'
                ax.step(x, yellow_arr, where='mid', color='#856404',
                        linestyle='--', linewidth=1,   alpha=0.75,
                        label=f'50% of SLA{_legend_suffix}', zorder=2)
                ax.step(x, orange_arr, where='mid', color='#7a3500',
                        linestyle='--', linewidth=1.4, alpha=0.85,
                        label=f'SLA limit{_legend_suffix}', zorder=2)

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
            """Hash the CSV data for a period to detect changes. Thresholds are
            now derived from each row's sla_target_sec, so hashing the rows
            alone captures any threshold change."""
            _mk  = period_dates[0][:7] if period_dates else ''
            _mrs = sorted(_rows_by_month.get(_mk, []),
                          key=lambda r: (r['timestamp'], r['array_name']))
            _raw = json.dumps(_mrs, sort_keys=True)
            return hashlib.md5(_raw.encode()).hexdigest()

        # ── Generate one chart-set per period (SLA + 4 alert severity combos) ─
        period_labels = []
        sla_charts    = []
        sla_meta      = []
        alrt_ii = []; alrt_ic = []; alrt_wc = []; alrt_c = []   # ii=Info+Warn, ic=Info, wc=Warn, c=Critical-only
        alrt_ii_meta = []; alrt_ic_meta = []; alrt_wc_meta = []; alrt_c_meta = []
        lag_charts = {}   # {label: {array: b64_line_chart}}
        for label, period_dates, x_labels, sla_agg, alrt_agg in periods:
            period_labels.append(label)
            pt = label if use_months else "Daily"

            # Check chart cache (monthly mode only). Cache hit also requires
            # the meta payloads added with the hover-tooltip feature, and the
            # v2 schema (per-bar 'date' field used by the daily-report links);
            # entries written by older builds lack one or both and must be
            # regenerated so the JS overlay has the data it needs.
            _ph     = _period_hash(period_dates) if use_months else None
            _cached = _chart_cache.get(label, {}) if _ph else {}
            _hit    = bool(_ph and _cached.get('hash') == _ph
                           and isinstance(_cached.get('sla_meta'), dict)
                           and _cached['sla_meta'].get('v') == 2
                           and isinstance(_cached.get('alrt_ii_meta'), dict)
                           and _cached['alrt_ii_meta'].get('v') == 2)

            if _hit:
                sla_charts.append(_cached['sla'])
                sla_meta.append(_cached['sla_meta'])
                alrt_ii.append(_cached['alrt_ii'])
                alrt_ic.append(_cached['alrt_ic'])
                alrt_wc.append(_cached['alrt_wc'])
                alrt_c.append(_cached['alrt_c'])
                alrt_ii_meta.append(_cached['alrt_ii_meta'])
                alrt_ic_meta.append(_cached['alrt_ic_meta'])
                alrt_wc_meta.append(_cached['alrt_wc_meta'])
                alrt_c_meta.append(_cached['alrt_c_meta'])
                lag_charts[label] = _cached['lag']
            else:
                _sla, _sla_m = _sla_bar_b64(period_dates, x_labels, sla_agg,
                                            f"SLA Violations – {pt}")
                _aii, _aii_m = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                             f"Alerts – {pt}", True,  True)
                _aic, _aic_m = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                             f"Alerts – {pt}", True,  False)
                _awc, _awc_m = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                             f"Alerts – {pt}", False, True)
                _ac,  _ac_m  = _alrt_bar_b64(period_dates, x_labels, alrt_agg,
                                             f"Alerts – {pt}", False, False)
                _arr = {}
                for a in arrays_set:
                    # Pass the per-day SLA list so the bands and threshold
                    # step-lines move with the SLA each day. The cache key
                    # already changes if any SLA value in the CSV changes.
                    _sla_list = [daily_sla_target[d][a] for d in period_dates]
                    _arr[a] = _lag_line_b64(
                        period_dates, x_labels, {d: daily_lag[d][a] for d in period_dates},
                        f"{a}  \u2013  {pt}  Avg Replication Lag",
                        sla_min=_sla_list)
                sla_charts.append(_sla); sla_meta.append(_sla_m)
                alrt_ii.append(_aii); alrt_ic.append(_aic)
                alrt_wc.append(_awc); alrt_c.append(_ac)
                alrt_ii_meta.append(_aii_m); alrt_ic_meta.append(_aic_m)
                alrt_wc_meta.append(_awc_m); alrt_c_meta.append(_ac_m)
                lag_charts[label] = _arr
                if _ph:
                    _chart_cache[label] = {
                        'hash': _ph, 'sla': _sla, 'sla_meta': _sla_m,
                        'alrt_ii': _aii, 'alrt_ic': _aic,
                        'alrt_wc': _awc, 'alrt_c': _ac,
                        'alrt_ii_meta': _aii_m, 'alrt_ic_meta': _aic_m,
                        'alrt_wc_meta': _awc_m, 'alrt_c_meta': _ac_m,
                        'lag': _arr,
                    }

        # ── Save updated chart cache ──────────────────────────────────────────
        try:
            with open(_cache_path, 'w', encoding='utf-8') as _cf:
                json.dump(_chart_cache, _cf)
        except Exception:
            pass   # cache write failure is non-fatal

        # ── Calendar day-status data (monthly mode only) ─────────────────────
        # Each lag cell carries both the avg-lag (m) and the SLA target (s)
        # in minutes so the JS can colour cells relative to that day's own
        # SLA — < 50% green, 50–100% yellow, > 100% orange.
        cal_data     = {}
        lag_cal_data = {}   # {label: {array: {day_key: {"m": mins, "s": sla}}}}
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
                        if v is None:
                            continue
                        s = daily_sla_target[d][a]
                        cell = {"m": round(v, 1)}
                        if s is not None:
                            cell["s"] = round(s, 1)
                        day_lag[xl] = cell
                    arr_lag[a] = day_lag
                lag_cal_data[label] = arr_lag

        # ── Inventory existing per-day Daily HTML reports ─────────────────────
        # The history HTML lives at reports/Pure_Array_History.html; the daily
        # reports live at reports/daily/Pure Array Report YYYY-MM-DD.html. The
        # URL map below uses paths relative to the history HTML so the links
        # work whether the file is served, opened from disk, or zipped up.
        from urllib.parse import quote as _urlquote
        _daily_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "reports", "daily")
        _daily_re = re.compile(r"^Pure Array Report (\d{4}-\d{2}-\d{2})\.html$")
        daily_reports = {}
        try:
            for _fn in os.listdir(_daily_dir):
                _m = _daily_re.match(_fn)
                if _m:
                    daily_reports[_m.group(1)] = "daily/" + _urlquote(_fn)
        except (FileNotFoundError, OSError):
            pass   # no reports/daily/ yet — links simply aren't rendered

        # ── Serialise chart arrays for JS ──────────────────────────────────────
        import json
        js_labels      = json.dumps(period_labels)
        js_sla         = json.dumps(sla_charts)
        js_sla_meta    = json.dumps(sla_meta)
        js_alrt_ii     = json.dumps(alrt_ii)   # Info + Warning + Critical
        js_alrt_ic     = json.dumps(alrt_ic)   # Info + Critical
        js_alrt_wc     = json.dumps(alrt_wc)   # Warning + Critical
        js_alrt_c      = json.dumps(alrt_c)    # Critical only
        js_alrt_ii_meta = json.dumps(alrt_ii_meta)
        js_alrt_ic_meta = json.dumps(alrt_ic_meta)
        js_alrt_wc_meta = json.dumps(alrt_wc_meta)
        js_alrt_c_meta  = json.dumps(alrt_c_meta)
        js_cal         = json.dumps(cal_data)
        js_lag_cal     = json.dumps(lag_cal_data)
        js_lag_charts  = json.dumps(lag_charts)
        js_array_names = json.dumps(sorted(arrays_set))
        js_daily_rpts  = json.dumps(daily_reports)
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
                     padding: 10px; display: inline-block; position: relative; }}
    img            {{ display: block; }}
    /* ── chart bar hover tooltip ───────────────────────────────────────── */
    .chart-tip     {{ position: absolute; pointer-events: none; display: none;
                     background: rgba(28,38,64,.94); color: #fff;
                     font-size: 9pt; line-height: 1.35;
                     padding: 6px 10px; border-radius: 5px;
                     box-shadow: 0 4px 14px rgba(0,0,0,.28);
                     max-width: 320px; z-index: 50; }}
    .chart-tip-head {{ font-weight: bold; color: #ffd96b;
                      border-bottom: 1px solid rgba(255,255,255,.18);
                      padding-bottom: 3px; margin-bottom: 4px; }}
    .chart-tip ul   {{ margin: 0; padding-left: 16px; }}
    .chart-tip li   {{ font-size: 8.5pt; }}
    /* ── per-bar Daily-report links + clickable bars ──────────────────── */
    .chart-wrap img      {{ cursor: pointer; }}
    .daily-link-row      {{ position: relative; height: 18px;
                            user-select: none; }}
    .daily-link          {{ position: absolute; top: 0;
                            transform: translateX(-50%);
                            font-size: 7.5pt; font-weight: bold;
                            color: #2E4D8C; text-decoration: none;
                            padding: 1px 4px; border-radius: 3px;
                            background: rgba(46,77,140,.10);
                            white-space: nowrap; cursor: pointer; }}
    .daily-link:hover    {{ background: rgba(46,77,140,.22);
                            text-decoration: underline; }}
    /* ── transient toast for "No report exists for that day." ─────────── */
    #chart-toast         {{ position: fixed; bottom: 32px; left: 50%;
                            transform: translateX(-50%);
                            background: rgba(28,38,64,.94); color: #fff;
                            padding: 9px 18px; border-radius: 6px;
                            font-size: 9.5pt;
                            box-shadow: 0 4px 14px rgba(0,0,0,.28);
                            opacity: 0; transition: opacity .2s;
                            pointer-events: none; z-index: 200; }}
    #chart-toast.show    {{ opacity: 1; }}
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
  <div class="chart-wrap" id="wrap-sla">
    <img id="img-sla" src="" alt="SLA violations chart">
    <div class="chart-tip" id="tip-sla"></div>
    <div class="daily-link-row" id="dlr-sla"></div>
  </div>

  <h2>Support Alerts</h2>
  <div class="chart-wrap" id="wrap-alrt">
    <img id="img-alrt" src="" alt="Alert count chart">
    <div class="chart-tip" id="tip-alrt"></div>
    <div class="daily-link-row" id="dlr-alrt"></div>
  </div>

  <!-- Transient notification when a clicked bar has no daily report -->
  <div id="chart-toast"></div>

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
    var SLA_META     = {js_sla_meta};
    var ALRT_II      = {js_alrt_ii};
    var ALRT_IC      = {js_alrt_ic};
    var ALRT_WC      = {js_alrt_wc};
    var ALRT_C       = {js_alrt_c};
    var ALRT_II_META = {js_alrt_ii_meta};
    var ALRT_IC_META = {js_alrt_ic_meta};
    var ALRT_WC_META = {js_alrt_wc_meta};
    var ALRT_C_META  = {js_alrt_c_meta};
    var CAL_DATA     = {js_cal};
    var LAG_CAL_DATA = {js_lag_cal};
    var LAG_CHARTS      = {js_lag_charts};
    var ARRAY_NAMES     = {js_array_names};
    /* Map of YYYY-MM-DD -> relative URL of an on-disk Daily HTML report.
       Populated at history-page generation time by inventorying
       reports/daily/ — see _health_history_impl. Empty if no daily
       reports exist yet. */
    var DAILY_RPTS      = {js_daily_rpts};
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

    /* Return the META array matching the currently visible alert chart. */
    function getAlrtMeta() {{
      var si = document.getElementById('chk-info').checked;
      var sw = document.getElementById('chk-warn').checked;
      if  (si &&  sw) return ALRT_II_META;
      if  (si && !sw) return ALRT_IC_META;
      if (!si &&  sw) return ALRT_WC_META;
      return ALRT_C_META;
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
      // Cells are colour-banded relative to each day's own SLA target
      // (stored on the cell as .s). When no SLA is available we fall back
      // to marking the day "No Data".
      var lagLegend   =
          '<span><span class="leg-swatch" style="background:#d4edda;"></span>&lt; 50% of SLA</span>'
        + '<span><span class="leg-swatch" style="background:#fff9c4;"></span>50&ndash;100% of SLA</span>'
        + '<span><span class="leg-swatch" style="background:#ffe0b2;"></span>&gt; SLA</span>'
        + '<span><span class="leg-swatch" style="background:#f5f5f5;border:1px solid #ccc;"></span>No Data</span>';
      var html = '';
      for (var ai = 0; ai < ARRAY_NAMES.length; ai++) {{
        var arr     = ARRAY_NAMES[ai];
        var lagData = monthData[arr] || {{}};
        var calHtml = buildCal(
          firstDay, daysInMonth, lagData,
          function(cell) {{
            if (!cell || cell.m === undefined || cell.m === null) return 'cal-nodata';
            if (!cell.s) return 'cal-nodata';
            var ratio = cell.m / cell.s;
            if (ratio < 0.5) return 'cal-lag-green';
            if (ratio < 1.0) return 'cal-lag-yellow';
            return 'cal-lag-orange';
          }},
          function(cell) {{
            if (!cell || cell.m === undefined || cell.m === null) return 'No data';
            var t = cell.m.toFixed(1) + ' min avg lag';
            if (cell.s) t += ' (SLA: ' + cell.s.toFixed(1) + ' min)';
            return t;
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

    /* ─── Hover tooltip plumbing for SLA / Alerts bar charts ─────────────
       Each chart's meta carries:
         left, right  : x-fraction (0..1) of the data axes inside the PNG
         xlim         : data-coord x-range matching those fractions
         bars[i]      : {{label, arrays:[{{name, value}}, ...]}}
       We map mouse-x over the rendered <img> to a bar index and show
       a tooltip listing the contributing arrays for that day. */
    function findBarIndex(meta, xfrac) {{
      if (!meta || !meta.bars) return -1;
      if (xfrac < meta.left || xfrac > meta.right) return -1;
      var t = (xfrac - meta.left) / (meta.right - meta.left);
      var x_data = meta.xlim[0] + t * (meta.xlim[1] - meta.xlim[0]);
      var i = Math.round(x_data);
      if (i < 0 || i >= meta.bars.length) return -1;
      return i;
    }}

    function renderTipBody(bar, kind) {{
      var unit = kind === 'sla' ? 'violation' : 'alert';
      var arrs = (bar && bar.arrays) ? bar.arrays : [];
      var head = '<div class="chart-tip-head">' + (bar.label || '') + '</div>';
      if (!arrs.length) {{
        return head + '<div style="font-size:8.5pt;color:#cfd6e6;">'
             + 'No ' + unit + 's</div>';
      }}
      var lis = arrs.map(function(a) {{
        var s = a.value === 1 ? '' : 's';
        return '<li>' + a.name + ' &mdash; '
             + a.value + ' ' + unit + s + '</li>';
      }}).join('');
      return head + '<ul>' + lis + '</ul>';
    }}

    function attachChartHover(wrapId, imgId, tipId, getMeta, kind) {{
      var wrap = document.getElementById(wrapId);
      var img  = document.getElementById(imgId);
      var tip  = document.getElementById(tipId);
      if (!wrap || !img || !tip) return;
      function onMove(e) {{
        var meta = getMeta();
        if (!meta) {{ tip.style.display = 'none'; return; }}
        var rect = img.getBoundingClientRect();
        var xfrac = (e.clientX - rect.left) / rect.width;
        var i = findBarIndex(meta, xfrac);
        if (i < 0) {{ tip.style.display = 'none'; return; }}
        tip.innerHTML = renderTipBody(meta.bars[i], kind);
        tip.style.display = 'block';
        /* Position tooltip relative to chart-wrap; offset slightly so it
           never sits under the cursor and never spills past the right edge. */
        var wrapRect = wrap.getBoundingClientRect();
        var tipW = tip.offsetWidth;
        var rawX = (e.clientX - wrapRect.left) + 14;
        var maxX = wrapRect.width - tipW - 6;
        if (rawX > maxX) rawX = maxX;
        if (rawX < 4)    rawX = 4;
        var rawY = (e.clientY - wrapRect.top) + 14;
        tip.style.left = rawX + 'px';
        tip.style.top  = rawY + 'px';
      }}
      function onLeave() {{ tip.style.display = 'none'; }}
      function onClick(e) {{
        var meta = getMeta();
        if (!meta) return;
        var rect = img.getBoundingClientRect();
        var xfrac = (e.clientX - rect.left) / rect.width;
        var i = findBarIndex(meta, xfrac);
        if (i < 0) return;
        openDaily(meta.bars[i].date);
      }}
      img.addEventListener('mousemove', onMove);
      img.addEventListener('mouseleave', onLeave);
      img.addEventListener('click', onClick);
    }}

    /* ─── Daily-report links + toast ─────────────────────────────────────
       For each bar whose underlying date has a matching file in
       reports/daily/, drop a small "Daily" link below the bar in an
       absolutely-positioned overlay row. Bar-pixel positions are
       recomputed on every render and on window resize so the links
       follow the chart at any rendered width. */
    function showToast(msg) {{
      var t = document.getElementById('chart-toast');
      if (!t) return;
      t.textContent = msg;
      t.classList.add('show');
      if (t._timer) clearTimeout(t._timer);
      t._timer = setTimeout(function() {{ t.classList.remove('show'); }}, 1800);
    }}

    function openDaily(date) {{
      if (!date) return;
      var url = DAILY_RPTS[date];
      if (url) window.open(url, '_blank');
      else     showToast('No report exists for that day.');
    }}

    function renderDailyLinks(rowId, imgId, getMeta) {{
      var row = document.getElementById(rowId);
      var img = document.getElementById(imgId);
      if (!row || !img) return;
      var meta = getMeta();
      if (!meta || !meta.bars || !meta.xlim) {{ row.innerHTML = ''; return; }}
      var imgRect  = img.getBoundingClientRect();
      var wrapRect = row.parentElement.getBoundingClientRect();
      var W = imgRect.width;
      if (!W) {{ row.innerHTML = ''; return; }}
      /* Image may be horizontally inset inside chart-wrap by its padding
         (10px). offX captures that so the links align with the bars. */
      var offX = imgRect.left - wrapRect.left;
      var span = (meta.xlim[1] - meta.xlim[0]);
      if (!span) {{ row.innerHTML = ''; return; }}
      var html = '';
      for (var i = 0; i < meta.bars.length; i++) {{
        var bar = meta.bars[i];
        if (!bar.date || !DAILY_RPTS[bar.date]) continue;
        var t = (i - meta.xlim[0]) / span;
        var xfrac = meta.left + t * (meta.right - meta.left);
        var px = offX + xfrac * W;
        html += '<a class="daily-link" style="left:' + px.toFixed(1)
              + 'px;" href="' + DAILY_RPTS[bar.date]
              + '" target="_blank" title="Open Daily Report for '
              + bar.date + '">Daily</a>';
      }}
      row.innerHTML = html;
    }}

    function renderAllDailyLinks() {{
      renderDailyLinks('dlr-sla',  'img-sla',
                       function() {{ return SLA_META[idx]; }});
      renderDailyLinks('dlr-alrt', 'img-alrt',
                       function() {{ return getAlrtMeta()[idx]; }});
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
      /* Hide any stale tooltip when navigating between periods. */
      var ts = document.getElementById('tip-sla');
      var ta = document.getElementById('tip-alrt');
      if (ts) ts.style.display = 'none';
      if (ta) ta.style.display = 'none';
      /* Clear stale daily links immediately, then refresh once each new
         chart image has actually loaded (we need its rendered width). */
      document.getElementById('dlr-sla').innerHTML  = '';
      document.getElementById('dlr-alrt').innerHTML = '';
    }}

    /* Wire hover handlers once on load; they look up SLA_META[idx] /
       alert meta on every mousemove so they always reflect the current
       period and severity-filter state. */
    attachChartHover('wrap-sla',  'img-sla',  'tip-sla',
                     function() {{ return SLA_META[idx]; }}, 'sla');
    attachChartHover('wrap-alrt', 'img-alrt', 'tip-alrt',
                     function() {{ return getAlrtMeta()[idx]; }}, 'alrt');

    /* The daily-link overlay needs the image's rendered width to map
       data x-coordinates to pixels, so it has to refresh whenever:
         (a) a new chart image finishes loading (after navigate / filter), or
         (b) the window is resized (the chart is fluid-width). */
    document.getElementById('img-sla').addEventListener('load',
        renderAllDailyLinks);
    document.getElementById('img-alrt').addEventListener('load',
        renderAllDailyLinks);
    window.addEventListener('resize', renderAllDailyLinks);

    idx = LABELS.length - 1;
    render();
    /* If chart images were already cached, the load events above may have
       fired before our listeners were attached. Force one refresh. */
    renderAllDailyLinks();
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

Array type is auto-detected at report time by issuing 'purearray list',
'purepod list' and 'purepgroup list' against each configured array — you
no longer enter arrays into separate per-type lists.


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

  Arrays (spreadsheet editor)
      A single two-column sheet with "Array" and "Location" columns
      replaces the old per-type array lists. Enter each array's hostname
      or IP in the Array column and an optional free-text site/location
      label in the Location column. The platform (FB, FA-File, FA-Block)
      is discovered automatically when you click Run Report.

      Sheet features:
        * Column widths can be dragged to any size (double-click a
          separator to auto-fit); widths are persisted in
          monitor_config.json under "arrays_col_widths".
        * The gutter to the left of "Array" shows a 1-based running
          count of rows that actually contain an array name.
        * Blank rows are maintained at the bottom automatically, so
          you can paste or type continuously without inserting rows.
        * Copy / paste / undo / right-click row-insert / row-delete
          are all enabled. Paste always writes starting at the caret
          cell (single-cell paste semantics).

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

RUN REPORT UX
-------------
While a report is in progress a small spinning logo appears directly
under the Everpure logo inside the Configuration panel, cycling through
the FlashBlade, FlashArray and Everpure logos on successive runs.

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
    python pure_monitor.py --fake-arrays Launch GUI with a synthetic
                                         12-array / 5-location demo dataset
                                         (real monitor_config.json is never
                                         read or overwritten in this mode)
    python pure_monitor.py --fake-arrays --nogui
                                         Generate the Daily HTML report
                                         from the synthetic dataset and exit
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

    --fake-arrays  Launch the GUI loaded with a synthetic 12-array / 5-location
                   demo dataset (New York, Chicago, Dallas, Seattle, London).
                   Implies --alert-debug. monitor_config.json is neither read
                   nor written, and Save Config is disabled, so an existing
                   real configuration is never touched. Useful for previewing
                   the Daily HTML report and Array Health History pages with
                   a fully populated dataset before deploying to real arrays.
                   Combine with --nogui to generate the Daily report headlessly:
                     python pure_monitor.py --fake-arrays --nogui

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
    python pure_monitor.py --fake-arrays
    python pure_monitor.py --fake-arrays --nogui
    python pure_monitor.py --help

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
