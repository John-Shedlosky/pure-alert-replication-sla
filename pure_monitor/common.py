"""Shared utilities for the pure_monitor package.

Hosts the leaf-layer helpers used by every other module:
SSH wrapper, parser/classifier helpers, time/SLA conversion, the
debug-fixture dataset, and the cross-module mutable state for the
password-prompt channel and the alert-collection lock. No internal
package imports, so any sibling module can ``from .common import *``
without risking a circular import.
"""
import threading
import queue
import re
import datetime
import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

# Cross-module password-prompt channel. ask_password_in_main (this
# module) signals the GUI thread via password_request_event; the GUI's
# check_queue handler writes the typed password back into
# global_password_response and sets password_response_event. Because
# the writing site lives in a *different* module after the split, the
# GUI assigns to ``common.global_password_response`` via a qualified
# reference rather than ``global global_password_response`` (which
# would only affect the GUI module's namespace).
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
FAKE_ARRAYS = "--fake-arrays" in sys.argv
ALERT_DEBUG = ("--alert-debug" in sys.argv) or FAKE_ARRAYS


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

def _parse_sla_days(value, default=0):
    """Parse a retention-SLA value into whole days.

    Accepts a bare integer string ("7"), a "Nd" form ("7d"), or any
    int/float; returns *default* on empty input or any parse failure.
    Negative values clamp to 0 so a typo can never invert the SLA.
    """
    try:
        if isinstance(value, bool):
            return default
        if isinstance(value, (int, float)):
            return max(0, int(value))
        s = (value or '').strip().lower()
        if not s:
            return default
        if s.endswith('d'):
            s = s[:-1].strip()
        return max(0, int(s))
    except (TypeError, ValueError):
        return default

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
    new-style "arrays" list-of-dicts (with name + location + notes +
    auth_user), plus SLA targets and a few replication pairs that cross
    sites so the Replication Pairs panel is non-empty.
    """
    arrays = [
        # New York (3)
        {'name': 'nyc-pure-fa-01', 'location': 'New York, NY',
         'notes': 'Primary VMware datastores', 'auth_user': 'pureuser'},
        {'name': 'nyc-pure-fa-02', 'location': 'New York, NY',
         'notes': 'SQL prod cluster', 'auth_user': 'pureuser'},
        {'name': 'nyc-pure-fb-01', 'location': 'New York, NY',
         'notes': 'Analytics + nightly backup target', 'auth_user': 'pureuser'},
        # Chicago (3)
        {'name': 'chi-pure-fa-01', 'location': 'Chicago, IL',
         'notes': 'DR target for NYC FA-Block', 'auth_user': 'pureuser'},
        {'name': 'chi-pure-fa-02', 'location': 'Chicago, IL',
         'notes': 'Oracle prod', 'auth_user': 'pureuser'},
        {'name': 'chi-pure-fb-01', 'location': 'Chicago, IL',
         'notes': 'NFS shares + DR for NYC FB', 'auth_user': 'pureuser'},
        # Dallas (2)
        {'name': 'dal-pure-fa-01', 'location': 'Dallas, TX',
         'notes': 'Mixed workload, Tier-2', 'auth_user': 'pureuser'},
        {'name': 'dal-pure-fb-01', 'location': 'Dallas, TX',
         'notes': 'S3 object storage', 'auth_user': 'pureuser'},
        # Seattle (2)
        {'name': 'sea-pure-fa-01', 'location': 'Seattle, WA',
         'notes': 'West-coast primary', 'auth_user': 'pureuser'},
        {'name': 'sea-pure-fa-02', 'location': 'Seattle, WA',
         'notes': 'Dev / test', 'auth_user': 'pureuser'},
        # London (2)
        {'name': 'lon-pure-fa-01', 'location': 'London, UK',
         'notes': 'EMEA primary', 'auth_user': 'pureuser'},
        {'name': 'lon-pure-fb-01', 'location': 'London, UK',
         'notes': 'EMEA backup target', 'auth_user': 'pureuser'},
    ]
    return {
        'sla_fb':   '1h 30m',
        'sla_faf':  '1h',
        'sla_fab':  '1h',
        'sla_retention_snap': '7',
        'sla_retention_repl': '7',
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


def auth_user_for_array(name, config, default='pureuser'):
    """Return the per-array SSH username for *name*, or *default*.

    Reads the ``auth_user`` field of the matching entry in
    ``config['arrays']``. Falls back to *default* when the field is
    blank/missing or the array name is unknown. Used by every SSH call
    site so each array can carry its own credential.
    """
    for item in (config.get('arrays') or []):
        if not isinstance(item, dict):
            continue
        if str(item.get('name', '') or '').strip() == name:
            u = str(item.get('auth_user', '') or '').strip()
            return u or default
    return default


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




__all__ = ['HAS_PARAMIKO', 'password_request_event', 'password_response_event', 'global_password_request_msg', 'global_password_response', 'credentials_cache', '_password_prompt_lock', '_alert_collection_lock', 'FAKE_ARRAYS', 'ALERT_DEBUG', 'ask_password_in_main', 'run_ssh_command', 'parse_time_to_seconds', 'format_seconds_human', '_parse_sla_days', '_fmt_alert_str', '_fake_arrays_config', '_get_debug_alerts', 'parse_pure_date', 'parse_arr_loc', 'align_rel_pairs_by_location', '_parse_csv_text', '_csv_to_dicts', '_classify_array_output', 'detect_array_type', 'parse_unified_arrays', 'unified_arrays_from_config', 'parse_unified_arrays_full', 'unified_arrays_from_config_full', 'auth_user_for_array']
