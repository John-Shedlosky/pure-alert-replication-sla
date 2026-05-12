"""Volume and Snapshot Protection report.

Owns:
* per-array protection-data collection (CSV parsers + SSH-driven
  collection orchestrator)
* the synthetic _fake_protection_data_for fixture
* aggregation of (array, pgroup) data into per-volume rows including
  the maximum local/replicated retention days
* the saved-comments loader
* build_protection_html, the Volume & Snapshot Protection HTML report
"""
import datetime
import html as _html
import json
import os
import re
import sys
import time as _time
from concurrent.futures import ThreadPoolExecutor

from .common import (
    HAS_PARAMIKO,
    FAKE_ARRAYS, ALERT_DEBUG,
    ask_password_in_main, run_ssh_command,
    _parse_sla_days, _parse_csv_text, _csv_to_dicts,
    parse_time_to_seconds,
    parse_unified_arrays, detect_array_type,
    auth_user_for_array,
)


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
    # Synthetic filesystem inventory for Table 2. Every array has a
    # plain `home` filesystem with two directories. Pod-paired arrays
    # additionally expose a pod-scoped `shared_fs` so the pod / replica
    # destination columns render. Snapshots use the
    # `[pod::]fs:dir.<suffix>` form returned by `puredir snapshot list`.
    filesystems = [{'name': 'home', 'pod': None, 'fs': 'home'}]
    directories = [
        {'name': 'home:users', 'fs': 'home', 'directory': 'users'},
        {'name': 'home:temp',  'fs': 'home', 'directory': 'temp'}]
    dir_snapshots = [
        {'name': 'home:users.daily.1', 'policy': 'daily'},
        {'name': 'home:users.daily.2', 'policy': 'daily'},
        {'name': 'home:temp.daily.1',  'policy': 'daily'}]
    policy_locks = {'daily': 'disabled', 'daily-locked': 'ratcheted'}
    # Array-wide safemode (purearray eradication-config). Half the
    # fake arrays demo 'all-disabled' with a 2d delay so the aggregator
    # forces Safemode=Enabled on rows with any snapshot regardless of
    # pgroup / policy retention-lock state; the others demo the
    # default 'all-enabled' (no array-wide safemode).
    if array in ('nyc-pure-fa-01', 'chi-pure-fa-01',
                 'dal-pure-fa-01', 'sea-pure-fa-01'):
        eradication = {'Manual Eradication': 'all-disabled',
                       'Enabled Delay': '2d'}
    else:
        eradication = {'Manual Eradication': 'all-enabled',
                       'Enabled Delay': ''}
    # Snapshot policies: the bare 'daily' policy is enabled with a
    # 7-day local rule; 'daily-locked' is enabled and ratcheted in
    # policy_locks so pod-stretched directories demo Safemode=Enabled.
    policy_snapshots = [
        {'name': 'daily',        'pod': None, 'policy': 'daily',        'enabled': 'true'},
        {'name': 'daily-locked', 'pod': None, 'policy': 'daily-locked', 'enabled': 'true'}]
    # Rule fields mirror the on-array CSV: Every / At / Keep For are
    # raw millisecond integers. 86_400_000 ms == 1 day; the popup
    # formats Every / At as d/h/m/s and Keep For as decimal days.
    policy_rules = {
        'daily':        [{'every': '86400000', 'at': '7200000',
                          'keep_for': '604800000', 'client_name': '',
                          'suffix': 'daily'}],
        'daily-locked': [{'every': '86400000', 'at': '10800000',
                          'keep_for': '1209600000', 'client_name': '',
                          'suffix': 'daily'}]}
    # Synthetic NFS/SMB policy + rule data driving the Export modal.
    policy_nfs_list  = {'nfs-default': {'user_mapping_enabled': 'True'}}
    policy_nfs_rules = {'nfs-default': [
        {'client': '*', 'access': 'root-squash', 'permission': 'rw',
         'anonuid': '65534', 'anongid': '65534',
         'version': 'nfsv3,nfsv4.1', 'security': 'sys'}]}
    policy_smb_list  = {'smb-default': {
        'access_based_enumeration_enabled': 'True',
        'continuous_availability_enabled':  'False'}}
    policy_smb_rules = {'smb-default': [
        {'client': '*', 'anonymous_access_allowed': 'False',
         'smb_encryption_required': 'True'}]}
    # Synthetic export listing: every home:users dir exposes one NFS
    # export; pod-stretched data dirs additionally expose an SMB share.
    # Enabled is left blank to mirror the real CSV behavior (the
    # column is empty when the export is enabled and reads
    # "Policy Disabled" when it is not).
    dir_exports = [
        {'directory': 'home:users', 'export_name': f'{array}_users_nfs',
         'server': f'{array}-server01', 'path': '/users', 'policy': 'nfs-default',
         'type': 'nfs', 'enabled': ''}]
    if array in pod_pairs:
        _lp = pod_pairs[array][1]
        filesystems.append({'name': f'{_lp}::shared_fs',
                            'pod': _lp, 'fs': 'shared_fs'})
        directories.append({'name': f'{_lp}::shared_fs:data',
                            'fs': f'{_lp}::shared_fs', 'directory': 'data'})
        dir_snapshots.append({'name': f'{_lp}::shared_fs:data.daily.1',
                              'policy': 'daily-locked'})
        dir_snapshots.append({'name': f'{_lp}::shared_fs:data.daily.2',
                              'policy': 'daily-locked'})
        dir_exports.append({
            'directory': f'{_lp}::shared_fs:data',
            'export_name': f'{_lp}_data_smb',
            'server': f'{_lp}-smb01', 'path': '/data',
            'policy': 'smb-default', 'type': 'smb', 'enabled': ''})
    return {'volumes': volumes, 'pod_links': pod_links, 'snapshots': snapshots,
            'pgroups': pgroups, 'pgroup_locks': pgroup_locks,
            'pgroup_schedules': pgroup_schedules,
            'pgroup_retentions': pgroup_retentions,
            'connections': connections,
            'filesystems': filesystems, 'directories': directories,
            'dir_snapshots': dir_snapshots,
            'policy_locks': policy_locks,
            'policy_snapshots': policy_snapshots,
            'policy_rules': policy_rules,
            'dir_exports': dir_exports,
            'policy_nfs_rules': policy_nfs_rules,
            'policy_nfs_list':  policy_nfs_list,
            'policy_smb_rules': policy_smb_rules,
            'policy_smb_list':  policy_smb_list,
            'eradication': eradication, 'error': None}


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


def _parse_puredir_list_csv(text):
    """Parse `puredir list --csv` -> [{'name','fs','directory'}, ...].
    The Name column is `<filesystem>:<directory>`; for pod-scoped
    filesystems the filesystem portion itself contains '::' (e.g.
    `pod::fs:dir`). The split is anchored on the LAST ':' so the pod
    prefix stays with the filesystem and matches the form returned by
    `purefs list`.
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name or ':' not in name:
            continue
        fs, dr = name.rsplit(':', 1)
        out.append({'name': name, 'fs': fs, 'directory': dr})
    return out


def _parse_purefs_list_csv(text):
    """Parse `purefs list --csv` -> [{'name','pod','fs'}, ...].
    Pod-scoped filesystems carry a '::' separator: `pod::fs`. Without
    one, `pod` is None and `fs` is the bare filesystem name.
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        if '::' in name:
            pod, fs = name.split('::', 1)
        else:
            pod, fs = None, name
        out.append({'name': name, 'pod': pod, 'fs': fs})
    return out


def _parse_puredir_snap_list_csv(text):
    """Parse `puredir snapshot list --csv` -> [{'name','policy'}, ...].
    Snapshot Name format is `[pod::]filesystem:directory.<suffix>`.
    The suffix can contain dots so the row is preserved verbatim and
    matched against known directories via prefix comparison in
    aggregate_fa_filesystem_rows. Policy is the snapshot-policy name
    that produced the snapshot; retention-lock state is looked up
    against `purepolicy snapshot retention-lock list`.
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        out.append({'name':   name,
                    'policy': (d.get('Policy') or '').strip()})
    return out


def _parse_purepolicy_snap_retention_lock_csv(text):
    """Parse `purepolicy snapshot retention-lock list --csv` rows.
    Returns {policy_name: retention_lock_value_lowercase}. The 'Name'
    column is preserved verbatim.
    """
    out = {}
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        rl = (d.get('Retention Lock') or '').strip().lower()
        out[name] = rl
    return out


def _parse_purearray_eradication_config_csv(text):
    """Parse `purearray eradication-config list --csv` rows.
    Single-row output describing the array-level eradication settings;
    returns the first row as {column: stripped_value}. Columns of
    interest are 'Manual Eradication' (e.g., 'all-disabled') and
    'Enabled Delay' (e.g., '2d'). Returns {} on empty input.
    """
    rows = _csv_to_dicts(text)
    if not rows:
        return {}
    return {k: (v or '').strip() for k, v in rows[0].items() if k}


def _fmt_eradication_delay(val):
    """Render an eradication-config Enabled Delay as 'N Day Protection'.
    Accepts the verbatim Purity value (e.g., '2d') and falls back to
    the original string when no `<int>d` prefix is found. Empty input
    returns the empty string.
    """
    s = (val or '').strip()
    if not s:
        return ''
    m = re.match(r'^(\d+)\s*d', s, re.IGNORECASE)
    if m:
        return f'{m.group(1)} Day Protection'
    return s


def _parse_purepolicy_snapshot_list_csv(text):
    """Parse `purepolicy snapshot list --csv` rows.
    Returns [{'name','pod','policy','enabled'}, ...] where 'name' is the
    verbatim Name column (including any `pod::` prefix), 'pod' is the
    extracted pod prefix or None, and 'policy' is the bare policy name.
    'enabled' is the Enabled column lowercased ('true'/'false').
    """
    out = []
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        if '::' in name:
            pod, pol = name.split('::', 1)
        else:
            pod, pol = None, name
        out.append({'name': name, 'pod': pod, 'policy': pol,
                    'enabled': (d.get('Enabled') or '').strip().lower()})
    return out


def _parse_purepolicy_snapshot_rule_list_csv(text):
    """Parse `purepolicy snapshot rule list --csv` rows.
    Returns {policy_full_name: [rule_dict, ...]} keyed by the verbatim
    Policy column (which includes any `pod::` prefix). Each rule dict
    preserves Every / At / Keep For / Client Name / Suffix verbatim.
    """
    out = {}
    for d in _csv_to_dicts(text):
        policy = (d.get('Policy') or d.get('Name') or '').strip()
        if not policy:
            continue
        out.setdefault(policy, []).append({
            'every':       (d.get('Every') or '').strip(),
            'at':          (d.get('At') or '').strip(),
            'keep_for':    (d.get('Keep For') or '').strip(),
            'client_name': (d.get('Client Name') or '').strip(),
            'suffix':      (d.get('Suffix') or '').strip()})
    return out


def _parse_puredir_export_list_csv(text):
    """Parse `puredir export list --csv` rows.
    Returns [{'directory','export_name','server','path','policy',
    'type','enabled'}, ...]. 'directory' carries the Directory column
    verbatim (`[pod::]fs:dir`); the lookup against the Table 2 row uses
    that same form so pod-scoped directories match.
    """
    out = []
    for d in _csv_to_dicts(text):
        directory = (d.get('Directory') or '').strip()
        if not directory:
            continue
        out.append({
            'directory':   directory,
            'export_name': (d.get('Export Name') or d.get('Name') or '').strip(),
            'server':      (d.get('Server') or '').strip(),
            'path':        (d.get('Path') or '').strip(),
            'policy':      (d.get('Policy') or '').strip(),
            'type':        (d.get('Type') or '').strip(),
            'enabled':     (d.get('Enabled') or '').strip()})
    return out


def _parse_purepolicy_nfs_rule_list_csv(text):
    """Parse `purepolicy nfs rule list --csv` rows.
    Returns {policy_name: [rule_dict, ...]} keyed by the Policy column.
    Each rule dict preserves Client / Access / Permission / Anonuid /
    Anongid / Version / Security verbatim.
    """
    out = {}
    for d in _csv_to_dicts(text):
        policy = (d.get('Policy') or '').strip()
        if not policy:
            continue
        out.setdefault(policy, []).append({
            'client':     (d.get('Client') or '').strip(),
            'access':     (d.get('Access') or '').strip(),
            'permission': (d.get('Permission') or '').strip(),
            'anonuid':    (d.get('Anonuid') or '').strip(),
            'anongid':    (d.get('Anongid') or '').strip(),
            'version':    (d.get('Version') or '').strip(),
            'security':   (d.get('Security') or '').strip()})
    return out


def _parse_purepolicy_nfs_list_csv(text):
    """Parse `purepolicy nfs list --csv` rows.
    Returns {policy_name: {'user_mapping_enabled': str}} keyed by the
    Name column.
    """
    out = {}
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        out[name] = {
            'user_mapping_enabled': (d.get('User Mapping Enabled') or '').strip()}
    return out


def _parse_purepolicy_smb_rule_list_csv(text):
    """Parse `purepolicy smb rule list --csv` rows.
    Returns {policy_name: [rule_dict, ...]} keyed by the Policy column.
    Each rule dict preserves Client / Anonymous Access Allowed / SMB
    Encryption Required verbatim.
    """
    out = {}
    for d in _csv_to_dicts(text):
        policy = (d.get('Policy') or '').strip()
        if not policy:
            continue
        out.setdefault(policy, []).append({
            'client':                   (d.get('Client') or '').strip(),
            'anonymous_access_allowed': (d.get('Anonymous Access Allowed') or '').strip(),
            'smb_encryption_required':  (d.get('SMB Encryption Required') or '').strip()})
    return out


def _parse_purepolicy_smb_list_csv(text):
    """Parse `purepolicy smb list --csv` rows.
    Returns {policy_name: {'access_based_enumeration_enabled': str,
    'continuous_availability_enabled': str}} keyed by the Name column.
    """
    out = {}
    for d in _csv_to_dicts(text):
        name = (d.get('Name') or '').strip()
        if not name:
            continue
        out[name] = {
            'access_based_enumeration_enabled':
                (d.get('Access Based Enumeration Enabled') or '').strip(),
            'continuous_availability_enabled':
                (d.get('Continuous Availability Enabled') or '').strip()}
    return out


def _parse_retention_to_days(val):
    """Convert a Purity 'Keep For' value to a number of days as float.
    `purepolicy snapshot rule list --csv` emits 'Keep For' as a raw
    integer in milliseconds; suffixed duration strings ('7d', '12h',
    '1w') are accepted for backward compatibility. Returns 0.0 when
    the input is empty or unparseable. The result may be decimal
    (e.g., 2.5 for a 60-hour keep value).
    """
    s = (val or '').strip()
    if not s:
        return 0.0
    try:
        ms = int(s)
        return ms / 86400000.0
    except (ValueError, TypeError):
        pass
    try:
        secs = parse_time_to_seconds(s)
        return float(secs) / 86400.0 if secs else 0.0
    except Exception:
        return 0.0


def _collect_one_fa_protection(array, user, detailed_logs, nogui=False):
    """Issue the FlashArray protection commands and parse their output.
    Returns {'volumes', 'pod_links', 'snapshots', 'error', 'policy_snapshots',
    'policy_rules', 'dir_exports', 'policy_nfs_rules', 'policy_nfs_list',
    'policy_smb_rules', 'policy_smb_list'}.
    """
    if ALERT_DEBUG:
        return _fake_protection_data_for(array)
    out = {'volumes': [], 'pod_links': [], 'snapshots': [],
           'pgroups': [], 'pgroup_locks': {}, 'pgroup_schedules': {},
           'pgroup_retentions': {}, 'connections': [],
           'filesystems': [], 'directories': [], 'dir_snapshots': [],
           'policy_locks': {}, 'eradication': {},
           'policy_snapshots': [], 'policy_rules': {}, 'dir_exports': [],
           'policy_nfs_rules': {}, 'policy_nfs_list': {},
           'policy_smb_rules': {}, 'policy_smb_list': {},
           'error': None}
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
    # Filesystem-side inventory feeds Table 2. These are independent of
    # `out['volumes']` so they're issued unconditionally; arrays that
    # have no FAFile licence will return empty CSV bodies.
    try:
        out['filesystems'] = _parse_purefs_list_csv(run_ssh_command(
            array, user, "purefs list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purefs list: {e}")
    try:
        out['directories'] = _parse_puredir_list_csv(run_ssh_command(
            array, user, "puredir list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"puredir list: {e}")
    try:
        out['dir_snapshots'] = _parse_puredir_snap_list_csv(run_ssh_command(
            array, user, "puredir snapshot list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"puredir snapshot list: {e}")
    try:
        out['policy_locks'] = _parse_purepolicy_snap_retention_lock_csv(
            run_ssh_command(
                array, user,
                "purepolicy snapshot retention-lock list --csv",
                log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy snapshot retention-lock list: {e}")
    try:
        out['eradication'] = _parse_purearray_eradication_config_csv(
            run_ssh_command(
                array, user,
                "purearray eradication-config list --csv",
                log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purearray eradication-config list: {e}")
    try:
        out['policy_snapshots'] = _parse_purepolicy_snapshot_list_csv(run_ssh_command(
            array, user, "purepolicy snapshot list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy snapshot list: {e}")
    try:
        out['policy_rules'] = _parse_purepolicy_snapshot_rule_list_csv(run_ssh_command(
            array, user, "purepolicy snapshot rule list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy snapshot rule list: {e}")
    try:
        out['dir_exports'] = _parse_puredir_export_list_csv(run_ssh_command(
            array, user, "puredir export list --csv",
            log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"puredir export list: {e}")
    try:
        out['policy_nfs_rules'] = _parse_purepolicy_nfs_rule_list_csv(
            run_ssh_command(array, user,
                "purepolicy nfs rule list --csv",
                log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy nfs rule list: {e}")
    try:
        out['policy_nfs_list'] = _parse_purepolicy_nfs_list_csv(
            run_ssh_command(array, user,
                "purepolicy nfs list --csv",
                log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy nfs list: {e}")
    try:
        out['policy_smb_rules'] = _parse_purepolicy_smb_rule_list_csv(
            run_ssh_command(array, user,
                "purepolicy smb rule list --csv",
                log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy smb rule list: {e}")
    try:
        out['policy_smb_list'] = _parse_purepolicy_smb_list_csv(
            run_ssh_command(array, user,
                "purepolicy smb list --csv",
                log_list=detailed_logs, nogui=nogui))
    except Exception as e:
        _errs.append(f"purepolicy smb list: {e}")
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

    # ── Step 1: Array-type detection (4 workers) ─────────────────────────
    detect_results = [None] * len(_arrays_in_order)
    def _detect_one(_idx_pair):
        _idx, (_name, _loc) = _idx_pair
        _p(f"Detecting array {_name} type...")
        _per_users = [('Array', auth_user_for_array(_name, config))]
        try:
            info = detect_array_type(_name, _per_users,
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
                            'pgroup_retentions': {}, 'connections': [],
                            'filesystems': [], 'directories': [],
                            'dir_snapshots': [], 'policy_locks': {},
                            'policy_snapshots': [], 'policy_rules': {},
                            'dir_exports': [], 'eradication': {},
                            'policy_nfs_rules': {}, 'policy_nfs_list': {},
                            'policy_smb_rules': {}, 'policy_smb_list': {}}
        if platform == 'FA':
            fa_targets.append((_name, info.get('user')
                                      or auth_user_for_array(_name, config)))

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


def _compute_pg_max_retention(schedule, retention):
    """Return (source_max_days, target_max_days) for a single protection group.

    Maps the 'snap' / 'replicate' rows in `purepgroup list --schedule` to
    the source (index 0) / target (index 1) rows in `purepgroup list
    --retention`:
      * Schedule Enabled == False              -> 0 days for that side.
      * Per Period == 0                        -> max = All For (in days).
      * Per Period >  0 and Days >  0          -> max = All For + Days.
      * Per Period >  0 and Days == 0/missing  -> max = 1000 days (forever sentinel).
    All For is parsed via parse_time_to_seconds and truncated to whole days.
    """
    sched_names   = list(schedule.get('Schedule') or [])
    sched_enabled = list(schedule.get('Enabled')  or [])

    def _is_enabled(label):
        for i, n in enumerate(sched_names):
            if (n or '').strip().lower() == label:
                if i < len(sched_enabled):
                    return (sched_enabled[i] or '').strip().lower() == 'true'
                return False
        return False

    af_list = list(retention.get('All For')    or [])
    pp_list = list(retention.get('Per Period') or [])
    dy_list = list(retention.get('Days')       or [])

    def _to_int(val):
        try:
            return int(str(val).strip())
        except Exception:
            return 0

    def _calc(idx):
        if idx >= len(af_list):
            return 0
        af_secs = parse_time_to_seconds(af_list[idx] or '')
        af_days = af_secs // 86400
        pp = _to_int(pp_list[idx]) if idx < len(pp_list) else 0
        dy = _to_int(dy_list[idx]) if idx < len(dy_list) else 0
        if pp == 0:
            return af_days
        if dy == 0:
            return 1000
        return af_days + dy

    src = _calc(0) if _is_enabled('snap') else 0
    if _is_enabled('replicate'):
        tgt = _calc(1) if len(af_list) > 1 else 0
    else:
        tgt = 0
    return src, tgt


def _pg_protection_flags(schedule, retention):
    """Return (snap_enabled, replicate_enabled, has_target) for one PG.

    Mirrors the schedule/retention parsing rules used by
    _compute_pg_max_retention but exposes the boolean flags directly so
    aggregate_fa_volume_rows can classify why a row's SLA cell is
    failing (schedule disabled vs missing target vs short retention).
    has_target is True when the retention CSV carries a second entry,
    which only appears when the PG is bound to a remote array.
    """
    sched_names   = list(schedule.get('Schedule') or [])
    sched_enabled = list(schedule.get('Enabled')  or [])
    def _is_enabled(label):
        for i, n in enumerate(sched_names):
            if (n or '').strip().lower() == label:
                if i < len(sched_enabled):
                    return (sched_enabled[i] or '').strip().lower() == 'true'
                return False
        return False
    af_list = list(retention.get('All For') or [])
    return (_is_enabled('snap'), _is_enabled('replicate'), len(af_list) > 1)


def aggregate_fa_volume_rows(per_array):
    """Build Table 1 rows from per-array protection data.

    Returns list of dicts:
        array, volume, in_pod, pod_direction, remote_pod,
        local_snaps, pod_snaps, replicated_snaps, replication_destinations,
        max_snap_retention_days, max_repl_retention_days
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

    # ── Array-wide safemode (purearray eradication-config). When
    # 'Manual Eradication' is 'all-disabled' on an array, any snapshot
    # that lives on that array is safemode-protected regardless of
    # pgroup or policy retention-lock state. The 'Enabled Delay' is
    # rendered as a second line under the Safemode cell when the
    # row's safemode is enabled.
    arr_force_sm = {}
    arr_sm_delay = {}
    for arr, data in per_array.items():
        erad = data.get('eradication') or {}
        me = (erad.get('Manual Eradication') or '').strip().lower()
        arr_force_sm[arr] = (me == 'all-disabled')
        arr_sm_delay[arr] = _fmt_eradication_delay(erad.get('Enabled Delay'))

    # ── Per-(array, pgroup) maximum snap and replication retention in days,
    # derived from the array's purepgroup --schedule + --retention output.
    # See _compute_pg_max_retention for the snap/replicate -> source/target
    # mapping rules. Pgroups missing from either output yield (0, 0).
    pg_max_retention = {}
    # Parallel map of per-PG (snap_enabled, replicate_enabled, has_target)
    # flags so the per-row SLA classification below can distinguish
    # "Schedule Not Enabled" from "No Target" from "Short Retention"
    # without re-parsing the schedule/retention dicts.
    pg_flags = {}
    for arr, data in per_array.items():
        sched = data.get('pgroup_schedules')  or {}
        rete  = data.get('pgroup_retentions') or {}
        for k in (set(sched.keys()) | set(rete.keys())):
            pg_max_retention[(arr, k)] = _compute_pg_max_retention(
                sched.get(k, {}), rete.get(k, {}))
            pg_flags[(arr, k)] = _pg_protection_flags(
                sched.get(k, {}), rete.get(k, {}))

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
                   'safemode_delay': '',
                   'max_snap_retention_days': 0,
                   'max_repl_retention_days': 0,
                   'local_status': 'no_pg', 'repl_status': 'no_pg',
                   'connected_hosts': []}

            if pod is None:
                row['local_snaps']      = local_snaps.get((arr, None, vol), 0)
                row['replicated_snaps'] = repl_snaps.get((arr, None, vol), 0)
                row['replication_destinations'] = sorted(
                    repl_dests.get((arr, None, vol), set()))
                pgs = pg_membership.get((arr, None, vol), set())
                row['protection_groups'] = sorted(pgs)
                _sm = any((arr, p) in pg_ratcheted for p in pgs)
                # Array-wide safemode forces the row when the source
                # array has Manual Eradication=all-disabled and the
                # volume has at least one snapshot on that array.
                if arr_force_sm.get(arr) and row['local_snaps'] > 0:
                    _sm = True
                row['safemode'] = _sm
                if _sm and arr_force_sm.get(arr):
                    row['safemode_delay'] = arr_sm_delay.get(arr, '')
                _src_max = 0
                _tgt_max = 0
                for _p in pgs:
                    _s, _t = pg_max_retention.get((arr, _p), (0, 0))
                    if _s > _src_max: _src_max = _s
                    if _t > _tgt_max: _tgt_max = _t
                row['max_snap_retention_days'] = _src_max
                row['max_repl_retention_days'] = _tgt_max
                # Per-side SLA classification. snap_enabled / replicate_enabled
                # / has_target are read from pg_flags (parallel to
                # pg_max_retention). Disabled-schedule PGs and replicate-no-
                # target PGs don't count toward the respective max value or
                # SLA — the row falls through to the matching status string
                # so the HTML renderer can show the reason.
                _loc_eligible    = any(pg_flags.get((arr, _p), (False, False, False))[0]
                                       for _p in pgs)
                _rep_any_enabled = any(pg_flags.get((arr, _p), (False, False, False))[1]
                                       for _p in pgs)
                _rep_eligible    = any(pg_flags.get((arr, _p), (False, False, False))[1]
                                       and pg_flags.get((arr, _p), (False, False, False))[2]
                                       for _p in pgs)
                if not pgs:
                    row['local_status'] = 'no_pg'
                    row['repl_status']  = 'no_pg'
                else:
                    row['local_status'] = 'protected' if _loc_eligible else 'schedule_disabled'
                    if _rep_eligible:
                        row['repl_status'] = 'protected'
                    elif _rep_any_enabled:
                        row['repl_status'] = 'no_target'
                    else:
                        row['repl_status'] = 'schedule_disabled'
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
                        # Record the replica array so the HTML renderer can
                        # resolve pgroup links that live only on the peer.
                        row['peer_array'] = peer_arr
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
                _sm = any((a, p) in pg_ratcheted
                          for a in _arrs_to_check for p in pgs)
                # Array-wide safemode: if either side's array forces
                # safemode and that side has at least one pod snapshot,
                # the row is protected. Prefer the source array's
                # delay; fall back to the peer's only when source isn't
                # forcing.
                _force_arr = ''
                if arr_force_sm.get(arr) and local_pod_snaps > 0:
                    _sm = True
                    _force_arr = arr
                if (stretch_info and stretch_info[0]
                        and arr_force_sm.get(stretch_info[0])
                        and peer_pod_snaps > 0):
                    _sm = True
                    if not _force_arr:
                        _force_arr = stretch_info[0]
                row['safemode'] = _sm
                if _sm and _force_arr:
                    row['safemode_delay'] = arr_sm_delay.get(_force_arr, '')
                # Maximum local-snap retention across all member pgroups,
                # considering profiles on both sides of the stretched pod
                # (pod-scoped pgroups can be defined on either side).
                _src_max = 0
                for _p in pgs:
                    for _a in _arrs_to_check:
                        _s, _t = pg_max_retention.get((_a, _p), (0, 0))
                        if _s > _src_max: _src_max = _s
                row['max_snap_retention_days'] = _src_max
                # Pod replication is driven by the pod's replica link, not
                # by a per-pgroup 'replicate' rule + remote target. When
                # the pod carries a replicating link, a single snap
                # schedule produces both local and replicated snapshots on
                # the peer with identical retention, so the replicated
                # retention mirrors the local retention. Without a link,
                # the pod is not replicating at all and the replicated
                # retention is zero regardless of any PG configuration.
                def _flags_any_side(_p, _arrs_to_check=_arrs_to_check):
                    for _a in _arrs_to_check:
                        f = pg_flags.get((_a, _p))
                        if f is not None:
                            return f
                    return (False, False, False)
                _loc_eligible = any(_flags_any_side(_p)[0] for _p in pgs)
                _pod_has_link = (arr, pod) in stretch
                row['max_repl_retention_days'] = (_src_max if _pod_has_link else 0)
                if not pgs:
                    row['local_status'] = 'no_pg'
                    row['repl_status']  = 'no_pg'
                else:
                    row['local_status'] = ('protected' if _loc_eligible
                                           else 'schedule_disabled')
                    if _loc_eligible and _pod_has_link:
                        row['repl_status'] = 'protected'
                    elif _loc_eligible:
                        # Snap schedule is enabled but the pod has no
                        # replica link, so nothing is being replicated.
                        row['repl_status'] = 'no_target'
                    else:
                        row['repl_status'] = 'schedule_disabled'
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


def aggregate_fa_filesystem_rows(per_array):
    """Build Table 2 rows from per-array filesystem inventory.
    
    Returns rows with snapshot policy details, retention calculations,
    and export information.
    """
    # Map (arr, fs_token) -> pod_or_None. The fs_token is the value
    # that appears in the `fs` portion of the directory's Name in
    # `puredir list`. With rsplit-on-':' parsing, that token already
    # carries the `pod::` prefix when the filesystem is pod-scoped, so
    # we register the filesystem under both its bare and pod-qualified
    # forms to make the lookup tolerant of either Pure output style.
    fs_pod_map = {}
    for arr, data in per_array.items():
        for fs in data.get('filesystems', []):
            pod = fs.get('pod')
            bare = fs.get('fs', '')
            full = fs.get('name', '')
            if bare:
                fs_pod_map.setdefault((arr, bare), pod)
            if full and full != bare:
                fs_pod_map.setdefault((arr, full), pod)

    # Pod stretch map (replicating links only): mirrors the structure
    # used by aggregate_fa_volume_rows so each pod-stretched directory
    # can be folded into a single row on the source side.
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

    def _resolve_pod(arr, fs_token):
        if '::' in fs_token:
            return fs_token.split('::', 1)[0]
        return fs_pod_map.get((arr, fs_token))

    # Array-wide safemode (purearray eradication-config). Directories
    # on an array with Manual Eradication=all-disabled get Safemode
    # forced when they have at least one snapshot on that side. The
    # Enabled Delay value is rendered alongside the cell.
    arr_force_sm = {}
    arr_sm_delay = {}
    for arr, data in per_array.items():
        erad = data.get('eradication') or {}
        me = (erad.get('Manual Eradication') or '').strip().lower()
        arr_force_sm[arr] = (me == 'all-disabled')
        arr_sm_delay[arr] = _fmt_eradication_delay(erad.get('Enabled Delay'))

    # Build policy objects per array
    policy_objects = {}  # (arr, policy_name) -> {enabled, retention_lock, deletion, rules}
    for arr, data in per_array.items():
        for pol in data.get('policy_snapshots', []):
            key = (arr, pol['name'])
            policy_objects[key] = {
                'name': pol['name'],
                'pod': pol.get('pod'),
                'policy': pol.get('policy'),
                'enabled': pol['enabled'],
                'retention_lock': '',
                'deletion': '',
                'rules': []
            }
        
        # Merge retention lock info
        policy_locks = data.get('policy_locks', {}) or {}
        for pol_name, lock_state in policy_locks.items():
            key = (arr, pol_name)
            if key in policy_objects:
                policy_objects[key]['retention_lock'] = lock_state
        
        # Merge rules
        policy_rules = data.get('policy_rules', {}) or {}
        for pol_name, rules in policy_rules.items():
            key = (arr, pol_name)
            if key in policy_objects:
                policy_objects[key]['rules'] = rules
    
    # Map directories to their snapshot policies via dir_snapshots. A
    # snapshot's Name is `[pod::]fs:directory.<suffix>` where the suffix
    # itself can contain dots, so the only reliable extraction is by
    # prefix-matching against the array's known directories. The
    # source-side dir_snapshots feed the local policy set; pod-stretched
    # directories additionally collect policies from the destination
    # array's dir_snapshots (the replicated copies under `peer_pod::`).
    dir_policies = {}  # (arr, fs_token, directory) -> [policy_names]
    for arr, data in per_array.items():
        snaps = data.get('dir_snapshots', []) or []
        for d in data.get('directories', []) or []:
            fs_token  = d.get('fs', '')
            directory = d.get('directory', '')
            if not fs_token or not directory:
                continue
            fs_bare = (fs_token.split('::', 1)[1]
                       if '::' in fs_token else fs_token)
            pod = _resolve_pod(arr, fs_token)
            prefix = (f'{pod}::{fs_bare}:{directory}.' if pod
                      else f'{fs_bare}:{directory}.')
            ordered = []
            seen = set()
            for snap in snaps:
                if (snap.get('name') or '').startswith(prefix):
                    pol = (snap.get('policy') or '').strip()
                    if pol and pol not in seen:
                        seen.add(pol)
                        ordered.append(pol)
            if ordered:
                dir_policies[(arr, fs_token, directory)] = ordered

    # Map directories to exports
    dir_exports = {}  # (arr, directory_name) -> [export_objects]
    for arr, data in per_array.items():
        for exp in data.get('dir_exports', []):
            directory = exp.get('directory', '')
            if not directory:
                continue
            key = (arr, directory)
            if key not in dir_exports:
                dir_exports[key] = []
            dir_exports[key].append(exp)
    
    # Drop set: dest-side directories of stretched pods. The matching
    # source-side row carries the directory once, with the Array Name
    # column rendered as "source -> dest".
    drop = set()
    for arr, data in per_array.items():
        for d in data.get('directories', []):
            fs_token  = d.get('fs', '')
            directory = d.get('directory', '')
            pod = _resolve_pod(arr, fs_token)
            if not pod:
                continue
            info = stretch.get((arr, pod))
            if info and info[2] == 'dest':
                drop.add((arr, fs_token, directory))

    rows = []
    for arr, data in per_array.items():
        for d in data.get('directories', []):
            fs_token  = d.get('fs', '')
            directory = d.get('directory', '')
            if (arr, fs_token, directory) in drop:
                continue
            pod = _resolve_pod(arr, fs_token)
            fs_bare = (fs_token.split('::', 1)[1]
                       if '::' in fs_token else fs_token)
            # Display name is "filesystem:directory" verbatim from
            # puredir; preserve any pod prefix it may carry.
            row = {
                'array': arr,
                'source_array': arr,
                'peer_array': '',
                'fs':        fs_bare,
                'fs_token':  fs_token,
                'directory': directory,
                'name':      d.get('name', f'{fs_token}:{directory}'),
                'pod_name':  pod or '',
                'in_pod':    bool(pod),
                'remote_pod': '',
                'replication_destinations': [],
                'local_snaps': 0,
                'replicated_pod_snaps': 0,
                'safemode': False,
                'safemode_delay': ''}

            stretch_info = stretch.get((arr, pod)) if pod else None
            if stretch_info:
                peer_arr, peer_pod, role = stretch_info
                if peer_arr:
                    row['replication_destinations'] = [peer_arr]
                    row['peer_array'] = peer_arr
                row['remote_pod'] = peer_pod
                # Source-side rows display the stretch direction in
                # the Array Name column; dest-side rows are already
                # filtered out by the drop set above.
                if role == 'source':
                    row['array'] = f'{arr} \u2192 {peer_arr}'

            # Snapshot policies for this directory. The source-array's
            # policies come from its own dir_snapshots; pod-stretched
            # directories also pull policies from the peer array's
            # replicated copies so the cell reflects every enabled
            # policy retaining a snapshot for the directory.
            dir_key = (arr, fs_token, directory)
            policies = list(dir_policies.get(dir_key, []))
            if stretch_info:
                _peer_arr, _peer_pod, _ = stretch_info
                _peer_key = (_peer_arr, f'{_peer_pod}::{fs_bare}', directory)
                for _pol in dir_policies.get(_peer_key, []):
                    if _pol not in policies:
                        policies.append(_pol)

            # policy_objects is keyed by (array, policy_name) where
            # policy_name is the verbatim Name column from
            # `purepolicy snapshot list`. The snapshot's `policy` field
            # in `puredir snapshot list` may carry just the bare policy
            # for non-pod policies; pod-scoped policies show up under
            # the peer array's policy_objects map, so try both arrays.
            def _resolve_policy_obj(pol_name):
                obj = policy_objects.get((arr, pol_name))
                if obj is None and stretch_info:
                    obj = policy_objects.get((stretch_info[0], pol_name))
                return obj

            # Max local retention: longest Keep For across the enabled
            # policies' rules. Max repl retention mirrors local, but is
            # only meaningful when the directory's pod has a replica
            # link (replication is pod-mediated).
            max_local_days = 0
            for pol_name in policies:
                pol_obj = _resolve_policy_obj(pol_name)
                if pol_obj and (pol_obj.get('enabled') or '').lower() == 'true':
                    for rule in pol_obj.get('rules', []):
                        days = _parse_retention_to_days(rule.get('keep_for', ''))
                        if days > max_local_days:
                            max_local_days = days
            max_repl_days = max_local_days if stretch_info else 0

            # Connected hosts: a directory may have multiple exports
            # (one NFS + one SMB, etc.). Preserve order of appearance.
            export_key = (arr, d.get('name', ''))
            exports = dir_exports.get(export_key, [])
            export_names = []
            export_objects = {}
            for e in exports:
                en = e.get('export_name') or e.get('policy') or ''
                if not en or en in export_objects:
                    continue
                export_names.append(en)
                export_objects[en] = e

            row.update({
                'snapshot_policies': policies,
                'policy_objects': {p: _resolve_policy_obj(p) for p in policies},
                'max_local_retention_days': max_local_days,
                'max_repl_retention_days': max_repl_days,
                'connected_hosts':  export_names,
                'export_objects':   export_objects,
            })
            
            # Safemode is Enabled if ANY snapshot associated with the
            # directory is produced by a policy whose Retention Lock is
            # 'ratcheted' in `purepolicy snapshot retention-lock list`.
            # Both the source-side and (for pod-stretched directories)
            # destination-side snapshots are considered, each compared
            # against their own array's policy_locks map.
            src_locks = data.get('policy_locks', {}) or {}
            safemode  = False

            # Count local snapshots by prefix match. Snapshot Name is
            # `[pod::]fs:dir.<suffix>`; the trailing dot anchors the
            # match so e.g. directory `db` doesn't pick up snapshots
            # of `db_logs`.
            if pod:
                prefix = f'{pod}::{fs_bare}:{directory}.'
            else:
                prefix = f'{fs_bare}:{directory}.'
            cnt = 0
            for snap in data.get('dir_snapshots', []):
                if (snap.get('name') or '').startswith(prefix):
                    cnt += 1
                    pol = (snap.get('policy') or '').strip()
                    if pol and src_locks.get(pol) == 'ratcheted':
                        safemode = True
            row['local_snaps'] = cnt

            # Replicated pod snapshots: by definition only pod-resident
            # directories replicate (via the pod replica link). When the
            # pod has a remote peer, the replica copies appear in the
            # destination array's `puredir snapshot list` under the peer
            # pod's namespace (`remote_pod::fs:dir.<suffix>`). Count
            # those on the destination array, and fold their policies
            # into the safemode check using the destination array's own
            # policy_locks map.
            if stretch_info:
                peer_arr, peer_pod, _role = stretch_info
                dest_data   = per_array.get(peer_arr) or {}
                dest_locks  = dest_data.get('policy_locks', {}) or {}
                dest_prefix = f'{peer_pod}::{fs_bare}:{directory}.'
                rcnt = 0
                for snap in dest_data.get('dir_snapshots', []):
                    if (snap.get('name') or '').startswith(dest_prefix):
                        rcnt += 1
                        pol = (snap.get('policy') or '').strip()
                        if pol and dest_locks.get(pol) == 'ratcheted':
                            safemode = True
                row['replicated_pod_snaps'] = rcnt

            # Array-wide safemode forces the row when either side's
            # array has Manual Eradication=all-disabled and has at
            # least one snapshot. Source-array forcing wins for the
            # delay display; the peer is consulted only as a fallback.
            _force_arr = ''
            if arr_force_sm.get(arr) and row['local_snaps'] > 0:
                safemode = True
                _force_arr = arr
            if (stretch_info and stretch_info[0]
                    and arr_force_sm.get(stretch_info[0])
                    and row['replicated_pod_snaps'] > 0):
                safemode = True
                if not _force_arr:
                    _force_arr = stretch_info[0]

            row['safemode'] = safemode
            if safemode and _force_arr:
                row['safemode_delay'] = arr_sm_delay.get(_force_arr, '')
            rows.append(row)

    rows.sort(key=lambda r: (r['array'].lower(),
                             (0 if r['in_pod'] else 1),
                             r['fs'].lower(),
                             r['directory'].lower()))
    return rows


def _load_recent_comments():
    """Return saved per-row comments from the most recent comments JSON
    file under reports/protection/. The file is hand-edited by the user
    (typically reports/protection/comments_active.json) and is keyed by
    source_array + displayed volume name so pod-stretched rows match
    across runs. Comments are inlined into the report at generation
    time only; the rendered HTML is read-only.
    Returns {} if no file exists or it cannot be parsed.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
    out_dir = os.path.join(script_dir, 'reports', 'protection')
    if not os.path.isdir(out_dir):
        return {}
    candidates = []
    for name in os.listdir(out_dir):
        if name.startswith('comments_') and name.endswith('.json'):
            full = os.path.join(out_dir, name)
            try:
                candidates.append((os.path.getmtime(full), full))
            except OSError:
                continue
    if not candidates:
        return {}
    candidates.sort()
    path = candidates[-1][1]
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    for entry in (data.get('comments') or []):
        arr = entry.get('array')
        vol = entry.get('volume')
        if arr and vol:
            out[(arr, vol)] = {
                'comment':      entry.get('comment', '') or '',
                'last_updated': entry.get('last_updated', '') or '',
            }
    return out


def build_protection_html(per_array, config):
    """Generate the Volume and Filesystem Protection HTML report.

    Three sections:
      1. FlashArray Volumes  (fully populated)
      2. FlashArray Filesystems  (Directory Name, Array Name,
         Replication Destination, Pod, Remote Pod, Local Snapshots
         populated; remaining columns pending the next iteration)
      3. FlashBlade Filesystems  (placeholder, spec pending)
    """
    import html as _html
    import time as _time
    import json as _json

    tz      = _time.tzname[_time.daylight]
    now_str = datetime.datetime.now().strftime("%A, %B %d, %Y at %I:%M:%S %p")
    rows    = aggregate_fa_volume_rows(per_array)
    saved_comments = _load_recent_comments()

    # Per-array error banner contents.
    err_lines = []
    for arr in sorted(per_array.keys()):
        info = per_array[arr]
        if info.get('error'):
            err_lines.append(f"{_html.escape(arr)}: {_html.escape(str(info['error']))}")

    # Retention SLA thresholds (whole days) for the per-volume Local
    # Snap Retention / Replicated Snap Retention columns. A 0 (or
    # missing) threshold disables the SLA check for that side and
    # renders both states as a neutral dash so the column still sorts
    # but never falsely flags rows red.
    _cfg = config or {}
    sla_snap_days = _parse_sla_days(_cfg.get('sla_retention_snap', 0), 0)
    sla_repl_days = _parse_sla_days(_cfg.get('sla_retention_repl', 0), 0)

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

    # Per-(array, snapshot_policy) profile for the Table 2 modal. Each
    # entry carries the policy's Enabled state, Retention Lock (from
    # `purepolicy snapshot retention-lock list`), and the list of rules
    # (every / at / keep_for) from `purepolicy snapshot rule list`. The
    # modal looks the entry up by the link's data-arr / data-pol attrs.
    policy_profiles = {}
    for arr, info in per_array.items():
        snaps = info.get('policy_snapshots') or []
        if not snaps:
            continue
        locks = info.get('policy_locks') or {}
        rules = info.get('policy_rules') or {}
        policy_profiles[arr] = {}
        for pol in snaps:
            nm = pol.get('name', '')
            if not nm:
                continue
            policy_profiles[arr][nm] = {
                'enabled':        pol.get('enabled', ''),
                'retention_lock': locks.get(nm, ''),
                'rules':          rules.get(nm, [])}
    policy_profiles_json = _json.dumps(policy_profiles)

    # Per-(array, export_name) profile for the Connected Hosts modal
    # in Table 2. Each entry preserves the verbatim columns from
    # `puredir export list` (Server, Path, Policy, Type, Enabled),
    # plus per-type rule/policy detail pulled from the matching
    # `purepolicy <nfs|smb> rule list` and `purepolicy <nfs|smb> list`
    # outputs. The 'kind' field tells the JS which column set to
    # render in the secondary tables.
    export_profiles = {}
    for arr, info in per_array.items():
        nfs_rules = info.get('policy_nfs_rules') or {}
        nfs_list  = info.get('policy_nfs_list')  or {}
        smb_rules = info.get('policy_smb_rules') or {}
        smb_list  = info.get('policy_smb_list')  or {}
        for exp in (info.get('dir_exports') or []):
            en = exp.get('export_name') or exp.get('policy') or ''
            if not en:
                continue
            etype  = (exp.get('type')   or '').lower()
            polnm  = exp.get('policy', '')
            kind   = ('smb' if 'smb' in etype
                      else ('nfs' if 'nfs' in etype else ''))
            if kind == 'smb':
                rules_block  = smb_rules.get(polnm, [])
                policy_block = smb_list.get(polnm, {})
            elif kind == 'nfs':
                rules_block  = nfs_rules.get(polnm, [])
                policy_block = nfs_list.get(polnm, {})
            else:
                rules_block, policy_block = [], {}
            export_profiles.setdefault(arr, {})[en] = {
                'directory':    exp.get('directory', ''),
                'server':       exp.get('server', ''),
                'path':         exp.get('path', ''),
                'policy':       polnm,
                'type':         exp.get('type', ''),
                'enabled':      exp.get('enabled', ''),
                'kind':         kind,
                'rules':        rules_block,
                'policy_attrs': policy_block}
    export_profiles_json = _json.dumps(export_profiles)

    # Per-(array, directory) export listing for the Directory-name
    # popup. Each entry is the list of `puredir export list` rows that
    # match the directory's verbatim name (Filesystem:Directory or
    # Pod::Filesystem:Directory). Multiple exports per directory are
    # preserved in CSV order so the popup can list them all.
    dir_export_profiles = {}
    for arr, info in per_array.items():
        for exp in (info.get('dir_exports') or []):
            d_nm = exp.get('directory', '')
            if not d_nm:
                continue
            dir_export_profiles.setdefault(arr, {}).setdefault(
                d_nm, []).append({
                    'server':      exp.get('server', ''),
                    'export_name': exp.get('export_name', ''),
                    'path':        exp.get('path', ''),
                    'type':        exp.get('type', ''),
                    'enabled':     exp.get('enabled', '')})
    dir_export_profiles_json = _json.dumps(dir_export_profiles)

    # Cells for the protection-related columns are shaded based on
    # whether they carry meaningful content. Numeric snapshot cells use
    # the "ok" colour when the count is > 0. Shared by both Table 1 and
    # Table 2 row builders.
    _OK   = 'background:#d4edda;'  # light green
    _BAD  = 'background:#f8d7da;'  # light red
    _DASH = '<span style="color:#888;">&mdash;</span>'

    # ── Build Table 1 rows ────────────────────────────────────────────────
    tr_html = ""
    if not rows:
        tr_html = ('<tr><td colspan="16" style="text-align:center;color:#888;">'
                   'No FlashArray volumes discovered.</td></tr>')
    else:
        for r in rows:
            dests_ok = bool(r['replication_destinations'])
            dests = (', '.join(_html.escape(d) for d in r['replication_destinations'])
                     if dests_ok else _DASH)
            pgs_list = r.get('protection_groups', [])
            pgs_ok = bool(pgs_list)
            # Each pgroup name renders as a clickable link. For a plain
            # volume the link is bound to source_array; for a pod-stretched
            # volume the pgroup may have been defined only on the replica
            # array, so resolve per pgroup: prefer source if it owns the
            # profile, else fall back to peer_array. The modal popup then
            # pulls schedule and retention from PG_PROFILES[arr][pgname].
            _src  = r.get('source_array', r['array'])
            _peer = r.get('peer_array')
            def _pg_owner(pname, _src=_src, _peer=_peer):
                if pname in (pg_profiles.get(_src) or {}):
                    return _src
                if _peer and pname in (pg_profiles.get(_peer) or {}):
                    return _peer
                return _src
            pgs = (', '.join(
                f'<a href="#" class="pg-link" '
                f'data-arr="{_html.escape(_pg_owner(p), quote=True)}" '
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
            # When the row's safemode is array-wide (purearray
            # eradication-config), append the Enabled Delay value as a
            # second line (e.g., "2 Day Protection") below the status.
            sm_delay = r.get('safemode_delay', '') if sm_on else ''
            sm_inner = sm_text
            if sm_delay:
                sm_inner += (
                    f'<br><span style="font-weight:normal;font-size:smaller;">'
                    f'{_html.escape(sm_delay)}</span>')
            hosts_list = r.get('connected_hosts', [])
            hosts_ok = bool(hosts_list)
            hosts = (', '.join(_html.escape(h) for h in hosts_list)
                     if hosts_ok else _DASH)
            local_n = int(r['local_snaps'])
            pod_n   = int(r['pod_snaps'])
            rep_n   = int(r['replicated_snaps'])
            # Max retention columns: green when > 0 days, red dash when
            # the volume's pgroup membership grants no retention on that
            # side (no pgroups, or schedule disabled). 1000 is the
            # forever sentinel from _compute_pg_max_retention.
            snap_ret_n = int(r.get('max_snap_retention_days', 0))
            repl_ret_n = int(r.get('max_repl_retention_days', 0))
            snap_ret_disp = (str(snap_ret_n) if snap_ret_n > 0 else _DASH)
            repl_ret_disp = (str(repl_ret_n) if repl_ret_n > 0 else _DASH)
            # Local/Repl Snap Retention vs SLA columns: green check
            # when the row passes (has an enabled schedule, has a
            # target on the replication side, and the max retention
            # meets or exceeds the user-defined threshold); red with a
            # short reason otherwise. A threshold of 0 disables the
            # check and renders a neutral dash so the column still
            # sorts but never falsely flags rows.
            _CHECK   = '\u2713'
            _GREEN_S = (_OK + 'text-align:center;font-weight:bold;font-size:13pt;')
            _RED_S   = (_BAD + 'text-align:center;font-weight:bold;font-size:9pt;')
            def _sla_cell(status, ret_n, sla_days):
                if sla_days <= 0:
                    return ('<td style="text-align:center;">'
                            f'{_DASH}</td>')
                if status == 'protected' and ret_n >= sla_days:
                    return f'<td style="{_GREEN_S}">{_CHECK}</td>'
                if status == 'protected':
                    reason = 'Short Retention'
                elif status == 'no_target':
                    reason = 'No Target'
                else:
                    reason = 'Schedule Not Enabled'
                return f'<td style="{_RED_S}">{reason}</td>'
            snap_sla_cell = _sla_cell(r.get('local_status', 'no_pg'),
                                       snap_ret_n, sla_snap_days)
            repl_sla_cell = _sla_cell(r.get('repl_status',  'no_pg'),
                                       repl_ret_n, sla_repl_days)
            # Comments cell: pre-populated from the most recent
            # comments_*.json keyed by (source_array, displayed volume)
            # at report-generation time. The cell is read-only in the
            # browser; to update a comment, edit comments_active.json
            # under reports/protection/ and re-run the report.
            _src_arr = r.get('source_array', r['array'])
            _saved   = saved_comments.get((_src_arr, vol_disp), {})
            _ctxt    = _saved.get('comment', '') or ''
            _cupd    = _saved.get('last_updated', '') or ''
            tr_html += (
                '<tr>'
                f'<td>{_html.escape(vol_disp)}</td>'
                f'<td>{_html.escape(r["array"])}</td>'
                f'<td style="{_OK if dests_ok else _BAD}">{dests}</td>'
                f'<td style="{_OK if pod_ok else _BAD}text-align:center;">{pod_cell}</td>'
                f'<td style="{_OK if remote_ok else _BAD}text-align:center;">{remote_pod}</td>'
                f'<td style="{_OK if local_n > 0 else _BAD}text-align:right;">{local_n}</td>'
                f'<td style="{_OK if pod_n > 0 else _BAD}text-align:right;">{pod_n}</td>'
                f'<td style="{_OK if rep_n > 0 else _BAD}text-align:right;">{rep_n}</td>'
                f'<td style="{sm_style}">{sm_inner}</td>'
                f'<td style="{_OK if pgs_ok else _BAD}">{pgs}</td>'
                f'<td style="{_OK if snap_ret_n > 0 else _BAD}text-align:right;">{snap_ret_disp}</td>'
                f'<td style="{_OK if repl_ret_n > 0 else _BAD}text-align:right;">{repl_ret_disp}</td>'
                f'{snap_sla_cell}'
                f'{repl_sla_cell}'
                f'<td style="{_OK if hosts_ok else _BAD}">{hosts}</td>'
                f'<td class="comment-cell">'
                f'<div class="comment-text">'
                f'{_html.escape(_ctxt) if _ctxt else "&mdash;"}'
                f'</div>'
                f'<div class="comment-meta">'
                f'<span class="comment-meta-lbl">Last updated:</span> '
                f'<span class="comment-updated">'
                f'{_html.escape(_cupd) if _cupd else "&mdash;"}'
                f'</span>'
                f'</div>'
                f'</td>'
                '</tr>\n')

    # ── Build Table 2 rows (FlashArray Filesystems) ─────────────────────
    # Currently the first eight columns carry real data (Directory Name,
    # Array Name, Replication Destination, Pod, Remote Pod, Local
    # Snapshots, Replicated Pod Snapshots, Safemode). The remaining
    # columns are rendered as neutral em-dashes pending the next
    # iteration of the spec.
    rows_t2 = aggregate_fa_filesystem_rows(per_array)
    tr_html_t2 = ""
    if not rows_t2:
        tr_html_t2 = ('<tr><td colspan="15" style="text-align:center;color:#888;">'
                      'No FlashArray filesystem directories discovered.</td></tr>')
    else:
        for r in rows_t2:
            dests_ok = bool(r['replication_destinations'])
            dests = (', '.join(_html.escape(d) for d in r['replication_destinations'])
                     if dests_ok else _DASH)
            pod_ok = bool(r['in_pod'] and r.get('pod_name'))
            pod_cell = _html.escape(r['pod_name']) if pod_ok else _DASH
            remote_ok = bool(r['remote_pod'])
            remote_pod = _html.escape(r['remote_pod']) if remote_ok else _DASH
            dir_disp = (r.get('name')
                        or f'{r["fs_token"]}:{r["directory"]}')
            # Directory cell is a link that opens the puredir-export
            # popup for that directory. The lookup is bound to the
            # source array (peer-array exports for stretched pods
            # are not surfaced here \u2014 they're collapsed into the
            # source row).
            _src_arr_dir = r.get('source_array') or r['array']
            dir_cell_inner = (
                f'<a href="#" class="pg-link" '
                f'data-arr="{_html.escape(_src_arr_dir, quote=True)}" '
                f'data-dir="{_html.escape(dir_disp, quote=True)}" '
                f'onclick="showDir(this);return false;">'
                f'{_html.escape(dir_disp)}</a>')
            local_n = int(r['local_snaps'])
            # Replicated pod snapshots: only pod-resident directories
            # whose pod has a remote peer are eligible. The cell is
            # painted red whenever the count is missing/zero (the
            # non-eligible case still renders an em-dash, but in red
            # so the row visibly flags as unprotected).
            rep_eligible = bool(r['in_pod'] and r['remote_pod']
                                and r['replication_destinations'])
            rep_n = int(r.get('replicated_pod_snaps', 0) or 0)
            if rep_n > 0:
                rep_cell = f'<td style="{_OK}text-align:right;">{rep_n}</td>'
            elif rep_eligible:
                rep_cell = f'<td style="{_BAD}text-align:right;">{rep_n}</td>'
            else:
                rep_cell = f'<td style="{_BAD}text-align:right;">{_DASH}</td>'
            sm_on = bool(r.get('safemode'))
            sm_text = 'Enabled' if sm_on else 'Disabled'
            sm_style = ((_OK if sm_on else _BAD)
                        + 'text-align:center;font-weight:bold;')
            # Optional second line for array-wide safemode delay
            # ("2 Day Protection" etc.); empty when safemode is off or
            # the array's eradication-config does not force it.
            sm_delay = r.get('safemode_delay', '') if sm_on else ''
            sm_inner = sm_text
            if sm_delay:
                sm_inner += (
                    f'<br><span style="font-weight:normal;font-size:smaller;">'
                    f'{_html.escape(sm_delay)}</span>')
            # Snapshot policies cell: each name clickable; resolves to
            # source array first, peer array as fallback (pod-scoped
            # policies may exist only on the peer side).
            _src_arr  = r.get('source_array') or r['array']
            _peer_arr = r.get('peer_array') or ''
            def _pol_owner(pname, _s=_src_arr, _p=_peer_arr):
                if pname in (policy_profiles.get(_s) or {}):
                    return _s
                if _p and pname in (policy_profiles.get(_p) or {}):
                    return _p
                return _s
            pols_list = r.get('snapshot_policies') or []
            pols_ok = bool(pols_list)
            pols_cell = (', '.join(
                f'<a href="#" class="pg-link" '
                f'data-arr="{_html.escape(_pol_owner(p), quote=True)}" '
                f'data-pol="{_html.escape(p, quote=True)}" '
                f'onclick="showPolicy(this);return false;">{_html.escape(p)}</a>'
                for p in pols_list) if pols_ok else _DASH)
            # Connected hosts cell: each export name clickable, bound
            # to the source array's export_profiles entry.
            exp_list = r.get('connected_hosts') or []
            exp_ok = bool(exp_list)
            exp_cell = (', '.join(
                f'<a href="#" class="pg-link" '
                f'data-arr="{_html.escape(_src_arr, quote=True)}" '
                f'data-exp="{_html.escape(e, quote=True)}" '
                f'onclick="showExport(this);return false;">{_html.escape(e)}</a>'
                for e in exp_list) if exp_ok else _DASH)
            # Max Local / Repl Snap Retention (Days). Values are
            # decimal days derived from the policy rule's millisecond
            # Keep For; render with `{:g}` after rounding so whole
            # values print as integers ('7') and fractional values
            # keep their precision ('2.5').
            loc_ret_n  = float(r.get('max_local_retention_days', 0) or 0)
            repl_ret_n = float(r.get('max_repl_retention_days', 0) or 0)
            loc_ret_disp  = (f'{round(loc_ret_n,  2):g}'
                             if loc_ret_n  > 0 else _DASH)
            repl_ret_disp = (f'{round(repl_ret_n, 2):g}'
                             if repl_ret_n > 0 else _DASH)
            # SLA comparison cells. A threshold of 0 disables the
            # check (neutral dash); otherwise green check when the
            # row's retention meets the threshold and the row is
            # eligible (policies present; pod replica link for the
            # repl side). Red with a short reason on failure.
            _CHECK_T2 = '\u2713'
            _GREEN_T2 = (_OK + 'text-align:center;font-weight:bold;'
                         'font-size:13pt;')
            _RED_T2   = (_BAD + 'text-align:center;font-weight:bold;'
                         'font-size:9pt;')
            def _t2_local_sla():
                if sla_snap_days <= 0:
                    return f'<td style="text-align:center;">{_DASH}</td>'
                if not pols_list:
                    return f'<td style="{_RED_T2}">No Snapshot Policy</td>'
                if loc_ret_n >= sla_snap_days:
                    return f'<td style="{_GREEN_T2}">{_CHECK_T2}</td>'
                return f'<td style="{_RED_T2}">Short Retention</td>'
            def _t2_repl_sla():
                if sla_repl_days <= 0:
                    return f'<td style="text-align:center;">{_DASH}</td>'
                if not rep_eligible:
                    return f'<td style="{_RED_T2}">No Replica Link</td>'
                if not pols_list:
                    return f'<td style="{_RED_T2}">No Snapshot Policy</td>'
                if repl_ret_n >= sla_repl_days:
                    return f'<td style="{_GREEN_T2}">{_CHECK_T2}</td>'
                return f'<td style="{_RED_T2}">Short Retention</td>'
            tr_html_t2 += (
                '<tr>'
                f'<td>{dir_cell_inner}</td>'
                f'<td>{_html.escape(r["array"])}</td>'
                f'<td style="{_OK if dests_ok else _BAD}">{dests}</td>'
                f'<td style="{_OK if pod_ok else _BAD}text-align:center;">{pod_cell}</td>'
                f'<td style="{_OK if remote_ok else _BAD}text-align:center;">{remote_pod}</td>'
                f'<td style="{_OK if local_n > 0 else _BAD}text-align:right;">{local_n}</td>'
                f'{rep_cell}'
                f'<td style="{sm_style}">{sm_inner}</td>'
                f'<td style="{_OK if pols_ok else _BAD}">{pols_cell}</td>'
                f'<td style="{_OK if loc_ret_n > 0 else _BAD}text-align:right;">{loc_ret_disp}</td>'
                f'<td style="{_OK if repl_ret_n > 0 else _BAD}text-align:right;">{repl_ret_disp}</td>'
                f'{_t2_local_sla()}'
                f'{_t2_repl_sla()}'
                f'<td style="{_OK if exp_ok else _BAD}">{exp_cell}</td>'
                f'<td>{_DASH}</td>'
                '</tr>\n')

    err_banner = ''
    if err_lines:
        err_banner = ('<div class="err-banner"><strong>Collection errors:</strong><br>'
                      + '<br>'.join(err_lines) + '</div>')

    placeholder = ('<p style="color:#666;font-style:italic;">'
                   'Specification pending &mdash; coming in next update.</p>')

    # Static info banner above Table 1 reminding the reader that the
    # Comments column is read-only and how to update it. Only rendered
    # when there's at least one volume row to attach comments to.
    if rows:
        comments_toolbar = (
            '<div class="comments-toolbar">'
            '<span class="comments-toolbar-hint">Comments are read-only '
            'in this report. To edit them, update '
            '<code>reports/protection/comments_active.json</code> and '
            're-run the report.</span>'
            '</div>')
    else:
        comments_toolbar = ''

    # Modal markup + JS for the detail popups. Three modals coexist on
    # the page: pg-modal (Table 1 protection groups), policy-modal
    # (Table 2 snapshot policies), and export-modal (Table 2 connected
    # hosts). Each is rendered only when at least one array has data
    # for that modal so empty datasets don't ship dead JS. Shared
    # helpers (escHtml, modal close, Escape handler) are emitted once
    # whenever any of the three modals is present.
    _has_pg     = bool(pg_profiles)
    _has_policy = bool(policy_profiles)
    _has_export = bool(export_profiles)
    _has_dir    = bool(dir_export_profiles)
    modal_block_parts = []
    if _has_pg:
        modal_block_parts.append(
            '<div id="pg-modal" class="modal" '
            'onclick="closeModalIfBg(event,\'pg-modal\')">'
            '<div class="modal-content">'
            '<span class="modal-close" onclick="closeModalById(\'pg-modal\')">&times;</span>'
            '<h3 id="pg-title"></h3>'
            '<p class="meta" id="pg-array"></p>'
            '<h4>Schedule</h4><div id="pg-schedule"></div>'
            '<h4>Retention</h4><div id="pg-retention"></div>'
            '</div></div>')
    if _has_policy:
        modal_block_parts.append(
            '<div id="policy-modal" class="modal" '
            'onclick="closeModalIfBg(event,\'policy-modal\')">'
            '<div class="modal-content">'
            '<span class="modal-close" onclick="closeModalById(\'policy-modal\')">&times;</span>'
            '<h3 id="policy-title"></h3>'
            '<p class="meta" id="policy-meta"></p>'
            '<h4>Rules</h4><div id="policy-rules"></div>'
            '</div></div>')
    if _has_export:
        modal_block_parts.append(
            '<div id="export-modal" class="modal" '
            'onclick="closeModalIfBg(event,\'export-modal\')">'
            '<div class="modal-content">'
            '<span class="modal-close" onclick="closeModalById(\'export-modal\')">&times;</span>'
            '<h3 id="export-title"></h3>'
            '<p class="meta" id="export-meta"></p>'
            '<div id="export-detail"></div>'
            '<h4 id="export-rules-h" style="display:none;">Export Rules</h4>'
            '<div id="export-rules"></div>'
            '<h4 id="export-policy-h" style="display:none;">Policy Attributes</h4>'
            '<div id="export-policy"></div>'
            '</div></div>')
    if _has_dir:
        modal_block_parts.append(
            '<div id="dir-modal" class="modal" '
            'onclick="closeModalIfBg(event,\'dir-modal\')">'
            '<div class="modal-content">'
            '<span class="modal-close" onclick="closeModalById(\'dir-modal\')">&times;</span>'
            '<h3 id="dir-title"></h3>'
            '<p class="meta" id="dir-meta"></p>'
            '<div id="dir-detail"></div>'
            '</div></div>')
    modal_block = '\n'.join(modal_block_parts)

    if _has_pg or _has_policy or _has_export or _has_dir:
        script_parts = ['<script>\n']
        # Shared payload variables — emitted unconditionally for the
        # modals that exist so the JS can reference them as constants.
        if _has_pg:
            script_parts.append('const PG_PROFILES = '
                                + profiles_json + ';\n')
        if _has_policy:
            script_parts.append('const POLICY_PROFILES = '
                                + policy_profiles_json + ';\n')
        if _has_export:
            script_parts.append('const EXPORT_PROFILES = '
                                + export_profiles_json + ';\n')
        if _has_dir:
            script_parts.append('const DIR_EXPORT_PROFILES = '
                                + dir_export_profiles_json + ';\n')
        # Shared helpers: HTML escape, modal open/close, Escape key.
        script_parts.append(
            'function escHtml(s){'
            'return String(s).replace(/[&<>\"\\\']/g,'
            'ch=>({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","\\\'":"&#39;"})[ch]);'
            '}\n'
            'function closeModalById(id){const m=document.getElementById(id);'
            'if(m)m.style.display="none";}\n'
            'function closeAllModals(){'
            '["pg-modal","policy-modal","export-modal","dir-modal"]'
            '.forEach(closeModalById);}\n'
            'function closeModalIfBg(e,id){if(e.target.id===id)closeModalById(id);}\n'
            'document.addEventListener("keydown",function(e){'
            'if(e.key==="Escape")closeAllModals();});\n')
        if _has_pg:
            # PG-modal-specific formatters and renderer (verbatim from
            # the previous single-modal build, unchanged).
            script_parts.append(
                'function fmtSeconds(s){'
                'if(!/^[0-9]+$/.test(String(s)))return s;'
                'let n=parseInt(s,10);if(n===0)return "0 Seconds";'
                'const d=Math.floor(n/86400);n-=d*86400;'
                'const h=Math.floor(n/3600);n-=h*3600;'
                'const m=Math.floor(n/60);n-=m*60;'
                'const p=[];if(d)p.push(d+" Days");if(h)p.push(h+" Hours");'
                'if(m)p.push(m+" Minutes");if(n)p.push(n+" Seconds");'
                'return p.join(" ");}\n'
                'function fmtMsAsSeconds(s){'
                'if(!/^[0-9]+$/.test(String(s)))return fmtSeconds(s);'
                'return fmtSeconds(String(Math.floor(parseInt(s,10)/1000)));}\n'
                'function fmtClock(s){'
                'if(!/^[0-9]+$/.test(String(s)))return s;'
                'const t=parseInt(s,10);'
                'const h24=Math.floor(t/3600)%24,mm=Math.floor((t%3600)/60);'
                'const ap=h24<12?"AM":"PM";let h=h24%12;if(h===0)h=12;'
                'return h+":"+(mm<10?"0"+mm:mm)+" "+ap;}\n'
                'function fmtBlackout(s){'
                'if(s===""||s==null)return s;'
                'const m=String(s).match(/^([0-9]+)-([0-9]+)$/);'
                'if(!m)return s;'
                'return fmtClock(m[1])+"-"+fmtClock(m[2]);}\n'
                'const PG_FMT={'
                '"Per Period":v=>String(v),'
                '"Days":v=>String(v),'
                '"Period Length":v=>fmtMsAsSeconds(v),'
                '"Blackout":v=>fmtBlackout(v)};\n'
                'const PG_DISPLAY={"All For":"Retained for",'
                '"Per Period":"Additional Snaps Retained Per",'
                '"Days":"For an Additional # of Days",'
                '"Frequency":"Snapshot/Replication Frequency"};\n'
                'function renderTable(o){'
                'if(!o)return \'<p style="color:#888;">No data.</p>\';'
                'const cols=Object.keys(o);'
                'if(cols.length===0)return \'<p style="color:#888;">No data.</p>\';'
                'const n=Math.max(0,...cols.map(c=>(o[c]||[]).length));'
                'if(n===0)return \'<p style="color:#888;">No data.</p>\';'
                'let h=\'<table class="pg-detail"><thead><tr>\';'
                'cols.forEach(c=>{h+=\'<th>\'+escHtml(PG_DISPLAY[c]||c)+\'</th>\';});'
                'h+=\'</tr></thead><tbody>\';'
                'for(let i=0;i<n;i++){'
                'h+=\'<tr>\';'
                'cols.forEach(c=>{const v=(o[c]||[])[i];'
                'const f=PG_FMT[c]||fmtSeconds;'
                'const dv=(v===""||v==null)?null:f(v);'
                'h+=\'<td>\'+(dv!==null&&dv!==""?escHtml(dv):\'<span style="color:#888;">&mdash;</span>\')+\'</td>\';});'
                'h+=\'</tr>\';}'
                'return h+\'</tbody></table>\';}\n'
                'function showPg(el){'
                'const arr=el.getAttribute("data-arr"),pg=el.getAttribute("data-pg");'
                'const data=(PG_PROFILES[arr]||{})[pg]||null;'
                'document.getElementById("pg-title").textContent='
                '"Protection Group Details - "+pg;'
                'document.getElementById("pg-array").textContent="Source array: "+arr;'
                'document.getElementById("pg-schedule").innerHTML=renderTable(data&&data.schedule);'
                'document.getElementById("pg-retention").innerHTML=renderTable(data&&data.retention);'
                'document.getElementById("pg-modal").style.display="flex";}\n')
        if _has_policy:
            # Snapshot Policy modal: lists rules (Every / At / Keep
            # For / Suffix) and surfaces Enabled + Retention Lock in
            # a one-line meta. Every / At arrive from Purity as
            # milliseconds (At is ms since the last event) and
            # render as d/h/m/s; Keep For renders as decimal days.
            # An empty rules array renders a "No rules" placeholder
            # so the modal opens cleanly for sparse data.
            script_parts.append(
                'function fmtMsAsDhms(s){'
                'if(!/^[0-9]+$/.test(String(s)))return s;'
                'let n=Math.floor(parseInt(s,10)/1000);'
                'if(n===0)return "0 Seconds";'
                'const d=Math.floor(n/86400);n-=d*86400;'
                'const h=Math.floor(n/3600);n-=h*3600;'
                'const m=Math.floor(n/60);n-=m*60;'
                'const p=[];if(d)p.push(d+" Days");if(h)p.push(h+" Hours");'
                'if(m)p.push(m+" Minutes");if(n)p.push(n+" Seconds");'
                'return p.join(" ");}\n'
                'function fmtMsAsDays(s){'
                'if(!/^[0-9]+$/.test(String(s)))return s;'
                'const v=parseInt(s,10)/86400000;'
                'if(v===0)return "0 Days";'
                'const r=Math.round(v*100)/100;'
                'return r+" Days";}\n'
                'function showPolicy(el){'
                'const arr=el.getAttribute("data-arr"),pol=el.getAttribute("data-pol");'
                'const data=(POLICY_PROFILES[arr]||{})[pol]||null;'
                'document.getElementById("policy-title").textContent='
                '"Snapshot Policy Details - "+pol;'
                'const enabled=data?data.enabled:"";'
                'const lock=data?data.retention_lock:"";'
                'const meta="Array: "+arr+"  -  Enabled: "+(enabled||"unknown")'
                '+"  -  Retention Lock: "+(lock||"none");'
                'document.getElementById("policy-meta").textContent=meta;'
                'const rules=(data&&data.rules)||[];'
                'let h;'
                'if(!rules.length){h=\'<p style="color:#888;">No rules.</p>\';}'
                'else{h=\'<table class="pg-detail"><thead><tr>'
                '<th>Every</th><th>At</th><th>Keep For</th><th>Suffix</th>'
                '</tr></thead><tbody>\';'
                'rules.forEach(r=>{h+=\'<tr>\''
                '+\'<td>\'+escHtml(fmtMsAsDhms(r.every||""))+\'</td>\''
                '+\'<td>\'+escHtml(fmtMsAsDhms(r.at||""))+\'</td>\''
                '+\'<td>\'+escHtml(fmtMsAsDays(r.keep_for||""))+\'</td>\''
                '+\'<td>\'+escHtml(r.suffix||"")+\'</td></tr>\';});'
                'h+=\'</tbody></table>\';}'
                'document.getElementById("policy-rules").innerHTML=h;'
                'document.getElementById("policy-modal").style.display="flex";}\n')
        if _has_export:
            # Export modal: three stacked tables.
            #   1) verbatim columns from `puredir export list`
            #      (directory / server / path / policy / type /
            #      enabled). The CSV's Enabled column is empty when
            #      the export is enabled and reads "Policy Disabled"
            #      when it is not, so the renderer maps an empty
            #      string to the literal "Enabled".
            #   2) per-rule detail from `purepolicy <kind> rule
            #      list`, selected by the export's type (smb vs
            #      nfs). Suppressed when no rules are attached.
            #   3) policy-level attributes from `purepolicy <kind>
            #      list`. Suppressed when no attributes are present.
            # NFS_RULE_COLS / SMB_RULE_COLS hold paired [key, label]
            # tuples so the table headers stay readable while the
            # underlying export_profile keys remain snake_cased.
            script_parts.append(
                'const NFS_RULE_COLS=['
                '["client","Client"],'
                '["access","Access"],'
                '["permission","Permission"],'
                '["anonuid","Anonuid"],'
                '["anongid","Anongid"],'
                '["version","Version"],'
                '["security","Security"]];\n'
                'const SMB_RULE_COLS=['
                '["client","Client"],'
                '["anonymous_access_allowed","Anonymous Access Allowed"],'
                '["smb_encryption_required","SMB Encryption Required"]];\n'
                'const NFS_POLICY_COLS=['
                '["user_mapping_enabled","User Mapping Enabled"]];\n'
                'const SMB_POLICY_COLS=['
                '["access_based_enumeration_enabled","Access Based Enumeration Enabled"],'
                '["continuous_availability_enabled","Continuous Availability Enabled"]];\n'
                'function renderExportRules(rules,cols){'
                'if(!rules||!rules.length)return "";'
                'let h=\'<table class="pg-detail"><thead><tr>\';'
                'cols.forEach(c=>{h+=\'<th>\'+escHtml(c[1])+\'</th>\';});'
                'h+=\'</tr></thead><tbody>\';'
                'rules.forEach(r=>{h+=\'<tr>\';'
                'cols.forEach(c=>{h+=\'<td>\'+escHtml(r[c[0]]||"")+\'</td>\';});'
                'h+=\'</tr>\';});'
                'return h+\'</tbody></table>\';}\n'
                'function renderExportPolicy(attrs,cols){'
                'if(!attrs||!cols.length)return "";'
                'let any=false;cols.forEach(c=>{if(attrs[c[0]])any=true;});'
                'if(!any)return "";'
                'let h=\'<table class="pg-detail"><tbody>\';'
                'cols.forEach(c=>{h+=\'<tr><th>\'+escHtml(c[1])+\'</th><td>\''
                '+escHtml(attrs[c[0]]||"")+\'</td></tr>\';});'
                'return h+\'</tbody></table>\';}\n'
                'function showExport(el){'
                'const arr=el.getAttribute("data-arr"),en=el.getAttribute("data-exp");'
                'const data=(EXPORT_PROFILES[arr]||{})[en]||null;'
                'document.getElementById("export-title").textContent='
                '"Export Details - "+en;'
                'document.getElementById("export-meta").textContent="Source array: "+arr;'
                'let h;'
                'if(!data){h=\'<p style="color:#888;">No data.</p>\';}'
                'else{const enDisp=(data.enabled&&data.enabled.length)?data.enabled:"Enabled";'
                'h=\'<table class="pg-detail"><tbody>\''
                '+\'<tr><th>Directory</th><td>\'+escHtml(data.directory||"")+\'</td></tr>\''
                '+\'<tr><th>Server</th><td>\'+escHtml(data.server||"")+\'</td></tr>\''
                '+\'<tr><th>Path</th><td>\'+escHtml(data.path||"")+\'</td></tr>\''
                '+\'<tr><th>Policy</th><td>\'+escHtml(data.policy||"")+\'</td></tr>\''
                '+\'<tr><th>Type</th><td>\'+escHtml(data.type||"")+\'</td></tr>\''
                '+\'<tr><th>Enabled</th><td>\'+escHtml(enDisp)+\'</td></tr>\''
                '+\'</tbody></table>\';}'
                'document.getElementById("export-detail").innerHTML=h;'
                'const kind=data?(data.kind||""):"";'
                'const ruleCols=(kind==="smb")?SMB_RULE_COLS:'
                '((kind==="nfs")?NFS_RULE_COLS:[]);'
                'const polCols=(kind==="smb")?SMB_POLICY_COLS:'
                '((kind==="nfs")?NFS_POLICY_COLS:[]);'
                'const rulesHtml=data?renderExportRules(data.rules,ruleCols):"";'
                'const polHtml=data?renderExportPolicy(data.policy_attrs,polCols):"";'
                'document.getElementById("export-rules").innerHTML=rulesHtml;'
                'document.getElementById("export-rules-h").style.display='
                'rulesHtml?"block":"none";'
                'document.getElementById("export-policy").innerHTML=polHtml;'
                'document.getElementById("export-policy-h").style.display='
                'polHtml?"block":"none";'
                'document.getElementById("export-modal").style.display="flex";}\n')
        if _has_dir:
            # Directory popup: list every `puredir export list` row
            # whose Directory column matches the row's verbatim
            # directory name. The CSV Enabled column is empty when
            # the export is enabled and reads "Policy Disabled" when
            # it is not, so the renderer maps empty -> "Enabled"
            # and "Policy Disabled" -> "Disabled".
            script_parts.append(
                'function fmtDirEnabled(v){'
                'if(!v||!v.length)return "Enabled";'
                'if(String(v).toLowerCase()==="policy disabled")return "Disabled";'
                'return v;}\n'
                'function showDir(el){'
                'const arr=el.getAttribute("data-arr"),dn=el.getAttribute("data-dir");'
                'const list=(DIR_EXPORT_PROFILES[arr]||{})[dn]||[];'
                'document.getElementById("dir-title").textContent='
                '"Directory Exports - "+dn;'
                'document.getElementById("dir-meta").textContent="Source array: "+arr;'
                'let h;'
                'if(!list.length){h=\'<p style="color:#888;">No exports.</p>\';}'
                'else{h=\'<table class="pg-detail"><thead><tr>'
                '<th>Server</th><th>Export Name</th><th>Path</th>'
                '<th>Type</th><th>Enabled</th>'
                '</tr></thead><tbody>\';'
                'list.forEach(e=>{h+=\'<tr>\''
                '+\'<td>\'+escHtml(e.server||"")+\'</td>\''
                '+\'<td>\'+escHtml(e.export_name||"")+\'</td>\''
                '+\'<td>\'+escHtml(e.path||"")+\'</td>\''
                '+\'<td>\'+escHtml(e.type||"")+\'</td>\''
                '+\'<td>\'+escHtml(fmtDirEnabled(e.enabled))+\'</td></tr>\';});'
                'h+=\'</tbody></table>\';}'
                'document.getElementById("dir-detail").innerHTML=h;'
                'document.getElementById("dir-modal").style.display="flex";}\n')
        script_parts.append('</script>')
        script_block = ''.join(script_parts)
    else:
        script_block = ''

    # Sort/filter wiring. The thead is built programmatically so the
    # column count, sort indicators, and per-column filter inputs stay
    # aligned. Any table marked class="sf" with a sibling thead row of
    # th[onclick="sfHeaderClick(...)"] cells inherits the behavior, so
    # Tables 2/3 will participate once they grow real columns.
    _T1_COLS = [
        'Volume Name', 'Array Name', 'Replication Destination',
        'Pod', 'Remote Pod', 'Local Snapshots', 'Pod Snapshots',
        'Replicated Snapshots', 'Safemode', 'Protection Groups',
        'Max Local Snap Retention (Days)', 'Max Repl Snap Retention (Days)',
        'Local Snap Retention vs SLA', 'Repl Snap Retention vs SLA',
        'Connected Hosts', 'Non Protection Reasoning']
    # Table 2 mirrors Table 1's column structure with "Directory Name"
    # in column 1 in place of "Volume Name". The "Pod Snapshots" column
    # from Table 1 is omitted because FA directory replication is
    # always pod-mediated (the pod is the snapshot domain), so the
    # column collapses with "Replicated Pod Snapshots".
    _T2_COLS = [
        'Directory Name', 'Array Name', 'Replication Destination',
        'Pod', 'Remote Pod', 'Local Snapshots',
        'Replicated Pod Snapshots', 'Safemode', 'Snapshot Policies',
        'Max Local Snap Retention (Days)', 'Max Repl Snap Retention (Days)',
        'Local Snap Retention vs SLA', 'Repl Snap Retention vs SLA',
        'Connected Hosts', 'Non Protection Reasoning']
    def _build_thead(cols):
        return ('<thead><tr>' + ''.join(
            f'<th class="sortable" onclick="sfHeaderClick(event,{i})">'
            f'<div class="th-lbl">{_html.escape(c)}'
            f'<span class="sort-ind"> \u21d5</span></div>'
            f'<input class="filter-input" type="text" placeholder="filter\u2026" '
            f'oninput="sfFilter(this,{i})" '
            f'onclick="event.stopPropagation()"></th>'
            for i, c in enumerate(cols)) + '</tr></thead>')
    thead_t1 = _build_thead(_T1_COLS)
    thead_t2 = _build_thead(_T2_COLS)

    sf_script = r"""<script>
(function(){
  function cellVal(c){
    if(!c) return '';
    var t = c.textContent.trim();
    if(t === '\u2014' || t === '-') return '';
    return t;
  }
  function isNumLike(s){
    if(s === '') return false;
    return /^[-+]?[0-9]+(\.[0-9]+)?$/.test(
      s.replace(/,/g,'').replace(/\s+/g,''));
  }
  function cmp(a,b,dir){
    if(a==='' && b==='') return 0;
    if(a==='') return 1;
    if(b==='') return -1;
    if(isNumLike(a) && isNumLike(b)){
      return (parseFloat(a.replace(/,/g,''))
            - parseFloat(b.replace(/,/g,''))) * dir;
    }
    return a.toLowerCase().localeCompare(b.toLowerCase()) * dir;
  }
  function setInd(th, dir){
    var ind = th.querySelector('.sort-ind');
    if(!ind) return;
    ind.textContent = dir === 1 ? ' \u25b2'
                    : (dir === -1 ? ' \u25bc' : ' \u21d5');
  }
  // Cell colour detection. Inline styles like background:#d4edda end up
  // as backgroundColor 'rgb(212, 237, 218)' once parsed by the browser.
  function colorOf(c){
    if(!c) return '';
    var bg = c.style && c.style.backgroundColor;
    if(bg === 'rgb(212, 237, 218)') return 'g';
    if(bg === 'rgb(248, 215, 218)') return 'r';
    return '';
  }
  function detectColorCols(tbl){
    var ncols = tbl.tHead.rows[0].cells.length;
    var has = new Array(ncols).fill(false);
    for(var i=0;i<tbl._sf.orig.length;i++){
      var row = tbl._sf.orig[i];
      for(var j=0;j<ncols;j++){
        if(colorOf(row.cells[j])) has[j] = true;
      }
    }
    return has;
  }
  // For columns whose body cells use the green/red shading, append a
  // pair of clickable dots to the header label. Each dot toggles a
  // "greens first" or "reds first" sort on that column. Dots
  // stopPropagation so clicking them never triggers the underlying
  // text-sort cycle on the th.
  function injectColorWidgets(tbl){
    var has = detectColorCols(tbl);
    var ths = tbl.tHead.rows[0].cells;
    for(let j=0;j<ths.length;j++){
      if(!has[j]) continue;
      var lbl = ths[j].querySelector('.th-lbl');
      if(!lbl) continue;
      var w = document.createElement('span');
      w.className = 'color-sort';
      w.innerHTML = ' <span class="cs-g" title="Sort greens first">'
                  + '\u25CF</span><span class="cs-r" '
                  + 'title="Sort reds first">\u25CF</span>';
      lbl.appendChild(w);
      w.querySelector('.cs-g').addEventListener('click', function(e){
        e.stopPropagation();
        cycleColorSort(tbl, j, 'g');
      });
      w.querySelector('.cs-r').addEventListener('click', function(e){
        e.stopPropagation();
        cycleColorSort(tbl, j, 'r');
      });
    }
  }
  function cycleColorSort(tbl, idx, target){
    var s = tbl._sf;
    // Selecting a colour sort clears any active text/numeric sort so
    // the table is only ever ordered by one criterion at a time.
    s.col = -1; s.dir = 0;
    if(s.colorCol === idx && s.colorMode === target){
      s.colorCol = -1; s.colorMode = '';
    } else {
      s.colorCol = idx; s.colorMode = target;
    }
    applyTable(tbl);
  }
  function applyTable(tbl){
    var tbody = tbl.tBodies[0];
    var s = tbl._sf;
    if(!s) return;
    var filtered = s.orig.filter(function(r){
      for(var i=0;i<s.filters.length;i++){
        var f = s.filters[i];
        if(!f) continue;
        var c = r.cells[i];
        if(!c) return false;
        if(cellVal(c).toLowerCase().indexOf(f) < 0) return false;
      }
      return true;
    });
    if(s.colorCol >= 0 && s.colorMode){
      var cc = s.colorCol, mode = s.colorMode;
      var rank = function(cell){
        var col = colorOf(cell);
        if(mode === 'g') return col === 'g' ? 0 : (col === '' ? 1 : 2);
        return col === 'r' ? 0 : (col === '' ? 1 : 2);
      };
      filtered = filtered.slice().sort(function(a,b){
        return rank(a.cells[cc]) - rank(b.cells[cc]);
      });
    } else if(s.col >= 0 && s.dir !== 0){
      filtered = filtered.slice().sort(function(a,b){
        return cmp(cellVal(a.cells[s.col]),
                   cellVal(b.cells[s.col]), s.dir);
      });
    }
    var ths = tbl.tHead.rows[0].cells;
    for(var i=0;i<ths.length;i++){
      ths[i].classList.remove('sort-asc','sort-desc');
      var csG = ths[i].querySelector('.cs-g');
      var csR = ths[i].querySelector('.cs-r');
      if(csG) csG.classList.remove('active');
      if(csR) csR.classList.remove('active');
      if(i === s.col && s.dir !== 0){
        ths[i].classList.add(s.dir === 1 ? 'sort-asc' : 'sort-desc');
        setInd(ths[i], s.dir);
      } else {
        setInd(ths[i], 0);
      }
      if(i === s.colorCol && s.colorMode){
        var cs = ths[i].querySelector(
          s.colorMode === 'g' ? '.cs-g' : '.cs-r');
        if(cs) cs.classList.add('active');
      }
    }
    var ds = new Set(filtered);
    for(var j=0;j<s.orig.length;j++){
      s.orig[j].style.display = ds.has(s.orig[j]) ? '' : 'none';
    }
    for(var k=0;k<filtered.length;k++){
      tbody.appendChild(filtered[k]);
    }
  }
  function initTable(tbl){
    var tbody = tbl.tBodies[0];
    if(!tbody) return;
    var orig = [];
    for(var i=0;i<tbody.rows.length;i++) orig.push(tbody.rows[i]);
    var ncols = (tbl.tHead && tbl.tHead.rows[0])
              ? tbl.tHead.rows[0].cells.length : 0;
    tbl._sf = {orig:orig, col:-1, dir:0,
               colorCol:-1, colorMode:'',
               filters:new Array(ncols).fill('')};
    injectColorWidgets(tbl);
  }
  window.sfHeaderClick = function(e, idx){
    if(e.target.tagName === 'INPUT') return;
    if(e.target.closest && e.target.closest('.color-sort')) return;
    var tbl = e.currentTarget.closest('table');
    if(!tbl || !tbl._sf) return;
    var s = tbl._sf;
    // Selecting a text sort clears any active colour sort.
    s.colorCol = -1; s.colorMode = '';
    if(s.col !== idx){ s.col = idx; s.dir = 1; }
    else if(s.dir === 1) s.dir = -1;
    else if(s.dir === -1){ s.dir = 0; s.col = -1; }
    else s.dir = 1;
    applyTable(tbl);
  };
  window.sfFilter = function(input, idx){
    var tbl = input.closest('table');
    if(!tbl || !tbl._sf) return;
    tbl._sf.filters[idx] = input.value.toLowerCase();
    applyTable(tbl);
  };
  document.addEventListener('DOMContentLoaded', function(){
    document.querySelectorAll('table.sf').forEach(initTable);
  });
})();
</script>"""

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Everpure - Volume and Filesystem Protection Report</title>
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
    /* Pin the table header to the top of the viewport while the body
       scrolls. `border-collapse: collapse` causes the cell's own borders
       to scroll away with the content, so we redraw the top + bottom
       edges with a box-shadow that stays anchored to the sticky cell.
       z-index keeps the header above body cells but below the modal
       overlay (which is z-index 1000). */
    thead th {{ position: sticky; top: 0; z-index: 2;
                box-shadow: inset 0 1px 0 #b8cfe8,
                            inset 0 -1px 0 #b8cfe8; }}
    /* Volumes table: own scroll container so the sticky thead and
       the frozen left columns both anchor to this wrap (not the
       page). min-width on the table forces horizontal overflow on
       narrow viewports; box-sizing makes the fixed widths on the
       frozen columns match the left offsets exactly. */
    .t1-wrap {{ overflow: auto; max-height: calc(100vh - 220px);
                border: 1px solid #b8cfe8; margin-top: 6px; }}
    .t1-wrap > table.t1 {{ margin-top: 0; min-width: 1700px; }}
    .t1-wrap > table.t1 th,
    .t1-wrap > table.t1 td {{ box-sizing: border-box; }}
    /* Freeze "Volume Name" (col 1) and "Array Name" (col 2). Sticky
       cells need an opaque background since rows scroll under them;
       the inset right-edge shadow draws the seam between the frozen
       pane and the scrolling body. With table-layout:auto a plain
       `width` on a cell is only a hint -- the browser may render the
       column wider or narrower based on content. min-width+max-width
       lock the rendered width exactly so col 2's `left: 220px`
       offset matches col 1's actual right edge (otherwise a gap or
       overlap appears between the two frozen columns). word-break
       lets long volume names wrap inside the fixed width. */
    .t1-wrap > table.t1 th:nth-child(1),
    .t1-wrap > table.t1 td:nth-child(1) {{
        position: sticky; left: 0;
        width: 220px; min-width: 220px; max-width: 220px;
        word-break: break-word;
        background: #fff; z-index: 1;
        box-shadow: inset -1px 0 0 #b8cfe8; }}
    .t1-wrap > table.t1 th:nth-child(2),
    .t1-wrap > table.t1 td:nth-child(2) {{
        position: sticky; left: 220px;
        width: 170px; min-width: 170px; max-width: 170px;
        word-break: break-word;
        background: #fff; z-index: 1;
        box-shadow: inset -1px 0 0 #b8cfe8; }}
    /* Re-apply the even-row tint on the frozen cells so the body
       stripe doesn't show through the opaque sticky background. */
    .t1-wrap > table.t1 tbody tr:nth-child(even) td:nth-child(1),
    .t1-wrap > table.t1 tbody tr:nth-child(even) td:nth-child(2) {{
        background: #f7faff; }}
    /* Header corner cells: sticky on both axes. z-index 3 keeps
       them above plain sticky-top headers (z-index 2) and plain
       sticky-left body cells (z-index 1). Re-add the top/bottom
       inset edges plus the right-edge seam. */
    .t1-wrap > table.t1 thead th:nth-child(1),
    .t1-wrap > table.t1 thead th:nth-child(2) {{
        background: #dce6f1; z-index: 3;
        box-shadow: inset 0 1px 0 #b8cfe8,
                    inset 0 -1px 0 #b8cfe8,
                    inset -1px 0 0 #b8cfe8; }}
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
    /* Sort/filter chrome for tables marked class="sf". Each header cell
       stacks a clickable label (with sort indicator) above a per-column
       filter input; the input swallows its own clicks so typing into it
       does not also toggle the column's sort. */
    table.sf thead th {{ vertical-align: top; padding-top: 4px; }}
    table.sf thead th.sortable {{ cursor: pointer; user-select: none; }}
    table.sf thead th.sortable:hover {{ background: #c8d8ec; }}
    table.sf thead th .th-lbl {{ white-space: normal; line-height: 1.15;
                                 max-width: 110px; }}
    table.sf thead th .sort-ind {{ color: #6b89ad; font-weight: normal;
                                   font-size: 9pt; }}
    table.sf thead th.sort-asc .sort-ind,
    table.sf thead th.sort-desc .sort-ind {{ color: #1f3a5c;
                                             font-weight: bold; }}
    table.sf thead th .filter-input {{
        display: block; box-sizing: border-box; width: 100%;
        margin-top: 3px; padding: 1px 3px;
        font: 9pt 'Segoe UI', Arial, sans-serif; font-weight: normal;
        border: 1px solid #b8cfe8; border-radius: 2px;
        background: #fff; }}
    table.sf thead th .filter-input:focus {{
        outline: 1px solid #1a5fb4; }}
    /* Color-sort widget: a pair of clickable green/red dots appended to
       the header label of any column whose body cells use the green/red
       shading (Replication Destination, Pod, Safemode, etc.). The JS
       injects these only on color-bearing columns. Selecting a color
       sort clears any text/numeric sort on the same table. */
    table.sf thead th .color-sort {{
        margin-left: 4px; user-select: none; font-size: 9pt; }}
    table.sf thead th .color-sort .cs-g,
    table.sf thead th .color-sort .cs-r {{
        cursor: pointer; padding: 0 2px;
        opacity: 0.55; border-radius: 2px; }}
    table.sf thead th .color-sort .cs-g {{ color: #28a745; }}
    table.sf thead th .color-sort .cs-r {{ color: #dc3545; }}
    table.sf thead th .color-sort .cs-g:hover,
    table.sf thead th .color-sort .cs-r:hover {{ opacity: 0.9; }}
    table.sf thead th .color-sort .cs-g.active {{
        opacity: 1; background: #d4edda;
        outline: 1px solid #28a745; }}
    table.sf thead th .color-sort .cs-r.active {{
        opacity: 1; background: #f8d7da;
        outline: 1px solid #dc3545; }}
    /* Comments column: read-only display with the saved comment text
       on top and a Last Updated timestamp directly underneath. The
       column is populated from reports/protection/comments_active.json
       at report-generation time; in-browser editing is intentionally
       not supported. */
    td.comment-cell {{ min-width: 220px; padding: 4px; }}
    td.comment-cell .comment-text {{
        font: 9pt 'Segoe UI', Arial, sans-serif;
        white-space: pre-wrap; word-break: break-word; }}
    td.comment-cell .comment-meta {{
        margin-top: 3px; font-size: 8.5pt; color: #555;
        white-space: nowrap; overflow: hidden;
        text-overflow: ellipsis; }}
    td.comment-cell .comment-meta-lbl {{ color: #888; }}
    td.comment-cell .comment-updated {{ color: #1f3a5c; }}
    /* Read-only banner above Table 1 explaining where the Comments
       column data comes from and how to update it. */
    .comments-toolbar {{ margin: 6px 0 8px 0; }}
    .comments-toolbar .comments-toolbar-hint {{
        font-size: 9pt; color: #666; }}
    .comments-toolbar .comments-toolbar-hint code {{
        background: #f0f4fa; padding: 0 4px; border-radius: 2px;
        font-family: Consolas, 'Courier New', monospace; }}
  </style>
</head>
<body>
  <h1>Everpure &mdash; Volume and Filesystem Protection Report</h1>
  <p class="meta">Generated {now_str} {tz}</p>
  <p class="meta">Arrays inventoried: {len(per_array)} &middot;
                  FlashArray volume rows: {len(rows)} &middot;
                  FlashArray filesystem rows: {len(rows_t2)}</p>
  <p class="meta">Minimum Snapshot Retention SLA:
                  {('%d day(s)' % sla_snap_days) if sla_snap_days > 0 else 'Not set'}
                  &middot; Minimum Replicated Retention SLA:
                  {('%d day(s)' % sla_repl_days) if sla_repl_days > 0 else 'Not set'}</p>
  {err_banner}

  <h2>1. FlashArray Volumes</h2>
  {comments_toolbar}
  <div class="t1-wrap">
    <table class="sf t1">
      {thead_t1}
      <tbody>
{tr_html}      </tbody>
    </table>
  </div>

  <h2>2. FlashArray Filesystems</h2>
  <div class="t1-wrap">
    <table class="sf t1">
      {thead_t2}
      <tbody>
{tr_html_t2}      </tbody>
    </table>
  </div>

  <h2>3. FlashBlade Filesystems</h2>
  {placeholder}

  {modal_block}
  {script_block}
  {sf_script}
</body>
</html>
"""


__all__ = ['_fake_protection_data_for', '_parse_purevol_list_csv', '_parse_purepod_replica_link_csv', '_parse_purevol_snap_csv', '_parse_purepgroup_list_csv', '_parse_purepgroup_retention_csv', '_parse_purevol_connect_csv', '_parse_purepgroup_schedule_csv', '_parse_purepgroup_retention_full_csv', '_parse_puredir_list_csv', '_parse_purefs_list_csv', '_parse_puredir_snap_list_csv', '_parse_purepolicy_snap_retention_lock_csv', '_parse_purepolicy_snapshot_list_csv', '_parse_purepolicy_snapshot_rule_list_csv', '_parse_puredir_export_list_csv', '_parse_purepolicy_nfs_rule_list_csv', '_parse_purepolicy_nfs_list_csv', '_parse_purepolicy_smb_rule_list_csv', '_parse_purepolicy_smb_list_csv', '_parse_retention_to_days', '_parse_purearray_eradication_config_csv', '_fmt_eradication_delay', '_collect_one_fa_protection', 'run_protection_collection_core', '_compute_pg_max_retention', 'aggregate_fa_volume_rows', 'aggregate_fa_filesystem_rows', '_load_recent_comments', 'build_protection_html']
