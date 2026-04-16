# pure-alert-replication-sla
Tool to do basic alert and replication sla reporting
Alert and Replication SLA Monitor
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

