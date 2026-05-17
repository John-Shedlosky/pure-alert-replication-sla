"""PureMonitorApp — the Tk/CustomTkinter desktop application.

This is the only module in the package that imports tkinter,
customtkinter, PIL or tksheet. It depends on common, alert_report and
protection_report for everything below the GUI layer.
"""
import csv
import datetime
import json
import os
import queue
import re
import smtplib
import sys
import tempfile
import threading
import tkinter as tk
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from tkinter import ttk, scrolledtext, messagebox, simpledialog, filedialog

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

try:
    import customtkinter as ctk
    HAS_CTK = True
    # Dark-mode + a default accent that reads well on the muted slate
    # background CTk uses in dark mode.
    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")
except ImportError:
    HAS_CTK = False
    ctk = None  # falls back to ttk widgets if CTk isn't installed

# Single source of truth for the modern font family used across the GUI.
# CTk's CTkFont accepts any installed family; if Segoe UI isn't on the host
# system Tk silently falls back to the platform default.
UI_FONT_FAMILY = "Segoe UI"

# Subtle gridline tone for tksheet on dark backgrounds — chosen to be
# visible but not visually compete with the row content text. Sits a hair
# above the dark-theme cell background so the lines read as "structure"
# rather than as a separate accent color.
SHEET_GRID_FG = "#3a3a3a"

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

from . import common as _common
from .common import (
    HAS_PARAMIKO,
    FAKE_ARRAYS, ALERT_DEBUG,
    password_request_event, password_response_event,
    credentials_cache, _alert_collection_lock,
    ask_password_in_main, run_ssh_command,
    parse_time_to_seconds, format_seconds_human,
    _parse_sla_days, _fmt_alert_str,
    _fake_arrays_config, _get_debug_alerts,
    parse_pure_date, parse_arr_loc, align_rel_pairs_by_location,
    _parse_csv_text, _csv_to_dicts, _classify_array_output, detect_array_type,
    parse_unified_arrays, unified_arrays_from_config,
    parse_unified_arrays_full, unified_arrays_from_config_full,
    auth_user_for_array,
)
from .alert_report import (
    collect_hw_health, collect_replication_relationships,
    run_collection_core, build_nogui_header,
    append_history_csv, send_html_report, build_status_html,
)
from .protection_report import (
    _fake_protection_data_for,
    _collect_one_fa_protection, run_protection_collection_core,
    aggregate_fa_volume_rows,
    EXCEPTION_CHOICES, _load_exceptions, _save_exceptions,
    build_protection_html,
)


# When CustomTkinter is installed the app inherits its dark-mode root
# (CTk) so all child widgets pick up the modern theme automatically.
# Falls back to plain tk.Tk if the package is unavailable so the app
# still runs in environments without the extra dependency.
_AppBase = ctk.CTk if HAS_CTK else tk.Tk


class PureMonitorApp(_AppBase):
    def __init__(self):
        super().__init__()
        _title = "Everpure (Pure Storage) - Alert and Replication SLA Status Report"
        if FAKE_ARRAYS:
            _title += "  [DEMO: 12 fake arrays, 5 locations]"
        self.title(_title)
        # Pre-load monitor_config.json once so the saved window_geometry /
        # main_sash_pos can be honored before _setup_ui builds widgets, and
        # so _setup_ui doesn't have to re-read the file. Stored on self so
        # _set_initial_sash and _save_config can both reach it.
        self.config_data = self._load_config()
        # 850 px tall × 1375 px wide fits a 15-row Arrays sheet with the
        # output log occupying ~15% of the window on first paint, and gives
        # the configuration columns enough horizontal room that the SLA row,
        # logo, and per-array Notes column don't wrap on standard 1080p.
        # When the user has saved a prior geometry (via Save Config), reuse
        # it so size and on-screen position survive a restart.
        _saved_geom = self.config_data.get('window_geometry') if not FAKE_ARRAYS else None
        if isinstance(_saved_geom, str) and re.match(r'^\d+x\d+(\+-?\d+\+-?\d+)?$', _saved_geom):
            try:
                self.geometry(_saved_geom)
            except Exception:
                self.geometry("1375x850")
        else:
            self.geometry("1375x850")

        # Title-bar / taskbar icon. On Windows, iconphoto() is overridden
        # by any later iconbitmap() call — and CustomTkinter's CTk class
        # schedules its own iconbitmap() ~200 ms after init (loading
        # CustomTkinter_icon_Windows.ico, the blue-square default). To
        # win, we generate a .ico from pure_logo.png via PIL and call
        # iconbitmap() ourselves, which sets CTk's _iconbitmap_method_called
        # flag and suppresses their override entirely. iconphoto() is also
        # set as a fallback for non-Windows platforms and for child
        # Toplevel windows (Reports, Help, modals) via default=True.
        _icon = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images", "pure_logo.png")
        if os.path.exists(_icon):
            try:
                if HAS_PIL:
                    pil_icon = Image.open(_icon)
                    self._icon_img = ImageTk.PhotoImage(pil_icon)
                else:
                    pil_icon = None
                    self._icon_img = tk.PhotoImage(file=_icon)
                self.iconphoto(True, self._icon_img)
                if HAS_PIL and sys.platform.startswith("win"):
                    # PIL's ICO writer needs RGBA and supports multi-size
                    # icons; bundling 16/32/48/64/128/256 lets Windows
                    # pick the best size for title bar / taskbar / Alt-Tab.
                    ico_path = os.path.join(tempfile.gettempdir(),
                                            "everpure_pure_logo.ico")
                    if pil_icon.mode != "RGBA":
                        pil_icon = pil_icon.convert("RGBA")
                    pil_icon.save(ico_path, format="ICO",
                                  sizes=[(16, 16), (32, 32), (48, 48),
                                         (64, 64), (128, 128), (256, 256)])
                    self.iconbitmap(ico_path)
                    self._icon_ico_path = ico_path
            except Exception:
                pass

        self.detailed_log_data  = ""
        self.array_stats        = []
        self.last_summary_path  = None
        self.last_log_path      = None
        self.last_html_path     = None
        self._setup_ui()
        self.after(100, self.check_queue)
        # Defer until the Arrays sheet's row_positions and the logo's
        # rendered height are known so the sidebar spacer can size
        # itself precisely. 150 ms is enough for the initial layout
        # pass on Windows; the call is idempotent and re-runs whenever
        # the sheet fires <Configure> (see _setup_ui binding).
        self.after(150, lambda: self._align_sidebar_to_arrays_row(4))
        
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
        """Build the unified Arrays/Location/Notes/Auth-User editor.

        Grids at row=1, columns 1-4 of the Configuration frame; uses
        ``tksheet`` when available, otherwise falls back to a stack of
        synced ScrolledText boxes so the app still runs without tksheet.
        """
        # Pull (name, location, notes) from the standard helper, then
        # overlay the per-array auth_user (4th column) by re-reading the
        # raw arrays list-of-dicts so the SSH credential survives a
        # save/reload cycle alongside the other fields.
        _by_name = {}
        for _item in (config.get('arrays') or []):
            if isinstance(_item, dict):
                _n = str(_item.get('name', '') or '').strip()
                if _n:
                    _by_name[_n] = str(_item.get('auth_user', '') or '').strip()
        rows = [[n, l, nt, _by_name.get(n, '')]
                for n, l, nt in unified_arrays_from_config_full(config)]
        # Pad with 50 blank rows at the end on startup so the user has ample
        # scratch space to paste into without having to insert rows first.
        # _ensure_trailing_blank_rows then appends another 50 in one batch
        # whenever the user fills the last visible blank row, so the sheet
        # auto-grows in 50-row chunks rather than one row at a time.
        # NOTE: must construct each row as its own list \u2014 [['','','','']]*50 would
        # create 50 references to the same inner list, so writing one cell
        # would propagate the value into every padding row.
        rows.extend([['', '', '', ''] for _ in range(50)])

        # Plain tk.Frame (not ttk.Frame) with bg matching the Sheet's
        # outer canvas (#000000 from the dark-blue theme). ttk.Frame on
        # Windows defaults to background='SystemButtonFace' (~#F0F0F0)
        # which leaks a light-gray rim around the table whenever the
        # Sheet doesn't perfectly cover every pixel of its parent.
        # bd=0 + highlightthickness=0 also drop any focus / border chrome.
        sheet_frame = tk.Frame(parent, bg="#000000",
                               bd=0, highlightthickness=0)
        # Span columns 0\u20134 now that the "Arrays:" label has been removed,
        # so the tksheet sits flush with the left edge of the column.
        # row=0 places the sheet directly under the "Configuration"
        # header strip; the right sidebar (logo / Save Config / SLA
        # cards) lives in column 5 of the same row.
        sheet_frame.grid(row=0, column=0, columnspan=5,
                         sticky=tk.NSEW, padx=(0, 0), pady=2)
        # Kept on self so the <Configure> handler in _on_sheet_resize can
        # query the live frame width and redistribute column widths.
        self._sheet_frame = sheet_frame
        # Let the sheet grow when the user resizes the window or drags the
        # main vertical PanedWindow sash to give Configuration more height.
        parent.rowconfigure(0, weight=1)
        # weight=1 lets the grid cell expand horizontally when the parent
        # frame is wider than the sheet's reserved minsize, so the sheet's
        # drawing area tracks the visible width rather than being clipped.
        parent.columnconfigure(0, minsize=200, weight=1)
        parent.columnconfigure(1, minsize=160, weight=1)
        parent.columnconfigure(2, minsize=160, weight=1)
        parent.columnconfigure(3, minsize=130, weight=1)

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
            # theme="dark blue" gives the sheet a dark backdrop matching
            # the surrounding CTk dark mode; the gridline color is then
            # softened via set_options below so the lines don't compete
            # visually with the cell content. font hint propagates Segoe UI
            # into header/index/cell rendering when the family is
            # available on the host system.
            _sheet_theme = "dark blue" if HAS_CTK else "light blue"
            self.arrays_sheet = Sheet(
                sheet_frame,
                headers=["Array", "Location", "Notes", "Auth User"],
                data=rows,
                # Larger requested height so the row 1 grid cell pulls
                # more vertical real estate at startup. rowconfigure
                # weight=1 still lets the sheet absorb any additional
                # space when the window/pane is taller than this.
                width=585, height=900,
                show_row_index=True,
                show_top_left=False,
                theme=_sheet_theme,
                font=(UI_FONT_FAMILY, 10, "normal"),
                header_font=(UI_FONT_FAMILY, 10, "bold"),
                index_font=(UI_FONT_FAMILY, 10, "normal"),
                # Render both scrollbars so over-wide columns or long
                # row counts remain reachable.
                show_x_scrollbar=True,
                show_y_scrollbar=True,
                # Inherit scrollbar element layout from the 'clam' ttk
                # theme so colors set via set_options() actually paint.
                # 'default' resolves to vista on Windows, which paints
                # native chrome and ignores Style color options.
                scrollbar_theme_inheritance="clam",
                # Arrows must be hidden: tksheet only installs the
                # layout that uses its 'clam'-derived prefixed elements
                # when scrollbar_show_arrows=False (see sheet.py:420 in
                # tksheet 7.6). With arrows on, the bar falls back to
                # the active theme's layout and the dark color
                # overrides have no visible effect — confirmed via
                # PIL.ImageGrab pixel sampling: trough rendered
                # (240,240,240) with arrows on, (28,28,28) with off.
                scrollbar_show_arrows=False,
            )
            # Soften gridlines, suppress the bright outer rectangle the
            # dark-blue theme draws around the sheet (outline_thickness=0
            # collapses tksheet's highlight ring to zero pixels), and
            # repaint both scrollbars in dark tones so they match the
            # CTk dark canvas instead of falling back to OS-native gray.
            # set_scrollbar_options() must run after set_options() so the
            # new colors get pushed into the ttk Style entries that back
            # tksheet's clam-derived scrollbars.
            try:
                self.arrays_sheet.set_options(
                    table_grid_fg=SHEET_GRID_FG,
                    index_grid_fg=SHEET_GRID_FG,
                    header_grid_fg=SHEET_GRID_FG,
                    outline_thickness=0,
                    header_fg="#bf5a15",
                    index_fg="#bf5a15",
                    top_left_fg="#bf5a15",
                    top_left_fg_highlight="#bf5a15",
                    vertical_scroll_background="#2e2e2e",
                    vertical_scroll_troughcolor="#1c1c1c",
                    vertical_scroll_lightcolor="#2a2a2a",
                    vertical_scroll_darkcolor="#161616",
                    vertical_scroll_bordercolor="#1c1c1c",
                    vertical_scroll_not_active_bg="#2e2e2e",
                    vertical_scroll_active_bg="#3a3a3a",
                    vertical_scroll_pressed_bg="#444444",
                    horizontal_scroll_background="#2e2e2e",
                    horizontal_scroll_troughcolor="#1c1c1c",
                    horizontal_scroll_lightcolor="#2a2a2a",
                    horizontal_scroll_darkcolor="#161616",
                    horizontal_scroll_bordercolor="#1c1c1c",
                    horizontal_scroll_not_active_bg="#2e2e2e",
                    horizontal_scroll_active_bg="#3a3a3a",
                    horizontal_scroll_pressed_bg="#444444",
                )
                try:
                    self.arrays_sheet.set_scrollbar_options()
                except Exception:
                    pass
            except Exception:
                pass
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
                w0 = int(_saved_w[0]) if len(_saved_w) > 0 and _saved_w[0] else 180
            except Exception:
                w0 = 180
            try:
                w1 = int(_saved_w[1]) if len(_saved_w) > 1 and _saved_w[1] else 150
            except Exception:
                w1 = 150
            try:
                w2 = int(_saved_w[2]) if len(_saved_w) > 2 and _saved_w[2] else 180
            except Exception:
                w2 = 180
            try:
                w3 = int(_saved_w[3]) if len(_saved_w) > 3 and _saved_w[3] else 130
            except Exception:
                w3 = 130
            try:
                self.arrays_sheet.column_width(column=0, width=w0)
                self.arrays_sheet.column_width(column=1, width=w1)
                self.arrays_sheet.column_width(column=2, width=w2)
                self.arrays_sheet.column_width(column=3, width=w3)
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
        """Return [(name, location, notes, auth_user), ...] from the sheet.

        Drops rows whose name is blank after whitespace trimming. The
        4th element is the per-array SSH username used by the SSH-based
        detection / collection paths when no key-based auth is set up;
        callers that only need 3 fields can ignore the trailing entry.
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
                loc       = str(row[1] if len(row) > 1 else '').strip()
                notes     = str(row[2] if len(row) > 2 else '').strip()
                auth_user = str(row[3] if len(row) > 3 else '').strip()
                out.append((name, loc, notes, auth_user))
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
            out.append((name, loc, notes, ''))
        return out

    def _attach_tooltip(self, widget, text):
        """Attach a hover tooltip to widget. Bound to <Enter>/<Leave>.

        The tooltip is a transient borderless Toplevel positioned just
        below the widget; it uses the dark CTk palette and the same
        accent (#bf5a15) used for headers / button text so the
        reminder reads as part of the same UI vocabulary.
        """
        state = {'win': None}
        def _show(_e=None):
            if state['win'] is not None:
                return
            try:
                x = widget.winfo_rootx() + 10
                y = widget.winfo_rooty() + widget.winfo_height() + 4
            except Exception:
                return
            w = tk.Toplevel(self)
            w.wm_overrideredirect(True)
            try:
                w.wm_attributes('-topmost', True)
            except Exception:
                pass
            w.wm_geometry(f"+{x}+{y}")
            tk.Label(w, text=text, justify=tk.LEFT,
                     bg="#1f1f1f", fg="#bf5a15",
                     relief='solid', borderwidth=1,
                     font=(UI_FONT_FAMILY, 9, "normal"),
                     padx=8, pady=4).pack()
            state['win'] = w
        def _hide(_e=None):
            if state['win'] is not None:
                try: state['win'].destroy()
                except Exception: pass
                state['win'] = None
        widget.bind('<Enter>', _show, add='+')
        widget.bind('<Leave>', _hide, add='+')

    def _build_alerts_sheet(self, parent, config):
        """Build the Excluded Alerts sheet (3 visible rows × 3 data columns).

        Mirrors the Arrays sheet's dark theme, fonts, scrollbars, and
        accent colors. The first column is rendered via tksheet's
        show_row_index gutter (always 1..N, non-editable) so the user
        never has to maintain row numbers manually. Three editable
        columns follow: Alert Codes, Alert Code Description, Exclude
        Justification. The container frame is fixed at ~124 px so only
        3 data rows are visible; the y-scrollbar reaches the rest.
        """
        # Pull row data from the new alerts_excluded_rows list (preferred)
        # or fall back to splitting the legacy alerts_excluded string so
        # an existing monitor_config.json upgrades cleanly.
        rows_data = config.get('alerts_excluded_rows')
        rows = []
        if isinstance(rows_data, list):
            for r in rows_data:
                if isinstance(r, dict):
                    rows.append([str(r.get('code', '')),
                                 str(r.get('description', '')),
                                 str(r.get('justification', ''))])
        else:
            legacy = config.get('alerts_excluded', DEFAULT_EXCLUDED_ALERTS) or ''
            for code in legacy.replace('\n', ',').split(','):
                c = code.strip()
                if c and 'e.g.' not in c:
                    rows.append([c, '', ''])
        # Pad to 50 rows so the user has scratch space (mirrors Arrays).
        while len(rows) < 50:
            rows.append(['', '', ''])

        # Fixed-height container so the sheet is clamped to ~3 visible
        # rows even when the surrounding panel grows. bg=#000000 matches
        # the dark-blue tksheet canvas to avoid a light-gray rim.
        sheet_frame = tk.Frame(parent, bg="#000000",
                               bd=0, highlightthickness=0,
                               height=124)
        # padx=0 is required so the sheet's left/right edges align
        # exactly with the arrays sheet above; matching pady=2 and the
        # parent panel's padx=(0, 0) grid keeps both tksheets at the
        # same horizontal extents.
        sheet_frame.pack(side=tk.TOP, fill=tk.X, padx=0, pady=(2, 6))
        sheet_frame.pack_propagate(False)
        # Stash on self so _on_alerts_sheet_resize can read the live
        # frame width for proportional column redistribution.
        self._alerts_sheet_frame = sheet_frame

        if HAS_TKSHEET:
            _theme = "dark blue" if HAS_CTK else "light blue"
            self.alerts_sheet = Sheet(
                sheet_frame,
                headers=["Alert Codes", "Alert Code Description",
                         "Exclude Justification"],
                data=rows,
                width=720, height=124,
                show_row_index=True,
                show_top_left=False,
                theme=_theme,
                font=(UI_FONT_FAMILY, 10, "normal"),
                header_font=(UI_FONT_FAMILY, 10, "bold"),
                index_font=(UI_FONT_FAMILY, 10, "normal"),
                show_x_scrollbar=True,
                show_y_scrollbar=True,
                scrollbar_theme_inheritance="clam",
                scrollbar_show_arrows=False,
            )
            try:
                self.alerts_sheet.set_options(
                    table_grid_fg=SHEET_GRID_FG,
                    index_grid_fg=SHEET_GRID_FG,
                    header_grid_fg=SHEET_GRID_FG,
                    outline_thickness=0,
                    header_fg="#bf5a15",
                    index_fg="#bf5a15",
                    top_left_fg="#bf5a15",
                    top_left_fg_highlight="#bf5a15",
                    vertical_scroll_background="#2e2e2e",
                    vertical_scroll_troughcolor="#1c1c1c",
                    vertical_scroll_lightcolor="#2a2a2a",
                    vertical_scroll_darkcolor="#161616",
                    vertical_scroll_bordercolor="#1c1c1c",
                    vertical_scroll_not_active_bg="#2e2e2e",
                    vertical_scroll_active_bg="#3a3a3a",
                    vertical_scroll_pressed_bg="#444444",
                    horizontal_scroll_background="#2e2e2e",
                    horizontal_scroll_troughcolor="#1c1c1c",
                    horizontal_scroll_lightcolor="#2a2a2a",
                    horizontal_scroll_darkcolor="#161616",
                    horizontal_scroll_bordercolor="#1c1c1c",
                    horizontal_scroll_not_active_bg="#2e2e2e",
                    horizontal_scroll_active_bg="#3a3a3a",
                    horizontal_scroll_pressed_bg="#444444",
                )
                try: self.alerts_sheet.set_scrollbar_options()
                except Exception: pass
            except Exception:
                pass
            self.alerts_sheet.enable_bindings((
                "single_select", "drag_select", "arrowkeys", "edit_cell",
                "copy", "paste", "delete", "undo",
                "right_click_popup_menu", "rc_insert_row", "rc_delete_row",
                "column_width_resize", "double_click_column_resize",
            ))
            try: self._install_single_cell_paste(self.alerts_sheet)
            except Exception: pass
            # Always-visible 1..N row numbers in the index gutter; mirrors
            # the spreadsheet feel of the Arrays sheet without needing a
            # _refresh handler since the labels never change.
            try:
                self.alerts_sheet.row_index([str(i+1) for i in range(len(rows))])
            except Exception:
                pass
            # Match the Arrays sheet's row-index gutter width exactly so
            # both sheets line up at the same x-coordinate.
            try: self.alerts_sheet.set_index_width(40)
            except Exception: pass
            # Honor saved column widths when present (the user may have
            # resized columns manually); fall back to a 140 / 260 / 260
            # split that mirrors the Arrays sheet's default proportions.
            _saved_aw = config.get('alerts_col_widths') or []
            try:
                aw0 = int(_saved_aw[0]) if len(_saved_aw) > 0 and _saved_aw[0] else 140
            except Exception:
                aw0 = 140
            try:
                aw1 = int(_saved_aw[1]) if len(_saved_aw) > 1 and _saved_aw[1] else 260
            except Exception:
                aw1 = 260
            try:
                aw2 = int(_saved_aw[2]) if len(_saved_aw) > 2 and _saved_aw[2] else 260
            except Exception:
                aw2 = 260
            try:
                self.alerts_sheet.column_width(column=0, width=aw0)
                self.alerts_sheet.column_width(column=1, width=aw1)
                self.alerts_sheet.column_width(column=2, width=aw2)
            except Exception:
                pass
            self.alerts_sheet.pack(fill=tk.BOTH, expand=True)
            # <Configure> on the frame fires once per geometry change;
            # _on_alerts_sheet_resize redistributes columns proportionally
            # so the data area always tracks the visible sheet width.
            sheet_frame.bind('<Configure>', self._on_alerts_sheet_resize)
            # alerts_entry kept None when sheet is active so any stray
            # caller hits the sheet path in _get_excluded_codes.
            self.alerts_entry = None
        else:
            # Fallback when tksheet is unavailable: the original
            # ScrolledText, so the app still runs end-to-end.
            self.alerts_sheet = None
            self.alerts_entry = scrolledtext.ScrolledText(sheet_frame,
                                                         width=30, height=3)
            self.alerts_entry.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            self.alerts_entry.insert(tk.END,
                config.get("alerts_excluded", DEFAULT_EXCLUDED_ALERTS))

    def _get_excluded_codes(self):
        """Return the list of alert-code strings from the Excluded Alerts sheet.

        Filters blank cells and any 'e.g.' placeholder text. Used by
        the Alert/SLA Report header, run_collection cfg, _save_config's
        legacy alerts_excluded string, and the protection report header
        — every spot that previously parsed self.alerts_entry.
        """
        sheet = getattr(self, 'alerts_sheet', None)
        if sheet is not None:
            try:
                data = sheet.get_sheet_data() or []
            except Exception:
                data = []
            return [str(r[0]).strip() for r in data
                    if r and str(r[0]).strip() and 'e.g.' not in str(r[0])]
        entry = getattr(self, 'alerts_entry', None)
        if entry is None:
            return []
        return [x.strip() for x in entry.get("1.0", tk.END).replace('\n', ',').split(',')
                if x.strip() and 'e.g.' not in x]

    def _get_alerts_rows(self):
        """Return the list of {code, description, justification} dicts.

        Used by _save_config to persist the full sheet content to JSON.
        Trailing fully-blank rows are trimmed so the saved file stays
        compact, but interior blanks are preserved so users can space
        their entries however they like without losing structure on
        the next load.
        """
        sheet = getattr(self, 'alerts_sheet', None)
        if sheet is None:
            return []
        try:
            data = sheet.get_sheet_data() or []
        except Exception:
            return []
        last_filled = -1
        for i, r in enumerate(data):
            if r and any(str(c).strip() for c in r[:3]):
                last_filled = i
        out = []
        for i in range(last_filled + 1):
            r = data[i] if i < len(data) else []
            out.append({
                'code':          str(r[0] if len(r) > 0 else '').strip(),
                'description':   str(r[1] if len(r) > 1 else '').strip(),
                'justification': str(r[2] if len(r) > 2 else '').strip(),
            })
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
        """Redistribute Array/Location/Notes/Auth-User column widths when
        the arrays sheet frame is resized (window resize, sash drag, etc.).

        All four columns are stretched proportionally to their current
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
        if avail < 240:
            return
        try:
            w0 = int(sheet.column_width(column=0))
            w1 = int(sheet.column_width(column=1))
            w2 = int(sheet.column_width(column=2))
            w3 = int(sheet.column_width(column=3))
        except Exception:
            return
        total = max(w0 + w1 + w2 + w3, 1)
        new_w0 = max(60, int(avail * (w0 / total)))
        new_w1 = max(60, int(avail * (w1 / total)))
        new_w2 = max(60, int(avail * (w2 / total)))
        new_w3 = max(60, avail - new_w0 - new_w1 - new_w2)
        if (new_w0 == w0 and new_w1 == w1 and new_w2 == w2 and new_w3 == w3):
            return
        self._sheet_resize_guard = True
        try:
            try:
                sheet.column_width(column=0, width=new_w0)
                sheet.column_width(column=1, width=new_w1)
                sheet.column_width(column=2, width=new_w2)
                sheet.column_width(column=3, width=new_w3)
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
        # Re-key the right sidebar's vertical offset off the new sheet
        # geometry so the Save Config button stays aligned with row 4.
        try:
            self._align_sidebar_to_arrays_row(4)
        except Exception:
            pass

    def _on_alerts_sheet_resize(self, event=None):
        """Redistribute the 3 Excluded-Alerts columns when the sheet frame
        is resized (window resize, sash drag, user-driven column resize).

        Mirrors _on_sheet_resize: keeps each column's proportional share
        of the available drawing width so the columns always span the
        full visible sheet area edge-to-edge, with a 60 px floor per
        column to prevent collapse.
        """
        sheet = getattr(self, 'alerts_sheet', None)
        sf = getattr(self, '_alerts_sheet_frame', None)
        if sheet is None or sf is None:
            return
        if getattr(self, '_alerts_resize_guard', False):
            return
        try:
            # Same accounting as the arrays sheet: subtract 40 px row
            # index gutter + ~18 px vertical scrollbar + ~4 px borders.
            avail = sf.winfo_width() - 62
        except Exception:
            return
        if avail < 180:
            return
        try:
            w0 = int(sheet.column_width(column=0))
            w1 = int(sheet.column_width(column=1))
            w2 = int(sheet.column_width(column=2))
        except Exception:
            return
        total = max(w0 + w1 + w2, 1)
        new_w0 = max(60, int(avail * (w0 / total)))
        new_w1 = max(60, int(avail * (w1 / total)))
        new_w2 = max(60, avail - new_w0 - new_w1)
        if new_w0 == w0 and new_w1 == w1 and new_w2 == w2:
            return
        self._alerts_resize_guard = True
        try:
            try:
                sheet.column_width(column=0, width=new_w0)
                sheet.column_width(column=1, width=new_w1)
                sheet.column_width(column=2, width=new_w2)
                try:
                    sheet.refresh()
                except Exception:
                    pass
            except Exception:
                pass
        finally:
            self._alerts_resize_guard = False

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
                        new_data = list(data) + [['', '', '', ''] for _ in range(needed)]
                        sheet.set_sheet_data(new_data, redraw=True)
                except Exception:
                    # Fallback: rebuild the data in one shot.
                    new_data = list(data) + [['', '', '', ''] for _ in range(needed)]
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
        # Config was pre-loaded in __init__ so saved window_geometry could
        # be applied before any widgets exist; reuse that single instance
        # here rather than re-reading monitor_config.json.
        config = self.config_data

        # Menu bar: Email/SMTP → Reports → Help. The Reports cascade
        # replaces the standalone "Open Summary" / "Open Logs" buttons
        # that used to crowd the lower button bar.
        #
        # Windows ignores Tk's bg/fg on the native menubar strip itself,
        # so under CTk we drop the native bar and build a custom CTk
        # strip across the top of the window. The Reports / Help
        # dropdowns are still tk.Menu instances (with dark colors set
        # explicitly) so existing entryconfig() callers continue to work.
        _menu_bg, _menu_fg = "#2b2b2b", "#dce4ee"
        _menu_active_bg, _menu_active_fg = "#1f6aa5", "#ffffff"
        def _mk_dropdown(parent):
            return tk.Menu(parent, tearoff=0,
                           bg=_menu_bg, fg=_menu_fg,
                           activebackground=_menu_active_bg,
                           activeforeground=_menu_active_fg,
                           disabledforeground="#7a7a7a",
                           borderwidth=0, relief="flat")

        self.reports_menu = _mk_dropdown(self)
        self.reports_menu.add_command(label="Open Summary",
                                      command=self._open_summary, state=tk.DISABLED)
        self.reports_menu.add_command(label="Open Logs",
                                      command=self._open_logs, state=tk.DISABLED)
        # Cached entry indexes so run-completion handlers can flip these
        # menu items to NORMAL via reports_menu.entryconfig(idx, ...).
        self._open_summary_idx = 0
        self._open_logs_idx    = 1
        # Email / SMTP cascade. "Email Daily Report" stays disabled until
        # a report has been generated and SMTP server + recipient are
        # configured (mirrors the previous email button gating). The
        # configuration dialog entry is always enabled so the user can
        # set up SMTP credentials before running their first report.
        self.email_menu = _mk_dropdown(self)
        self.email_menu.add_command(label="Email Daily Report",
                                    command=self._email_daily_report,
                                    state=tk.DISABLED)
        self.email_menu.add_command(label="Email / SMTP Configuration",
                                    command=self._show_email_config)
        self._email_daily_idx = 0
        help_menu = _mk_dropdown(self)
        help_menu.add_command(label="Usage / Help...", command=self._show_help)

        if HAS_CTK:
            # Custom CTk strip: a thin frame across the top with flat
            # CTkButtons that look like a Win32 menubar but inherit the
            # dark theme. Each button either invokes its command directly
            # (Email/SMTP) or pops up the corresponding tk.Menu dropdown
            # anchored to the button's bottom-left corner.
            menu_strip = ctk.CTkFrame(self, height=30, corner_radius=0,
                                      fg_color=("gray86", "gray17"))
            menu_strip.pack(side=tk.TOP, fill=tk.X)
            menu_strip.pack_propagate(False)
            _mb_font = ctk.CTkFont(family=UI_FONT_FAMILY, size=12)
            _mb_kw = dict(
                height=26, corner_radius=0,
                fg_color="transparent",
                hover_color=("gray75", "gray25"),
                text_color=("gray10", "gray90"),
                font=_mb_font,
            )
            def _popup_below(menu, btn):
                try:
                    menu.tk_popup(btn.winfo_rootx(),
                                  btn.winfo_rooty() + btn.winfo_height())
                finally:
                    try: menu.grab_release()
                    except Exception: pass

            _eb = ctk.CTkButton(menu_strip, text="Email / SMTP", width=110, **_mb_kw)
            _eb.configure(command=lambda b=_eb: _popup_below(self.email_menu, b))
            _eb.pack(side=tk.LEFT, padx=(2, 0), pady=2)
            _rb = ctk.CTkButton(menu_strip, text="Logs", width=80, **_mb_kw)
            _rb.configure(command=lambda b=_rb: _popup_below(self.reports_menu, b))
            _rb.pack(side=tk.LEFT, pady=2)
            _hb = ctk.CTkButton(menu_strip, text="Help", width=60, **_mb_kw)
            _hb.configure(command=lambda b=_hb: _popup_below(help_menu, b))
            _hb.pack(side=tk.LEFT, pady=2)
        else:
            # Non-CTk fallback: native Tk menubar (Windows draws the
            # strip itself; only the dropdown panels honor bg/fg).
            menubar = tk.Menu(self)
            menubar.add_cascade(label="Email / SMTP", menu=self.email_menu)
            menubar.add_cascade(label="Logs", menu=self.reports_menu)
            menubar.add_cascade(label="Help", menu=help_menu)
            self.config(menu=menubar)

        # Cached CTkFont so every CTk widget below (and the dialogs) share
        # the same Segoe UI styling. Falls back to a plain font tuple when
        # CTk isn't available so non-CTk widgets like tksheet still get
        # the family hint.
        if HAS_CTK:
            self._ui_font      = ctk.CTkFont(family=UI_FONT_FAMILY, size=12)
            self._ui_font_bold = ctk.CTkFont(family=UI_FONT_FAMILY, size=12, weight="bold")
            self._ui_font_h    = ctk.CTkFont(family=UI_FONT_FAMILY, size=14, weight="bold")
        else:
            self._ui_font      = (UI_FONT_FAMILY, 10)
            self._ui_font_bold = (UI_FONT_FAMILY, 10, "bold")
            self._ui_font_h    = (UI_FONT_FAMILY, 11, "bold")
        # Header / panel tones default to None so CTkFrame applies the
        # stock CustomTkinter dark-theme fg_color. Kept as attributes (not
        # inlined) so future restyling has a single point to override.
        self._header_color = None
        self._panel_color  = None

        Frame = ctk.CTkFrame if HAS_CTK else ttk.Frame
        Label = ctk.CTkLabel if HAS_CTK else ttk.Label
        Entry = ctk.CTkEntry if HAS_CTK else ttk.Entry

        main_frame = Frame(self)
        if HAS_CTK:
            main_frame.configure(fg_color="transparent")
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Vertical PanedWindow lets the user redistribute height between the
        # Configuration pane (which hosts the Arrays sheet) and the lower
        # pane (button bar + run-output log). PanedWindow has no CTk
        # equivalent so the ttk version is retained.
        main_paned = ttk.PanedWindow(main_frame, orient=tk.VERTICAL)
        main_paned.pack(fill=tk.BOTH, expand=True)
        self._main_paned = main_paned

        # Configuration pane: an outer CTkFrame holds a slightly-lighter
        # "header" strip on top and the gridded body underneath, so the
        # body's grid layout (SLA row 0, Arrays sheet row 1, etc.) is
        # unchanged from the legacy ttk.LabelFrame version.
        config_outer = Frame(main_paned)
        main_paned.add(config_outer, weight=3)

        if HAS_CTK:
            header_strip = ctk.CTkFrame(config_outer, corner_radius=6, height=34)
            header_strip.pack(side=tk.TOP, fill=tk.X, padx=4, pady=(4, 0))
            header_strip.pack_propagate(False)
            ctk.CTkLabel(header_strip, text="Configuration",
                         font=self._ui_font_h,
                         text_color="#bf5a15").pack(side=tk.LEFT, padx=12, pady=4)

        config_frame = Frame(config_outer)
        if HAS_CTK:
            config_frame.configure(fg_color="transparent")
        config_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True,
                          padx=4, pady=(2, 4))
        # Kept on self so _show_busy_spinner can grid the inline spinner
        # next to the Arrays sheet (column 5).
        self._config_frame = config_frame

        # Shared button kwargs / Button class. Hoisted to self.* so the
        # Save Config toolbar (row 1 of config_frame) and the actions
        # matrix (row 3, column 5 of config_frame) build buttons with
        # the same look. Burnt-amber surface (#bf5a15) with charcoal-
        # gray text (#1F1F1F) reads as a primary CTA; border_color is
        # set to the same #bf5a15 so the border blends with the surface
        # and the unified tone is shared across every button in the app
        # (the Help dialog's Close button mirrors it). The heavier
        # weight font + 25 %-taller height (28 → 35) keeps the labels
        # prominent against the surrounding dark canvas.
        if HAS_CTK:
            self._ui_font_action = ctk.CTkFont(family=UI_FONT_FAMILY,
                                               size=13, weight="bold")
            self._btn_border = "#a04c12"
            self._btn_kw = dict(
                font=self._ui_font_action, width=170, height=35,
                corner_radius=18,
                fg_color="transparent",
                hover_color="#3A3A3A",
                border_color=self._btn_border,
                border_width=2,
                text_color=self._btn_border,
            )
            self._Button = ctk.CTkButton
        else:
            self._btn_kw = {}
            self._btn_border = None
            self._Button = ttk.Button

        # ── Row 0: Everpure logo only ───────────────────────────────────────
        # The horizontal SLA row that used to live here has been moved
        # into the vertical right-sidebar at row 2 column 5 so the SLA
        # inputs sit alongside the Arrays sheet rather than above it.
        # The Save Config toolbar that used to live at row 1 has also
        # moved into that sidebar.

        # Hover tooltip text shared by all three SLA label / entry pairs
        # so users get the same format reminder regardless of which box
        # they hover. Bound to both the label and the entry below.
        _sla_tip = ("Enter SLA's in the format of d h m s. "
                    "For example, 3h 30m 10s.")

        # Shared outer width for the protection report action card and
        # the retention SLA cards in the right sidebar. Pinning both
        # to this exact pixel width (via an invisible shim row) lets
        # them align edge-for-edge regardless of font-metric jitter.
        # Defined here \u2014 ahead of both the right sidebar and the
        # actions matrix \u2014 because the sidebar is built first.
        self._PROT_CARD_WIDTH = 190

        def _sla_box(parent, text, value, width=10, tooltip=None,
                     card_width=None, label_wraplength=None):
            """Vertical SLA group: a slightly-lighter card with the
            descriptive label on top and the entry directly under it.
            The card background (#333333) is one shade lighter than the
            surrounding Configuration pane so each label/entry pair
            reads as a grouped unit. Cards pack left-justified
            (anchor=W) at their natural width so the sidebar's contents
            line up flush against its left edge. Internal label\u2194entry
            spacing is intentionally tight (half the previous gap) so
            each card reads as a compact unit. *tooltip* defaults to
            the d/h/m/s reminder shared by the lag SLAs; the retention
            SLAs supply a days-only message instead. *card_width* pins
            the outer card width via an invisible shim row so the box
            can be made to match a sibling card (e.g. the protection
            report action card) exactly. *label_wraplength* forces the
            label text to wrap at the given pixel width \u2014 used to
            keep long retention labels on two lines without widening
            the card."""
            if HAS_CTK:
                box = ctk.CTkFrame(parent, corner_radius=6,
                                   fg_color="#333333",
                                   border_color="#1F1F1F",
                                   border_width=1)
                box.pack(side=tk.TOP, anchor=tk.W, padx=0, pady=(0, 4))
                # If the caller pinned a target width, lay down a
                # zero-height transparent shim before any visible
                # children so the frame's requested width is forced
                # to *card_width*. fill=tk.X then makes the label and
                # entry stretch to that exact width.
                if card_width is not None:
                    _shim = ctk.CTkFrame(box, fg_color="transparent",
                                         width=card_width, height=1)
                    _shim.pack(side=tk.TOP, fill=tk.X)
                # Bold + #bf5a15 mirrors the "Alert and Replication SLA
                # Report" card title style; fill=tk.X lets the label
                # span the full card width so CTkLabel's default
                # centered anchor positions the text in the middle of
                # the box.
                _lbl_kw = dict(font=self._ui_font_bold,
                               text_color="#bf5a15")
                if label_wraplength:
                    _lbl_kw["wraplength"] = label_wraplength
                    _lbl_kw["justify"]    = "center"
                lbl = ctk.CTkLabel(box, text=text, **_lbl_kw)
                lbl.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(3, 0))
                e = ctk.CTkEntry(box, font=self._ui_font, width=160)
            else:
                box = ttk.Frame(parent)
                box.pack(side=tk.TOP, anchor=tk.W, padx=0, pady=(0, 4))
                lbl = ttk.Label(box, text=text,
                                wraplength=label_wraplength or 160,
                                justify=tk.CENTER, anchor="center")
                lbl.pack(side=tk.TOP, fill=tk.X)
                e = ttk.Entry(box, width=width)
            e.insert(0, value)
            e.pack(side=tk.TOP, anchor=tk.W,
                   padx=8 if HAS_CTK else 0,
                   pady=(1, 4) if HAS_CTK else 0)
            _tip = tooltip if tooltip else _sla_tip
            self._attach_tooltip(lbl, _tip)
            self._attach_tooltip(e,   _tip)
            return e
        # Stash the helper on self so the right-sidebar block (after
        # the Arrays sheet has been built) can build the three SLA
        # cards inside the sidebar without redefining it.
        self._sla_box = _sla_box

        # Logo image: stashed on self.logo_img here; the actual on-screen
        # placement happens inside the right sidebar below so the logo
        # shares column 5 with the Save Config button and SLA cards
        # rather than occupying its own grid row. Under CTk the image is
        # wrapped in CTkImage so HighDPI scaling works correctly (a plain
        # tk.PhotoImage triggers a CTkLabel warning and renders un-scaled
        # on hi-dpi monitors).
        def _place_logo(img):
            self.logo_img = img

        _img_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
        if HAS_PIL and os.path.exists(os.path.join(_img_dir, "Everpure_logo.jpg")):
            try:
                pil_img = Image.open(os.path.join(_img_dir, "Everpure_logo.jpg"))
                base_w = 200
                w_percent = (base_w / float(pil_img.size[0]))
                h_size = int((float(pil_img.size[1]) * float(w_percent)))
                pil_img = pil_img.resize((base_w, h_size), Image.Resampling.LANCZOS)
                if HAS_CTK:
                    _place_logo(ctk.CTkImage(light_image=pil_img,
                                             dark_image=pil_img,
                                             size=(base_w, h_size)))
                else:
                    _place_logo(ImageTk.PhotoImage(pil_img))
            except Exception as e:
                print(f"Error loading logo: {e}")
        elif os.path.exists(os.path.join(_img_dir, "everpure_logo.png")):
            try:
                _png_path = os.path.join(_img_dir, "everpure_logo.png")
                if HAS_CTK and HAS_PIL:
                    pil_img = Image.open(_png_path)
                    if pil_img.size[0] > 200:
                        pil_img = pil_img.resize(
                            (pil_img.size[0] // 3, pil_img.size[1] // 3),
                            Image.Resampling.LANCZOS)
                    _place_logo(ctk.CTkImage(light_image=pil_img,
                                             dark_image=pil_img,
                                             size=pil_img.size))
                else:
                    _img = tk.PhotoImage(file=_png_path)
                    if _img.width() > 200:
                        _img = _img.subsample(3, 3)
                    _place_logo(_img)
            except: pass

        # ── Row 0: Arrays sheet (cols 0-4) + right sidebar (col 5) ──────────
        # The Arrays sheet sits directly under the "Configuration" header
        # strip; _build_arrays_sheet calls rowconfigure(0, weight=1) so
        # the sheet (and the sidebar to its right) absorb any extra
        # vertical space the user gives the Configuration pane.
        # The sidebar at column 5 stacks the Everpure logo at the top,
        # then the Save Config button, then the three SLA-target cards,
        # so the SLA inputs sit alongside the Arrays grid rather than
        # above it. padx=(12, 4) on the sidebar gives the requested
        # breathing room between the sheet and the sidebar widgets.
        self._build_arrays_sheet(config_frame, config)

        # sticky=NW (rather than NS) anchors the sidebar to the LEFT
        # edge of the grid cell so its contents line up with the
        # actions matrix below, whose own outer container also begins
        # at padx=(8, 4). Without NW the sidebar floats centered in
        # the column (which the wider matrix sizes), so the SLA cards
        # would not horizontally align with the "Alert and Replication
        # SLA Report" card. pady=(2, 1) tightens the gap between the
        # sidebar's
        # last SLA card and the actions matrix in row 1.
        right_sidebar = Frame(config_frame)
        if HAS_CTK:
            right_sidebar.configure(fg_color="transparent")
        right_sidebar.grid(row=0, column=5, sticky=tk.NW,
                           padx=(8, 4), pady=(2, 1))

        # Logo at the top of the sidebar. CTkLabel with text="" and a
        # transparent fg_color blends into the dark Configuration pane.
        # The label is captured on self so _align_sidebar_to_arrays_row
        # can subtract its rendered height when sizing the spacer below.
        self._sidebar_logo_label = None
        if getattr(self, 'logo_img', None) is not None:
            if HAS_CTK:
                self._sidebar_logo_label = ctk.CTkLabel(
                    right_sidebar, image=self.logo_img,
                    text="", fg_color="transparent")
            else:
                self._sidebar_logo_label = ttk.Label(
                    right_sidebar, image=self.logo_img)
            self._sidebar_logo_label.pack(side=tk.TOP, anchor=tk.W,
                                          padx=0, pady=(0, 6))

        # Spacer Frame whose height is computed at runtime so the Save
        # Config button's TOP aligns with row 7 of the Arrays sheet.
        # _align_sidebar_to_arrays_row reads arrays_sheet.MT.row_positions
        # after the GUI has been laid out and writes the result to
        # spacer.configure(height=N).
        self._sidebar_top_spacer = Frame(right_sidebar, height=0)
        if HAS_CTK:
            self._sidebar_top_spacer.configure(fg_color="transparent")
        # propagate=False keeps the spacer at its requested height even
        # when it has no children adding to its natural size.
        try:
            self._sidebar_top_spacer.pack_propagate(False)
        except Exception:
            pass
        self._sidebar_top_spacer.pack(side=tk.TOP, fill=tk.X)

        # Save Config below the spacer, then the three SLA cards. Each
        # is packed with anchor=W (no fill) so the widgets are left-
        # justified within the sidebar at their natural width \u2014 the
        # same horizontal alignment as the actions matrix below.
        self._sidebar_save_btn = self._Button(
            right_sidebar, text="Save Config",
            command=self._save_config, **self._btn_kw)
        self._sidebar_save_btn.pack(side=tk.TOP, anchor=tk.W,
                                    padx=0, pady=(0, 7))

        # Two-column SLA card area. The left column holds the three
        # replication-lag SLAs (unchanged); the right column holds the
        # two Protection Group retention SLAs introduced for the
        # Volume & Snapshot Protection report. The right column is
        # padded so its left edge lines up with the "Volume and
        # Filesystem Protection Report" card in the actions matrix
        # below.
        sla_cols = Frame(right_sidebar)
        if HAS_CTK:
            sla_cols.configure(fg_color="transparent")
        sla_cols.pack(side=tk.TOP, anchor=tk.W, fill=tk.X)

        sla_left_col = Frame(sla_cols)
        if HAS_CTK:
            sla_left_col.configure(fg_color="transparent")
        sla_left_col.pack(side=tk.LEFT, anchor=tk.NW, padx=(0, 6))

        sla_right_col = Frame(sla_cols)
        if HAS_CTK:
            sla_right_col.configure(fg_color="transparent")
        sla_right_col.pack(side=tk.LEFT, anchor=tk.NW)

        self.sla_fb_entry  = self._sla_box(sla_left_col,
            "FlashBlade Target SLA:",       config.get("sla_fb",  "1h 30m"))
        self.sla_faf_entry = self._sla_box(sla_left_col,
            "FlashArray Pod Target SLA:",   config.get("sla_faf", "1h"))
        self.sla_fab_entry = self._sla_box(sla_left_col,
            "FlashArray Async Target SLA:", config.get("sla_fab", "1h"))

        # Protection-retention SLA tooltip: integer days only (e.g. 7,
        # 14, 30). The Max PG retention values reported in the Volume
        # & Snapshot Protection HTML are calculated in days, so these
        # thresholds compare apples-to-apples without unit conversion.
        _ret_tip = ("Enter the minimum acceptable retention in whole "
                    "days (e.g. 7, 14, 30). 0 disables the SLA check.")
        # card_width pins the outer card to the same pixel width as
        # the "Volume and Filesystem Protection Report" action card
        # below; label_wraplength forces the long retention label
        # text onto two lines so the title fits within that width.
        self.sla_retention_snap_entry = self._sla_box(sla_right_col,
            "Minimum Snapshot Retention SLA:",
            config.get("sla_retention_snap", "7"), tooltip=_ret_tip,
            card_width=self._PROT_CARD_WIDTH, label_wraplength=150)
        self.sla_retention_repl_entry = self._sla_box(sla_right_col,
            "Minimum Replicated Retention SLA:",
            config.get("sla_retention_repl", "7"), tooltip=_ret_tip,
            card_width=self._PROT_CARD_WIDTH, label_wraplength=150)

        # ── Row 1: Excluded Alerts wrapped in a CTkFrame "panel" ────────────
        # The panel covers columns 0-4 (columnspan=5) so its right edge
        # lines up exactly with the Arrays sheet above; the actions
        # matrix lives in the separate column 5 (under the right
        # sidebar) so it doesn't change the alerts sheet's width.
        # The embedded tksheet
        # hosts all four columns (# index gutter, Alert Codes,
        # Description, Justification) and the header strip carries the
        # title on the left and the FB / FA alert-code reference links
        # on the right. The sheet itself sits below at a fixed 3-row
        # height with vertical scroll for the remaining rows.
        alerts_panel = Frame(config_frame)
        if HAS_CTK:
            alerts_panel.configure(fg_color="transparent")
        alerts_panel.grid(row=1, column=0, columnspan=5, sticky=tk.NSEW,
                          padx=(0, 0), pady=2)

        alerts_header = Frame(alerts_panel)
        if HAS_CTK:
            alerts_header.configure(fg_color="transparent")
        alerts_header.pack(side=tk.TOP, fill=tk.X, padx=0, pady=(6, 0))
        Label(alerts_header,
              text="Excluded Alerts (Partial Match or ID Range):",
              font=self._ui_font_bold if HAS_CTK else None,
              justify=tk.LEFT).pack(side=tk.LEFT, anchor=tk.W)

        # FB / FA alert-code reference links anchored to the right edge
        # of the header so the sheet body can use the full panel width.
        _link_fg = "#5aa9ff" if HAS_CTK else "#1a5fb4"
        def _make_link(parent, text, url):
            lbl = tk.Label(parent, text=text, fg=_link_fg, cursor="hand2",
                           font=(UI_FONT_FAMILY, 10, "underline"))
            if HAS_CTK:
                # Match the surrounding dark canvas so the link doesn't
                # render on a bright tk default background.
                try:
                    lbl.configure(bg=ctk.ThemeManager.theme["CTk"]["fg_color"][1])
                except Exception:
                    pass
            lbl.bind("<Button-1>", lambda _e, u=url: webbrowser.open_new_tab(u))
            return lbl
        _fb_alerts_url = ("https://support.purestorage.com/bundle/m_purityfb_alerts/"
                          "page/FlashBlade/Purity_FB/topics/concept/c_purityfb_alerts.html")
        _fa_alerts_url = ("https://support.purestorage.com/bundle/m_purityfa_alerts/"
                          "page/FlashArray/PurityFA/topics/concept/c_purityfa_alerts.html")
        _make_link(alerts_header, "FlashArray Alert Code Reference",
                   _fa_alerts_url).pack(side=tk.RIGHT, padx=(8, 0))
        _make_link(alerts_header, "FlashBlade Alert Code Reference",
                   _fb_alerts_url).pack(side=tk.RIGHT, padx=(8, 0))

        # Build the dark-themed alerts tksheet directly inside the
        # panel. _build_alerts_sheet handles row data load (preferring
        # the new alerts_excluded_rows list, falling back to the legacy
        # alerts_excluded string) and pins self.alerts_sheet for the
        # save/run paths to read via _get_excluded_codes / _get_alerts_rows.
        self._build_alerts_sheet(alerts_panel, config)

        # ── Row 1, Column 5: Actions matrix ─────────────────────────────────
        # Two visually-grouped cards (CTkFrame with a 2px border each)
        # consolidate all report actions: the left card drives the
        # Alert and Replication SLA Report (Run / Open / History); the
        # right card drives the Volume and Filesystem Protection Report
        # (Run / Open). The matrix lives in column 5 (the same column
        # as the right sidebar above) so its width does not steal
        # horizontal space from the Arrays / Alerts sheets, which keep
        # their column 0-4 extent.
        self._build_actions_matrix(config_frame, row=1, column=5)

        # ── Row 2: Ignore Source Lag checkbox + explanatory note ────────────
        self.ignore_source_lag_var = tk.BooleanVar(value=config.get("ignore_source_lag", False))
        fab_note_frame = Frame(config_frame)
        if HAS_CTK:
            fab_note_frame.configure(fg_color="transparent")
        fab_note_frame.grid(row=2, column=0, columnspan=6, sticky=tk.W,
                            padx=5, pady=(5, 2))

        if HAS_CTK:
            # Match the "Modern Dark" button palette so the checkbox box
            # reads as the same recessed-bezel widget. checkmark_color
            # is light (#D1D1D1) so the tick stays visible against the
            # dark fg_color when the box is checked.
            ctk.CTkCheckBox(fab_note_frame,
                            text="Ignore Source Side Replica Reporting.",
                            variable=self.ignore_source_lag_var,
                            font=self._ui_font,
                            corner_radius=4,
                            fg_color="#2B2B2B",
                            hover_color="#323232",
                            border_color="#1F1F1F",
                            border_width=2,
                            text_color="#D1D1D1",
                            checkmark_color="#bf5a15",
                            ).pack(side=tk.LEFT, padx=(0, 12))
        else:
            ttk.Checkbutton(fab_note_frame,
                            text="Ignore Source Side Replica Reporting.      ",
                            variable=self.ignore_source_lag_var).pack(side=tk.LEFT)
        note_text_frame = Frame(fab_note_frame)
        if HAS_CTK:
            note_text_frame.configure(fg_color="transparent")
        note_text_frame.pack(side=tk.LEFT)
        sub_line_frame = Frame(note_text_frame)
        if HAS_CTK:
            sub_line_frame.configure(fg_color="transparent")
        sub_line_frame.pack(side=tk.TOP, anchor=tk.W)
        Label(sub_line_frame, text="Use only ",
              font=self._ui_font if HAS_CTK else None).pack(side=tk.LEFT)
        Label(sub_line_frame, text="Destination",
              font=self._ui_font_bold if HAS_CTK else ("Segoe UI", 9, "bold")
              ).pack(side=tk.LEFT)
        Label(sub_line_frame,
              text=" array for FA-Block snapshot replication reporting.",
              font=self._ui_font if HAS_CTK else None).pack(side=tk.LEFT)

        # Email config stored as plain attrs; edited via Email/SMTP menu dialog
        self._smtp_server = config.get("smtp_server", "")
        self._smtp_port   = config.get("smtp_port",   "587")
        self._smtp_from   = config.get("smtp_from",   "")
        self._smtp_to     = config.get("smtp_to",     "")

        # ── Lower pane: button bar + run-output log ──────────────────────────
        lower_pane = Frame(main_paned)
        if HAS_CTK:
            lower_pane.configure(fg_color="transparent")
        main_paned.add(lower_pane, weight=2)

        # Force the sash so the lower pane (button bar + output log) takes
        # ~15% of the window height on first paint, leaving the rest for
        # the Configuration pane / Arrays sheet. A previously saved
        # main_sash_pos (written by _save_config) wins when present and
        # still inside the live paned area, so user resize survives a
        # restart. Otherwise the position is computed from the live window
        # geometry so it scales with the initial size.
        # ttk.PanedWindow runs its own weight-based natural layout
        # asynchronously after Configure events, so an immediate
        # after_idle sashpos() call gets overwritten by that layout
        # pass. Instead, latch the first <Configure> with a real
        # paned height and apply the sash there. A delayed after()
        # also fires as a backup in case <Configure> doesn't run
        # (e.g. when the saved geometry exactly matches the default).
        # Once applied, _initial_sash_done blocks further overrides
        # so the user's later sash drags are preserved.
        self._initial_sash_done = False
        def _set_initial_sash(event=None):
            if self._initial_sash_done:
                return
            try:
                paned_h = main_paned.winfo_height()
                if paned_h < 200:   # still laying out
                    return
                # Floor for the top pane: at least 12 visible rows in
                # the Arrays sheet at first paint.
                #   12 rows × 24 px + 23 px header + 14 px h-scrollbar
                #   ≈ 325 px sheet, plus ~170 px for the SLA row /
                #   Excluded Alerts panel / ignore checkbox row inside
                #   config_frame, plus ~34 px for the Configuration
                #   header strip on top of config_outer.
                min_top = 12 * 24 + 23 + 14 + 170 + 34   # ≈ 529 px
                max_top = max(min_top + 1, paned_h - 60)
                _saved = self.config_data.get('main_sash_pos') if not FAKE_ARRAYS else None
                if isinstance(_saved, int) and 100 < _saved < max(101, paned_h - 60):
                    sash = min(max(_saved, min_top), max_top)
                else:
                    bottom_px = int(self.winfo_height() * 0.07) + 30
                    sash = min(max(paned_h - bottom_px, min_top), max_top)
                main_paned.sashpos(0, sash)
                # Verify; some Tk builds re-run layout once more after
                # this call, so re-apply on the next idle to win the
                # final pass.
                self.after_idle(lambda: main_paned.sashpos(0, sash))
                self._initial_sash_done = True
            except Exception:
                pass
        main_paned.bind('<Configure>', _set_initial_sash, add='+')
        self.after(150, _set_initial_sash)

        # The legacy lower button bar has been retired: Save Config now
        # lives in its own toolbar above the Arrays sheet, and the five
        # report-action buttons (Run / Open / History for Alert+SLA, and
        # Run / Open for Vol+FS) have been consolidated into the actions
        # matrix in the Configuration pane (see _build_actions_matrix).
        # last_protection_path is still pinned here so the protection
        # collector can stash today's path on completion.
        self.last_protection_path = None

        # height clamps the requested size so the textbox doesn't out-bid
        # the Configuration pane on first paint; fill/expand still let it
        # grow when the user drags the sash. The sashpos override above
        # drives the actual rendered height (~15% of window).
        if HAS_CTK:
            self.text_out = ctk.CTkTextbox(lower_pane, wrap="none", height=120,
                                           font=self._ui_font, border_width=1)
            self.text_out.pack(fill=tk.BOTH, expand=True, padx=4, pady=(0, 4))
        else:
            self.text_out = scrolledtext.ScrolledText(lower_pane, wrap=tk.NONE, height=6)
            self.text_out.pack(fill=tk.BOTH, expand=True)

    def _build_actions_matrix(self, parent, row, column):
        """Build the 2-column actions matrix to the right of the alerts sheet.

        Layout (rows top-to-bottom, columns left-to-right):
            ┌─ Alert and Replication ─┐ ┌─ Volume and Filesystem ─┐
            │      SLA Report         │ │   Protection Report     │
            │  [ Run Daily Report   ] │ │  [ Run Daily Report   ] │
            │  [ Open Daily Report  ] │ │  [ Open Daily Report  ] │
            │  [ Open History Rpt.  ] │ │  [ Manage Exceptions  ] │
            └─────────────────────────┘ └─────────────────────────┘

        Each column is its own bordered CTkFrame so the grouping reads
        as a single action surface. Buttons are pinned to self.* names
        so external code (run_report toggling, protection completion
        handler, startup helpers) can configure their state.
        """
        # Outer container holds both grouped columns side-by-side.
        Frame = ctk.CTkFrame if HAS_CTK else ttk.Frame
        outer = Frame(parent)
        if HAS_CTK:
            outer.configure(fg_color="transparent")
        outer.grid(row=row, column=column, sticky=tk.NW,
                   padx=(8, 4), pady=(1, 2))

        # Per-column "card": bordered CTkFrame with a header label and
        # a stack of full-width buttons. The shared width
        # (self._PROT_CARD_WIDTH) is set at the top of
        # _build_arrays_sheet so the right-sidebar SLA cards can
        # also pin to it.
        def _make_card(title, side_padx):
            if HAS_CTK:
                card = ctk.CTkFrame(outer,
                                    fg_color="#252525",
                                    border_color="#1F1F1F",
                                    border_width=2,
                                    corner_radius=8)
            else:
                card = ttk.LabelFrame(outer, text=title)
            card.pack(side=tk.LEFT, fill=tk.Y, padx=side_padx, pady=0)
            if HAS_CTK:
                # Width-pinning shim: zero-height transparent frame
                # whose explicit width promotes the parent card's
                # requested width to exactly _PROT_CARD_WIDTH, so
                # both action cards (and the retention SLA cards in
                # the right sidebar) match edge-for-edge.
                _shim = ctk.CTkFrame(card, fg_color="transparent",
                                     width=self._PROT_CARD_WIDTH,
                                     height=1)
                _shim.pack(side=tk.TOP, fill=tk.X)
                # wraplength=170 keeps the longer card titles on two
                # lines without forcing the cards wider than the
                # ~180 px button stack beneath them.
                ctk.CTkLabel(card, text=title,
                             font=self._ui_font_bold,
                             wraplength=170,
                             justify="center",
                             text_color="#bf5a15").pack(side=tk.TOP,
                                                        padx=10,
                                                        pady=(8, 4))
            return card

        alert_card = _make_card("Alert and Replication SLA Report",
                                side_padx=(0, 6))
        vol_card   = _make_card("Volume and Filesystem Protection Report",
                                side_padx=(0, 0))

        # Slightly narrower buttons than the legacy lower-bar style so
        # both columns fit comfortably alongside the alerts sheet on
        # typical 1440-px-wide windows. Inherits the rest of the
        # "Modern Dark" palette from self._btn_kw.
        _matrix_kw = dict(self._btn_kw)
        if HAS_CTK:
            _matrix_kw["width"] = 160

        def _btn(card, text, command, state=tk.NORMAL):
            b = self._Button(card, text=text, command=command,
                             state=state, **_matrix_kw)
            b.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(0, 8))
            return b

        # ── Alert and SLA column ────────────────────────────────────────
        self.run_btn = _btn(alert_card, "Run Daily Report", self.run_report)
        self.open_daily_btn = _btn(alert_card, "Open Daily Report",
                                   self._open_daily_report,
                                   state=tk.DISABLED)
        _btn(alert_card, "Open History Report", self._show_health_history)

        # ── Vol and FS column ───────────────────────────────────────────
        self.protect_btn = _btn(vol_card, "Run Daily Report",
                                self._run_protection_report)
        self.open_protection_btn = _btn(vol_card, "Open Daily Report",
                                        self._open_today_protection_report,
                                        state=tk.DISABLED)
        # "Manage Exceptions" opens the per-row Config Drift Exceptions
        # editor sourced from the exceptions.json sidecar that
        # build_protection_html updates at report-generation time.
        _btn(vol_card, "Manage Exceptions",
             self._open_exceptions_dialog)

        # Enable the open-buttons at startup if today's reports are
        # already on disk from an earlier run/launch, so the user can
        # re-open without first having to click "Run Daily Report".
        try:
            if os.path.exists(self._today_daily_report_path()):
                self.open_daily_btn.configure(state=tk.NORMAL)
        except Exception:
            pass
        try:
            date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
            script_dir = os.path.dirname(os.path.abspath(__file__))
            _vol_path  = os.path.join(script_dir, "reports", "protection",
                f"Pure_Volume_Snapshot_Protection_{date_str}.html")
            if os.path.exists(_vol_path):
                self.open_protection_btn.configure(state=tk.NORMAL)
        except Exception:
            pass
        # Pre-enable the Logs menu's "Open Summary" / "Open Logs" entries
        # if today's files are already on disk, mirroring the matrix's
        # Open Daily Report buttons. Without this the entries stay disabled
        # on a fresh launch even though the files exist, which makes
        # the menu items appear unresponsive.
        try:
            if os.path.exists(self._today_summary_log_path()):
                self.reports_menu.entryconfig(self._open_summary_idx,
                                              state=tk.NORMAL)
        except Exception:
            pass
        try:
            if os.path.exists(self._today_detailed_log_path()):
                self.reports_menu.entryconfig(self._open_logs_idx,
                                              state=tk.NORMAL)
        except Exception:
            pass

    def check_queue(self):
        if password_request_event.is_set():
            password_request_event.clear()
            pwd = simpledialog.askstring("Password Required", _common.global_password_request_msg, show='*', parent=self)
            _common.global_password_response = pwd
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
        arrays = [{"name": n, "location": l, "notes": nt, "auth_user": au}
                  for n, l, nt, au in self._get_arrays_from_sheet()]
        # Capture the current Arrays-sheet column widths so any user-driven
        # resize (drag separator / double-click auto-fit) survives restart.
        col_widths = []
        try:
            _sheet = getattr(self, 'arrays_sheet', None)
            if _sheet is not None:
                col_widths = [int(_sheet.column_width(column=0)),
                              int(_sheet.column_width(column=1)),
                              int(_sheet.column_width(column=2)),
                              int(_sheet.column_width(column=3))]
        except Exception:
            col_widths = []
        # Capture the current window geometry ("WIDTHxHEIGHT+X+Y") and the
        # main vertical PanedWindow sash position. Both are re-applied in
        # __init__ / _set_initial_sash on next launch so the user sees the
        # same layout they had when they hit Save Config.
        try:
            window_geometry = self.geometry()
        except Exception:
            window_geometry = ""
        main_sash_pos = None
        try:
            _paned = getattr(self, '_main_paned', None)
            if _paned is not None:
                main_sash_pos = int(_paned.sashpos(0))
        except Exception:
            main_sash_pos = None
        # Persist alerts both as the new structured rows list (full 3
        # columns) and as a comma-joined legacy string so older builds
        # of the app or any external readers that still parse the old
        # alerts_excluded field continue to work without a schema bump.
        alerts_rows  = self._get_alerts_rows()
        alerts_codes = self._get_excluded_codes()
        # Capture the current Alerts-sheet column widths the same way as
        # the Arrays sheet so any user-driven resize survives restart.
        alerts_col_widths = []
        try:
            _asheet = getattr(self, 'alerts_sheet', None)
            if _asheet is not None:
                alerts_col_widths = [int(_asheet.column_width(column=0)),
                                     int(_asheet.column_width(column=1)),
                                     int(_asheet.column_width(column=2))]
        except Exception:
            alerts_col_widths = []
        data = {
            "alerts_excluded_rows": alerts_rows,
            "alerts_excluded":      ", ".join(alerts_codes),
            "alerts_col_widths":    alerts_col_widths,
            "arrays": arrays,
            "arrays_col_widths": col_widths,
            "window_geometry": window_geometry,
            "main_sash_pos":   main_sash_pos,
            "sla_fb": self.sla_fb_entry.get().strip(),
            "sla_faf": self.sla_faf_entry.get().strip(),
            "sla_fab": self.sla_fab_entry.get().strip(),
            "sla_retention_snap": self.sla_retention_snap_entry.get().strip(),
            "sla_retention_repl": self.sla_retention_repl_entry.get().strip(),
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

        excluded_codes = self._get_excluded_codes()
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
            for n, l, _nt, _au in self._get_arrays_from_sheet():
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

    def _align_sidebar_to_arrays_row(self, row_index=4):
        """Resize ``self._sidebar_top_spacer`` so the Save Config button
        sits at the same Y coordinate as the *row_index*-th row of the
        Arrays sheet (1-based: row 4 by default).

        Reads ``self.arrays_sheet.MT.row_positions`` to learn where each
        row begins relative to the sheet's data canvas, then subtracts
        the already-consumed vertical space (logo height + its bottom
        padding) so the spacer fills exactly the remaining gap. Safe to
        call before the sheet is fully realized \u2014 falls back to a
        sensible default if row positions are not yet computed.

        When the window's available vertical space is shorter than the
        row-4 offset would demand, the spacer is clamped so the Save
        Config button and the three SLA cards stay visible; only when
        even a 0-height spacer can't fit them all do they get clipped.
        """
        spacer = getattr(self, '_sidebar_top_spacer', None)
        sheet  = getattr(self, 'arrays_sheet',       None)
        if spacer is None or sheet is None:
            return
        try:
            sheet.update_idletasks()
            # row_positions[i] = top-Y of row (i+1) relative to MT.
            n = max(1, int(row_index)) - 1
            row_positions = list(sheet.MT.row_positions)
            if n >= len(row_positions):
                n = len(row_positions) - 1
            row_top_in_mt = row_positions[n]
            # Header sits above MT; its height pushes row 0 down on
            # screen but doesn't appear in row_positions.
            header_h = sheet.CH.winfo_height() if hasattr(sheet, 'CH') else 0
            target_offset = header_h + row_top_in_mt
            # Subtract the logo's rendered height + its 6 px bottom pad
            # so the spacer fills only the remainder.
            logo = getattr(self, '_sidebar_logo_label', None)
            logo_h = 0
            if logo is not None:
                try:
                    logo.update_idletasks()
                    logo_h = logo.winfo_height() + 6
                except Exception:
                    logo_h = 0
            desired_spacer = max(0, target_offset - logo_h)
            # Clamp against the actual height available alongside the
            # arrays sheet so a vertical window-shrink pulls the Save
            # Config button + SLA cards upward instead of pushing them
            # off the bottom. _sheet_frame is the row-0 grid cell with
            # rowconfigure weight=1, so its winfo_height tracks the
            # live row height; the natural reqheight of Save Config +
            # the 3 SLA card boxes (incl. their pady) is the minimum
            # space that must remain below the spacer.
            try:
                sheet_frame = getattr(self, '_sheet_frame', None)
                if sheet_frame is not None:
                    sheet_frame.update_idletasks()
                    avail_h = sheet_frame.winfo_height()
                    if avail_h <= 1:
                        avail_h = sheet_frame.winfo_reqheight()
                    controls_h = 0
                    save_btn = getattr(self, '_sidebar_save_btn', None)
                    if save_btn is not None:
                        save_btn.update_idletasks()
                        # pack pady=(0, 7) on the Save Config button.
                        controls_h += save_btn.winfo_reqheight() + 7
                    # The left column drives the spacer height; it is
                    # always at least as tall as the right column (3
                    # cards vs. 2). Iterating only the left-column
                    # entries keeps the existing alignment math
                    # unchanged when the right-column retention SLA
                    # cards were introduced.
                    for entry_attr in ('sla_fb_entry',
                                       'sla_faf_entry',
                                       'sla_fab_entry'):
                        e = getattr(self, entry_attr, None)
                        if e is not None and e.winfo_exists():
                            box = e.master
                            box.update_idletasks()
                            # _sla_box packs each card with pady=(0, 4).
                            controls_h += box.winfo_reqheight() + 4
                    max_spacer = avail_h - logo_h - controls_h
                    if max_spacer < 0:
                        max_spacer = 0
                    if desired_spacer > max_spacer:
                        desired_spacer = max_spacer
            except Exception:
                pass
            spacer.configure(height=desired_spacer)
        except Exception:
            pass

    def _show_busy_spinner(self, message="Running report..."):
        """No-op kept for call-site compatibility.

        The inline status label that used to render in the upper-right
        of the Configuration pane has been removed at the user's
        request \u2014 per-phase progress now appears only in the lower
        text log (``self.text_out``), which ``_update_busy_status``
        already writes to. The *message* argument is unused.
        """
        # Pin attribute names so _hide_busy_spinner / _update_busy_status
        # always see something defined even on first call.
        self._busy_stop = True
        self._busy_status_label = None
        self._busy_spinner_win  = None

    def _spin_busy_tick(self):
        """Retained for backward compatibility \u2014 the rotating-logo
        animation has been removed, so this is now a no-op."""
        return

    def _hide_busy_spinner(self):
        """Tear down the inline busy spinner if it's currently shown.
        Destroying the container Frame also removes its spinner and
        status-text children from the Configuration grid, so the slot
        under the Everpure logo is freed until the next Run Daily Report
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
        self.run_btn.configure(state=tk.NORMAL) # Reset in thread
        self.run_btn.configure(state=tk.DISABLED)
        self.text_out.delete("1.0", tk.END)
        self.text_out.insert(tk.END, "Polling arrays... Please wait.\n\n")
        self._show_busy_spinner("Polling arrays... Please wait.")
        # Unified arrays list (name, location). SSH-based classification happens
        # inside run_collection_core, which fans out arr_fb/arr_faf/arr_fab.
        _arrays = [{'name': n, 'location': l, 'notes': nt, 'auth_user': au}
                   for n, l, nt, au in self._get_arrays_from_sheet()]
        cfg = {
            'arrays': _arrays,
            'sla_fb': parse_time_to_seconds(self.sla_fb_entry.get()),
            'sla_faf': parse_time_to_seconds(self.sla_faf_entry.get()),
            'sla_fab': parse_time_to_seconds(self.sla_fab_entry.get()),
            'excluded': self._get_excluded_codes(),
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

    def _today_summary_log_path(self):
        """Return today's Summary .log path on disk (matches _auto_save_reports)."""
        date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, "reports", "summary",
            f"Pure Alert and Replication Lag Summary {date_str}.log")

    def _today_detailed_log_path(self):
        """Return today's Detailed .log path on disk (matches _auto_save_reports)."""
        date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, "reports", "logs",
            f"Pure Alert and Replication Lag Logs {date_str}.log")

    def _open_summary(self):
        # Prefer the in-session path the run-completion handler stashed
        # so the menu always opens the latest file from this run; fall
        # back to today's file on disk so the menu still works on a
        # fresh launch where last_summary_path is still None.
        path = self.last_summary_path
        if not path or not os.path.exists(path):
            today_path = self._today_summary_log_path()
            if os.path.exists(today_path):
                path = today_path
                self.last_summary_path = path
        if path and os.path.exists(path):
            os.startfile(os.path.abspath(path))
        else:
            messagebox.showinfo(
                "No summary for today",
                "No Summary log has been generated yet for today.\n\n"
                'Click "Run Daily Report" under "Alert and Replication SLA '
                'Report" first.',
                parent=self)

    def _open_logs(self):
        # Same fallback strategy as _open_summary: prefer the session
        # path, fall back to today's detailed log on disk.
        path = self.last_log_path
        if not path or not os.path.exists(path):
            today_path = self._today_detailed_log_path()
            if os.path.exists(today_path):
                path = today_path
                self.last_log_path = path
        if path and os.path.exists(path):
            os.startfile(os.path.abspath(path))
        else:
            messagebox.showinfo(
                "No logs for today",
                "No Detailed log has been generated yet for today.\n\n"
                'Click "Run Daily Report" under "Alert and Replication SLA '
                'Report" first.',
                parent=self)

    def _today_daily_report_path(self):
        """Return the absolute path to today's Daily HTML report on disk.

        Mirrors the path that _auto_save_reports writes to:
            reports/daily/Pure Array Report <YYYY-MM-DD>.html
        Used by both _open_daily_report (as a fallback when no report
        has been generated in the current session) and the startup
        helper that pre-enables the Open Daily Report button when today's
        file is already present from an earlier launch.
        """
        date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, "reports", "daily",
                            f"Pure Array Report {date_str}.html")

    def _open_daily_report(self):
        # Prefer the in-session path the run-completion handler stashed
        # so the button always opens the latest file from this run; fall
        # back to today's file on disk so the button still works on a
        # fresh launch where last_html_path is still None.
        path = self.last_html_path
        if not path or not os.path.exists(path):
            today_path = self._today_daily_report_path()
            if os.path.exists(today_path):
                path = today_path
                self.last_html_path = path
        if path and os.path.exists(path):
            os.startfile(os.path.abspath(path))
        else:
            messagebox.showinfo(
                "No report for today",
                "No Daily report has been generated yet for today.\n\n"
                'Click "Run Daily Report" under "Alert and Replication SLA '
                'Report" first.',
                parent=self)

    # ── Volume & Snapshot Protection ────────────────────────────────────
    def _run_protection_report(self):
        """Kick off the independent protection-report collection in a
        worker thread so the GUI stays responsive."""
        self.protect_btn.configure(state=tk.DISABLED)
        self.text_out.insert(tk.END,
            "\nCollecting volume & snapshot protection data... Please wait.\n")
        self.text_out.see(tk.END)
        self._show_busy_spinner("Collecting volume & snapshot protection data...")
        _arrays = [{'name': n, 'location': l, 'notes': nt, 'auth_user': au}
                   for n, l, nt, au in self._get_arrays_from_sheet()]
        cfg = {
            'arrays':   _arrays,
            # Whole-day retention SLA thresholds for the Volume &
            # Snapshot Protection report. _parse_sla_days accepts a
            # bare integer ("7") or "Nd" form; falls back to 0 (which
            # disables the per-row SLA pass/fail check).
            'sla_retention_snap': _parse_sla_days(
                self.sla_retention_snap_entry.get(), 0),
            'sla_retention_repl': _parse_sla_days(
                self.sla_retention_repl_entry.get(), 0),
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
                # Enable the matrix's Open-Report button now that today's
                # report exists on disk; the user opens it explicitly via
                # that button rather than having it auto-pop a browser.
                try:
                    if hasattr(self, 'open_protection_btn'):
                        self.open_protection_btn.configure(state=tk.NORMAL)
                except Exception:
                    pass
            self.after(0, _done)
        except Exception as e:
            self.after(0, lambda err=e: self.text_out.insert(
                tk.END, f"Protection report failed: {err}\n"))
        finally:
            self.after(0, self._hide_busy_spinner)
            self.after(0, lambda: self.protect_btn.configure(state=tk.NORMAL))

    def _open_protection_report(self):
        if self.last_protection_path and os.path.exists(self.last_protection_path):
            os.startfile(os.path.abspath(self.last_protection_path))

    def _open_today_protection_report(self):
        """Open today's Vol and FS protection report from disk.

        Resolves the same path _run_protection_report writes to
        (reports/protection/Pure_Volume_Snapshot_Protection_<YYYY-MM-DD>.html)
        so the user can re-open today's report without re-collecting,
        even on a fresh launch where last_protection_path is None.
        """
        date_str   = datetime.datetime.now().strftime("%Y-%m-%d")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_path   = os.path.join(
            script_dir, "reports", "protection",
            f"Pure_Volume_Snapshot_Protection_{date_str}.html")
        if os.path.exists(out_path):
            try: os.startfile(os.path.abspath(out_path))
            except Exception as e:
                messagebox.showerror("Open failed",
                                     f"Could not open the report:\n{e}",
                                     parent=self)
        else:
            messagebox.showinfo(
                "No report for today",
                f"No Volume and Filesystem Protection report has been "
                f"generated yet for {date_str}.\n\n"
                "Click \"Run Daily Report\" under \"Volume and Filesystem "
                "Protection Report\" to generate one.",
                parent=self)

    def _open_exceptions_dialog(self):
        """Open the Config Drift Exceptions editor.

        Loads reports/protection/exceptions.json (populated by the last
        protection-report run), shows one row per recorded entry, and
        lets the user pick an exception reason per row from a fixed
        list. On Save changed rows are timestamped MM-DD-YYYY-HH:MM:SS
        and rows transitioning from red to a non-"Breaking SLA" reason
        flip color to grey; selecting "None" or clearing returns the
        row to its discovered color on next report run.
        """
        data = _load_exceptions()
        if not data:
            messagebox.showinfo(
                "No exceptions to manage",
                "No exceptions.json has been created yet.\n\n"
                "Run the Volume and Filesystem Protection Report first; "
                "rows are auto-discovered into "
                "reports/protection/exceptions.json on each run.",
                parent=self)
            return
        # Modal CTk/Tk window. CTkToplevel is used when CTk is available
        # so the dialog inherits the dark theme; falls back to plain
        # tk.Toplevel otherwise.
        Top = ctk.CTkToplevel if HAS_CTK else tk.Toplevel
        win = Top(self)
        win.title("Manage Config Drift Exceptions")
        win.geometry("960x560")
        win.transient(self)
        try: win.grab_set()
        except Exception: pass
        # Scrollable body: CTkScrollableFrame when CTk is installed,
        # plain Canvas + Frame fallback for the no-CTk path.
        if HAS_CTK:
            body = ctk.CTkScrollableFrame(win, width=920, height=470)
            body.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 0))
            inner = body
        else:
            canv = tk.Canvas(win, highlightthickness=0)
            vbar = ttk.Scrollbar(win, orient=tk.VERTICAL,
                                 command=canv.yview)
            canv.configure(yscrollcommand=vbar.set)
            canv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True,
                      padx=(10,0), pady=(10,0))
            vbar.pack(side=tk.LEFT, fill=tk.Y, pady=(10,0))
            inner = tk.Frame(canv)
            canv.create_window((0,0), window=inner, anchor='nw')
            inner.bind('<Configure>', lambda e: canv.configure(
                scrollregion=canv.bbox('all')))
        # Column headers
        hdr_cells = ('Array', 'Type', 'Name', 'Last Updated',
                     'Exception Reason')
        for ci, txt in enumerate(hdr_cells):
            lbl_kw = {'text': txt, 'font': (UI_FONT_FAMILY, 10, 'bold')}
            if HAS_CTK:
                lbl = ctk.CTkLabel(inner, **lbl_kw)
            else:
                lbl = tk.Label(inner, **lbl_kw)
            lbl.grid(row=0, column=ci, sticky='w', padx=6, pady=(0, 6))
        # Build one row per (sorted) key. Capture the ComboBox / Var so
        # _save can diff against the original exception_reason value.
        sorted_keys = sorted(data.keys(),
                             key=lambda k: (data[k].get('array_name',''),
                                            data[k].get('volume_name','')))
        row_widgets = []
        _COLOR_FG = {'green': '#2f7a3a', 'red': '#a32030',
                     'grey':  '#5a6268'}
        for ri, key in enumerate(sorted_keys, start=1):
            rec = data[key]
            arr  = rec.get('array_name', '')
            typ  = rec.get('array_type', '')
            nm   = rec.get('volume_name', '')
            upd  = rec.get('Last_Update', '') or '—'
            cur  = rec.get('exception_reason', 'None') or 'None'
            color = (rec.get('color') or 'grey').lower()
            fg    = _COLOR_FG.get(color, _COLOR_FG['grey'])
            self._cde_render_row(inner, ri, arr, typ, nm, upd, cur, fg,
                                 row_widgets, key)
        # Footer with Save / Cancel buttons.
        btn_row = tk.Frame(win) if not HAS_CTK else ctk.CTkFrame(
            win, fg_color="transparent")
        btn_row.pack(fill=tk.X, padx=10, pady=10)
        def _save():
            self._cde_save(data, row_widgets, win)
        def _cancel():
            try: win.grab_release()
            except Exception: pass
            win.destroy()
        if HAS_CTK:
            ctk.CTkButton(btn_row, text="Save",
                          command=_save, width=110).pack(
                side=tk.RIGHT, padx=(6, 0))
            ctk.CTkButton(btn_row, text="Cancel",
                          command=_cancel, width=110,
                          fg_color="gray").pack(side=tk.RIGHT)
        else:
            tk.Button(btn_row, text="Save", command=_save,
                      width=12).pack(side=tk.RIGHT, padx=(6, 0))
            tk.Button(btn_row, text="Cancel", command=_cancel,
                      width=12).pack(side=tk.RIGHT)

    def _cde_render_row(self, parent, ri, arr, typ, nm, upd, cur, fg,
                        row_widgets, key):
        """Render a single Manage Exceptions row and record its widgets.

        The Name column is colored to match the row's exceptions.json
        color verdict (green / red / grey). The exception-reason
        CTkComboBox is populated from EXCEPTION_CHOICES; the current
        value is selected if it's a member of the list, otherwise the
        value is shown verbatim (so manual JSON edits survive).
        """
        cells = [(arr, None), (typ, None), (nm, fg), (upd, None)]
        for ci, (txt, color) in enumerate(cells):
            kw = {'text': txt, 'anchor': 'w'}
            if HAS_CTK:
                if color:
                    kw['text_color'] = color
                lbl = ctk.CTkLabel(parent, **kw)
            else:
                if color:
                    kw['fg'] = color
                lbl = tk.Label(parent, **kw)
            lbl.grid(row=ri, column=ci, sticky='w', padx=6, pady=2)
        # Exception-reason picker. Use CTkComboBox when available so
        # the dropdown matches the rest of the UI; fall back to ttk
        # otherwise. Width is wide enough for the longest preset.
        if HAS_CTK:
            cb = ctk.CTkComboBox(parent, values=list(EXCEPTION_CHOICES),
                                 width=260, state='readonly')
            cb.set(cur if cur in EXCEPTION_CHOICES else cur)
        else:
            cb = ttk.Combobox(parent, values=list(EXCEPTION_CHOICES),
                              width=36, state='readonly')
            cb.set(cur if cur in EXCEPTION_CHOICES else cur)
        cb.grid(row=ri, column=4, sticky='w', padx=6, pady=2)
        row_widgets.append({'key': key, 'cb': cb, 'original': cur})

    def _cde_save(self, data, row_widgets, win):
        """Persist the dialog's edits back to exceptions.json.

        Only rows whose ComboBox selection differs from their original
        value are touched. Updated entries get a fresh MM-DD-YYYY-HH:MM:SS
        timestamp; rows whose new reason is not "None - Breaking SLA"
        flip color to grey so the next report acknowledges the waiver.
        Rows set back to "None - Breaking SLA" leave the stored color
        alone (it will be re-evaluated on the next protection run).
        """
        now_str = datetime.datetime.now().strftime("%m-%d-%Y-%H:%M:%S")
        changed = 0
        for rw in row_widgets:
            key = rw['key']
            new_val = rw['cb'].get().strip()
            if not new_val:
                new_val = 'None'
            if new_val == rw['original']:
                continue
            rec = data.get(key)
            if not rec:
                continue
            rec['exception_reason'] = new_val
            rec['Last_Update']      = now_str
            # Any non-breaking reason marks the row as an acknowledged
            # exception; recolor to grey so the HTML overlay reflects
            # the waived status. The first list entry is the explicit
            # "still breaking SLA" sentinel.
            if new_val != EXCEPTION_CHOICES[0]:
                rec['color'] = 'grey'
            changed += 1
        try:
            _save_exceptions(data)
        except Exception as e:
            messagebox.showerror("Save failed",
                f"Could not write exceptions.json:\n{e}",
                parent=win)
            return
        try: win.grab_release()
        except Exception: pass
        win.destroy()
        messagebox.showinfo("Exceptions saved",
            f"{changed} exception row(s) updated.\n\n"
            "Re-run the Volume and Filesystem Protection Report or "
            "re-open today's HTML to see the new color and reason.",
            parent=self)

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
        excluded = self._get_excluded_codes()
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
            try:
                self.reports_menu.entryconfig(self._open_summary_idx,
                                              state=tk.NORMAL)
            except Exception:
                pass
        except Exception:
            pass

        # Detailed log
        try:
            path = os.path.join(dir_logs, f"Pure Alert and Replication Lag Logs {date_str}.log")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(header + detailed)
            self.last_log_path = path
            try:
                self.reports_menu.entryconfig(self._open_logs_idx,
                                              state=tk.NORMAL)
            except Exception:
                pass
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
            self.open_daily_btn.configure(state=tk.NORMAL)
            # Enable the "Email Daily Report" menu entry only when the
            # email config has at minimum a server and recipient.
            if (self._smtp_server and self._smtp_to):
                try:
                    self.email_menu.entryconfig(self._email_daily_idx,
                                                state=tk.NORMAL)
                except Exception:
                    pass
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

        # Briefly relabel the menu entry to "Sending…" and disable it so
        # the user can't fire a duplicate send while the worker thread
        # is in flight; the finally block restores the original label
        # and state regardless of success or failure.
        try:
            self.email_menu.entryconfig(self._email_daily_idx,
                                        label="Sending…", state=tk.DISABLED)
        except Exception:
            pass

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
                def _reset():
                    try:
                        self.email_menu.entryconfig(self._email_daily_idx,
                                                    label="Email Daily Report",
                                                    state=tk.NORMAL)
                    except Exception:
                        pass
                self.after(0, _reset)

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

        # Dark theme palette to match the rest of the app. Without these
        # the ScrolledText body inherits the default white-on-white tk
        # styling under CTk dark mode and the help text is invisible.
        _bg_panel = "#2b2b2b"
        _bg_text  = "#1f1f1f"
        _fg_text  = "#dce4ee"
        _accent   = "#a04c12"
        try:
            win.configure(bg=_bg_panel)
        except Exception:
            pass

        # Header frame: logo upper-left. Plain tk.Frame so bg honors the
        # dark palette (ttk.Frame ignores bg under most themes).
        header_frame = tk.Frame(win, bg=_bg_panel)
        header_frame.pack(fill=tk.X, padx=10, pady=(8, 4))
        if hasattr(self, 'logo_img'):
            # self.logo_img is a CTkImage under CTk and a tk.PhotoImage
            # otherwise; CTkImage is incompatible with tk.Label, so the
            # CTk branch uses CTkLabel with a transparent background to
            # match the surrounding dark header panel.
            if HAS_CTK and isinstance(self.logo_img, ctk.CTkImage):
                ctk.CTkLabel(header_frame, image=self.logo_img,
                             text="", fg_color="transparent"
                             ).pack(side=tk.LEFT)
            else:
                tk.Label(header_frame, image=self.logo_img,
                         bg=_bg_panel).pack(side=tk.LEFT)

        text = scrolledtext.ScrolledText(win, wrap=tk.WORD, padx=12, pady=10,
                                         font=("Segoe UI", 9),
                                         bg=_bg_text, fg=_fg_text,
                                         insertbackground=_fg_text,
                                         selectbackground="#1f6aa5",
                                         selectforeground="#ffffff",
                                         relief="flat", borderwidth=0,
                                         highlightthickness=0)
        # ScrolledText wraps the Text + Scrollbar in its own tk.Frame
        # (text.frame). The Frame inherits the system theme bg \u2014 a
        # light gray on Windows \u2014 which renders as a thin pale band
        # around the text body. Re-tint the wrapper plus the scrollbar
        # so the dialog reads as a single dark surface end-to-end.
        try:
            text.frame.configure(bg=_bg_panel, borderwidth=0,
                                 highlightthickness=0)
        except Exception:
            pass
        try:
            text.vbar.configure(bg=_bg_panel, troughcolor=_bg_text,
                                activebackground="#3A3A3A",
                                borderwidth=0, highlightthickness=0,
                                relief="flat")
        except Exception:
            pass
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
      is discovered automatically when you click Run Daily Report.

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

  Run Daily Report     Appears on two cards in the actions matrix:

                         * Alert and Replication SLA Report
                             Polls all configured arrays for open alerts
                             and replication lag, and displays results in
                             the output panel below.
                         * Volume and Filesystem Protection Report
                             Generates the read-only HTML report listing
                             FlashArray volumes, snapshot counts, pod
                             stretch, SafeMode, and Protection Group
                             coverage. See "VOLUME AND FILESYSTEM
                             PROTECTION REPORT" below for how the
                             Config Drift Exceptions column is populated.

  Manage Exceptions    On the Volume and Filesystem Protection card
                       only. Opens the Config Drift Exceptions editor
                       backed by reports/protection/exceptions.json.
                       Pick a reason per row from a fixed list
                       (Dev/Test - No SLA, Migration in Progress,
                       Decommission Pending, etc.); saving stamps the
                       row with the current MM-DD-YYYY-HH:MM:SS and,
                       for any non-"None - Breaking SLA" reason, flips
                       the row color to grey on the next report run.

  Open Daily Report    Appears on the same two cards. Opens today's most
                       recent HTML report for the corresponding card in
                       the system browser. Each card looks for its own
                       file under reports/daily/ or reports/protection/
                       and stays disabled until the file exists.

  Open History Report  On the Alert and Replication SLA card only.
                       Opens the rolling history report compiled from
                       Pure Array History.csv across previous runs.

  Save Report Summary  Saves the summary output (Alerts + Replication
                       sections) to a dated .log file of your choice.

  Save All Logs        Saves the full SSH command log (raw output from
                       every command sent to every array) to a dated
                       .log file of your choice.

  Save Word Report     Exports a Word-compatible (.docx) summary report
                       after a Run Daily Report has been completed. The document
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


VOLUME AND FILESYSTEM PROTECTION REPORT
---------------------------------------
"Run Daily Report" under the "Volume and Filesystem Protection Report"
card writes an HTML file to:

    reports/protection/Pure_Volume_Snapshot_Protection_<YYYY-MM-DD>.html

The report enumerates each FlashArray volume with its replication
destinations, pod stretch state, local / pod / replicated snapshot
counts, SafeMode flag, Protection Group coverage, and connected hosts.
Filter and sort controls live in every column header; a green/red dot
pair next to color-bearing columns sorts by status.

  Config Drift Exceptions column (read-only in HTML)
      The right-most column on each of the three tables (FA Volumes,
      FA Filesystems, FB Filesystems) shows the recorded exception
      reason for each row plus a "Last Updated" timestamp. Both
      values are read from a JSON sidecar at report-generation time:

          reports/protection/exceptions.json

      The HTML report itself is read-only — there is no in-browser
      editor. Use the "Manage Exceptions" button on the Volume and
      Filesystem Protection card to edit reasons, or hand-edit
      exceptions.json and re-run the report.

      How rows are populated:
        * Every time "Run Daily Report" runs, all FA volumes,
          FA filesystems, and FB filesystems discovered on the
          configured arrays are merged into exceptions.json.
        * New rows are written with exception_reason="None -
          Breaking SLA" and a color verdict reflecting their
          current SLA state (red = SLA breach, green = compliant).
        * Rows that already exist in the JSON keep their stored
          reason / color / Last_Update — your edits are preserved
          across runs.

      How the HTML uses the JSON (post-processing pass):
        * The first column (Volume / Directory / Filesystem name)
          is colored according to the row's stored color value
          (green / red / grey); unknown values default to grey.
        * The Config Drift Exceptions cell shows the reason text
          plus the MM-DD-YYYY-HH:MM:SS Last_Update timestamp.
        * Saving a non-"None - Breaking SLA" reason in Manage
          Exceptions flips the stored color to grey, so the next
          report renders the row as an acknowledged exception
          rather than as a red SLA breach.

      File format (UTF-8 JSON, keyed by "<array>|<type>|<name>"):

          {
            "flasharray-prod|FA-Volume|vol-app-01": {
              "array_name":       "flasharray-prod",
              "array_type":       "FA-Volume",
              "volume_name":      "vol-app-01",
              "exception_reason": "Dev/Test - No SLA",
              "color":            "grey",
              "Last_Update":      "05-17-2026-09:15:00"
            },
            ...
          }


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

RUN DAILY REPORT UX
-------------------
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

        # CTkButton when available so the close control inherits the
        # same ghost-button palette used elsewhere: transparent surface
        # so the dialog bg shows through, orange border + orange text
        # via self._btn_border, with a subtle dark-lift on hover so the
        # orange text stays readable on hover (filling with orange on
        # hover would erase the orange text).
        if HAS_CTK:
            ctk.CTkButton(win, text="Close", command=win.destroy,
                          width=90, height=28, corner_radius=6,
                          fg_color="transparent", hover_color="#3A3A3A",
                          border_color=getattr(self, '_btn_border',
                                               "#a04c12"),
                          border_width=2,
                          text_color=_accent).pack(pady=8)
        else:
            ttk.Button(win, text="Close", command=win.destroy).pack(pady=8)

    def _append_history_csv(self, stats):
        """Append per-array stats from the current run to Pure Array History.csv."""
        append_history_csv(stats)

    def _update_gui(self, text, detailed, stats):
        self.detailed_log_data = detailed
        self.array_stats = stats
        self.text_out.delete("1.0", tk.END)
        self.text_out.insert(tk.END, text)
        self.run_btn.configure(state=tk.NORMAL)
        self._append_history_csv(stats)
        self._auto_save_reports(text, detailed, stats)



__all__ = ['PureMonitorApp']
