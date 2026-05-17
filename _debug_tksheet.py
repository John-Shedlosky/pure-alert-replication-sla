"""Quick diagnostic: inspect tksheet's initial selection state with 20 blank rows."""
import tkinter as tk
from tksheet import Sheet

root = tk.Tk()
rows = [["one", "loc1"], ["two", "loc2"]] + [["", ""] for _ in range(20)]
s = Sheet(root, headers=["Array", "Location"], data=rows,
          width=360, height=260, show_row_index=False, show_top_left=False)
s.enable_bindings((
    "single_select", "drag_select", "arrowkeys", "edit_cell",
    "copy", "paste", "delete", "undo",
    "right_click_popup_menu", "rc_insert_row", "rc_delete_row",
))
s.pack(fill="both", expand=True)
root.update()
root.update_idletasks()

# Report current selected + selection_boxes state.
print("get_currently_selected ->", s.get_currently_selected())
print("selection_boxes ->", dict(s.MT.selection_boxes))

# Simulate clicking cell (2, 0) and typing — use API calls.
s.select_cell(2, 0)
root.update()
print("after select_cell(2,0):")
print("  selected ->", s.get_currently_selected())
print("  selection_boxes ->", dict(s.MT.selection_boxes))

# Set a single cell value via the public API.
s.set_cell_data(2, 0, "typed_value")
root.update()

data_after = s.get_sheet_data()
nonblank = [(i, r) for i, r in enumerate(data_after) if any(str(c or '').strip() for c in r)]
print("Non-blank rows after set_cell_data(2,0,'typed_value'):")
for i, r in nonblank:
    print(f"  row {i}: {r}")

root.destroy()
