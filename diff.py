import duckdb
import pandas as pd
import preswald as st

# Connect to existing DuckDB file
con = duckdb.connect("mydata.duckdb")

# Fetch joined data from both table sets
df_old = con.execute("""
    SELECT e.id, e.name, d.dept_name
    FROM employee e
    JOIN department d ON e.dept_id = d.id
""").fetchdf()

df_new = con.execute("""
    SELECT e.id, e.name, d.dept_name
    FROM employee2 e
    JOIN department2 d ON e.dept_id = d.id
""").fetchdf()

# Perform outer merge to detect mismatches
df_merged = df_old.merge(df_new, on=["id", "name", "dept_name"], how="outer", indicator=True)

# Split non-matching rows
df_left = df_merged[df_merged["_merge"] == "left_only"].drop(columns=["_merge"])
df_right = df_merged[df_merged["_merge"] == "right_only"].drop(columns=["_merge"])

# UI formatting constants
title_left = "Non-Matching Rows in Old Table"
title_right = "Non-Matching Rows in New Table"
headers = ["id", "name", "dept_name"]
col_widths = [5, 15, 20]  # Adjust width if needed

def format_row(row, widths):
    return "  ".join(str(col).ljust(w) for col, w in zip(row, widths))

# Format rows
header_line = format_row(headers, col_widths)
left_rows = [format_row(row, col_widths) for row in df_left.itertuples(index=False)]
right_rows = [format_row(row, col_widths) for row in df_right.itertuples(index=False)]

# Pad for visual alignment
max_len = max(len(left_rows), len(right_rows))
left_rows += [" " * sum(col_widths)] * (max_len - len(left_rows))
right_rows += [" " * sum(col_widths)] * (max_len - len(right_rows))

# Combine lines
lines = [
    title_left.ljust(sum(col_widths) + 6) + "|   " + title_right,
    header_line.ljust(sum(col_widths) + 6) + "|   " + header_line
]
for l, r in zip(left_rows, right_rows):
    lines.append(l.ljust(sum(col_widths) + 6) + "|   " + r)

# Display in UI
st.text("""
```
""" + "\n".join(lines) + "\n```""")
