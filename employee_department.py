import duckdb
import pandas as pd
import preswald as st

# Connect to DuckDB
con = duckdb.connect("mydata.duckdb")

# Create required tables
con.execute("CREATE TABLE IF NOT EXISTS employee (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER);")
con.execute("CREATE TABLE IF NOT EXISTS department (id INTEGER PRIMARY KEY, dept_name TEXT);")
con.execute("CREATE TABLE IF NOT EXISTS employee2 (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER);")
con.execute("CREATE TABLE IF NOT EXISTS department2 (id INTEGER PRIMARY KEY, dept_name TEXT);")

#  Clear existing data
con.execute("DELETE FROM employee")
con.execute("DELETE FROM department")
con.execute("DELETE FROM employee2")
con.execute("DELETE FROM department2")

#  Insert data into original tables
con.executemany("INSERT INTO employee VALUES (?, ?, ?);", [
    (1, 'Alice', 101),
    (2, 'Bob', 102),
    (3, 'Charlie', 103)
])
con.executemany("INSERT INTO department VALUES (?, ?);", [
    (101, 'HR'),
    (102, 'Engineering'),
    (103, 'Support')
])

# Step 4: Insert modified data into new tables (simulate mismatches)
con.executemany("INSERT INTO employee2 VALUES (?, ?, ?);", [
    (1, 'Alicia', 201),     # Mismatch
    (2, 'Bob', 102),        # Match
    (3, 'Charles', 203)     # Mismatch 
])
con.executemany("INSERT INTO department2 VALUES (?, ?);", [
    (201, 'Human Resources'),
    (102, 'Engineering'),      # Match
    (203, 'Customer Care')
])

# Query both full tables
df1 = con.execute("""
    SELECT e.id, e.name, d.dept_name
    FROM employee e
    JOIN department d ON e.dept_id = d.id
""").fetchdf()

df2 = con.execute("""
    SELECT e.id, e.name, d.dept_name
    FROM employee2 e
    JOIN department2 d ON e.dept_id = d.id
""").fetchdf()

# Find non-matching rows using outer merge with indicator
df_merged = df1.merge(df2, on=["id", "name", "dept_name"], how='outer', indicator=True)

# Split left and right unmatched
df_left = df_merged[df_merged["_merge"] == "left_only"].drop(columns=["_merge"])
df_right = df_merged[df_merged["_merge"] == "right_only"].drop(columns=["_merge"])

# Format UI display
title_left = "Non-Matching Rows in Old Table"
title_right = "Non-Matching Rows in New Table"
headers = ["id", "name", "dept_name"]
col_widths = [5, 12, 18]

def format_row(row, widths):
    return "  ".join(str(col).ljust(w) for col, w in zip(row, widths))

header_line = format_row(headers, col_widths)
left_rows = [format_row(row, col_widths) for row in df_left.itertuples(index=False)]
right_rows = [format_row(row, col_widths) for row in df_right.itertuples(index=False)]

# Pad for alignment
max_len = max(len(left_rows), len(right_rows))
left_rows += [" " * sum(col_widths)] * (max_len - len(left_rows))
right_rows += [" " * sum(col_widths)] * (max_len - len(right_rows))

# Step 8: Combine final display
lines = [
    title_left.ljust(sum(col_widths) + 6) + "|   " + title_right,
    header_line.ljust(sum(col_widths) + 6) + "|   " + header_line
]
for l, r in zip(left_rows, right_rows):
    lines.append(l.ljust(sum(col_widths) + 6) + "|   " + r)

st.text("```\n" + "\n".join(lines) + "\n```")
