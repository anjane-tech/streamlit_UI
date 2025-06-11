import streamlit as st
import snowflake.connector
import duckdb

# ---------- Configuration ----------
DUCKDB_FILE = "mydata.duckdb"
INTERMEDIATE_DB = ":memory:"

def get_snowflake_config():
    return {
        "user": "Ambika",
        "password": "Snowflake#2025",
        "account": "POEVRBR-DW28551",
        "warehouse": "COMPUTE_WH",
        "database": "RAW",
        "schema": "TEST",
        "role": "ACCOUNTADMIN"
    }

# ---------- Functions ----------
def fetch_query_snowflake_to_duckdb(sql_query, conn_params, intermediate_db, target_table_name):
    try:
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                data = cur.fetchall()
                columns = [desc[0] for desc in cur.description]

        if not data:
            st.warning("Source query returned no data!")
            return columns, []

        with duckdb.connect(intermediate_db) as con:
            con.execute(f'DROP TABLE IF EXISTS "{target_table_name}"')
            col_defs = ", ".join([f'"{col}" TEXT' for col in columns])
            con.execute(f'CREATE TABLE "{target_table_name}" ({col_defs})')

            placeholders = ", ".join(["?"] * len(columns))
            con.executemany(
                f'INSERT INTO "{target_table_name}" VALUES ({placeholders})',
                data
            )

        return columns, data
    except Exception as e:
        st.error(f"Error fetching data from Snowflake: {e}")
        return [], []

def fetch_data_from_duckdb_query(sql_query, db_file):
    try:
        with duckdb.connect(database=db_file) as con:
            res = con.execute(sql_query)
            cols = [desc[0] for desc in res.description]
            rows = res.fetchall()
        return cols, rows
    except Exception as e:
        st.error(f"Error fetching data from DuckDB: {e}")
        return [], []

def compare_data(source_data, target_data):
    source_set = set(map(lambda r: (r,) if not isinstance(r, (list, tuple)) else tuple(r), source_data))
    target_set = set(map(lambda r: (r,) if not isinstance(r, (list, tuple)) else tuple(r), target_data))
    in_source_not_target = list(source_set - target_set)
    in_target_not_source = list(target_set - source_set)
    return in_source_not_target, in_target_not_source

def normalize_columns(cols):
    return [col.strip().lower() for col in cols]

def flatten_rows(rows):
    """Flatten rows of one-column data: [(1,), (2,), ...] -> [1, 2, ...]."""
    if rows and len(rows[0]) == 1:
        return [r[0] for r in rows]
    return rows

def render_table(columns, data, title):
    st.write(f"#### {title}")
    html = "<table style='border-collapse: collapse; width: 100%;'>"
    html += "<tr>" + "".join([f"<th style='border:1px solid #999; padding:4px;'>{col}</th>" for col in columns]) + "</tr>"
    for row in data:
        html += "<tr>" + "".join([f"<td style='border:1px solid #999; padding:4px;'>{val}</td>" for val in row]) + "</tr>"
    html += "</table>"
    st.markdown(html, unsafe_allow_html=True)

# ---------- UI ----------
st.set_page_config(page_title="Data Comparison Tool - Differences Only", layout="wide")
st.title("🔍 Data Comparison Tool - Query-based Differences")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Source Query")
    source_type = st.selectbox("Source Type", ["Snowflake", "DuckDB"])
    source_query = st.text_area(
        "Enter SQL query for Source",
        "SELECT * FROM my_table LIMIT 10"
    )

with col2:
    st.subheader("Target Query (DuckDB)")
    target_query = st.text_area(
        "Enter SQL query for Target (DuckDB)",
        "SELECT * FROM my_table LIMIT 10"
    )

if st.button("Show Differences Only"):
    st.write("---")

    # Fetch Source data
    if source_type == "Snowflake":
        sf_config = get_snowflake_config()
        sf_cols, sf_data = fetch_query_snowflake_to_duckdb(
            source_query,
            sf_config,
            INTERMEDIATE_DB,
            "intermediate_source"
        )
    else:
        sf_cols, sf_data = fetch_data_from_duckdb_query(source_query, DUCKDB_FILE)

    # Fetch Target data
    duckdb_cols, duckdb_data = fetch_data_from_duckdb_query(target_query, DUCKDB_FILE)

    # Display raw data (no tuples)
    if sf_cols and sf_data:
        sf_data = [list(row) if isinstance(row, (list, tuple)) else [row] for row in sf_data]
        render_table(sf_cols, sf_data, "🔹 Source Query Results")
    else:
        st.warning("No data in Source Query results.")

    if duckdb_cols and duckdb_data:
        duckdb_data = [list(row) if isinstance(row, (list, tuple)) else [row] for row in duckdb_data]
        render_table(duckdb_cols, duckdb_data, "🔸 Target Query Results")
    else:
        st.warning("No data in Target Query results.")

    # Normalize and flatten for comparison
    sf_cols_normalized = normalize_columns(sf_cols)
    duckdb_cols_normalized = normalize_columns(duckdb_cols)

    if sf_cols_normalized != duckdb_cols_normalized:
        st.warning("⚠️ Column mismatch detected! (but data differences will still be shown)")
        st.write("Source columns:", sf_cols)
        st.write("Target columns:", duckdb_cols)

    sf_data_flat = flatten_rows(sf_data)
    duckdb_data_flat = flatten_rows(duckdb_data)

    only_in_source, only_in_target = compare_data(sf_data_flat, duckdb_data_flat)

    if only_in_source or only_in_target:
        st.write("### 🟢🔴 Data Differences Interleaved")
        max_rows = max(len(only_in_source), len(only_in_target))
        html = "<table style='border-collapse: collapse; width: 100%;'>"
        html += "<tr><th>Data Source</th>" + "".join([f"<th>{col}</th>" for col in sf_cols]) + "</tr>"

        for i in range(max_rows):
            if i < len(only_in_source):
                row = only_in_source[i]
                color = "#00FF00"
                row_data = list(row) if isinstance(row, (list, tuple)) else [row]
                html += "<tr><td style='color:{}; font-weight:bold;'>Source</td>".format(color)
                html += "".join([f"<td style='color:{color};'>{val}</td>" for val in row_data]) + "</tr>"
            if i < len(only_in_target):
                row = only_in_target[i]
                color = "#FF0000"
                row_data = list(row) if isinstance(row, (list, tuple)) else [row]
                html += "<tr><td style='color:{}; font-weight:bold;'>Target</td>".format(color)
                html += "".join([f"<td style='color:{color};'>{val}</td>" for val in row_data]) + "</tr>"

        html += "</table>"
        st.markdown(html, unsafe_allow_html=True)
    else:
        st.success("🎉 No differences found! Source and Target data are identical.")
