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
def get_snowflake_tables(conn_params):
    try:
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                tables = [row[1] for row in cur.fetchall()]
        return tables
    except Exception as e:
        st.error(f"Error fetching Snowflake tables: {e}")
        return []

def get_duckdb_tables(db_file):
    try:
        with duckdb.connect(database=db_file) as con:
            res = con.execute("SHOW TABLES").fetchall()
        return [row[0] for row in res]
    except Exception as e:
        st.error(f"Error fetching DuckDB tables: {e}")
        return []

def fetch_snowflake_to_duckdb(source_table, conn_params, intermediate_db, target_table_name):
    try:
        # Fetch data from Snowflake
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM "{source_table}"')
                data = cur.fetchall()
                columns = [desc[0] for desc in cur.description]

        if not data:
            st.warning("Source table has no data!")
            return columns, []

        # Create intermediate DuckDB table
        with duckdb.connect(intermediate_db) as con:
            con.execute(f'DROP TABLE IF EXISTS "{target_table_name}"')

            # Create table schema
            col_defs = ", ".join([f'"{col}" TEXT' for col in columns])  # Using TEXT for simplicity
            con.execute(f'CREATE TABLE "{target_table_name}" ({col_defs})')

            # Insert data using executemany
            placeholders = ", ".join(["?"] * len(columns))
            con.executemany(
                f'INSERT INTO "{target_table_name}" VALUES ({placeholders})',
                data
            )

        return columns, data
    except Exception as e:
        st.error(f"Error transferring data from Snowflake to DuckDB: {e}")
        return [], []

def fetch_data_from_duckdb(table, db_file):
    try:
        with duckdb.connect(database=db_file) as con:
            res = con.execute(f'SELECT * FROM "{table}"')
            cols = [desc[0] for desc in res.description]
            rows = res.fetchall()
        return cols, rows
    except Exception as e:
        st.error(f"Error fetching data from DuckDB: {e}")
        return [], []

def compare_data(source_data, target_data):
    source_set = set(map(tuple, source_data))
    target_set = set(map(tuple, target_data))
    in_source_not_target = list(source_set - target_set)
    in_target_not_source = list(target_set - source_set)
    return in_source_not_target, in_target_not_source

def normalize_columns(cols):
    return [col.strip().lower() for col in cols]

# ---------- UI ----------
st.set_page_config(page_title="Data Comparison Tool - Differences Only", layout="wide")
st.title("🔍 Data Comparison Tool - Show Differences Interleaved")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Source Table")
    source_type = st.selectbox("Source Type", ["Snowflake", "DuckDB"])
    if source_type == "Snowflake":
        sf_config = get_snowflake_config()
        sf_tables = get_snowflake_tables(sf_config)
        source_table = st.selectbox("Select Source Table", sf_tables)
    else:
        duckdb_tables = get_duckdb_tables(DUCKDB_FILE)
        source_table = st.selectbox("Select Source Table (DuckDB)", duckdb_tables)

with col2:
    st.subheader("Target Table (DuckDB)")
    duckdb_tables = get_duckdb_tables(DUCKDB_FILE)
    target_table = st.selectbox("Select Target Table", duckdb_tables)

if st.button("Show Differences Only"):
    st.write("---")

    if source_type == "Snowflake":
        sf_cols, sf_data = fetch_snowflake_to_duckdb(
            source_table,
            sf_config,
            INTERMEDIATE_DB,
            "intermediate_source"
        )
        source_db_file = INTERMEDIATE_DB
        source_table_name = "intermediate_source"
    else:
        source_db_file = DUCKDB_FILE
        source_table_name = source_table
        sf_cols, sf_data = fetch_data_from_duckdb(source_table, DUCKDB_FILE)

    duckdb_cols, duckdb_data = fetch_data_from_duckdb(target_table, DUCKDB_FILE)

    sf_cols_normalized = normalize_columns(sf_cols)
    duckdb_cols_normalized = normalize_columns(duckdb_cols)

    if sf_cols_normalized != duckdb_cols_normalized:
        st.error("⚠️ Column mismatch detected!")
        st.write("Source columns:", sf_cols)
        st.write("Target columns:", duckdb_cols)
    else:
        only_in_source, only_in_target = compare_data(sf_data, duckdb_data)

        if only_in_source or only_in_target:
            st.write("### 🟢🔴 Differences Interleaved")
            max_rows = max(len(only_in_source), len(only_in_target))
            comparison_rows = []
            for i in range(max_rows):
                if i < len(only_in_source):
                    comparison_rows.append(("Source", only_in_source[i]))
                if i < len(only_in_target):
                    comparison_rows.append(("Target", only_in_target[i]))

            html_table = """
            <table class='comparison-table' style='width:100%; border: 1px solid #444; text-align: center; border-collapse: collapse;'>
                <tr>
                    <th style='background-color:#333; color:#DDDDDD;'>Data Source</th>
                    {}
                </tr>
            """.format(
                "".join([f"<th style='background-color:#333; color:#DDDDDD;'>{col}</th>" for col in sf_cols])
            )

            for source_or_target, values in comparison_rows:
                color = '#00FF00' if source_or_target == 'Source' else '#FF0000'
                html_table += "<tr>"
                html_table += f"<td style='color:{color}; font-weight:bold;'>{source_or_target}</td>"
                html_table += "".join([f"<td style='color:{color};'>{val}</td>" for val in values])
                html_table += "</tr>"

            html_table += "</table>"
            st.markdown(html_table, unsafe_allow_html=True)
        else:
            st.success("🎉 No differences found! Source and Target are identical.")
