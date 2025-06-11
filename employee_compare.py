import streamlit as st
import snowflake.connector
import duckdb

# ---------- Configuration ----------
DUCKDB_FILE = "mydata.duckdb"

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
        conn = snowflake.connector.connect(**conn_params)
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[1] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return tables
    except Exception as e:
        st.error(f"Error fetching Snowflake tables: {e}")
        return []

def get_duckdb_tables(db_file):
    try:
        con = duckdb.connect(database=db_file)
        res = con.execute("SHOW TABLES").fetchall()
        con.close()
        return [row[0] for row in res]
    except Exception as e:
        st.error(f"Error fetching DuckDB tables: {e}")
        return []

def fetch_data_from_snowflake(table, conn_params):
    conn = snowflake.connector.connect(**conn_params)
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table}')
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return cols, rows

def fetch_data_from_duckdb(table, db_file):
    con = duckdb.connect(database=db_file)
    res = con.execute(f'SELECT * FROM {table}')
    cols = [desc[0] for desc in res.description]
    rows = res.fetchall()
    con.close()
    return cols, rows

def store_non_matching_rows(table_name, cols, rows, db_file):
    if not rows:
        return
    con = duckdb.connect(database=db_file)
    col_defs = ", ".join([f"{col} TEXT" for col in cols])
    con.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})')
    placeholders = ", ".join(["?" for _ in cols])
    con.executemany(
        f'INSERT INTO {table_name} VALUES ({placeholders})',
        [tuple(str(item) for item in row) for row in rows]
    )
    con.close()

def compare_data(source_data, target_data):
    source_set = set(source_data)
    target_set = set(target_data)

    in_source_not_target = list(source_set - target_set)
    in_target_not_source = list(target_set - source_set)

    return in_source_not_target, in_target_not_source

def map_rows_to_dicts(cols, rows):
    """Helper to map rows to dicts using lowercase column names."""
    cols_lower = [col.lower() for col in cols]
    return [dict(zip(cols_lower, row)) for row in rows]

# ---------- Streamlit UI ----------
st.title("Data Comparison Tool with Column-Insensitive Row-wise Differences")

# Layout
col1, col2 = st.columns(2)

with col1:
    st.header("Source Table")
    source_type = st.selectbox("Source Type", ["Snowflake", "DuckDB"], key="source_type")
    sf_config = None
    sf_table = None

    if source_type == "Snowflake":
        sf_config = get_snowflake_config()
        if all(sf_config.values()):
            sf_tables = get_snowflake_tables(sf_config)
            sf_table = st.selectbox("Select Source Table", sf_tables)
        else:
            st.error("Snowflake credentials incomplete.")
            st.stop()
    else:
        duckdb_tables = get_duckdb_tables(DUCKDB_FILE)
        sf_table = st.selectbox("Select Source Table (DuckDB)", duckdb_tables)

with col2:
    st.header("Target Table (DuckDB)")
    duckdb_tables = get_duckdb_tables(DUCKDB_FILE)
    target_table = st.selectbox("Select Target Table", duckdb_tables)

# ---------- Compare Button ----------
if st.button("Do Diff and Store Differences"):
    # Fetch data
    if source_type == "Snowflake":
        sf_cols, sf_data = fetch_data_from_snowflake(sf_table, sf_config)
    else:
        sf_cols, sf_data = fetch_data_from_duckdb(sf_table, DUCKDB_FILE)

    duckdb_cols, duckdb_data = fetch_data_from_duckdb(target_table, DUCKDB_FILE)

    # Convert rows to dicts with lowercase column names
    sf_dict_data = map_rows_to_dicts(sf_cols, sf_data)
    duckdb_dict_data = map_rows_to_dicts(duckdb_cols, duckdb_data)

    # Convert dicts to sorted tuples of (key, value) pairs to make them hashable and comparable
    sf_tuples = [tuple(sorted(d.items())) for d in sf_dict_data]
    duckdb_tuples = [tuple(sorted(d.items())) for d in duckdb_dict_data]

    # Compare
    only_in_source, only_in_target = compare_data(sf_tuples, duckdb_tuples)

    st.write("---")
    st.subheader("Comparison Results (Row-wise Differences)")

    # Show and store differences
    if only_in_source:
        st.markdown("**Rows in Source but not in Target:**")
        in_source_dicts = [dict(row) for row in only_in_source]
        st.table(in_source_dicts)
        # Extract columns from the first row
        store_non_matching_rows("only_in_source", [k for k, _ in only_in_source[0]], [tuple(v for _, v in row) for row in only_in_source], DUCKDB_FILE)
        st.success(f"Stored {len(only_in_source)} rows as 'only_in_source' in DuckDB.")
    else:
        st.success("No extra rows in Source.")

    if only_in_target:
        st.markdown("**Rows in Target but not in Source:**")
        in_target_dicts = [dict(row) for row in only_in_target]
        st.table(in_target_dicts)
        store_non_matching_rows("only_in_target", [k for k, _ in only_in_target[0]], [tuple(v for _, v in row) for row in only_in_target], DUCKDB_FILE)
        st.success(f"Stored {len(only_in_target)} rows as 'only_in_target' in DuckDB.")
    else:
        st.success("No extra rows in Target.")

# Style
st.markdown(
    """
    <style>
    .stTable { background-color: #151515; color: #DDDDDD; }
    </style>
    """,
    unsafe_allow_html=True,
)
