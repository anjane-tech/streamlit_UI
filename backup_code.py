import streamlit as st
import pandas as pd
import snowflake.connector
import duckdb

def compare_tables(source_table, target_table, sf_config, duckdb_path):
    try:
        # --- Load Snowflake data ---
        sf_conn = snowflake.connector.connect(**sf_config)
        sf_cursor = sf_conn.cursor()
        sf_cursor.execute(f"SELECT * FROM {source_table}")
        sf_data = sf_cursor.fetchall()
        sf_columns = [col[0] for col in sf_cursor.description]
        sf_cursor.close()
        sf_conn.close()

        df_sf = pd.DataFrame(sf_data, columns=sf_columns)

        # --- Load DuckDB data ---
        duck = duckdb.connect(duckdb_path)
        df_duck = duck.execute(f"SELECT * FROM {target_table}").fetchdf()
        duck.close()

        # --- Normalize column names ---
        df_sf.columns = [col.lower() for col in df_sf.columns]
        df_duck.columns = [col.lower() for col in df_duck.columns]

        # --- Align by common columns ---
        common_cols = sorted(set(df_sf.columns).intersection(set(df_duck.columns)))
        df_sf = df_sf[common_cols].reset_index(drop=True)
        df_duck = df_duck[common_cols].reset_index(drop=True)

        # --- Trim to smallest row count ---
        min_len = min(len(df_sf), len(df_duck))
        df_sf = df_sf.iloc[:min_len]
        df_duck = df_duck.iloc[:min_len]

        # --- Compare row by row ---
        has_mismatch = False
        for i in range(min_len):
            mismatched_cols = [col for col in common_cols if df_sf.at[i, col] != df_duck.at[i, col]]
            if mismatched_cols:
                has_mismatch = True
                st.markdown(f"### 🔎 Row {i+1} Mismatch")

                source_row = pd.DataFrame({
                    "Source Table": [f"Row {i+1}"] * len(mismatched_cols),
                    "Column": mismatched_cols,
                    "Value": [df_sf.at[i, col] for col in mismatched_cols]
                })

                target_row = pd.DataFrame({
                    "Target Table": [f"Row {i+1}"] * len(mismatched_cols),
                    "Column": mismatched_cols,
                    "Value": [df_duck.at[i, col] for col in mismatched_cols]
                })

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("🟦 **Snowflake (Source)**")
                    st.dataframe(source_row, use_container_width=True)
                with col2:
                    st.markdown("🟨 **DuckDB (Target)**")
                    st.dataframe(target_row, use_container_width=True)

        if not has_mismatch:
            st.success("✅ No mismatches found between Snowflake and DuckDB.")

    except Exception as e:
        st.error(f"❌ Error during comparison: {e}")


SNOWFLAKE_CONFIG = {
    "user": "Ambika",
    "password": "Snowflake#2025",
    "account": "POEVRBR-DW28551",
    "warehouse": "COMPUTE_WH",
    "database": "RAW",
    "schema": "TEST",
    "role": "ACCOUNTADMIN"
}