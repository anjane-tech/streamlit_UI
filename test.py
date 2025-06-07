import duckdb

con = duckdb.connect('earthquakes.duckdb')

# Create table (optional if you want persistent schema)
con.execute("""
    CREATE TABLE IF NOT EXISTS callitrichidae AS
    SELECT * FROM read_csv_auto('data/employee_data.csv')
""")

# Query the table
df = con.execute("SELECT * FROM callitrichidae").fetchdf()
print(df)
