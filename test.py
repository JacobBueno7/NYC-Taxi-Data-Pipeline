import duckdb
con = duckdb.connect("data/raw/database.duckdb")
print(con.execute("SHOW ALL TABLES").df())

