import os
import duckdb
import pandas as pd
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "all_stocks.csv")
DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "duckdb")
DUCKDB_PATH = os.path.join(DUCKDB_DIR, "stocks.db")

def write_to_duckdb():
	os.makedirs(DUCKDB_DIR, exist_ok=True)
	df = pd.read_csv(RAW_CSV)
	print("CSV columns:", df.columns.tolist())
	df.columns = df.columns.str.lower()
	# Ensure date is parsed as datetime
	if 'date' in df.columns:
		df['date'] = pd.to_datetime(df['date'])
	with measure("duckdb_write", data_path=DUCKDB_DIR) as m:
		con = duckdb.connect(DUCKDB_PATH)
		con.execute("DROP TABLE IF EXISTS stocks")
		con.execute("""
			CREATE TABLE stocks (
				date DATE,
				ticker VARCHAR,
				open DOUBLE,
				high DOUBLE,
				low DOUBLE,
				close DOUBLE,
				volume DOUBLE
			)
		""")
		con.executemany(
			"INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?)",
			df[["date", "ticker", "open", "high", "low", "close", "volume"]].values.tolist()
		)
		con.close()
	print("DuckDB write benchmark:", m.value)

def read_from_duckdb():
	with measure("duckdb_read", data_path=DUCKDB_DIR) as m:
		con = duckdb.connect(DUCKDB_PATH)
		df = con.execute("SELECT * FROM stocks").fetchdf()
		con.close()
	print("DuckDB read benchmark:", m.value)
	return df

def query_duckdb():
	with measure("duckdb_query", data_path=DUCKDB_DIR) as m:
		con = duckdb.connect(DUCKDB_PATH)
		result = con.execute(
			"""
			SELECT ticker, AVG(close) AS avg_close, MAX(close) AS max_close, MIN(close) AS min_close
			FROM stocks
			GROUP BY ticker
			"""
		).fetchdf()
		con.close()
	print("DuckDB query benchmark:", m.value)
	return result

if __name__ == "__main__":
	write_to_duckdb()
	read_from_duckdb()
	query_duckdb()
