import os
import pandas as pd
from sqlalchemy import create_engine, text
import re

# ========== MYSQL CONFIG ==========
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Ranchero0',
}
DB_NAME = 'sc_crash_db'
DATA_DIR = './datasets/SC'

# ========== CONNECT TO MYSQL ==========
def create_connection():
    url = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}"
    engine = create_engine(url, pool_recycle=3600, future=True)
    return engine

def create_database():
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}",
        future=True
    )
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    engine.dispose()

def connect_database():
    url = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{DB_NAME}"
    engine = create_engine(url, pool_recycle=3600, future=True)
    return engine

# ========== UTILITY FUNCTIONS ==========
def load_dataset(file_path, **read_kwargs):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path, **read_kwargs, low_memory=False)
    elif file_path.endswith(".xlsx"):
        return pd.read_excel(file_path, **read_kwargs)
    elif file_path.endswith(".txt"):
        return pd.read_csv(file_path, delimiter="|", **read_kwargs, encoding='ISO-8859-1', low_memory=False)
    else:
        raise ValueError(f"Unsupported file format for {file_path}")

def check_column_consistency(file_list, table_name):
    base_cols = None
    for file in file_list:
        df = load_dataset(file)
        cols = set(df.columns)
        if base_cols is None:
            base_cols = cols
        else:
            if cols != base_cols:
                raise ValueError(f"Column mismatch detected in {table_name}: {file}")
    print(f"All {table_name} files have consistent columns.")

# ========== TRANSFORMATIONS ==========
def merge_sc_files():
    crash_files = sorted([os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if re.match(r"Statewide 20(1[3-9]|2[0-2])\.xlsx$", f)])
    unit_files = sorted([os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith("UNITS.xlsx")])
    print(f"Found {len(crash_files)} crash files and {len(unit_files)} unit files.")

    if not crash_files or not unit_files:
        raise ValueError("No SC files found for merging.")

    # Check column consistency
    check_column_consistency(crash_files, "SC Statewide")
    check_column_consistency(unit_files, "SC Statewide UNIT")

    # Merge crash files
    crash_dfs = [load_dataset(file) for file in crash_files]
    crash_merged = pd.concat(crash_dfs, ignore_index=True)
    crash_merged.to_csv(os.path.join(DATA_DIR, "Statewide_2013-22.csv"), index=False)
    print("Merged Statewide crash data saved.")

    # Merge unit files
    unit_dfs = [load_dataset(file) for file in unit_files]
    unit_merged = pd.concat(unit_dfs, ignore_index=True)
    unit_merged.to_csv(os.path.join(DATA_DIR, "Statewide_Unit_2013-22.csv"), index=False)
    print("Merged Statewide unit data saved.")

# ========== LOAD SC DATA ==========
def load_and_insert_csv(engine, table_name, file_path, if_exists='append', delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, chunksize=10000)

def load_all_sc_data():
    engine = connect_database()

    # Load Statewide crash data
    crash_file = os.path.join(DATA_DIR, "Statewide_2013-22.csv")
    print("Loading Statewide_2013-22.csv into statewide table...")
    load_and_insert_csv(engine, 'statewide', crash_file, if_exists='replace')

    # Load Statewide unit data
    unit_file = os.path.join(DATA_DIR, "Statewide_Unit_2013-22.csv")
    print("Loading Statewide_Unit_2013-22.csv into statewide_unit table...")
    load_and_insert_csv(engine, 'statewide_unit', unit_file, if_exists='replace')

    # Load Fips
    print(f"Loading COUNTY_FIPS.csv into county_fips table...")
    load_and_insert_csv(engine, 'county_fips', os.path.join(DATA_DIR, 'COUNTY_FIPS.csv'), if_exists='replace', delimiter='|')

    print("All SC files loaded successfully into MySQL.")

# ========== MAIN ==========
if __name__ == "__main__":
    merge_sc_files()
    create_database()
    load_all_sc_data()
