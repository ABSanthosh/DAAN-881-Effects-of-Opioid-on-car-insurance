import os
import pandas as pd
from sqlalchemy import create_engine, text

# ========== MYSQL CONFIG ==========
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Ranchero0',
}
DB_NAME = 'pa_crash_db'
DATA_DIR = './datasets/PA'

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
def merge_pa_files():
    table_types = ["CRASH", "PERSON", "VEHICLE", "FLAG"]
    for table in table_types:
        folder = os.path.join(DATA_DIR, "")
        files = sorted([os.path.join(folder, f) for f in os.listdir(folder) if f.startswith(table) and f.endswith(".csv")])
        if not files:
            print(f"No files found for {table}.")
            continue

        # Check column consistency first
        check_column_consistency(files, table)

        # Merge files
        dfs = [load_dataset(file) for file in files]
        print(f"Merging {len(dfs)} files for {table}...")
        merged = pd.concat(dfs, ignore_index=True)
        print(f"Merged {table} with {len(merged)} rows.")

        # Save merged output
        merged_filename = os.path.join(folder, f"{table}_2013-22.csv")
        merged.to_csv(merged_filename, index=False)
        print(f"Merged {table} saved to {merged_filename}")

# ========== LOAD FILES ==========
def load_and_insert_csv(engine, table_name, file_path, if_exists='append', delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, chunksize=10000)

def load_all_pa_data():
    engine = connect_database()

    # 1. Load CRASH_2013-2022
    crash_files = [f for f in os.listdir(DATA_DIR) if f.startswith("CRASH_2013-22") and f.endswith(".csv")]
    print(f"Found {len(crash_files)} crash files.")
    crash_table_created = False
    for file in crash_files:
        print(f"Loading {file} into crash table...")
        load_and_insert_csv(engine, 'crash', os.path.join(DATA_DIR, file), if_exists='replace' if not crash_table_created else 'append')
        crash_table_created = True

    # 2. Load FLAG
    flag_files = [f for f in os.listdir(DATA_DIR) if f.startswith("FLAG_2013-22") and f.endswith(".csv")]
    flag_table_created = False
    for file in flag_files:
        print(f"Loading {file} into flag table...")
        load_and_insert_csv(engine, 'flag', os.path.join(DATA_DIR, file), if_exists='replace' if not flag_table_created else 'append')
        flag_table_created = True

    # 3. Load PERSON
    person_files = [f for f in os.listdir(DATA_DIR) if f.startswith("PERSON_2013-22") and f.endswith(".csv")]
    person_table_created = False
    for file in person_files:
        print(f"Loading {file} into person table...")
        load_and_insert_csv(engine, 'person', os.path.join(DATA_DIR, file), if_exists='replace' if not person_table_created else 'append')
        person_table_created = True

    # 4. Load VEHICLE
    vehicle_files = [f for f in os.listdir(DATA_DIR) if f.startswith("VEHICLE_2013-22") and f.endswith(".csv")]
    vehicle_table_created = False
    for file in vehicle_files:
        print(f"Loading {file} into vehicle table...")
        load_and_insert_csv(engine, 'vehicle', os.path.join(DATA_DIR, file), if_exists='replace' if not vehicle_table_created else 'append')
        vehicle_table_created = True

    # 5. Load COUNTY_FIPS
    print(f"Loading COUNTY_FIPS.csv into county_fips table...")
    load_and_insert_csv(engine, 'county_fips', os.path.join(DATA_DIR, 'COUNTY_FIPS.csv'), if_exists='replace', delimiter='|')

    print("All files loaded successfully into MySQL.")

# ========== MAIN ==========
if __name__ == '__main__':
    merge_pa_files()
    create_database()
    load_all_pa_data()
    pass
