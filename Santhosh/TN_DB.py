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
DB_NAME = 'tn_crash_db'
DATA_DIR = './datasets/TN'

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

# ========== LOAD FILES ==========
def load_and_insert_csv(engine, table_name, file_path, if_exists='append', delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter, encoding='ISO-8859-1', low_memory=False)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, chunksize=10000)

def load_all_tn_data():
    engine = connect_database()

    # Load vwCollision.txt
    collision_file = os.path.join(DATA_DIR, "vwCollision.txt")
    print("Loading vwCollision.txt into collision table...")
    load_and_insert_csv(engine, 'collision', collision_file, if_exists='replace', delimiter='|')

    # Load vwPerson.txt
    person_file = os.path.join(DATA_DIR, "vwPerson.txt")
    print("Loading vwPerson.txt into person table...")
    load_and_insert_csv(engine, 'person', person_file, if_exists='replace', delimiter='|')

    # Load vwPersonDrug.txt
    person_drug_file = os.path.join(DATA_DIR, "vwPersonDrug.txt")
    print("Loading vwPersonDrug.txt into person_drug table...")
    load_and_insert_csv(engine, 'person_drug', person_drug_file, if_exists='replace', delimiter='|')

    # Load vwUnit.txt
    unit_file = os.path.join(DATA_DIR, "vwUnit.txt")
    print("Loading vwUnit.txt into unit table...")
    load_and_insert_csv(engine, 'unit', unit_file, if_exists='replace', delimiter='|')

    # Load vwPersonDetail.txt
    person_detail_file = os.path.join(DATA_DIR, "vwPersonDetail.txt")
    print("Loading vwPersonDetail.txt into person_detail table...")
    load_and_insert_csv(engine, 'person_detail', person_detail_file, if_exists='replace', delimiter='|')

    # Load COUNTY_FIPS.csv
    county_fips_file = os.path.join(DATA_DIR, "COUNTY_FIPS.csv")
    print("Loading COUNTY_FIPS.csv into county_fips table...")
    load_and_insert_csv(engine, 'county_fips', county_fips_file, if_exists='replace', delimiter='|')

    print("All TN files loaded successfully into MySQL.")


# ========== MAIN ==========
if __name__ == '__main__':
    create_database()
    load_all_tn_data()
    pass
