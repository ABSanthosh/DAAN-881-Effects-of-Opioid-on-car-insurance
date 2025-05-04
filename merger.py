import os
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text

# ========== MYSQL CONFIG ==========
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Ranchero0',
}

DATA_DIR = "./datasets"

# ========== CONNECT TO MYSQL ==========
def connect_database(DB_NAME):
    url = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{DB_NAME}"
    engine = create_engine(url, pool_recycle=3600, future=True)
    return engine

def create_pa_processed_table(engine):
    merge_query = """
    CREATE OR REPLACE TABLE pa_processed AS
    SELECT
        c.CRN AS crash_id,
        'PA' AS state,
        STR_TO_DATE(CONCAT(c.CRASH_YEAR, '-', LPAD(c.CRASH_MONTH, 2, '0'), '-01'), '%Y-%m-%d') AS crash_date,
        cf.COUNTYFP AS county_fips,
        cf.COUNTYNAME AS county_name,
        c.FATAL_COUNT AS fatalities,
        c.INJURY_COUNT AS injuries,
        c.MAX_SEVERITY_LEVEL AS severity_level,
        CASE WHEN f.OPIOID_RELATED = 1 THEN 1 ELSE 0 END AS opioid_flag,
        CASE WHEN f.DRUG_RELATED = 1 THEN 1 ELSE 0 END AS any_drug_flag,
        CASE WHEN f.ALCOHOL_RELATED = 1 THEN 1 ELSE 0 END AS alcohol_flag,
        CASE WHEN p.AGE < 21 THEN 1 ELSE 0 END AS young_driver_flag,
        CASE WHEN p.AGE >= 65 THEN 1 ELSE 0 END AS mature_driver_flag,
        p.AGE AS driver_age,
        CASE 
            WHEN p.SEX = 'M' THEN 0
            WHEN p.SEX = 'F' THEN 1
            ELSE NULL
        END AS driver_sex
    FROM crash c
    LEFT JOIN flag f ON c.CRN = f.CRN
    LEFT JOIN person p ON c.CRN = p.CRN
    LEFT JOIN county_fips cf ON c.COUNTY = cf.COUNTYFP
    ;
    """
    with engine.connect() as conn:
        # Create indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_crash_crn ON crash(CRN);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_flag_crn ON flag(CRN);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_person_crn ON person(CRN);"))

        # Merge data into pa_processed table
        conn.execute(text(merge_query))
        conn.commit()

def create_sc_processed_table(engine):
    merge_query = """
    CREATE OR REPLACE TABLE sc_processed AS
    SELECT
        s.crash_number AS crash_id,
        'SC' AS state,
        CAST(s.date AS DATETIME) AS crash_date,
        cf.COUNTYFP AS county_fips,
        cf.COUNTYNAME AS county_name,
        s.persons_killed AS fatalities,
        s.persons_injured AS injuries,
        (s.persons_killed + s.persons_injured + s.fatal_injury + s.suspected_serious_injury + s.suspected_minor_injury + s.possible_injury + s.no_apparent_injury) AS severity_level,
        CASE WHEN u.drug_test_results = 'O' THEN 1 ELSE 0 END AS opioid_flag,
        CASE WHEN u.drug_test_results = 'Y' THEN 1 ELSE 0 END AS any_drug_flag,
        CASE WHEN u.alcohol_test_results > 0.0 THEN 1 ELSE 0 END AS alcohol_flag,
        CASE WHEN u.driver_age < 21 THEN 1 ELSE 0 END AS young_driver_flag,
        CASE WHEN u.driver_age >= 65 THEN 1 ELSE 0 END AS mature_driver_flag,
        u.driver_age AS driver_age,
        CASE 
            WHEN u.driver_sex = 'M' THEN 0
            WHEN u.driver_sex = 'F' THEN 1
            ELSE NULL
        END AS driver_sex
    FROM statewide s
    LEFT JOIN statewide_unit u ON s.crash_number = u.crash_number
    LEFT JOIN county_fips cf ON TRIM(REPLACE(s.county, 'County', '')) = TRIM(REPLACE(cf.COUNTYNAME, 'County', ''))
    WHERE cf.STATE = 'SC';
    """
        
    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_crash_number ON statewide(crash_number)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_crash_number_unit ON statewide_unit(crash_number)"))

        conn.execute(text(merge_query))
        conn.commit()

def create_tn_processed_table(engine):
    with engine.connect() as conn:
        # conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mstrrecnbrtxt_collision ON collision(MstrRecNbrTxt)"))
        # conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mstrrecnbrtxt_person ON person(MstrRecNbrTxt)"))
        # conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mstrrecnbrtxt_person_drug ON person_drug(MstrRecNbrTxt)"))
        # conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mstrrecnbrtxt_unit ON unit(MstrRecNbrTxt)"))

        merge_query = """
        CREATE OR REPLACE TABLE tn_processed AS
        SELECT
            c.MstrRecNbrTxt AS crash_id,
            'TN' AS state,
            CAST(c.CollisionDte AS DATETIME) AS crash_date,
            cf.COUNTYFP AS county_fips,
            cf.COUNTYNAME AS county_name,
            c.NbrFatalitiesNmb AS fatalities,
            c.NbrInjuredNmb AS injuries,
            (c.NbrFatalitiesNmb + c.NbrInjuredNmb) AS severity_level,
            CASE WHEN pd.DrugTestResultCde = '04' THEN 1 ELSE 0 END AS opioid_flag,
            CASE WHEN c.DrugInd = 'Y' THEN 1 ELSE 0 END AS any_drug_flag,
            CASE WHEN c.AlcoholInd = 'Y' THEN 1 ELSE 0 END AS alcohol_flag,
            CASE WHEN pd_detail.AgeNmb < 21 THEN 1 ELSE 0 END AS young_driver_flag,
            CASE WHEN pd_detail.AgeNmb >= 65 THEN 1 ELSE 0 END AS mature_driver_flag,
            pd_detail.AgeNmb AS driver_age,
            CASE 
                WHEN pd_detail.GenderTxt = 'M' THEN 0
                WHEN pd_detail.GenderTxt = 'F' THEN 1
                ELSE NULL
            END AS driver_sex
        FROM collision c
        LEFT JOIN person_drug pd ON c.MstrRecNbrTxt = pd.MstrRecNbrTxt
        LEFT JOIN person_detail pd_detail ON c.MstrRecNbrTxt = pd_detail.MstrRecNbrTxt
        LEFT JOIN county_fips cf ON c.CountyStateCde = cf.COUNTYFP
        WHERE cf.STATE = 'TN';
        """

        conn.execute(text(merge_query))
        conn.commit()


# ========== EXPORT TO CSV ==========
def export_pa_processed_to_csv(engine, OUTPUT_CSV, table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, con=engine)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Exported final {table_name}.csv to {OUTPUT_CSV}")

if __name__ == "__main__":
    # Process PA data
    # engine = connect_database('pa_crash_db')
    # create_pa_processed_table(engine)
    # export_pa_processed_to_csv(engine, "./datasets/PA/PA_PROCESSED.csv", "pa_processed")
    # engine.dispose()

    # Process SC data
    # engine = connect_database('sc_crash_db')
    # create_sc_processed_table(engine)
    # export_pa_processed_to_csv(engine, "./datasets/SC/SC_PROCESSED.csv", "sc_processed")
    # engine.dispose()

    # Process TN data
    engine = connect_database('tn_crash_db')
    create_tn_processed_table(engine)
    export_pa_processed_to_csv(engine, "./datasets/TN/TN_PROCESSED.csv", "tn_processed")
    engine.dispose()

    