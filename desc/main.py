import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text

# Ensure directories exist
os.makedirs("./scripts/desc/figures", exist_ok=True)
os.makedirs("./scripts/desc/statistics", exist_ok=True)

# ========== DATABASE CONNECTION ==========
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Ranchero0',
}

def connect_database(DB_NAME):
    url = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{DB_NAME}"
    engine = create_engine(url, pool_recycle=3600, future=True)
    return engine

# ========== FUNCTION 1: Generate Statistics ==========
def generate_opioid_crash_statistics(dbPrefix):
    engine = connect_database(f"{dbPrefix}_crash_db")
    query = text(f"""
        SELECT 
            YEAR(crash_date) AS year, 
            county_name, 
            severity_level,
            driver_age,
            driver_sex
        FROM {dbPrefix}_processed
        WHERE opioid_flag = 1
          AND severity_level IN (1, 2)
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    # Save summary statistics
    summary = {
        "Total Cases": len(df),
        "Cases by Year": df['year'].value_counts().sort_index().to_dict(),
        "Top 10 Counties": df['county_name'].value_counts().head(10).to_dict(),
        "Driver Age Summary": df['driver_age'].describe().to_dict(),
        "Driver Sex Distribution": df['driver_sex'].value_counts().to_dict()
    }

    # Save to file
    stats_path = f"./scripts/desc/statistics/{dbPrefix}_opioid_fatal_serious_stats.txt"
    with open(stats_path, "w") as f:
        for key, value in summary.items():
            f.write(f"{key}: {value}\n")

    # Print for immediate reference
    for key, value in summary.items():
        print(f"{key}: {value}")

    return df

# ========== FUNCTION 2: Plot Yearly Trend ==========
def plot_yearly_trend(df, dbPrefix):
    yearly_counts = df.groupby('year').size().reset_index(name='count')
    plt.figure(figsize=(10,6))
    sns.lineplot(data=yearly_counts, x='year', y='count', marker='o', palette=sns.color_palette("viridis", as_cmap=True))
    plt.title(f"{dbPrefix.upper()} - Opioid-Related Fatal and Serious Crashes Over Years")
    plt.xlabel("Year")
    plt.ylabel("Number of Crashes")
    plt.grid(True)

    for index, row in yearly_counts.iterrows():
        plt.text(row['year'], row['count'], str(row['count']), ha='center', va='bottom', fontsize=8)

    plt.tight_layout()
    plt.savefig(f"./scripts/desc/figures/{dbPrefix.upper()}_opioid-fatal-serious-trend.png")
    plt.close()

# ========== FUNCTION 3: Plot Top Counties ==========
def plot_top_counties(df, dbPrefix):
    top_counties = df['county_name'].value_counts().head(10).reset_index()
    top_counties.columns = ['county_name', 'count']
    plt.figure(figsize=(12,8))
    sns.barplot(data=top_counties, x='county_name', y='count', palette=sns.color_palette("viridis"), hue='county_name')
    plt.title(f"{dbPrefix.upper()} - Top 10 Counties for Opioid-Related Fatal and Serious Crashes")
    plt.xlabel("County")
    plt.ylabel("Number of Crashes")
    plt.xticks(rotation=45, ha='right')
    plt.grid(True, axis='y')

    for index, row in top_counties.iterrows():
        plt.text(index, row['count'], str(row['count']), ha='center', va='bottom', fontsize=8)

    plt.tight_layout()
    plt.savefig(f"./scripts/desc/figures/{dbPrefix.upper()}_top-10-counties-opioid.png")
    plt.close()

# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    df_tn = generate_opioid_crash_statistics("tn")
    plot_yearly_trend(df_tn, "tn")
    plot_top_counties(df_tn, "tn")

    df_pa = generate_opioid_crash_statistics("pa")
    plot_yearly_trend(df_pa, "pa")
    plot_top_counties(df_pa, "pa")

    df_sc = generate_opioid_crash_statistics("sc")
    plot_yearly_trend(df_sc, "sc")
    plot_top_counties(df_sc, "sc")
