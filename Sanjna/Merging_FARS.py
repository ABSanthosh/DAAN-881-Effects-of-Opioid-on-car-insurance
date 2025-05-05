import pandas as pd

input_csv = '/content/merged_fars_data_2017.csv'
output_csv = 'FARS_merged_data_2017.csv'
year_value = 2017

# Load the data
df = pd.read_csv(input_csv, low_memory=False)

# Combine DRUGRES1NAME–3 into one for opioid and drug checks
drug_cols = ['DRUGRES1NAME', 'DRUGRES2NAME', 'DRUGRES3NAME']
df['DRUG_ALL_NAMES'] = df[drug_cols].astype(str).agg(' | '.join, axis=1).str.lower()

# Expanded list of opioid identifiers
opioid_keywords = [
    'opioid', 'heroin', 'fentanyl', 'morphine', 'oxycodone', 'hydrocodone',
    'methadone', 'codeine', 'tramadol', 'buprenorphine', 'oxymorphone',
    'hydromorphone', 'tapentadol', 'meperidine', 'alfentanil', 'sufentanil',
    'naloxone', 'carfentanil', 'dihydrocodeine', 'etorphine', 'levorphanol'
]

# Drug presence values that mean "none"
no_drug_values = [
    'test not given', 'reported as unknown if tested for drugs',
    'other drug (specify:)', 'not reported', 'tested for drugs',
    'drugs detected, type unknown/positive', 'no (drugs not involved)'
]

# Define crash_date from known fields
df['YEAR'] = year_value
df['crash_date'] = (
    df['YEAR'].astype(str) + '-' +
    df['MONTHNAME'].astype(str) + '-' +
    df['DAYNAME'].astype(str) + ' ' +
    df['HOURNAME'].astype(str) + ':' +
    df['MINUTENAME'].astype(str)
)

# Injury level mapping
severity_map = {
    0: 'Property Damage Only',
    1: 'Fatal',
    2: 'Suspected Serious Injury',
    3: 'Suspected Minor Injury',
    4: 'Possible Injury',
    8: 'Injury – Unknown Severity',
    9: 'Unknown if Injured'
}
df['severity_level'] = df['INJ_SEV'].map(severity_map)

# Opioid flag
df['opioid_flag'] = df['DRUG_ALL_NAMES'].apply(
    lambda x: 1 if any(op in x for op in opioid_keywords) else 0
)

# Any drug flag
df['any_drug_flag'] = df['DRUG_ALL_NAMES'].apply(
    lambda x: 0 if any(ndv in x for ndv in no_drug_values) else 1
)

# Alcohol flag
df['alcohol_flag'] = df['DRINKINGNAME'].map({
    'Yes (Alcohol Involved)': 1,
    'No (Alcohol Not Involved)': 0,
    'Unknown (Police Reported)': None
})

# Young / Mature driver flags
df['young_driver_flag'] = df['AGE'].apply(lambda x: 1 if pd.notna(x) and x < 21 else 0)
df['mature_driver_flag'] = df['AGE'].apply(lambda x: 1 if pd.notna(x) and x >= 65 else 0)

# Driver-specific fields
df['driver_age'] = df.apply(
    lambda row: row['AGE'] if row['PER_TYPNAME'] == 'Driver of a Motor Vehicle In-Transport' else None, axis=1
)
df['driver_sex'] = df.apply(
    lambda row: 0 if row['PER_TYPNAME'] == 'Driver of a Motor Vehicle In-Transport' and row['SEXNAME'] == 'Male'
    else 1 if row['PER_TYPNAME'] == 'Driver of a Motor Vehicle In-Transport' and row['SEXNAME'] == 'Female'
    else None,
    axis=1
)

# Injuries flag
df['injuries'] = df['severity_level'].apply(lambda x: 0 if x == 'Property Damage Only' else 1)

# Final renaming and selection
df.rename(columns={
    'STATE': 'state',
    'COUNTY': 'county_fips',
    'FATALS': 'fatalities'
}, inplace=True)

final_columns = [
    'ST_CASE', 'VEH_NO', 'PER_NO', 'VE_FORMS',
    'state', 'crash_date', 'county_fips', 'fatalities', 'injuries',
    'severity_level', 'opioid_flag', 'any_drug_flag', 'alcohol_flag',
    'young_driver_flag', 'mature_driver_flag', 'driver_age', 'driver_sex',
    'RACE', 'RACENAME'
]

df_final = df[final_columns]

# Save final cleaned file
df_final.to_csv(output_csv, index=False)

print(f"Saved harmonized FARS 2017 data to: {output_csv}")
