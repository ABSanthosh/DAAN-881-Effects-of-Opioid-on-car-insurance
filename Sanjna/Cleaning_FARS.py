import pandas as pd
import re
from datetime import datetime

# Load your CSV file
file_path = '/content/FARS_combined_2013_2023.csv'  # Replace with your actual file path
df = pd.read_csv(file_path)

# Print original row count
original_row_count = len(df)
print(f"Original dataset has {original_row_count} rows")

# Function to convert the incorrect date format to the correct one
def convert_date_format(row):
    date_str = row['crash_date']

    # Check if the date is already in the correct format (either 24-hour or AM/PM format)
    if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:00:00$', str(date_str)) or re.match(r'^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:00:00 [AP]M$', str(date_str)):
        return date_str

    try:
        # Format 1: YYYY-Month-DD h:00am/pm-h:59am/pm:SS
        pattern1 = r'(\d{4})-(\w+)-(\d+) (\d+):00(am|pm)-\d+:59(am|pm):?\d*'
        match = re.match(pattern1, str(date_str))

        if match:
            year = match.group(1)
            month_name = match.group(2)
            day = match.group(3).zfill(2)  # Ensure day is two digits
            hour = int(match.group(4))
            am_pm = match.group(5)

            # Convert month name to number
            month_dict = {
                'January': '01', 'February': '02', 'March': '03', 'April': '04',
                'May': '05', 'June': '06', 'July': '07', 'August': '08',
                'September': '09', 'October': '10', 'November': '11', 'December': '12'
            }
            month = month_dict.get(month_name, '01')

            # Convert hour to 24-hour format first
            if am_pm.lower() == 'pm' and hour < 12:
                hour += 12
            elif am_pm.lower() == 'am' and hour == 12:
                hour = 0

            # Convert to AM/PM format
            if hour == 0:
                formatted_hour = 12
                formatted_am_pm = "AM"
            elif hour < 12:
                formatted_hour = hour
                formatted_am_pm = "AM"
            elif hour == 12:
                formatted_hour = 12
                formatted_am_pm = "PM"
            else:
                formatted_hour = hour - 12
                formatted_am_pm = "PM"

            formatted_date = f"{int(month)}/{day}/{year}  {formatted_hour}:00:00 {formatted_am_pm}"
            return formatted_date

        # Format 2: YYYY-Month-DD Unknown Hours:Unknown Minutes
        pattern2 = r'(\d{4})-(\w+)-(\d+) Unknown Hours:Unknown Minutes'
        match = re.match(pattern2, str(date_str))

        if match:
            year = match.group(1)
            month_name = match.group(2)
            day = int(match.group(3))

            # Convert month name to number
            month_dict = {
                'January': '01', 'February': '02', 'March': '03', 'April': '04',
                'May': '05', 'June': '06', 'July': '07', 'August': '08',
                'September': '09', 'October': '10', 'November': '11', 'December': '12'
            }
            month = month_dict.get(month_name, '01')

            # Create a date object to determine if it's a weekday or weekend
            date_obj = datetime(int(year), int(month), day)
            weekday = date_obj.weekday()

            # Assign hour based on weekday (0-4) or weekend (5-6)
            hour = weekday_median_hour if weekday < 5 else weekend_median_hour

            # Convert to AM/PM format
            if hour == 0:
                formatted_hour = 12
                formatted_am_pm = "AM"
            elif hour < 12:
                formatted_hour = hour
                formatted_am_pm = "AM"
            elif hour == 12:
                formatted_hour = 12
                formatted_am_pm = "PM"
            else:
                formatted_hour = hour - 12
                formatted_am_pm = "PM"

            formatted_date = f"{int(month)}/{day:02d}/{year}  {formatted_hour}:00:00 {formatted_am_pm}"
            return formatted_date
        else:
            # If neither pattern matches, return the original string
            print(f"No pattern matched for: {date_str}")
            return date_str
    except Exception as e:
        print(f"Error converting date: {date_str}, Error: {e}")
        return date_str

# First pass: Calculate median hours for weekdays and weekends
# Initially process the data that already has correct format and the ones with am/pm format
known_hours = []
weekday_hours = []
weekend_hours = []

for idx, row in df.iterrows():
    date_str = str(row['crash_date'])

    # Already correct format (24-hour)
    pattern_correct = r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):00:00'
    match = re.match(pattern_correct, date_str)
    if match:
        year, month, day, hour = match.groups()
        hour = int(hour)

        # Convert to AM/PM format for consistency with our final output
        if hour == 0:
            formatted_hour = 12
            am_pm = "AM"
        elif hour < 12:
            formatted_hour = hour
            am_pm = "AM"
        elif hour == 12:
            formatted_hour = 12
            am_pm = "PM"
        else:
            formatted_hour = hour - 12
            am_pm = "PM"

        formatted_date = f"{int(month)}/{int(day)}/{year}  {formatted_hour}:00:00 {am_pm}"

        date_obj = datetime(int(year), int(month), int(day))
        weekday = date_obj.weekday()
        known_hours.append((weekday, hour))
        if weekday < 5:  # Monday-Friday
            weekday_hours.append(hour)
        else:  # Saturday-Sunday
            weekend_hours.append(hour)
        continue

    # am/pm format
    pattern_ampm = r'(\d{4})-(\w+)-(\d+) (\d+):00(am|pm)-\d+:59(am|pm):?\d*'
    match = re.match(pattern_ampm, date_str)
    if match:
        year = match.group(1)
        month_name = match.group(2)
        day = int(match.group(3))
        hour = int(match.group(4))
        am_pm = match.group(5)

        # Convert month name to number
        month_dict = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }
        month = month_dict.get(month_name, '01')

        # Convert hour to 24-hour format
        if am_pm.lower() == 'pm' and hour < 12:
            hour += 12
        elif am_pm.lower() == 'am' and hour == 12:
            hour = 0

        date_obj = datetime(int(year), int(month), day)
        weekday = date_obj.weekday()
        known_hours.append((weekday, hour))
        if weekday < 5:  # Monday-Friday
            weekday_hours.append(hour)
        else:  # Saturday-Sunday
            weekend_hours.append(hour)

# Calculate median hours (defaulting to 12 if no data)
weekday_median_hour = int(pd.Series(weekday_hours).median()) if weekday_hours else 12
weekend_median_hour = int(pd.Series(weekend_hours).median()) if weekend_hours else 12

print(f"Calculated median hour for weekdays: {weekday_median_hour}:00")
print(f"Calculated median hour for weekends: {weekend_median_hour}:00")

# Second pass: Apply conversion to all rows after index 148043
if 'crash_date' in df.columns:
    # Make a copy of the slice to work with
    df_slice = df.loc[148043:].copy()

    # Apply the conversion and show examples
    print("Converting dates - showing first 5 examples of conversion:")
    for i, (idx, row) in enumerate(df_slice.head(5).iterrows()):
        original = row['crash_date']
        df_slice.at[idx, 'crash_date'] = convert_date_format(row)
        print(f"  {i+1}. Original: {original} → Converted: {df_slice.at[idx, 'crash_date']}")

    # Apply the conversion to all rows in the slice
    df_slice['crash_date'] = df_slice.apply(convert_date_format, axis=1)

    # Update the original dataframe with the converted values
    df.loc[148043:, 'crash_date'] = df_slice['crash_date']

    # Print the new row count to verify no rows were lost
    new_row_count = len(df)
    print(f"New dataset has {new_row_count} rows")

    if original_row_count != new_row_count:
        print(f"WARNING: Row count changed! Original: {original_row_count}, New: {new_row_count}")
else:
    print("Error: 'crash_date' column not found in the dataset")

# Save the corrected dataframe back to CSV
df.to_csv('corrected_dataset.csv', index=False)

print("Date format correction complete!")
