import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv("/content/final_merged_cleaned_2022.csv")
# Filter opioid-related crashes
opioid_df = df[df['Is_Opioid'] == 1]
state_counts = opioid_df['STATENAME'].value_counts().nlargest(10).reset_index()
state_counts.columns = ['State', 'Crashes']

plt.figure(figsize=(10, 6))
bar = sns.barplot(data=state_counts, x='State', y='Crashes', palette = 'magma')
for i, row in state_counts.iterrows():
    bar.text(i, row['Crashes'] + 5, row['Crashes'], ha='center', va='bottom')
plt.title('Top 10 States by Opioid-Involved Crashes')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
bar = sns.countplot(data=df, x='INJ_SEVNAME', hue='Is_Opioid', palette = "crest")
# Add counts
for p in bar.patches:
    height = int(p.get_height())
    bar.annotate(f'{height}', (p.get_x() + p.get_width() / 2., height),
                 ha='center', va='bottom', fontsize=9)
plt.title('Crash Severity by Opioid Involvement')
plt.xlabel('Injury Severity')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.legend(title='Opioid Involved', labels=['No', 'Yes'])
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
sns.histplot(opioid_df['AGE'], bins=30, kde=True)
plt.title('Age Distribution in Opioid-Involved Crashes')
plt.xlabel('Driver Age')
plt.ylabel('Frequency')
plt.tight_layout()
plt.show()

opioid_by_hour = opioid_df['HOUR'].value_counts().sort_index()

plt.figure(figsize=(12, 6))
bar = sns.barplot(x=opioid_by_hour.index, y=opioid_by_hour.values, palette = "viridis")
for i, value in enumerate(opioid_by_hour.values):
    bar.text(i, value + 2, str(value), ha='center', va='bottom')
plt.title('Opioid-Involved Crashes by Hour of Day')
plt.xlabel('Hour (0–23)')
plt.ylabel('Number of Crashes')
plt.tight_layout()
plt.show()

import pandas as pd
import matplotlib.pyplot as plt
import squarify

# Filter for opioid-involved crashes
opioid_df = df[df['Is_Opioid'] == 1]

# Aggregate crash counts by county and state
top_counties = (
    opioid_df
    .groupby(['STATENAME', 'COUNTY'])
    .size()
    .reset_index(name='Opioid_Crashes')
    .sort_values('Opioid_Crashes', ascending=False)
    .head(10)
)

# Create labels
top_counties['Label'] = top_counties['COUNTY'].astype(str) + ', ' + top_counties['STATENAME'] + '\n' + top_counties['Opioid_Crashes'].astype(str)

# Plot treemap
plt.figure(figsize=(12, 8))
squarify.plot(
    sizes=top_counties['Opioid_Crashes'],
    label=top_counties['Label'],
    color=sns.color_palette('Set2', len(top_counties)),
    pad=True,
    text_kwargs={'fontsize': 10}
)

plt.title('Top 10 Counties by Opioid-Involved Crashes (Treemap)')
plt.axis('off')
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
sns.histplot(data=df, x='AGE', hue='Is_Opioid', bins=30, multiple='stack')
plt.title('Age Distribution by Opioid Involvement')
plt.xlabel('Age')
plt.ylabel('Crash Count')
plt.tight_layout()
plt.show()

plt.figure(figsize=(8, 5))
bar = sns.countplot(data=df, x='SEXNAME', hue='Is_Opioid', palette = "crest")
for p in bar.patches:
    height = int(p.get_height())
    bar.annotate(f'{height}', (p.get_x() + p.get_width() / 2., height),
                 ha='center', va='bottom', fontsize=9)
plt.title('Opioid Involvement by Gender')
plt.xlabel('Sex')
plt.ylabel('Crash Count')
plt.legend(title='Opioid Involved', labels=['No', 'Yes'])
plt.tight_layout()
plt.show()
