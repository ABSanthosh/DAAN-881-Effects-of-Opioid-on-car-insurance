import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Ensure directories exist
os.makedirs("./scripts/pres/figures", exist_ok=True)
os.makedirs("./scripts/pres/statistics", exist_ok=True)

# Load predicted county-level risk scores manually or from file
# Example placeholder: you should replace with actual results
predicted_county_risk = {
    'PA': {'CountyA': 0.85, 'CountyB': 0.42, 'CountyC': 0.76},
    'TN': {'CountyX': 0.67, 'CountyY': 0.43, 'CountyZ': 0.52},
    'SC': {'CountyM': 0.59, 'CountyN': 0.29, 'CountyO': 0.73}
}

# ========== FUNCTION 1: Premium Adjustment Simulation ==========
def simulate_premium_adjustment(state, county_risk, base_premium=1000, risk_threshold=0.5, adjustment_rate=0.1):
    premiums = {}
    for county, risk in county_risk.items():
        if risk > risk_threshold:
            premiums[county] = base_premium * (1 + adjustment_rate)
        else:
            premiums[county] = base_premium

    df = pd.DataFrame.from_dict(premiums, orient='index', columns=['AdjustedPremium'])
    df['OriginalPremium'] = base_premium
    df['Delta'] = df['AdjustedPremium'] - df['OriginalPremium']
    df = df.sort_values('Delta', ascending=False)

    df.to_csv(f"./scripts/pres/statistics/{state}_premium_adjustment.csv")

    # Plot
    plt.figure(figsize=(10,6))
    sns.barplot(x=df.index, y=df['AdjustedPremium'], palette='viridis')
    plt.title(f"{state} - Adjusted Premiums by County")
    plt.xlabel("County")
    plt.ylabel("Adjusted Premium ($)")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f"./scripts/pres/figures/{state}_premium_adjustment.png")
    plt.close()

# ========== FUNCTION 2: Coverage Restriction Simulation ==========
def simulate_coverage_restriction(state, county_risk, risk_threshold=0.5):
    total_counties = len(county_risk)
    restricted_counties = sum(1 for risk in county_risk.values() if risk > risk_threshold)

    restricted_ratio = restricted_counties / total_counties

    result = {
        'TotalCounties': total_counties,
        'RestrictedCounties': restricted_counties,
        'RestrictedRatio': restricted_ratio
    }

    with open(f"./scripts/pres/statistics/{state}_coverage_restriction.txt", "w") as f:
        for k, v in result.items():
            f.write(f"{k}: {v}\n")

    # Pie Chart
    plt.figure(figsize=(6,6))
    plt.pie([restricted_counties, total_counties-restricted_counties],
            labels=['Restricted', 'Unrestricted'],
            autopct='%1.1f%%',
            colors=sns.color_palette('viridis', 2))
    plt.title(f"{state} - Coverage Restriction Impact")
    plt.tight_layout()
    plt.savefig(f"./scripts/pres/figures/{state}_coverage_restriction.png")
    plt.close()

# ========== FUNCTION 3: Safety Campaign Simulation ==========
def simulate_safety_campaign(state, county_risk, risk_low=0.3, risk_high=0.5, expected_reduction_rate=0.1, avg_claim_cost=50000):
    moderate_risk_counties = [county for county, risk in county_risk.items() if risk_low <= risk <= risk_high]
    savings = len(moderate_risk_counties) * avg_claim_cost * expected_reduction_rate

    result = {
        'ModerateRiskCounties': len(moderate_risk_counties),
        'EstimatedSavings': savings
    }

    with open(f"./scripts/pres/statistics/{state}_safety_campaign.txt", "w") as f:
        for k, v in result.items():
            f.write(f"{k}: {v}\n")

    # Simple bar chart
    plt.figure(figsize=(6,4))
    sns.barplot(x=['Savings'], y=[savings], palette='viridis')
    plt.title(f"{state} - Estimated Savings from Safety Campaign")
    plt.ylabel("Estimated Savings ($)")
    plt.tight_layout()
    plt.savefig(f"./scripts/pres/figures/{state}_safety_campaign.png")
    plt.close()

# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    for state, county_risk in predicted_county_risk.items():
        simulate_premium_adjustment(state, county_risk)
        simulate_coverage_restriction(state, county_risk)
        simulate_safety_campaign(state, county_risk)
