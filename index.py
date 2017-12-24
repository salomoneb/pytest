import pandas as pd

# Match and filter national zip code data with our target zips
def merge_zips(): 
	target_zips = pd.read_csv("slcsp.csv")
	all_zips = pd.read_csv("zips.csv")
	target_zips = target_zips.dropna(axis=1)
	merged_zips = target_zips.merge(all_zips, on="zipcode")
	return merged_zips

# Return silver plans for our zips
def merge_plans(): 
	zips = merge_zips()
	all_plans = pd.read_csv("plans.csv")
	all_plans = all_plans.loc[all_plans["metal_level"] == "Silver"]
	all_silver_plans = zips.merge(all_plans, on=["state", "rate_area"])
	return all_silver_plans

# Remove duplicate silver plans with the same rate in the same region
def dedupe_plans(): 
	all_silver_plans = merge_plans()
	unique_silver_plans = all_silver_plans.drop_duplicates(subset=["zipcode", "state", "rate_area", "rate"])
	return unique_silver_plans

# Group, sort, and filter our plans to match target zips
def filter_plans(): 
	# Empty rate values for zips with multiple rate areas
	def identify_ambiguous_zips(group):
		zipped_group = zip(group["zipcode"], group["rate_area"])
		unique_zipped_group = list(set(zipped_group))
		if len(unique_zipped_group) > 1: 
			for item in unique_zipped_group:
				group["rate"] = ""
		return group
	# Sort plans in each group by rate
	def sort_rates_ascending(group):
		group = group.sort_values(by=["rate"])
		return group

	plans = dedupe_plans()
	grouped_plans = plans.groupby("zipcode", as_index=False)
	modified_plans = grouped_plans.apply(identify_ambiguous_zips)
	sorted_plans = modified_plans.groupby(["zipcode"], as_index=False).apply(sort_rates_ascending)
	final_plans = sorted_plans.groupby("zipcode").nth(1).reset_index()
	return final_plans

def match_final_plans(): 
	final_plans = filter_plans()
	target_zips = pd.read_csv("slcsp.csv")
	target_zips = target_zips.dropna(axis=1)
	merged_plans = target_zips.merge(final_plans, how="left", on="zipcode")
	print(merged_plans.loc[: , ["zipcode", "rate"]])

if __name__ == '__main__':
	match_final_plans()

