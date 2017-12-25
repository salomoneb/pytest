import pandas as pd

# Get full zip code data for our target zips
def merge_zips(): 
	target_zips = pd.read_csv("slcsp.csv")
	all_zips = pd.read_csv("zips.csv")
	merged_zips = target_zips.dropna(axis=1).merge(all_zips, on="zipcode")
	return merged_zips

# Merges plan data with zip data and returns all silver plans for our zips
def merge_plans(): 
	zips = merge_zips()
	all_plans = pd.read_csv("plans.csv")
	all_plans = all_plans.loc[all_plans["metal_level"] == "Silver"]
	all_silver_plans = zips.merge(all_plans, on=["state", "rate_area"])
	return all_silver_plans

'''
Sorts and cleans our silver plans, returning data that we can merge
with our original slcsp.csv sheet.
'''
def clean_plans(): 

	# Remove duplicate silver plans with the same rate in the same region
	def dedupe_plans(): 
		all_silver_plans = merge_plans()
		unique_silver_plans = all_silver_plans.drop_duplicates(subset=["zipcode", "state", "rate_area", "rate"])
		return unique_silver_plans	

	# Empty rate values for zips with multiple rate areas
	def identify_ambiguous_zips(group):
		zipped_group = zip(group["zipcode"], group["rate_area"])
		unique_zipped_group = list(set(zipped_group))
		return True if len(unique_zipped_group) < 2 else False

	# Sort plans in each group by rate
	def sort_rates_ascending(group):
		group = group.sort_values(by=["rate"])
		return group

	plans = dedupe_plans()

	# Final mergeable data
	final_plans = (
		plans.groupby("zipcode")
		.filter(identify_ambiguous_zips)
		.groupby("zipcode", as_index=False)
		.apply(sort_rates_ascending)
		.groupby("zipcode")
		.nth(1)  # Retrieves the second item in the sorted group
		.reset_index()
	)
	return final_plans

def match_final_plans(): 
	final_plans = clean_plans()
	final_merged_plans = (
		pd.read_csv("slcsp.csv")
			.dropna(axis=1)
			.merge(final_plans, how="left", on="zipcode")
			.loc[ : , ["zipcode", "rate"]]
			.fillna(value="", axis=1)
		)
	return final_merged_plans

def write_to_csv(output_file):
	final_output = match_final_plans()
	final_output.to_csv(output_file, index=False)

if __name__ == '__main__':
	write_to_csv("slcsp.csv")

