import csv
import pprint
import pandas as pd
from itertools import groupby

pp = pprint.PrettyPrinter(indent=2)

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
	# unique_sorted_silver_plans = unique_silver_plans.sort_values(by=["state", "rate_area", "rate"])
	return unique_silver_plans

def filter_plans(): 
	# Parses zips with multiple rate areas
	def filter_ambiguous_zips(group):
		zipped_group = zip(group["zipcode"], group["rate_area"])
		unique_zipped_group = list(set(zipped_group))
		if len(unique_zipped_group) < 2: 
			return True
		else:
			return False


	plans = dedupe_plans()
	grouped_plans = plans.groupby(["zipcode"]).filter(
			lambda x: filter_ambiguous_zips(x)
		)
	# grouped_plans.to_csv("plan_test2.csv")
	print(grouped_plans)
	

			
		

	# for key, group in grouped_plans:
	# 	print(group["rate_area"].unique(), group["zipcode"].unique())

	# for item, group in grouped_plans:
	# 	print((group["zipcode"], group["rate_area"]))
		


if __name__ == '__main__':
	filter_plans()
# def get_target_zips():
# 	with open("slcsp.csv", newline="") as slcsp:
# 		reader = csv.DictReader(slcsp)
# 		zips = [zip["zipcode"] for zip in reader]
# 		return zips

# def get_state_data(): 
# 	target_zips = get_target_zips()
# 	with open ("zips.csv", newline="") as all_zips:
# 		reader = csv.DictReader(all_zips)
# 		state_data = [(zip["zipcode"], zip["state"], zip["rate_area"]) for zip in reader if zip["zipcode"] in target_zips]
# 		return state_data
		
# def get_all_plans():
# 	state_data = get_state_data()
# 	# return state_data
# 	with open ("plans.csv", newline="") as all_plans:
# 		reader = csv.DictReader(all_plans)
# 		for entry in state_data:
# 			return

		# plan_data = [
		# 							(plan["state"], plan["rate_area"], plan["rate"], plan["metal_level"]) 
		# 							for plan in reader 
		# 							if (plan["state"], plan["rate_area"]) in state_data and plan["metal_level"] == "Silver"
		# 						]
		# # remove duplicate plans						
		# plan_data = set(plan_data)					
		# # sort our data by state and rate	
		# plan_data = sorted(plan_data, key = lambda item : (item[0], item[1], item[2]))	

		# return plan_data

# def sort_plans():
# 	unique_plans = get_all_plans()
# 	correct_plans = []
# 	for key, group in groupby(unique_plans, lambda x: (x[0], x[1])):
# 		print(key)
# 		group = list(group)
# 		pp.pprint(group)
# 		print(group[1])
		

