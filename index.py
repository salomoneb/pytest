import csv
import pprint
from itertools import groupby
pp = pprint.PrettyPrinter(indent=2)

'''
Need to rewrite so each function is building up the data set further
'''

def get_target_zips():
	with open("slcsp.csv", newline="") as slcsp:
		reader = csv.DictReader(slcsp)
		zips = [zip["zipcode"] for zip in reader]
		return zips

def get_state_data(): 
	target_zips = get_target_zips()
	with open ("zips.csv", newline="") as all_zips:
		reader = csv.DictReader(all_zips)
		state_data = [(zip["state"], zip["rate_area"]) for zip in reader if zip["zipcode"] in target_zips]
		return state_data
		
def get_all_plans():
	state_data = get_state_data()
	# return state_data
	with open ("plans.csv", newline="") as all_plans:
		reader = csv.DictReader(all_plans)
		for entry in state_data:
			print(entry)

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

def sort_plans():
	unique_plans = get_all_plans()
	correct_plans = []
	for key, group in groupby(unique_plans, lambda x: (x[0], x[1])):
		print(key)
		group = list(group)
		pp.pprint(group)
		print(group[1])
		


if __name__ == '__main__':
	pp.pprint(get_all_plans())