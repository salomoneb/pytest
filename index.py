import csv
import pprint
from itertools import groupby

def get_target_zips():
	with open("slcsp.csv", newline="") as slcsp:
		reader = csv.DictReader(slcsp)
		zips = [zip["zipcode"] for zip in reader]
		return zips

def get_all_zips(): 
	target_zips = get_target_zips()
	with open ("zips.csv", newline="") as all_zips:
		reader = csv.DictReader(all_zips)
		all_data = [(zip["state"], zip["rate_area"]) for zip in reader if zip["zipcode"] in target_zips]
		return all_data
		
def match_zips():
	all_data = get_all_zips()
	# return all_data
	with open ("plans.csv", newline="") as all_plans:
		reader = csv.DictReader(all_plans)
		plan_data = [
									(plan["plan_id"], plan["state"], plan["rate_area"], plan["rate"], plan["metal_level"]) 
									for plan in reader 
									if (plan["state"], plan["rate_area"]) in all_data and plan["metal_level"] == "Silver"
								]
		# sort our data so we can group it									
		plan_data = sorted(plan_data, key = lambda item : item[1])		
		return plan_data

def sort_plans():
	correct_plans = match_zips()
	for key, group in groupby(correct_plans, lambda x: x[0]):
		for item in group:
			print("State is {0}".format(item[1]))


if __name__ == '__main__':
	pp = pprint.PrettyPrinter(indent=2)
	pp.pprint(sort_plans())