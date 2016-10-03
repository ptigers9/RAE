import pandas as pd
import numpy as np
import csv
import urllib2

def load_day_dict(url):
	csv_response = urllib2.urlopen(url)
	csv_obj = csv.reader(csv_response)
	rows = []
	
	for row in csv_obj:
		if len(row) > 0:
			rows.append(row)
	
	cols = rows[0]
	rows.pop(0)
	all_rows = []
	day_dict = {}
	
	i = 0
	for col in cols:
		clean_col = col.replace('<br />','')
		day_dict[clean_col] = []
		
		for row in rows:
			day_dict[clean_col].append(row[i].replace('<br />',''))
		
		i+=1
	
	return day_dict


def get_city_daily_weather(city = 'Tulsa', state = 'Oklahoma', zip = 74104):
	locations_df = pd.read_csv('Locations.csv')
	city = locations_df[(locations_df['City'] == city) & (locations_df['State'] == state) & (locations_df['Zip'] == zip)]

	url = city['Url'][0]
	
	#Loop through the days from 1970 to today() and call load_day_dict
































