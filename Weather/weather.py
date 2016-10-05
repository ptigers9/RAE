import pandas as pd
import numpy as np
import csv
import urllib2
import datetime
import sys
import itertools

def get_day_dict(url):
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

def get_city_daily_weather(city = 'Tulsa', state = 'Oklahoma', zip = 74104,
	start_yr = 1970, start_month = 1, start_day = 1,
	end_yr = datetime.date.today().year, end_month = datetime.date.today().month, end_day = datetime.date.today().day):
	starttime = datetime.datetime.today()
	
	#Loop through the days from 1970 to today() and call get_day_dict
	start_date = datetime.date(start_yr, start_month, start_day)
	end_date = datetime.date(end_yr, end_month, end_day)
	
	log = open('Logs\load_weather_log_{dt}_{city}_{state}_{zip}.txt'.format(dt=str(datetime.date.today()),city=city,state=state,zip=zip), 'a')
	log.write('\n--------------------------------------------------------\n')
	log.write('Running on {dt}\n'.format(dt=str(datetime.datetime.today())))

	day_data_dicts = []
	locations_df = pd.read_csv('Locations.csv')
	selected_city = locations_df[(locations_df['City'] == city) & (locations_df['State'] == state) & (locations_df['Zip'] == zip)]
	
	if len(selected_city) == 0:
		log.write("No city was found for {city},{state},{zip}\n\n".format(city=city,state=state,zip=zip))
		log.write('--------------------------------------------------------\n')
		log.close()
		return

	url = selected_city['Url'].item()
	
	while start_date < end_date:
		day = start_date.day
		month = start_date.month
		year = start_date.year

		full_url = url.format(year=year, month=month, day=day)
		log.write("\nLoading data for {dt} using {url}\n".format(url=full_url,dt=start_date))
		print "Loading data for {dt} using {url}\n".format(url=full_url,dt=start_date)
		
		try:
			day_dict = get_day_dict(full_url)
			day_data_dicts.append(day_dict)
			
			log.write("Successfully loaded\n")
		except:
			log.write("ERROR:{0}\n".format(sys.exc_info()[0]))
		
		start_date = start_date + datetime.timedelta(1)
	
	days_df = pd.DataFrame()
	
	for day_dict in day_data_dicts:
		days_df = days_df.append(pd.DataFrame(day_dict))
	
	file_name = 'Data\historical_weather_{city}_{state}_{zip}.csv'.format(city=city,state=state,zip=zip).replace(' ','')
	days_df['City'] = city
	days_df['State'] = state
	days_df['Zip'] = zip
	
	days_df.to_csv(file_name)
	
	endtime = datetime.datetime.today()
	elapsed_time = endtime - starttime	
	
	log.write("\nCreated csv: {0}\n".format(file_name))
	log.write("\nTime to complete data load: {0}\n".format(str(elapsed_time)))
	log.write('--------------------------------------------------------\n')
	log.close()
	
	print "DONE"
	
	return days_df




















