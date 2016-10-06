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

def get_day_dicts_df(urls, log, city, state, zip):

	day_data_dicts = []
	
	for url in urls:
		log.write("Loading URL: {url}\n".format(url=url))
		
		try:
			day_dict = get_day_dict(url)
			day_data_dicts.append(day_dict)
			
			log.write("Successfully loaded\n\n")
		except:
			log.write("ERROR:{0}\n\n".format(sys.exc_info()[0]))
			
	days_df = pd.DataFrame()
	
	for day_dict in day_data_dicts:
		day_dict_df = pd.DataFrame(day_dict)
		days_df = days_df.append(day_dict_df)
	
	days_df['City'] = city
	days_df['State'] = state
	days_df['Zip'] = zip
	
	return days_df
	
def get_city_daily_weather(city = 'Tulsa', state = 'Oklahoma', zip = 74104,
	start_yr = 1970, start_month = 1, start_day = 1,
	end_yr = datetime.date.today().year, end_month = datetime.date.today().month, end_day = datetime.date.today().day):
	starttime = datetime.datetime.today()
	
	#Loop through the days from 1970 to today() and call get_day_dict
	start_date = datetime.date(start_yr, start_month, start_day)
	end_date = datetime.date(end_yr, end_month, end_day)
	
	#log = open('Logs/load_weather_log_{dt}_{city}_{state}_{zip}.txt'.format(dt=str(datetime.date.today()),city=city,state=state,zip=zip), 'a')
	log = open('Logs/temp_log.txt', 'a')
	log.write('\n--------------------------------------------------------\n')
	log.write('Running on {dt}\n'.format(dt=str(datetime.datetime.today())))

	url = get_url(city, state, zip)
	all_urls = []
	
	while start_date < end_date:
		day = start_date.day
		month = start_date.month
		year = start_date.year

		full_url = url.format(year=year, month=month, day=day)
		all_urls.append(full_url)
		
		start_date = start_date + datetime.timedelta(1)
	
	days_df = get_day_dicts_df(all_urls, log, city, state, zip)
	
	file_name = 'Data\historical_weather_{city}_{state}_{zip}.csv'.format(city=city,state=state,zip=zip).replace(' ','')

	days_df.to_csv(file_name, index=False)
	
	endtime = datetime.datetime.today()
	elapsed_time = endtime - starttime	
	
	log.write("\nCreated csv: {0}\n".format(file_name))
	log.write("\nTime to complete data load: {0}\n".format(str(elapsed_time)))
	log.write('--------------------------------------------------------\n')
	log.close()
	
	print "DONE"
	
	return days_df

def get_url(city, state, zip):
	locations_df = pd.read_csv('Locations.csv')
	selected_city = locations_df[(locations_df['City'] == city) & (locations_df['State'] == state) & (locations_df['Zip'] == zip)]
	
	if len(selected_city) == 0:
		log.write("No city was found for {city},{state},{zip}\n\n".format(city=city,state=state,zip=zip))
		log.write('--------------------------------------------------------\n')
		log.close()
		return

	url = selected_city['Url'].item()
	
	return url
	
def find_and_add_missing_city_date_data_to_df(df_in, start_date, end_date, city, state, zip):
	print "Loading missing data for {city},{state},{zip}".format(city=city,state=state,zip=zip)

	df = df_in.copy()
	
	starttime = datetime.datetime.today()
	log = open('Logs/historical_weather_loading_log.txt', 'a')
	log.write('\n--------------------------------------------------------\n')
	log.write('Loading missing dates for {city}, {state}, {zip}'.format(city=city,state=state,zip=zip))
	url = get_url(city, state, zip)

	try: url
	except: 
		log.write('City, state, zip not found')
		log.close()
		return

	if len(df) == 0:
		dates_for_location = []
	else:
		location_df = df[(df['City'] == city) & (df['Zip'] == zip) & (df['State'] == state)]
		dates_for_location = pd.to_datetime(location_df['DateUTC']).dt.date.unique()

	missing_dates = []

	#Get any missing dates for the given city,state,zip within the given date range
	while start_date <= end_date:
		if start_date not in dates_for_location:
			missing_dates.append(start_date)
		
		start_date = start_date + datetime.timedelta(1)

	log.write("{num_missing} missing dates found".format(num_missing=len(missing_dates)))
	print "{num_missing} missing dates found".format(num_missing=len(missing_dates))
		
	if len(missing_dates) == 0:
		print "No missing dates found!  Ya done it.\n"
		
		endtime = datetime.datetime.today()
		elapsed_time = endtime - starttime
		
		log.write("\nTime to complete: {0}\n".format(str(elapsed_time)))
		log.write('--------------------------------------------------------\n')    
		log.close()
			
		return df
	else:
		all_missing_urls =[]
		
		#Create full urls based on missing dates
		for missing_date in missing_dates:
			full_url = url.format(year=missing_date.year, month=missing_date.month, day=missing_date.day)
			all_missing_urls.append(full_url)
			
		new_data_df = get_day_dicts_df(all_missing_urls, log, city, state, zip)
		
		new_df = df.append(new_data_df)

	endtime = datetime.datetime.today()
	elapsed_time = endtime - starttime

	log.write("\nTime to complete: {0}\n".format(str(elapsed_time)))
	log.write('--------------------------------------------------------\n')    
	log.close()

	print "DONE\n"
	return new_df
	











	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	






