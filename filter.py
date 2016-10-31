"""
	Application created by Jason Cleveland on October 30th 2016 for the purpose of completeing
	a programming task as part of the interview process for Mariner Parters Inc.
	
	This application reads in three data files, each named 'reports', containing similar data and 
	merges them into a central CSV file. The application removes any entries with zero packets 
	serviced, then sorts the entries in ascending order based on the date recorded. Finally the 
	application creates a dictionary showcasing the number of records associated with each unique 
	'service-guid' and prints it to the terminal.
	
	The Python programming language was chosen for this task because importing and exporting various
	types of files is simpler than other similar languages and there is a wide variety of primative 
	data manipulation options available with simple statements.
	
	To run this program the following non-standard Python libraries must be installed:
	
	'xmltodict': This library is used to read the provided XML file. It was chosen as it exported 
	the data from the XML document in the format of a Python list making individual component access
	simple.
	'pandas': The pandas library was used for the sole task of ordering the data in the time column.
	Pandas could have also been used to read the data from the CSV and JSON files however Python's
	default csv and json libraries made the process simpler.
	
	The above libraries can be installed by running 'pip install xmltodict' and 'pip install pandas'
	with administrative permissions in the terminal / command prompt.
	
	After the appropriate libraries have been installed, the program can be run from the terminal
	window by executing the 'python filter.py' command in the project folder.
"""

import csv
import json
import xmltodict as xml
import pandas
from datetime import datetime
from collections import Counter

#Converts a time from a string to Epoch time
def convert_time(str_date):
	new_time = datetime(int(str_date[:4]), int(str_date[5:7]), int(str_date[8:10]), 
					int(str_date[11:13]), int(str_date[14:16]), int(str_date[17:19]))
	return new_time.timestamp()
	
def convert_time_epoch(int_date):
	return datetime.fromtimestamp(float(int_date)), 'ADT'
	
#Uses the panda library to sort data in the combined csv file
def sort_csv(file_name):
	dataframe = pandas.read_csv(file_name)
	dataframe = dataframe.sort_values(['request-time'])
	dataframe.to_csv(file_name)
	
def show_frequency():
	csv_file = open('combined.csv', 'r')
	csv_data = csv.reader(csv_file)
	service_guid = []
	for row in csv_data:
		service_guid.append(row[3])
	service_guid = Counter(service_guid)
	for key, value in service_guid.items():
		if key != 'service-guid':
			print(key, ":", value)
		else:
			pass

#Writes the original data to a temporary file
def write_to_file():
	#Open the CSV file
	csv_file = open('reports.csv', 'r')
	csv_data = csv.reader(csv_file)
	#Open the JSON file
	json_file = open('reports.json', 'r')
	json_data = json.load(json_file)
	#Open the XML file
	xml_file = open('reports.xml', 'r')
	xml_data = xml.parse(xml_file.read())
	#Creates the combined output file
	combined_data = open('temporary.csv', 'w', newline='')
	#Creates a writer object using the csv library
	csv_writer = csv.writer(combined_data, delimiter=',') 
	
	#Writes the original CSV data to the combined CSV file
	for counter, row in enumerate(csv_data):
		#Adds the header of the original CSV file to the combined file
		#This special case is used to prevent date conversion errors
		if counter == 0:
			csv_writer.writerow(row)
		else:
			if row[6] != '0':
				data = [row[0], 
						row[1], 
						convert_time(row[2]), 
						row[3], 
						row[4], 
						row[5], 
						row[6], 
						row[7]]
				csv_writer.writerow(data)
			else:
				pass
				
	#Writes the cleaned up JSON data to the combined CSV file
	for row in json_data:
		if row['packets-serviced'] != 0:
			data = [row['client-address'], 
					row['client-guid'], 
					int(str(row['request-time'])[:-3]), #Trims off miliseconds from time
					row['service-guid'], 
					row['retries-request'], 
					row['packets-requested'], 
					row['packets-serviced'], 
					row['max-hole-size']]
			csv_writer.writerow(data)
		else:
			pass
			
	#Writes the cleaned up XML data to the combined CSV file
	for row in xml_data['records']['report']:
		if row['packets-serviced'] != '0':
			data = [row['client-address'], 
					row['client-guid'], 
					convert_time(row['request-time']), 
					row['service-guid'], 
					row['retries-request'], 
					row['packets-requested'], 
					row['packets-serviced'], 
					row['max-hole-size']]
			csv_writer.writerow(data)
		else:
			pass

	#Closes the opened files
	csv_file.close()
	json_file.close()
	xml_file.close()
	combined_data.close()

def write_sorted_to_file():
	#Opens the temporary file
	csv_file = open('temporary.csv')
	csv_data = csv.reader(csv_file)
	#Creates the combined output file
	combined_data = open('combined.csv', 'w', newline='')
	#Creates a writer object using the csv library
	csv_writer = csv.writer(combined_data, delimiter=',') 
	
	#Writes the original CSV data to the combined CSV file
	for counter, row in enumerate(csv_data):
		#Adds the header of the original CSV file to the combined file
		#This special case is used to prevent date conversion errors
		if counter == 0:
			data = [row[1], 
					row[2], 
					row[3], 
					row[4], 
					row[5], 
					row[6], 
					row[7],
					row[8]]
			csv_writer.writerow(data)
		else:
			data = [row[1], 
					row[2], 
					str(convert_time_epoch(row[3])), 
					row[4], 
					row[5], 
					row[6], 
					row[7],
					row[8]]
			csv_writer.writerow(data)
	
	#Closes the open files
	csv_file.close()
	combined_data.close()

write_to_file()
sort_csv("temporary.csv")
write_sorted_to_file()
show_frequency()