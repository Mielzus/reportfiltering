"""
	CSV column order:
	client-address
	client-guid
	request-time
	service-guid
	retries-request
	packets-requested
	packets-serviced
	max-hole-size
	
	Removes records with packets-serviced column equal to zero
	
	Sorts records by request-time
	
	Prints a summary showing the number of records in the combined file associated with each service-guid
"""
import csv
import json
import xmltodict as xml
from datetime import datetime

#Converts a time from a string to Epoch time
def convert_time(str_date):
	new_time = datetime(int(str_date[:4]), int(str_date[5:7]), int(str_date[8:10]), 
					int(str_date[11:13]), int(str_date[14:16]), int(str_date[17:19]))
	return new_time.timestamp()

#Open the CSV file
csv_file = open('reports.csv', 'r')
csv_data = csv.reader(csv_file)

#Open the JSON file
json_file = open('reports.json')
json_data = json.load(json_file)
	
#Open the XML file
xml_file = open('reports.xml')
xml_data = xml.parse(xml_file.read())

#Creates the combined output file
combined_data = open('combined.csv', 'w', newline='')

#Creates a writer object using the csv library
csv_writer = csv.writer(combined_data, delimiter=',') 

#Writes the original CSV data to the combined CSV file (working)
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
			print(row[6], '1')
	
#Writes the cleaned up JSON data to the combined CSV file
for row in json_data:
	if row['packets-serviced'] != 0:
		data = [row['client-address'], 
				row['client-guid'], 
				row['request-time'], 
				row['service-guid'], 
				row['retries-request'], 
				row['packets-requested'], 
				row['packets-serviced'], 
				row['max-hole-size']]
		csv_writer.writerow(data)
	else:
		print(row['packets-serviced'], '2')

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
		print(row['packets-serviced'], '3')
		
#Closes the opened files
csv_file.close()
json_file.close()
xml_file.close()
combined_data.close()