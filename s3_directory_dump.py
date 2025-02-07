#!/usr/bin/env python3

#usage: python3 s3_directory_dump.py

import argparse 
import sys
import urllib3
import xml.etree.ElementTree as ET
import json
from datetime import date, timedelta, datetime
import requests
import os, errno
import fnmatch

CONFIG_FILE = "s3_directory_dump.config"

def main():

	with open(CONFIG_FILE) as json_file:
		data = json.load(json_file)
		s3bucket = data['s3bucket']
		global verbose
		verbose = data['verbose']
		global skipfiles
		skipfiles = data['skipfiles']
		
	global bucket_url
	bucket_url = s3bucket + '?max-keys=1000&marker='
	if verbose == 'true':
		print('s3bucket parameter: ', s3bucket)
		print('verbose parameter: ', verbose)
		for item in skipfiles:
			print('skipping all files named: ', item)
		print('')
		print('starting....')
    		
	directory_list = make_directory_list()
	if verbose == 'true':
		print('contents of directory_list object: ')
		for item in directory_list:
			print(item['filename'])
	print(len(directory_list), ' files are stored in directory_list object')

def make_directory_list():

	gdirectory_obj = []
	marker = ''

	directory_list = get_file_directory_object(gdirectory_obj, marker)
	
	return directory_list

def get_file_directory_object(container_in, marker):  

	output = ''
	isTruncated = 'true'

	url = bucket_url + str(marker)
	if verbose == 'true':
		print('s3 bucket request: ', url)
	
	response = getXML(url)
	archive_obj = parseResponse(response)
	
	#if item in archive_obj has '.DS_Store' then remove the item

	container = [];
	#append the container_in to the container
	container = [*container_in, *archive_obj]
	if verbose == 'true':
		i = len(container)
		print('files found: ', i)

	isTruncated = getIsTruncated(response)
	marker = getMarker(response)
	  
	if isTruncated == 'true':
		#there's more to process, so make (another) recursive call
		return get_file_directory_object(container, marker)	
	else:
		#end of processing here
		return container

def getXML(url):

	http = urllib3.PoolManager()
	response = http.request('GET', url)

	return response

def parseResponse(response):

	response_arry = []
	response_obj = ET.fromstring(response.data)
			
	#S3 reference info
	for item in response_obj.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
		name = item.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key')	
		
		#find the filename, if it's in the root directory, or the filename, if it's in a directory
		#then use that to check if it should be skipped
		try:
			filename_arry = name.text.split('/')
			check_name = filename_arry[1]
		except:
			check_name = name.text

		if check_name not in skipfiles:
			item = {}
			item['filename'] = name.text
			response_arry.append(item)
		else:
			if verbose == 'true':
				print('skipping this file: ', name.text)

	return response_arry
	
def getIsTruncated(response):

	isTruncated = ''
	response_obj = ET.fromstring(response.data)	

	#S3 reference info
	for item in response_obj.findall('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated'):
		isTruncated = item.text
			
	return isTruncated	
	
def getMarker(response):

	marker = ''
	response_obj = ET.fromstring(response.data)	

	#S3 reference info
	for item in response_obj.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
		marker = item.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
			
	return marker	
	
if __name__ == "__main__":
	main()	