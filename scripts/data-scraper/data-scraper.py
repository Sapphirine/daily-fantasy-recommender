import os
import csv
import shutil
import datetime
import sys

from bs4 import BeautifulSoup
from urllib2 import urlopen
from time import sleep

START_MONTH = 10
END_MONTH = 6

START_YEAR = 2014
END_YEAR = 2015

BASE_URL = 'http://rotoguru1.com/cgi-bin/hyday.pl?scsv=1&game=fd'
EXAMPLE_URL_1 = 'http://rotoguru1.com/cgi-bin/hyday.pl?scsv=1&game=fd&mon=11&day=16&year=2014'
EXAMPLE_URL_2 = 'http://rotoguru1.com/cgi-bin/hyday.pl?scsv=1&game=fd&mon=11&day=21&year=2014'

CSV_HEADER = ['Date', 'GID', 'Pos', 'Name', 'Starter', 'FD Pts',
	'FD Salary', 'Team', 'H/A','Oppt', 'Team Score', 'Oppt Score',
	'Minutes', 'Points','Rebounds','Assists', 'Steals',
	'Blocks', 'Turnovers', 'ThreePointers','FieldGoals','FreeThrows']
BASE_OUTPUT_PATH = 'data/'

def make_soup (url):
    html = urlopen(url).read()
    return BeautifulSoup(html, 'lxml')


def get_date (d, m, y):
	return datetime.datetime(y, m, d)


def get_all_possible_url_suffixes ():
	urls = []
	startDate = get_date(1, START_MONTH, START_YEAR)
	endDate = get_date(1, END_MONTH+1, END_YEAR)

	#Iterate through links and append to urls
	while startDate < endDate:
		urls.append('mon=' + str(startDate.month) + '&day=' + str(startDate.day) + '&year=' + str(startDate.year))
		startDate += datetime.timedelta(days=1)
	return urls


def fetch_raw_data (url):
	soup = make_soup(url)
	return soup.find('pre').string


def parse_raw_data (raw):
	# I think it's important we know which fields are blank
	# so that everything lines up when it makes it to the database.
	records = raw.split('\n')

	data = {}
	for i in xrange(0,len(records)):
		row = records[i].split(';')

		# If the data is malformed or the column names we skip it
		if (len(row) != 14 or i == 0):
			continue

		name = row[3]
		parsed = parse_stats(row[len(row)-1])
		row[len(row)-1] = parsed[0]

		for i in xrange(1,len(parsed)):
			row.append(parsed[i])

		data[name] = row

	return data

def parse_stats (line):
	stats = line.split()
	full_line = ['0'] * 9
	if (len(stats) > 9):
		# currently we've only seen nine categories in the stat lines.
		# We want to know if we're missing any
		print stats
		exit(1)

	for i in xrange(0,len(stats)):
		if ('pt' in stats[i]):
			full_line[0] = stats[i].rstrip('pt')
		if ('rb' in stats[i]):
			full_line[1] = stats[i].rstrip('rb')
		if ('as' in stats[i]):
			full_line[2] = stats[i].rstrip('as')
		if ('st' in stats[i]):
			full_line[3] = stats[i].rstrip('st')
		if ('bl' in stats[i]):
			full_line[4] = stats[i].rstrip('bl')
		if ('to' in stats[i]):
			full_line[5] = stats[i].rstrip('to')
		if ('trey' in stats[i]):
			full_line[6] = stats[i].rstrip('trey')
		if ('fg' in stats[i]):
			full_line[7] = stats[i].rstrip('fg')
		if ('ft' in stats[i]):
			full_line[8] = stats[i].rstrip('ft')

	return full_line


def merge (obj, newObj):
	for attr in newObj:
		try:
			val = obj[attr]
			obj[attr].append(newObj[attr])
		except:
			obj[attr] = []
			obj[attr].append(newObj[attr])
	return obj


def write (data):
	directory = BASE_OUTPUT_PATH
	if not os.path.exists(directory):
		os.makedirs(directory)
	else:
		shutil.rmtree(directory)
		os.makedirs(directory)

	for attr in data:
		filename = directory + '/' + attr + '.csv'
		write_csv(filename, data[attr])


def write_csv (filename, data):
	if not os.path.isfile(filename):
		with open(filename, 'w') as fp:
			a = csv.writer(fp, delimiter=',')

	for attr in data:
		with open(filename, 'a') as fp:
			a = csv.writer(fp, delimiter=',')
			a.writerow(attr)


if __name__ == '__main__':
	data = {}

## Script testing
#------------------------------------------------------------------------------#
	# data1 = fetch_raw_data(EXAMPLE_URL_1)
	# data1 = parse_raw_data(data1)
	#
	# data = merge(data, data1)
	#
	# data2 = fetch_raw_data(EXAMPLE_URL_2)
	# data2 = parse_raw_data(data2)
	#
	# data = merge(data, data2)
#------------------------------------------------------------------------------#


## What we run when its time to scrape the data once and for all
#------------------------------------------------------------------------------#
	urls = get_all_possible_url_suffixes()
	for url in urls:
		print url
		raw = fetch_raw_data(BASE_URL + '&' + url)
		parsed = parse_raw_data(raw)
		data = merge(data, parsed)
		# sleep(1)
#------------------------------------------------------------------------------#

	write(data)
