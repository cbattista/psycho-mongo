"""
Christian's MongoDB Tools FTW
"""

import pymongo
from pymongo import Connection
from bson import Code
import copy
import string
import glob
import os
import csv
import pylab
import numpy

def dictString(d):
	output = ""
	for k in d.keys():
		output += "%s_%s_" % (k, d[k])
	output = output.rstrip('_')
	return output

class Connect:
	"""
	This is a simple class to provide easy
	access to a mongo database
	"""
	def __init__(self, db="test_database", table=""):

		try:		
			self.connection = Connection()
		except:
			os.system("nohup mongod --dbpath /home/xian/ &")
		self.dbName = db
		self.db = self.connection[db]
		if table:
			self.setTable(table)
	
	def setTable(self, table):
		self.table = self.db[table]
		return self.table

def GetKeys(p):
	mr = Code("function() {for (var key in this) { emit(key, null);}}")
	r = Code("function(key, stuff) { return null;}")

	result = p.map_reduce(mr, r, "keyresults")

	keys = result.distinct("_id")

	keys.remove("_id")

	return keys

def GetValues(field, table, condition={}, asArray=False):
	output = []
	for row in table.find(condition):
		if row.has_key(field):
			output.append(row[field])
	if asArray:
		output = numpy.array(output)

	return output


def KeySafe(key):
	key = key.replace(".", "_")
	return key

def StringToType(value):
	if value.isdigit():
		val = int(value)

	elif value.count('.') == 1:
		try:
			val = float(value)
		except:
			val = value

		#val = value.split('.')
		#if val[0].isdigit() and val[1].isdigit():

	else:
		val = value

	return val

def strip(i):
	i = i.strip()
	i = i.replace('\t', '')
	return i

class ReadFile:
	"""
	Class to read single or a set of data files
	Args are...
	fileName (String) - name of the file, or a pattern to be globbed
	dbName (String) - name of the DB you'd like to submit this data to
	tableName (String) - name of the table you'd like to enter this data into
	clear (Boolean) - Whether or not to erase the contents of the table 
					  before uploading this data into it
	startLine (int) - The line on which the headers appear in the file
	columns (String List) - If you want to upload only specific columns
							from the data files, put the header names in this list
	sep (String) - the character which separates data in your data file
	"""
	def __init__(self, fileName, dbName, tableName, data=None, kind="", clear=False, startLine=0, columns=[], sep=",", addrow={}, empty="not_recoverable"):

		db = Connect(dbName, tableName)
		table = db.table

		if clear:
			table.remove()

		self.table = table
		self.empty = empty
		self.sep = sep
		self.startLine = startLine
		self.columns = columns
		self.addrow = addrow

		if fileName:

			if fileName.count('*'):
				self.fileList = glob.glob(fileName)
			else:
				self.fileList = [fileName]
				self.addrow = dict(addrow, **{'source_file':fileName})

			for myFile in self.fileList:
				lines = open(myFile, 'r').readlines()
				self.process(lines, myFile)


		else:
			self.process(data, fileName)


	def process(self, raw_lines, thefile):
		lines = []
		#remove illegal chars, convert to utf, remove whitespace
		for line in raw_lines:
			lines.append(filter(lambda x: x in string.printable, line).encode('utf8').strip())

		if lines[0] == "*** Header Start ***":
			self.processEPrime(lines)
		else:
			self.processCSV(thefile)


	def processCSV(self, thefile):
		f = open(thefile, 'r')

		r = csv.reader(f, delimiter=self.sep)

		print self.sep

		print thefile

		#get the headers, make the variables
		headers = r.next()

		if headers[0].startswith('STRING\t'):
			r = csv.reader(open(thefile, 'r'), delimiter='\t')

			print "CRAZY EPRIME FILE"
			r.next()
			r.next()
			r.next()

			headers = r.next()

		print headers

		#first let's check whether we have a crazy e prime file

		headers = map(strip, headers)
		#make sure the keys are all safe
		headers = map(KeySafe, headers)

		VARs = {}
		index = {}

		for k in headers:
			if self.columns:
				if k in self.columns:
					index[k] = headers.index(k)
					VARs[k] = []
			else:
				index[k] = headers.index(k)
				VARs[k] = []

		for line in r:
			print line
			line = map(strip, line)
			row = {}
			for k in VARs.keys():
				try:
					value = line[index[k]]
					if value:
						if StringToType(value) != self.empty:
							row[k] = StringToType(value)
				except:
						if line:
							print "Error uploading value  %s" % line[index[k]]

			if self.addrow:
				row = dict(row, **self.addrow)

			try:
				self.table.insert(row)
			except: 
				print "Error uploading row %s" % row

	def processEPrime(self, lines):
		i1 = lines.index("*** Header Start ***")
		i2 = lines.index("*** Header End ***")


		header = lines[i1+1:i2]

		info = {}

		data = {}

		for h in header:
			frags = h.split(":")
			frags = map(strip, frags)

			if self.columns:
				if frags[0] in self.columns:
					info[KeySafe(frags[0])] = StringToType(frags[1])
			else:
				info[KeySafe(frags[0])] = StringToType(frags[1])

		i1 = i2

		dataLines = lines[i1 + 1:]

		trial = 1
		row = {}
		for d in dataLines:
			for k in info.keys():
				row[k] = info[k]
			if d.count(":"):
				frags = d.split(":")
				frags = map(strip, frags)
				if self.columns:
					if frags[0] in self.columns:
						row[KeySafe(frags[0])] = StringToType(frags[1])
				else:
					row[KeySafe(frags[0])] = StringToType(frags[1])
	
			elif d == "*** LogFrame End ***":
				if row:
					row['trial'] = trial
					trial = trial + 1
					if self.addrow:
						row = dict(row, **self.addrow)
					self.table.insert(row)
				row = {}
