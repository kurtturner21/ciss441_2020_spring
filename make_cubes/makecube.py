import os, sys, sqlite3, json, csv, time

###
#Class: CISS 441
#Author: Kurt Turner
#Date: 02/05/2017
#Purpose: Create a dymanic data file parser to read csv files, set data types, create tables and load data
###

datafiles = {}
datadir = 'data'
dbfile = 'payroll_dc.db'

#main method to call all other methods. 
def main():
	finedatafiles()
	reviewdatafiles()
	createsqlitetables()
	loaddataintoDB()
	savedatafiles()

#This method will load data into SqlLite3 db
def loaddataintoDB():
	filecount = 0			#set filecount var
	for tbn in datafiles.keys():
		filecount += 1												#increment filecount var
		fpath = datafiles[tbn]['path']
		rowcount = 0												#set rowcount var
		errorct = 0
		conn = sqlite3.connect(dbfile)
		c = conn.cursor()
		print(filecount, ') starting on ', tbn)
		with open(fpath, 'r') as csvfile:
			data = csv.reader(csvfile)
			for row in data:
				rowcount += 1										#increment rowcount var
				if rowcount == 1: continue							#bypass the fields
				# if rowcount > 20000: continue						#to limit the number of rows being processed.
				
				dccount = 0											#set data cell count var
				values = []
				for datacell in row:
					dt = datafiles[tbn]['datatypes'][dccount]
					
					if 'text' == dt:
						datacell = datacell.replace('\'', '\'\'')
						if dt == '' or dt == None: datacell = 'nothing'
						values.append('\'{0}\''.format(datacell))
					else: 
						if dt == '' or dt == None: datacell = '0'
						for badchar in ['$', ',']:
							datacell = datacell.replace(badchar, '')
						values.append('{0}'.format(datacell))
						
					dccount += 1								#increment after useage because index for dictionary starts at 0	
	
				try:
					strsql = 'insert into {0} ({1}) values ({2});'.format(tbn, ','.join(datafiles[tbn]['fields']), ','.join(values))
					c.execute(strsql)
					conn.commit()
				except:
					errorct += 1
					if errorct < 5:
						print("Unexpected error:", sys.exc_info()[0])
						print(strsql)
						
		print(filecount, ') finished with ', tbn, ' rows: ', rowcount - 1)
		conn.close()			
	

#create tables from datafiles var:
def createsqlitetables():
	conn = sqlite3.connect(dbfile)
	c = conn.cursor()
	for tbn in datafiles.keys():
		mkfields = []
		for fn in datafiles[tbn]['fields']:
			if 'int' in datafiles[tbn]['fielddef'][fn].keys() and len(datafiles[tbn]['fielddef'][fn].keys()) == 1:
				mkfields.append(' {0} int'.format(fn))
				datafiles[tbn]['datatypes'].append('int')
			elif 'float' in datafiles[tbn]['fielddef'][fn].keys() and len(datafiles[tbn]['fielddef'][fn].keys()) == 1:
				mkfields.append(' {0} float'.format(fn))
				datafiles[tbn]['datatypes'].append('float')
			elif 'money' in datafiles[tbn]['fielddef'][fn].keys() and  'NULL' in datafiles[tbn]['fielddef'][fn].keys() and len(datafiles[tbn]['fielddef'][fn].keys()) == 2:
				mkfields.append(' {0} int'.format(fn))
				datafiles[tbn]['datatypes'].append('int')
			else:
				mkfields.append(' {0} text'.format(fn))
				datafiles[tbn]['datatypes'].append('text')

		strsql = 'CREATE TABLE {0} ({1});'.format(tbn, ', '.join(mkfields))
		c.execute(strsql)
		conn.commit()
	conn.close()
	
	
#walk all files in data folder. 
def finedatafiles():
	for root, dirs, files in os.walk(datadir, topdown=False):				#use os.walk method to find all the files in data dir
		for name in files:													#lets just look at the files list to create table list.
			fileext = name.split('.')[len(name.split('.'))-1]				#define the file extension
			if fileext.lower() not in ['txt', 'csv']: continue				#if the file name is not text or csv continue to next file
			tbn = str(name).lower().replace('.txt', '').replace('dt', '')	#define table name
			isfacttable = False												#define isfacttable to be false
			if 'fact' in tbn: isfacttable = True							#if the table name as fact in it, then declare facttable
			datafiles.update({tbn:{											#setup datafile dictionary
				'path':os.path.join(root, name),
				'fileext': fileext,
				'facttable': isfacttable,
				'fields': [],
				'datatypes': [],
				'fielddef': {}
				}})
		
#Review field names and data tiles. 				
def reviewdatafiles():
	filecount = 0													#set filecount var
	for tbn in datafiles.keys():
		filecount += 1												#increment filecount var
		fieldnames = []
		fielddef = {}
		fpath = datafiles[tbn]['path']
		rowcount = 0												#set rowcount var
		with open(fpath, 'r') as csvfile:
			data = csv.reader(csvfile)
			for row in data:
				rowcount += 1										#increment rowcount var
				
				if rowcount == 1:									#header row. 
					fieldnames = row								#document field names
					for fn in row: fielddef.update({fn: {}})		#setup the dict for the field def
				else:												#data rows
					dccount = 0										#set data cell count var
					for datacell in row:
					
						#the following if statment is just for testing. 
						#if dccount not in []: print(fieldnames[dccount], datacell, datacell.isnumeric(), testfloat(datacell))
					
						if datacell == '' or datacell == None:	#found blank
							if 'NULL' not in fielddef[fieldnames[dccount]].keys(): fielddef[fieldnames[dccount]].update({'NULL': 1})
							else: fielddef[fieldnames[dccount]]['NULL'] += 1
						elif '$' in datacell and testfloat(datacell.replace('$','').replace(',','')) == True:	#found money
							if 'money' not in fielddef[fieldnames[dccount]].keys(): fielddef[fieldnames[dccount]].update({'money': 1})
							else: fielddef[fieldnames[dccount]]['money'] += 1
						elif datacell.isnumeric() == False and testfloat(datacell) == True and '.' in datacell:	#found float
							if 'float' not in fielddef[fieldnames[dccount]].keys(): fielddef[fieldnames[dccount]].update({'float': 1})
							else: fielddef[fieldnames[dccount]]['float'] += 1
						elif datacell.isnumeric() == False:		#found text
							if 'text' not in fielddef[fieldnames[dccount]].keys(): fielddef[fieldnames[dccount]].update({'text': 1})
							else: fielddef[fieldnames[dccount]]['text'] += 1
						else: 									#assume int
							if 'int' not in fielddef[fieldnames[dccount]].keys(): fielddef[fieldnames[dccount]].update({'int': 1})
							else: fielddef[fieldnames[dccount]]['int'] += 1
						
							
							
						dccount += 1								#increment after useage because index for dictionary starts at 0							
						
				if rowcount > 100: break								#break after three rows. 
					
							
		datafiles[tbn]['fields'] = fieldnames						#add documented fields to the data files dict
		datafiles[tbn]['fielddef'] = fielddef						#add fields configs to the data files dict
		
		
def testfloat(var):
	try:
		testvar = float(var)
	except:
		return False
	return True
		
#save data file dict to a json file
def savedatafiles():
	jsondata = json.dumps(datafiles, sort_keys=True, indent=4, separators=(',', ': '))
	file = open('datafile.json', 'w')
	file.write(jsondata)
	file.close()

if __name__ == "__main__": 
	main()
	
