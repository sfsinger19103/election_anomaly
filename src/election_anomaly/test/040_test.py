import db_routines as dbr
import user_interface as ui
from sqlalchemy.orm import sessionmaker
import tkinter as tk
import os


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	interact = input('Run interactively (y/n)?\n')
	if interact == 'y':
		project_root = ui.get_project_root()
		db_paramfile = ui.pick_paramfile()
		db_name = ui.pick_database(project_root,db_paramfile)
		print("pick the the raw data file")
		raw_file_path = ui.pick_filepath(initialdir=project_root)


	else:
		d = ui.config(section='parameters',msg='Pick a paramfile for 050.')
		project_root = d['project_root']
		db_paramfile = d['db_paramfile']
		db_name = d['db_name']
		raw_file_path = d['data_path']



	# project_root = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src'

	# pick db to use
	# db_paramfile = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src\jurisdictions\database.ini'

	#db_name = 'NC'

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()


	# datafile & info
	# raw_file_dir = r'/Users/Steph-Airbook/Documents/Temp/Philadelphia/data/'
	#
	# #raw_file_path = os.path.join(raw_file_dir,'small20181106.txt')
	# raw_file_path = os.path.join(raw_file_dir,'2018_general.csv')
	#raw_file_path = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src\jurisdictions\NC\data\2018g\nc_general\results_pct_20181106.txt'

	# pick munger
	munger = ui.pick_munger(mungers_dir=os.path.join(project_root,'mungers'),
							project_root=project_root,session=sess)
	sep = munger.separator.replace('\\t','\t')  # TODO find right way to read \t
	encoding = munger.encoding
	juris_short_name = 'NC'
	juris = ui.pick_juris_from_filesystem(project_root,juris_name=juris_short_name)




	# get datafile & info
	[dfile_d, enum_d, data_path] = ui.pick_datafile(project_root,sess)

	# check munger against datafile.
	munger.check_against_datafile(data_path)

	# TODO check datafile/munger against db?

	# load new datafile
	ui.new_datafile(
		sess,munger,data_path,juris=juris,project_root=project_root)

	eng.dispose()
	print('Done! (user_interface)')

	exit()
