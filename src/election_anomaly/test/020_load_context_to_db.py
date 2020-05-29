import states_and_files as sf
import os
import user_interface as ui
import munge_routines as mr
import db_routines as dbr
from sqlalchemy.orm import sessionmaker



if __name__ == '__main__':


	interact = input('Run interactively (y/n)?\n')
	if interact == 'y':
		project_root = ui.get_project_root()
		db_paramfile = ui.pick_paramfile()
		db_name = ui.pick_database(project_root,db_paramfile)

	else:
		d = ui.config(section='election_anomaly',msg='Pick a paramfile for 050.')
		project_root = d['project_root']
		db_paramfile = d['db_paramfile']
		db_name = d['db_name']


	j_path = os.path.join(project_root, 'jurisdictions')
	juris = ui.pick_juris_from_filesystem(project_root, j_path, check_files=False)
	juris_short_name = None

	# project_root = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src'
	#
	# j_path = os.path.join(project_root,'jurisdictions')

	#juris_short_name = None
	# juris = ui.pick_juris_from_filesystem(project_root,j_path,check_files=False)
	#
	# # pick db to use
	# db_paramfile = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src\jurisdictions\database.ini'
	# db_name = ui.pick_database(project_root,db_paramfile)

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=db_paramfile,db_name=db_name)
	Session = sessionmaker(bind=eng)
	sess = Session()

	juris.load_context_to_db(sess,project_root)

	eng.dispose()
	exit()
