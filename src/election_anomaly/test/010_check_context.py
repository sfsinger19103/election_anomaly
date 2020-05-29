import states_and_files as sf
import os
import user_interface as ui


if __name__ == '__main__':
	print("""Ready to load some election result data?
			"This program will walk you through the process of creating or checking
			"an automatic munger that will load your data into a database in the 
			"NIST common data format.""")

	interact = input('Run interactively (y/n)?\n')
	if interact == 'y':
		project_root = ui.get_project_root()
		juris_name = None
		db_paramfile = ui.pick_paramfile()
		db_name = ui.pick_database(project_root,db_paramfile)
		munger_name = None

	else:
		d = ui.config(section='election_anomaly',msg='Pick a paramfile for 050.')
		project_root = d['project_root']
		juris_name = d['juris_name']
		db_paramfile = d['db_paramfile']
		db_name = d['db_name']
		munger_name = d['munger_name']


	j_path = os.path.join(project_root,'jurisdictions')

	juris = ui.pick_juris_from_filesystem(project_root,j_path,check_files=True)

	print(f'Context files for {juris.short_name} are internally consistent.')
	exit()
