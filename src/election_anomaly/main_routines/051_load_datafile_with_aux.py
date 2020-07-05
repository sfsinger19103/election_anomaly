from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from election_anomaly import juris_and_munger as jm
from sqlalchemy.orm import sessionmaker
import os


if __name__ == '__main__':

	d, error = ui.get_runtime_parameters(
		['project_root','juris_name','db_paramfile','db_name','munger_name','results_file','aux_data_dir'])
	# TODO handle 'aux_data_dir' as an optional parameter

	# pick jurisdiction
	juris = jm.Jurisdiction(d['juris_name'],'/Users/singer3/PycharmProjects/results_analysis/src/jurisdictions')

	# create db if it does not already exist
	dbr.establish_connection(paramfile=d['db_paramfile'],db_name=d['db_name'])

	# connect to db
	eng = dbr.sql_alchemy_connect(paramfile=d['db_paramfile'],db_name=d['db_name'])
	Session = sessionmaker(bind=eng)
	sess = Session()

	# pick munger
	munger, munger_error = ui.pick_munger(
		project_root=d['project_root'],
		mungers_dir=os.path.join(d['project_root'],'mungers'),session=sess,munger_name=d['munger_name'])

	# load new datafile
	ui.new_datafile(sess,munger,d['results_file'],juris=juris,project_root=d['project_root'],aux_data_dir=d['aux_data_dir'])

	eng.dispose()
	print('Done!')

	exit()
