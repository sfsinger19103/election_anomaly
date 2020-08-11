import csv
import os.path

import pandas as pd
from election_anomaly import user_interface as ui
from election_anomaly import munge_routines as mr
import datetime
import os
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from pandas.api.types import is_numeric_dtype
from election_anomaly import db_routines as dbr
import scipy.spatial.distance as dist
from scipy import stats



def child_rus_by_id(session,parents,ru_type=None):
	"""Given a list <parents> of parent ids (or just a single parent_id), return
	list containing all children of those parents.
	(By convention, a ReportingUnit counts as one of its own 'parents',)
	If (ReportingUnitType_Id,OtherReportingUnit) pair <rutype> is given,
	restrict children to that ReportingUnitType"""
	cruj = pd.read_sql_table('ComposingReportingUnitJoin',session.bind)
	children = list(cruj[cruj.ParentReportingUnit_Id.isin(parents)].ChildReportingUnit_Id.unique())
	if ru_type:
		assert len(ru_type) == 2,f'argument {ru_type} does not have exactly 2 elements'
		ru = pd.read_sql_table('ReportingUnit',session.bind,index_col='Id')
		right_type_ru = ru[(ru.ReportingUnitType_Id == ru_type[0]) & (ru.OtherReportingUnitType == ru_type[1])]
		children = [x for x in children if x in right_type_ru.index]
	return children


def create_rollup(
		session,target_dir,top_ru_id=None,sub_rutype_id=None,sub_rutype_othertext=None,election_id=None,
		datafile_id_list=None,by_vote_type=True,exclude_total=True):
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id>,<sub_rutype_othertext> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' is the only CountItemType)"""
	# Get name of db for error messages
	db = session.bind.url.database

	# ask user to select any info not supplied
	if top_ru_id is None:
		# TODO allow passage of top_ru name, from which id is deduced. Similarly for other args.
		print('Select the type of the top ReportingUnit for the rollup.')
		top_rutype_id, top_rutype_othertext, top_rutype = ui.pick_enum(session,'ReportingUnitType')
		print('Select the top ReportingUnit for the rollup')
		top_ru_id, top_ru = ui.pick_record_from_db(
			session,'ReportingUnit',known_info_d={
				'ReportingUnitType_Id':top_rutype_id, 'OtherReportingUnitType':top_rutype_othertext},required=True)
	else:
		top_ru_id, top_ru = ui.pick_record_from_db(session,'ReportingUnit',required=True,db_idx=top_ru_id)
	if election_id is None:
		print('Select the Election')
	election_id,election = ui.pick_record_from_db(session,'Election',required=True,db_idx=election_id)

	if datafile_id_list is None:
		# TODO allow several datafiles to be picked
		# TODO restrict to datafiles whose ReportingUnit intersects top_ru?
		# TODO note/enforce that no datafile double counts anything?
		print('Select the datafile')
		datafile_id_list = ui.pick_record_from_db(
			session,'_datafile',required=True,known_info_d={'Election_Id':election_id})[0]
	if sub_rutype_id is None:
		# TODO restrict to types that appear as sub-reportingunits of top_ru?
		#  Or that appear in VoteCounts associated to one of the datafiles?
		print('Select the ReportingUnitType for the lines of the rollup')
		sub_rutype_id, sub_rutype_othertext,sub_rutype = ui.pick_enum(session,'ReportingUnitType')
	else:
		sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','CandidateContestSelectionJoin',
		'BallotMeasureContestSelectionJoin','ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')

	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)

	#  limit to relevant Election-Contest pairs
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]

	# create contest_selection dataframe, adding Contest, Selection and ElectionDistrict_Id columns
	cc = df['CandidateContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='CandidateContest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='CandidateSelection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection','CandidateContest_Id':'Contest_Id',
				'CandidateSelection_Id':'Selection_Id'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	cc = cc[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id']]
	if cc.empty:
		cc['contest_type'] = None
	else:
		cc.loc[:,'contest_type'] = 'Candidate'

	# create ballotmeasure_selection dataframe
	bm = df['BallotMeasureContestSelectionJoin'].merge(
		df['BallotMeasureContest'],how='left',left_on='BallotMeasureContest_Id',right_index=True).rename(
		columns={'Name':'Contest'}).merge(
		df['BallotMeasureSelection'],how='left',left_on='BallotMeasureSelection_Id',right_index=True).rename(
		columns={'BallotMeasureSelection_Id':'Selection_Id','BallotMeasureContest_Id':'Contest_Id'}
	)
	bm = bm[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id']]
	if bm.empty:
		bm['contest_type'] = None
	else:
		bm.loc[:,'contest_type'] = 'BallotMeasure'

	#  combine all contest_selections into one dataframe
	contest_selection = pd.concat([cc,bm])

	# append contest_district_type column
	ru = df['ReportingUnit'][['ReportingUnitType_Id','OtherReportingUnitType']]
	contest_selection = contest_selection.merge(ru,how='left',left_on='ElectionDistrict_Id',right_index=True)
	contest_selection = mr.enum_col_from_id_othertext(contest_selection,'ReportingUnitType',df['ReportingUnitType'])
	contest_selection.rename(columns={'ReportingUnitType':'contest_district_type'},inplace=True)

	#  limit to relevant ContestSelection pairs
	contest_ids = ecj.Contest_Id.unique()
	csj = contest_selection[contest_selection.Contest_Id.isin(contest_ids)]

	# find ReportingUnits of the correct type that are subunits of top_ru
	sub_ru_ids = child_rus_by_id(session,[top_ru_id],ru_type=[sub_rutype_id, sub_rutype_othertext])
	if not sub_ru_ids:
		# TODO better error handling (while not sub_ru_list....)
		raise Exception(f'Database {db} shows no ReportingUnits of type {sub_rutype} nested inside {top_ru}')
	sub_ru = df['ReportingUnit'].loc[sub_ru_ids]

	# find all subReportingUnits of top_ru
	all_subs_ids = child_rus_by_id(session,[top_ru_id])

	# find all children of subReportingUnits
	children_of_subs_ids = child_rus_by_id(session,sub_ru_ids)
	ru_children = df['ReportingUnit'].loc[children_of_subs_ids]

	# check for any reporting units that should be included in roll-up but were missed
	# TODO list can be long and irrelevant. Instead list ReportingUnitTypes of the missing
	# missing = [str(x) for x in all_subs_ids if x not in children_of_subs_ids]
	# if missing:
	# TODO report these out to the export directory
	#	ui.report_problems(missing,msg=f'The following reporting units are nested in {top_ru["Name"]} '
	#							f'but are not nested in any {sub_rutype} nested in {top_ru["Name"]}')

	# limit to relevant vote counts
	ecsvcj = df['ElectionContestSelectionVoteCountJoin'][
		(df['ElectionContestSelectionVoteCountJoin'].ElectionContestJoin_Id.isin(ecj.index)) &
		(df['ElectionContestSelectionVoteCountJoin'].ContestSelectionJoin_Id.isin(csj.index))]

	# calculate specified dataframe with columns [ReportingUnit,Contest,Selection,VoteCount,CountItemType]
	#  1. create unsummed dataframe of results
	unsummed = ecsvcj.merge(
		df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
		df['ComposingReportingUnitJoin'],left_on='ReportingUnit_Id',right_on='ChildReportingUnit_Id').merge(
		ru_children,left_on='ChildReportingUnit_Id',right_index=True).merge(
		sub_ru,left_on='ParentReportingUnit_Id',right_index=True,suffixes=['','_Parent'])
	unsummed.rename(columns={'Name_Parent':'ReportingUnit'},inplace=True)
	# add columns with names
	unsummed = mr.enum_col_from_id_othertext(unsummed,'CountItemType',df['CountItemType'])
	unsummed = unsummed.merge(contest_selection,how='left',left_on='ContestSelectionJoin_Id',right_index=True)

	cis = 'unknown'  # TODO placeholder while CountItemStatus is unused
	if by_vote_type:
		cit_list = unsummed['CountItemType'].unique()
	else:
		cit_list = ['all']
		if exclude_total:
			unsummed = unsummed[unsummed.CountItemType != 'total']
	if len(cit_list) > 1:
		cit = 'mixed'
		if exclude_total:
			unsummed = unsummed[unsummed.CountItemType != 'total']
	elif len(cit_list) == 1:
		cit = cit_list[0]
	else:
		raise Exception(
			f'Results dataframe has no CountItemTypes; maybe dataframe is empty?')
	count_item = f'TYPE{cit}_STATUS{cis}'

	if by_vote_type:
		index_cols = ['contest_type','Contest','contest_district_type','Selection','ReportingUnit','CountItemType']
	else:
		index_cols = ['contest_type','Contest','contest_district_type','Selection','ReportingUnit']

	# sum by groups
	summed_by_name = unsummed[index_cols + ['Count']].groupby(index_cols).sum()

	inventory_columns = [
		'Election','ReportingUnitType','CountItemType','CountItemStatus',
		'source_db_url','timestamp']
	inventory_values = [
		election['Name'],sub_rutype,cit,cis,
		str(session.bind.url),datetime.date.today()]
	sub_dir = os.path.join(election['Name'],top_ru["Name"],f'by_{sub_rutype}')
	export_to_inventory_file_tree(
		target_dir,sub_dir,f'{count_item}.txt',inventory_columns,inventory_values,summed_by_name)

	return summed_by_name


def short_name(text,sep=';'):
	return text.split(sep)[-1]


def export_to_inventory_file_tree(target_dir,target_sub_dir,target_file,inventory_columns,inventory_values,df):
	# TODO standard order for columns
	# export to file system
	out_path = os.path.join(
		target_dir,target_sub_dir)
	Path(out_path).mkdir(parents=True,exist_ok=True)

	while os.path.isfile(os.path.join(out_path,target_file)):
		target_file = input(f'There is already a file called {target_file}. Pick another name.\n')

	out_file = os.path.join(out_path,target_file)
	df.to_csv(out_file,sep='\t')

	# create record in inventory.txt
	inventory_file = os.path.join(target_dir,'inventory.txt')
	inv_exists = os.path.isfile(inventory_file)
	if inv_exists:
		# check that header matches inventory_columns
		with open(inventory_file,newline='') as f:
			reader = csv.reader(f,delimiter='\t')
			file_header = next(reader)
			# TODO: offer option to delete inventory file
			assert file_header == inventory_columns, \
				f'Header of file {f} is\n{file_header},\ndoesn\'t match\n{inventory_columns}.'

	with open(inventory_file,'a',newline='') as csv_file:
		wr = csv.writer(csv_file,delimiter='\t')
		if not inv_exists:
			wr.writerow(inventory_columns)
		wr.writerow(inventory_values)

	print(f'Results exported to {out_file}')
	return


def create_scatter(session, top_ru_id, sub_rutype_id, election_id, datafile_id_list,
	candidate_1_id, candidate_2_id, count_item_type):
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id>,<sub_rutype_othertext> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' is the only CountItemType)"""
	# Get name of db for error messages
	db = session.bind.url.database

	top_ru_id, top_ru = ui.pick_record_from_db(session,'ReportingUnit',required=True,db_idx=top_ru_id)
	election_id,election = ui.pick_record_from_db(session,'Election',required=True,db_idx=election_id)

	sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','CandidateContestSelectionJoin',
		'BallotMeasureContestSelectionJoin','ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')

	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)

	#  limit to relevant Election-Contest pairs
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]

	# create contest_selection dataframe, adding Contest, Selection and ElectionDistrict_Id columns
	contest_selection = df['CandidateContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='CandidateContest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='CandidateSelection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection','CandidateContest_Id':'Contest_Id',
				'CandidateSelection_Id':'Selection_Id'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	contest_selection = contest_selection[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id',
		'Candidate_Id']]
	if contest_selection.empty:
		contest_selection['contest_type'] = None
	else:
		contest_selection.loc[:,'contest_type'] = 'Candidate'

	# append contest_district_type column
	ru = df['ReportingUnit'][['ReportingUnitType_Id','OtherReportingUnitType']]
	contest_selection = contest_selection.merge(ru,how='left',left_on='ElectionDistrict_Id',right_index=True)
	contest_selection = mr.enum_col_from_id_othertext(contest_selection,'ReportingUnitType',df['ReportingUnitType'])
	contest_selection.rename(columns={'ReportingUnitType':'contest_district_type'},inplace=True)

	# limit to relevant ContestSelection pairs
	contest_ids = ecj.Contest_Id.unique()
	csj = contest_selection[contest_selection.Contest_Id.isin(contest_ids)]

	#csj = contest_selection
	# limit to 
	candidate_ids = [candidate_1_id, candidate_2_id]
	csj = csj[csj.Candidate_Id.isin(candidate_ids)]

	# find ReportingUnits of the correct type that are subunits of top_ru
	sub_ru_ids = child_rus_by_id(session,[top_ru_id],ru_type=[sub_rutype_id, ''])
	if not sub_ru_ids:
		# TODO better error handling (while not sub_ru_list....)
		raise Exception(f'Database {db} shows no ReportingUnits of type {sub_rutype} nested inside {top_ru}')
	sub_ru = df['ReportingUnit'].loc[sub_ru_ids]

	# find all subReportingUnits of top_ru
	all_subs_ids = child_rus_by_id(session,[top_ru_id])

	# find all children of subReportingUnits
	children_of_subs_ids = child_rus_by_id(session,sub_ru_ids)
	ru_children = df['ReportingUnit'].loc[children_of_subs_ids]

	# limit to relevant vote counts
	ecsvcj = df['ElectionContestSelectionVoteCountJoin'][
		(df['ElectionContestSelectionVoteCountJoin'].ElectionContestJoin_Id.isin(ecj.index)) &
		(df['ElectionContestSelectionVoteCountJoin'].ContestSelectionJoin_Id.isin(csj.index))]

	# calculate specified dataframe with columns [ReportingUnit,Contest,Selection,VoteCount,CountItemType]
	#  1. create unsummed dataframe of results
	unsummed = ecsvcj.merge(
		df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
		df['ComposingReportingUnitJoin'],left_on='ReportingUnit_Id',right_on='ChildReportingUnit_Id').merge(
		ru_children,left_on='ChildReportingUnit_Id',right_index=True).merge(
		sub_ru,left_on='ParentReportingUnit_Id',right_index=True,suffixes=['','_Parent'])
	unsummed.rename(columns={'Name_Parent':'ReportingUnit'},inplace=True)
	# add columns with names
	unsummed = mr.enum_col_from_id_othertext(unsummed,'CountItemType',df['CountItemType'])
	unsummed = unsummed.merge(contest_selection,how='left',left_on='ContestSelectionJoin_Id',right_index=True)

	# filter based on vote count type
	unsummed = unsummed[unsummed['CountItemType'] == count_item_type]

	# package into dictionary
	x = dbr.name_from_id(session, 'Candidate', candidate_1_id)
	y = dbr.name_from_id(session, 'Candidate', candidate_2_id) 
	results = {
		"election": dbr.name_from_id(session, 'Election', election_id),
		"jurisdiction": dbr.name_from_id(session, 'ReportingUnit', top_ru_id),
		"contest": dbr.name_from_id(session, 'CandidateContest', unsummed.iloc[0]['Contest_Id']),
		"subdivision_type": dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id),
		"count_item_type": count_item_type,
		"x": x,
		"y": y,
		"counts": []
	}
	pivot_df = pd.pivot_table(unsummed, values='Count', 
		index=['ReportingUnit_Id', 'Name'], \
		columns='Selection').reset_index()
	for i, row in pivot_df.iterrows():
		results['counts'].append({
			'name': row['Name'],
			'x': row[x],
			'y': row[y],
			'score': 0
		})
		
	return results

def create_bar(session, top_ru_id, contest_type, contest, election_id, datafile_id_list):
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id>,<sub_rutype_othertext> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' is the only CountItemType)"""
	# Get name of db for error messages
	db = session.bind.url.database

	top_ru_id, top_ru = ui.pick_record_from_db(session,'ReportingUnit',required=True,db_idx=top_ru_id)
	election_id,election = ui.pick_record_from_db(session,'Election',required=True,db_idx=election_id)

	#sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','CandidateContestSelectionJoin',
		'BallotMeasureContestSelectionJoin','ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')

	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)

	#  limit to relevant Election-Contest pairs
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]

	# create contest_selection dataframe, adding Contest, Selection and ElectionDistrict_Id columns
	contest_selection = df['CandidateContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='CandidateContest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='CandidateSelection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection','CandidateContest_Id':'Contest_Id',
				'CandidateSelection_Id':'Selection_Id'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	contest_selection = contest_selection[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id',
		'Candidate_Id']]
	if contest_selection.empty:
		contest_selection['contest_type'] = None
	else:
		contest_selection.loc[:,'contest_type'] = 'Candidate'

	# append contest_district_type column
	ru = df['ReportingUnit'][['ReportingUnitType_Id','OtherReportingUnitType']]
	contest_selection = contest_selection.merge(ru,how='left',left_on='ElectionDistrict_Id',right_index=True)
	contest_selection = mr.enum_col_from_id_othertext(contest_selection,'ReportingUnitType',df['ReportingUnitType'])
	contest_selection.rename(columns={'ReportingUnitType':'contest_district_type'},inplace=True)

	if contest_type:
		contest_selection = contest_selection[contest_selection['contest_district_type'] == contest_type]
	if contest:
		contest_selection = contest_selection[contest_selection['Contest'] == contest]
	# limit to relevant ContestSelection pairs
	contest_ids = ecj.Contest_Id.unique()
	csj = contest_selection[contest_selection.Contest_Id.isin(contest_ids)]

	# limit to relevant vote counts
	ecsvcj = df['ElectionContestSelectionVoteCountJoin'][
		(df['ElectionContestSelectionVoteCountJoin'].ElectionContestJoin_Id.isin(ecj.index)) &
		(df['ElectionContestSelectionVoteCountJoin'].ContestSelectionJoin_Id.isin(csj.index))]

	# Create data frame of all our results at all levels
	unsummed = ecsvcj.merge(
		df['VoteCount'],left_on='VoteCount_Id',right_index=True).merge(
		df['ComposingReportingUnitJoin'],left_on='ReportingUnit_Id',right_on='ChildReportingUnit_Id').merge(
		df['ReportingUnit'],left_on='ChildReportingUnit_Id',right_index=True).merge(
		df['ReportingUnit'],left_on='ParentReportingUnit_Id',right_index=True,suffixes=['','_Parent'])
	
	# add columns with names
	unsummed = mr.enum_col_from_id_othertext(unsummed,'CountItemType',df['CountItemType'])
	unsummed = unsummed.merge(contest_selection,how='left',left_on='ContestSelectionJoin_Id',right_index=True)

	# cleanup: Rename, drop a duplicated column
	rename = {
		'Name_Parent': 'ParentName',
		'ReportingUnitType_Id_Parent': 'ParentReportingUnitType_Id'
	}
	unsummed.rename(columns=rename, inplace=True)
	unsummed.drop(columns=['_datafile_Id', 'OtherReportingUnitType', 
		'ChildReportingUnit_Id', 'ElectionContestJoin_Id','OtherReportingUnitType_Parent', 
		'ContestSelectionJoin_Id'],
		inplace=True)
	unsummed = unsummed[unsummed['ParentReportingUnit_Id'] != top_ru_id]

	ranked = assign_anomaly_score(unsummed)
	ranked_margin = calculate_margins(ranked)
	votes_at_stake = calculate_votes_at_stake(ranked_margin)
	return votes_at_stake
	top_ranked = get_most_anomalous(votes_at_stake, 3)
	#return top_ranked

	# package into list of dictionary
	result_list = []
	ids = top_ranked['unit_id'].unique()
	for id in ids:
		temp_df = top_ranked[top_ranked['unit_id'] == id]

		candidates = temp_df['Candidate_Id'].unique()
		x = dbr.name_from_id(session, 'Candidate', candidates[0])
		y = dbr.name_from_id(session, 'Candidate', candidates[1]) 
		results = {
			"election": dbr.name_from_id(session, 'Election', election_id),
			"jurisdiction": dbr.name_from_id(session, 'ReportingUnit', top_ru_id),
			"contest": dbr.name_from_id(session, 'CandidateContest', temp_df.iloc[0]['Contest_Id']),
			# TODO: remove hard coded subdivision type
			"subdivision_type": dbr.name_from_id(session, 'ReportingUnitType', 
				temp_df.iloc[0]['ReportingUnitType_Id']),
			"count_item_type": temp_df.iloc[0]['CountItemType'],
			"x": x,
			"y": y,
			"margin": temp_df.iloc[0]['margins'],
			"votes_at_stake": temp_df.iloc[0]['max_votes_at_stake'], 
			"counts": []
		}
		pivot_df = pd.pivot_table(temp_df, values='Count', 
			index=['ReportingUnit_Id', 'Name', 'score', 'margins'], \
			columns='Selection').sort_values('margins', ascending=False).reset_index()
		#reporting_units = pivot_df.Name.unique()
		#for reporting_unit in reporting_units:
		#	results["counts"][reporting_unit] = {}

		for i, row in pivot_df.iterrows():
			results['counts'].append({
				'name': row['Name'],
				'x': row[x],
				'y': row[y],
				'score': row['score'],
				'margin': row['margins']
			})
			# if row.Selection == x:
			# 	results["counts"][row.Name]["x"] = row.Count
			# elif row.Selection == y:
			# 	results["counts"][row.Name]["y"] = row.Count
			# results["counts"][row.Name]["score"] = row.score
		result_list.append(results)
		
	return result_list


def assign_anomaly_score(data):
	"""adds a new column called score between 0 and 1; 1 is more anomalous.
	Also adds a `unit_id` column which assigns a score to each unit of analysis
	that is considered. For example, we may decide to look at anomalies across each
	distinct combination of contest, reporting unit type, and vote type. Each 
	combination of those would get assigned an ID. This means rows may get added
	to the dataframe if needed."""

	######### FOR TESTING PURPOSES ONLY!!!!! ###########
	data = data[data['Contest_Id'] == 14949] # only 2 candidates
	#data = data[data['Contest_Id'] == 14777] # 3 candidates

	# Assign a ranking for each candidate by votes for each contest
	total_data = data[(data['CountItemType'] == 'total') & 
					(data['ReportingUnit_Id'] == data['ParentReportingUnit_Id'])]
	ranked_df = total_data.groupby(['Contest_Id', 'Selection'], as_index=False)['Count'] \
				.sum().sort_values(['Contest_Id', 'Count'], ascending=False)
	ranked_df['rank'] = ranked_df.groupby('Contest_Id')['Count'].rank('dense', ascending=False)
	ranked_df['contest_total'] = ranked_df['Count'].sum()
	ranked_df.rename(columns={'Count':'ind_total'}, inplace=True)

	# Group data by parent info. This works because each child is also its own
	# parent in the DB table
	grouped_df = data.groupby(['ParentReportingUnit_Id', 'ParentName', 
			'ParentReportingUnitType_Id', 'Candidate_Id',
            'CountItemType', 'Contest_Id', 'Contest', 'Selection', 
            'contest_type', 'contest_district_type'], as_index=False)['Count'].sum().reset_index()
	grouped_df.drop(columns='index', inplace=True)
	grouped_df.rename(columns={
		'ParentReportingUnit_Id': 'ReportingUnit_Id',
		'ParentName': 'Name',	
		'ParentReportingUnitType_Id': 'ReportingUnitType_Id'
	}, inplace=True)
	grouped_df = grouped_df.merge(ranked_df, how='inner', on=['Contest_Id', 'Selection'])

	# assign unit_ids to unique combination of contest, ru_type, and count type
	df_unit = grouped_df[['Contest_Id', 'ReportingUnitType_Id', 'CountItemType']].drop_duplicates()
	df_unit = df_unit.reset_index()
	df_unit['unit_id'] = df_unit.index
	df_with_units = grouped_df.merge(df_unit, how='left', on=['Contest_Id', 'ReportingUnitType_Id', 'CountItemType'])

	# loop through each unit ID and assign anomaly scores
	unit_ids = df_with_units['unit_id'].unique()
	df = pd.DataFrame()
	# for each unit ID
	for unit_id in unit_ids:
		# grab all the data there
		temp_df = df_with_units[df_with_units['unit_id'] == unit_id]
		total = temp_df.groupby('ReportingUnit_Id')['Count'].sum().reset_index()
		total.rename(columns={'Count': 'reporting_unit_total'}, inplace=True)
		temp_df = temp_df.merge(total, how='inner', on='ReportingUnit_Id')
		# if there are more than 2 candidates, just take the top 2
		# TODO: do pairwise comparison against winner
		# if there are more than 2 candidates
		#for i in range(2, temp_df['rank'].max() + 1):
		# if len(temp_df['Selection'].unique()) > 2:
		# 	# get the contest ID
		# 	contest_id = temp_df.iloc[0]['Contest_Id']
		# 	# get the reporting unit type ID
		# 	reporting_unit_type_id = temp_df.iloc[0]['ReportingUnitType_Id']
		# 	# create a new dataframe for total data
		# 	total_df = df_with_units[(df_with_units['Contest_Id'] == contest_id) &
		# 				(df_with_units['CountItemType'] == 'total') &
		# 				(df_with_units['ReportingUnitType_Id'] == reporting_unit_type_id)]
		# 	# get the total counts per candidate
		# 	counts = total_df.groupby('Selection')['Count'].sum().sort_values(ascending=False)
		# 	# get the top 2
		# 	top = list(counts.index[0:2])
		# 	# filter temp dataframe on top 2
		# 	temp_df = temp_df[temp_df['Selection'].isin(top)]
		# for i in range(len(temp_df['rank'].unique())
		# pivot so each candidate gets own column
		scored_df = pd.DataFrame()
		for i in range(2, int(temp_df['rank'].max()) + 1):
			selection_df = temp_df[temp_df['rank'].isin([1, i])]
			if selection_df.shape[0] >= 5:
				pivot_df = pd.pivot_table(selection_df, values='Count', \
					index=['ReportingUnit_Id', 'reporting_unit_total'], \
					columns='Selection').sort_values('ReportingUnit_Id').reset_index()
				pivot_df_values = pivot_df.drop(columns=['ReportingUnit_Id', 'reporting_unit_total'])
				to_drop = [selection_df[selection_df['rank'] == 1]['Selection'].unique()[0], \
					selection_df[selection_df['rank'] == i]['Selection'].unique()[0]]
				# pass in proportions instead of raw vlaues
				vote_proportions = pivot_df_values.div(pivot_df['reporting_unit_total'], axis=0)
				np.nan_to_num(vote_proportions, copy=False)
				# assign z score and then add back into final DF
				scored = euclidean_zscore(vote_proportions.to_numpy())
				#scored = density_score(vote_proportions)
				pivot_df['score'] = scored
				pivot_df = pivot_df[['ReportingUnit_Id', to_drop[1], 'score']]
				pivot_df['Selection'] = to_drop[1]
				pivot_df.rename(columns={to_drop[1]: 'Count'}, inplace=True)
				selection_df = selection_df.merge(pivot_df, how='left', \
					on=['ReportingUnit_Id', 'Selection', 'Count'])
				scored_df = pd.concat([scored_df, selection_df])
		df = pd.concat([df, scored_df])
	df['score'] = df['score'].fillna(0)
	return df


def get_most_anomalous(data, n):
	"""gets the n contests with the highest votes_at_stake score"""
	# get rid of all contest-counttypes with 0 votes
	# not sure we really want to do this in final version
	zeros_df = data[['Contest_Id', 'ReportingUnitType_Id', 'CountItemType', 'ReportingUnit_Id', 'Count']]
	zeros_df = zeros_df.groupby(['Contest_Id', 'ReportingUnitType_Id', 'ReportingUnit_Id', 'CountItemType']).sum()
	zeros_df = zeros_df.reset_index()
	no_zeros = zeros_df[zeros_df['Count'] != 0]
	data = data.merge(no_zeros, how='inner', on=['Contest_Id', 'ReportingUnitType_Id', 'ReportingUnit_Id', 'CountItemType'])
	data.rename(columns={'Count_x':'Count'}, inplace=True)
	data.drop(columns=['Count_y'], inplace=True)

	# Now do the filtering on most anomalous
	df = data.groupby('unit_id')['votes_at_stake'].max().reset_index()
	df.rename(columns={'votes_at_stake': 'max_votes_at_stake'}, inplace=True)
	data = data.merge(df, on='unit_id')
	unique_scores = sorted(set(df['max_votes_at_stake']), reverse=True)
	top_scores = unique_scores[:n]
	result = data[data['max_votes_at_stake'].isin(top_scores)]

	# Eventually we want to return the winner and the most anomalous
	# for each contest grouping (unit). For now, just 2 random ones
	ids = result['unit_id'].unique()
	df = pd.DataFrame()
	for id in ids:
		temp_df = result[result['unit_id'] == id]
		#unique = temp_df['Candidate_Id'].unique()
		#candidates = unique[0:2]
		#candidates = temp_df['Candidate_Id'].unique()
		#candidate_df = temp_df[temp_df['Candidate_Id'].isin(candidates)]
		#unique = candidate_df['ReportingUnit_Id'].unique()
		#reporting_units = unique[0:6]
		#reporting_units = candidate_df['ReportingUnit_Id'].unique() 
		#df_final = candidate_df[candidate_df['ReportingUnit_Id'].isin(reporting_units)]. \
		#	sort_values(['ReportingUnit_Id', 'score'], ascending=False)

		# sort by absolute value of the score, descending
		# This still isn't quite right...votes_at_stake won't directly
		# correspond to the anomaly score
		#temp_df['abs_score'] = temp_df['score'].abs()
		df_final = temp_df.sort_values('score', ascending=False)
		df_final = df_final.iloc[0:8]
		df = pd.concat([df, df_final])
	return df


def euclidean_zscore(li):
    """Take a list of vectors -- all in the same R^k,
    returns a list of the z-scores of the vectors -- each relative to the ensemble"""
    distance_list = [sum([dist.euclidean(item,y) for y in li]) for item in li]
    if len(set(distance_list)) == 1:
        # if all distances are the same, which yields z-score nan values
        return [0]*len(li)
    else:
        return list(stats.zscore(distance_list))


def density_score(points):
	"""Take a list of vectors -- all in the same R^k,
	return a list of comparison of density with or without the anomaly"""
	density_list = [0] * len(points)
	x_order = list(points[:, 0])
	xs = points[:, 0]
	xs.sort()
	head, *tail = xs
	density = (tail[-2] - tail[0]) / (len(tail) - 1)
	total_density = (xs[-2] - xs[0]) / (len(xs) - 1)
	density_asc = total_density / density
	density_asc_xval = xs[0]

	# Sort in reverse order
	xs = xs[::-1]
	head, *tail = xs
	density = (tail[-2] - tail[0]) / (len(tail) - 1)
	total_density = (xs[-2] - xs[0]) / (len(xs) - 1)
	density_desc = total_density / density
	density_desc_xval = xs[0]
	if density_asc > density_desc:
		i = x_order.index(density_asc_xval)
		density_list[i] = density_asc
	else:
		i = x_order.index(density_desc_xval)
		density_list[i] = density_desc
	return density_list


def calculate_margins(data):
	"""Takes a dataframe with an anomaly score and assigns
	a margin score"""
	rank_1_df = data[data['rank'] == 1][['unit_id', 'ReportingUnit_Id', 'Count']]
	rank_1_df = rank_1_df.rename(columns={'Count': 'rank_1_total'})
	data = data.merge(rank_1_df, how='inner', on=['unit_id', 'ReportingUnit_Id'])
	data['margins'] = (data['rank_1_total'] - data['Count']) / data['contest_total']
	return data


def calculate_votes_at_stake(data):
	"""Move the most anomalous pairing to the equivalent of the second-most anomalous
	and calculate the differences in votes that would be returned"""
	df = pd.DataFrame()
	unit_ids = data['unit_id'].unique()
	for unit_id in unit_ids:
		temp_df = data[data['unit_id'] == unit_id]
		if temp_df.shape[0] > 2:
			#try:
			#temp_df['abs_score'] = temp_df['score'].abs()
			#temp_df.sort_values('score', ascending=False, inplace=True)
			# The first 2 rows are the most anomalous candidate pairing
			#anomalous_df = temp_df.iloc[0:2]
			max_anomaly_score = temp_df['score'].max() 
			reporting_unit_id = temp_df[temp_df['score'] == max_anomaly_score] \
				.iloc[0]['ReportingUnit_Id']
			selection = temp_df.loc[temp_df['score'] == max_anomaly_score] \
				.iloc[0]['Selection']
			anomalous_df = temp_df[(temp_df['ReportingUnit_Id'] == reporting_unit_id) & \
				(((temp_df['Selection'] == selection)  & \
					(temp_df['score'] == max_anomaly_score)) |
				(temp_df['rank'] == 1))].sort_values('score', ascending=False)
			#temp_df.sort_values('score', ascending=False, inplace=True)
			# Now we need to know whether the original score was pos or neg
			#is_positive = (anomalous_df.iloc[0]['score'] > 0)
			#if is_positive:
			next_max_score = temp_df[temp_df['score'] < max_anomaly_score]['score'].max()
			#elif not is_positive:
			#	next_max_score = temp_df[temp_df['score'] > max_anomaly_score]['score'].min()
			next_reporting_unit_id = temp_df.loc[temp_df['score'] == next_max_score] \
				.iloc[0]['ReportingUnit_Id']
			next_anomalous_df =temp_df[(temp_df['ReportingUnit_Id'] == next_reporting_unit_id) & \
				(((temp_df['Selection'] == selection)  & \
					(temp_df['score'] == next_max_score)) |
				(temp_df['rank'] == 1))].sort_values('score', ascending=False)
			# anomalous_total = int(anomalous_df['Count'].sum())
			# next_anomalous_total = int(next_anomalous_df['Count'].sum())
			# candidate_1 = anomalous_df.iloc[0]['Candidate_Id']
			# candidate_2 = anomalous_df.iloc[1]['Candidate_Id']
			# candidate_1_cnt = anomalous_df[anomalous_df['Candidate_Id'] == candidate_1].iloc[0]['Count']
			# candidate_2_cnt = anomalous_df[anomalous_df['Candidate_Id'] == candidate_2].iloc[0]['Count']
			# candidate_1_prop = int(next_anomalous_df[next_anomalous_df['Candidate_Id'] == candidate_1].iloc[0]['Count']) / \
			# 					int(next_anomalous_total)
			# candidate_2_prop = int(next_anomalous_df[next_anomalous_df['Candidate_Id'] == candidate_2].iloc[0]['Count']) / \
			# 					int(next_anomalous_total)
			# margin = abs(candidate_1_prop * anomalous_total - candidate_1_cnt) + \
			# 			abs(candidate_2_prop * anomalous_total - candidate_2_cnt)
			# temp_df['votes_at_stake'] = margin / temp_df.iloc[0].margins
			temp_df['margin_ratio'] = (anomalous_df.iloc[0]['margins'] - next_anomalous_df.iloc[0]['margins']) \
				/ anomalous_df.iloc[0]['margins'] 
			#except:
			#	temp_df['votes_at_stake'] = 0
		else:
			temp_df['margin_ratio'] = 0
		df = pd.concat([df, temp_df])
	return df