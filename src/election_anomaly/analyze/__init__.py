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
		session,target_dir,top_ru_id,sub_rutype_id=None,sub_rutype_othertext=None,election_id=None,
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

	# get names from ids
	top_ru = dbr.name_from_id(session,'ReportingUnit',top_ru_id).replace(" ","-")
	election = dbr.name_from_id(session,'Election',election_id).replace(" ","-")
	sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', sub_rutype_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','ContestSelectionJoin',
		'ComposingReportingUnitJoin','Election','ReportingUnit',
		'ElectionContestJoin','CandidateContest','CandidateSelection','BallotMeasureContest',
		'BallotMeasureSelection','Office','Candidate']:
		# pull directly from db, using 'Id' as index
		df[element] = pd.read_sql_table(element,session.bind,index_col='Id')

	# avoid conflict if by chance VoteCount table has extra columns during data upload
	df['VoteCount'] = df['VoteCount'][['Count','CountItemType_Id','OtherCountItemType','ReportingUnit_Id']]

	# pull enums from db, keeping 'Id as a column, not the index
	for enum in ["ReportingUnitType","CountItemType"]:
		df[enum] = pd.read_sql_table(enum,session.bind)

	#  limit to relevant Election-Contest pairs
	ecj = df['ElectionContestJoin'][df['ElectionContestJoin'].Election_Id == election_id]

	# create contest_selection dataframe, adding Contest, Selection and ElectionDistrict_Id columns
	cc = df['ContestSelectionJoin'].merge(
		df['CandidateContest'],how='right',left_on='Contest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='Selection_Id',right_index=True).merge(
		df['Candidate'],how='left',left_on='Candidate_Id',right_index=True).rename(
		columns={'BallotName':'Selection'}).merge(
		df['Office'],how='left',left_on='Office_Id',right_index=True)
	cc = cc[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id']]
	if cc.empty:
		cc['contest_type'] = None
	else:
		cc = mr.add_constant_column(cc,'contest_type','Candidate')

	# create ballotmeasure_selection dataframe
	bm = df['ContestSelectionJoin'].merge(
		df['BallotMeasureContest'],how='right',left_on='Contest_Id',right_index=True).rename(
		columns={'Name':'Contest'}).merge(
		df['BallotMeasureSelection'],how='left',left_on='Selection_Id',right_index=True)
	bm = bm[['Contest_Id','Contest','Selection_Id','Selection','ElectionDistrict_Id']]
	if bm.empty:
		bm['contest_type'] = None
	else:
		bm = mr.add_constant_column(bm,'contest_type','BallotMeasure')

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
		# if there is a 'total' type as well as other types, need to avoid doubling.
		# if the only type is 'total', don't rule it out.
		if exclude_total and [x for x in unsummed['CountItemType'] if x != 'total']:
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
		election,sub_rutype,cit,cis,
		str(session.bind.url),datetime.date.today()]
	sub_dir = os.path.join(election, top_ru, f'by_{sub_rutype}')
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


def create_scatter(session, jurisdiction_id, subdivision_type_id, 
            h_election_id, h_category, h_count_id, h_type,
			v_election_id, v_category, v_count_id, v_type):
	"""<target_dir> is the directory where the resulting rollup will be stored.
	<election_id> identifies the election; <datafile_id_list> the datafile whose results will be rolled up.
	<top_ru_id> is the internal cdf name of the ReportingUnit whose results will be reported
	<sub_rutype_id>,<sub_rutype_othertext> identifies the ReportingUnitType
	of the ReportingUnits used in each line of the results file
	created by the routine. (E.g., county or ward)
	If <exclude_total> is True, don't include 'total' CountItemType
	(unless 'total' is the only CountItemType)"""
	# Get name of db for error messages
	dfh = get_data_for_scatter(session, jurisdiction_id, subdivision_type_id, h_election_id, \
		h_category, h_count_id, h_type)
	dfv = get_data_for_scatter(session, jurisdiction_id, subdivision_type_id, v_election_id, \
		v_category, v_count_id, v_type)
	unsummed = pd.concat([dfh, dfv])
	# package into dictionary
	if h_count_id == -1:
		x = f'All {h_type}'
	elif h_type == 'candidates':
		x = dbr.name_from_id(session, 'Candidate', h_count_id) 
	elif h_type == 'contests':
		x = dbr.name_from_id(session, 'CandidateContest', h_count_id) 
	if v_count_id == -1:
		y = f'All {v_type}'
	elif v_type == 'candidates':
		y = dbr.name_from_id(session, 'Candidate', v_count_id) 
	elif v_type == 'contests':
		y = dbr.name_from_id(session, 'CandidateContest', v_count_id) 
	results = {
		"x-election": dbr.name_from_id(session, 'Election', h_election_id),
		"y-election": dbr.name_from_id(session, 'Election', v_election_id),
		"jurisdiction": dbr.name_from_id(session, 'ReportingUnit', jurisdiction_id),
		#"contest": dbr.name_from_id(session, 'CandidateContest', unsummed.iloc[0]['Contest_Id']),
		"subdivision_type": dbr.name_from_id(session, 'ReportingUnitType', subdivision_type_id),
		"x-count_item_type": h_category,
		"y-count_item_type": v_category,
		"x": x,
		"y": y,
		"counts": {}
	}
	reporting_units = unsummed.Name.unique()
	for reporting_unit in reporting_units:
		results["counts"][reporting_unit] = {}

	for i, row in unsummed.iterrows():
		if row.Selection == x:
			if x in results["counts"][row.Name]:
				results["counts"][row.Name]["x"] += row.Count
			else:
				results["counts"][row.Name]["x"] = row.Count
		elif row.Selection == y:
			if y in results["counts"][row.Name]:
				results["counts"][row.Name]["y"] += row.Count
			else:
				results["counts"][row.Name]["y"] = row.Count
	# only keep the ones where there are an (x, y) to graph
	to_keep = {}
	for key in results['counts']:
		if len(results['counts'][key]) == 2:
			to_keep[key] = results['counts'][key]
	results['counts'] = to_keep
	return results


def get_data_for_scatter(session, jurisdiction_id, subdivision_type_id, 
	election_id, count_item_type, filter_id, count_type):
	"""Since this could be data across 2 elections, grab data one election at a time"""
	db = session.bind.url.database

	# get names from ids
	top_ru = dbr.name_from_id(session,'ReportingUnit',jurisdiction_id).replace(" ","-")
	election = dbr.name_from_id(session,'Election',election_id).replace(" ","-")
	sub_rutype = dbr.name_from_id(session, 'ReportingUnitType', subdivision_type_id)

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','ContestSelectionJoin',
		'ComposingReportingUnitJoin','Election','ReportingUnit',
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
	contest_selection = df['ContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='Contest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='Selection_Id',right_index=True).merge(
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

	# Based on count_type param, we either filter on contest or candidate
	if count_type == 'candidates':
		filter_column = 'Candidate_Id'
	elif count_type == 'contests':
		filter_column = 'Contest_Id'
	
	# if the filter_id is -1, that means we want all of them and we'll do 
	# a group by later. otherwise, filter on the correct column
	if filter_id != -1:
		csj = contest_selection[contest_selection[filter_column].isin([filter_id])]
	else:
		csj = contest_selection

	# find ReportingUnits of the correct type that are subunits of top_ru
	sub_ru_ids = child_rus_by_id(session,[jurisdiction_id],ru_type=[subdivision_type_id, ''])
	if not sub_ru_ids:
		# TODO better error handling (while not sub_ru_list....)
		raise Exception(f'Database {db} shows no ReportingUnits of type {sub_rutype} nested inside {top_ru}')
	sub_ru = df['ReportingUnit'].loc[sub_ru_ids]

	# find all subReportingUnits of top_ru
	all_subs_ids = child_rus_by_id(session,[jurisdiction_id])

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
	
	# filter based on 

	# cleanup for purposes of flexibility
	unsummed = unsummed[['ReportingUnit', 'Count', 'Selection', 'Contest_Id', 'Candidate_Id']]
	unsummed.rename(columns={'ReportingUnit': 'Name'}, inplace=True)

	# if filter_id is -1, then that means we have all contests or candidates
	# so we need to group by
	if filter_id == -1:
		unsummed['Selection'] = f'All {count_type}'
		unsummed['Contest_Id'] = filter_id
		unsummed['Candidate_Id'] = filter_id

	if count_type == 'contests' and filter_id != -1:
		selection = dbr.name_from_id(session, 'CandidateContest', filter_id)
		unsummed['Selection'] = selection
	elif count_type == 'contests' and filter_id == -1:
		unsummed['Selection'] = 'All contests'

	columns = list(unsummed.drop(columns='Count').columns)
	unsummed = unsummed.groupby(columns)['Count'].sum().reset_index()

	return unsummed


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

	# get names from ids
	top_ru = dbr.name_from_id(session,'ReportingUnit',top_ru_id).replace(" ","-")
	election = dbr.name_from_id(session,'Election',election_id).replace(" ","-")

	# pull relevant tables
	df = {}
	for element in [
		'ElectionContestSelectionVoteCountJoin','VoteCount','ContestSelectionJoin',
		'ComposingReportingUnitJoin','Election','ReportingUnit',
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
	contest_selection = df['ContestSelectionJoin'].merge(
		df['CandidateContest'],how='left',left_on='Contest_Id',right_index=True).rename(
		columns={'Name':'Contest','Id':'ContestSelectionJoin_Id'}).merge(
		df['CandidateSelection'],how='left',left_on='Selection_Id',right_index=True).merge(
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
	print(ranked)
	input()
	ranked_margin = calculate_margins(ranked)
	votes_at_stake = calculate_votes_at_stake(ranked_margin)
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
			index=['ReportingUnit_Id', 'Name', 'score', 'abs_score', 'margins'], \
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
		temp_df['abs_score'] = temp_df['score'].abs()
		df_final = temp_df.sort_values('abs_score', ascending=False)
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
	# dictionary to hold totals for each contest ID, so we don't
	# need to calculate multiple times for different count item types
	contests = []
	totals = []
	margins = []

	contest_ids = data['Contest_Id'].unique()
	for contest_id in contest_ids:
		contest_df = data[(data['Contest_Id'] == contest_id) &
						(data['CountItemType'] == 'total')]
		reporting_unit_type_id = contest_df.iloc[0]['ReportingUnitType_Id']
		contest_df = contest_df[contest_df['ReportingUnitType_Id'] == reporting_unit_type_id]
		counts = contest_df.groupby('Selection')['Count'].sum() 
		contests.append(contest_id)
		total = contest_df['Count'].sum()
		totals.append(total)
		if len(counts) > 1:
			to_subtract = int(counts[1])
		else:
			to_subtract = 0
		margins.append(abs(int(counts[0]) - to_subtract) / total)
	df_dict = {
		'Contest_Id': contests,
		'totals': totals,
		'margins': margins
	}
	df = pd.DataFrame.from_dict(df_dict)
	data = data.merge(df, how='inner', on='Contest_Id')

	return data


def calculate_votes_at_stake(data):
	"""Move the most anomalous pairing to the equivalent of the second-most anomalous
	and calculate the differences in votes that would be returned"""
	df = pd.DataFrame()
	unit_ids = data['unit_id'].unique()
	for unit_id in unit_ids:
		temp_df = data[data['unit_id'] == unit_id]
		if temp_df.shape[0] > 2:
			try:
				temp_df['abs_score'] = temp_df['score'].abs()
				temp_df.sort_values('abs_score', ascending=False, inplace=True)
				# The first 2 rows are the most anomalous candidate pairing
				anomalous_df = temp_df.iloc[0:2]
				# Resort so we have the DF back in order by scores
				temp_df.sort_values('score', ascending=False, inplace=True)
				# Now we need to know whether the original score was pos or neg
				is_positive = (anomalous_df.iloc[0]['score'] > 0)
				if is_positive:
					next_anomalous_df = temp_df[temp_df['score'] < anomalous_df.iloc[0]['score']][0:2]
				elif not is_positive:
					next_anomalous_df = temp_df[temp_df['score'] > anomalous_df.iloc[0]['score']][0:2]	
				anomalous_total = int(anomalous_df['Count'].sum())
				next_anomalous_total = int(next_anomalous_df['Count'].sum())
				candidate_1 = anomalous_df.iloc[0]['Candidate_Id']
				candidate_2 = anomalous_df.iloc[1]['Candidate_Id']
				candidate_1_cnt = anomalous_df[anomalous_df['Candidate_Id'] == candidate_1].iloc[0]['Count']
				candidate_2_cnt = anomalous_df[anomalous_df['Candidate_Id'] == candidate_2].iloc[0]['Count']
				candidate_1_prop = int(next_anomalous_df[next_anomalous_df['Candidate_Id'] == candidate_1].iloc[0]['Count']) / \
									int(next_anomalous_total)
				candidate_2_prop = int(next_anomalous_df[next_anomalous_df['Candidate_Id'] == candidate_2].iloc[0]['Count']) / \
									int(next_anomalous_total)
				margin = abs(candidate_1_prop * anomalous_total - candidate_1_cnt) + \
							abs(candidate_2_prop * anomalous_total - candidate_2_cnt)
				temp_df['votes_at_stake'] = margin / temp_df.iloc[0].margins
			except:
				temp_df['votes_at_stake'] = 0
		else:
			temp_df['votes_at_stake'] = 0
		df = pd.concat([df, temp_df])
	return df