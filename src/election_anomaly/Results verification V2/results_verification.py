#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import xlrd

#f_path = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src\election_anomaly\Results verification V2\TYPEtotal_STATUSunknown.txt'
#fmod_path = r'C:\Users\jsru2\Desktop\GitHub\election_anomaly\src\election_anomaly\Results verification V2\TYPEtotal_STATUSunknown_mod2.txt'

#read the results file and the modified results file

f_path = input(f'Enter file path of results file.\n')
Typetotal_df = pd.read_csv(f_path,sep='\t')

fmod_path = input(f'Enter file path of modified results file.\n')
TypetotalMod_df = pd.read_csv(fmod_path,sep='\t')

"""Find rows which are different between two DataFrames."""
comparison_df = Typetotal_df.merge(TypetotalMod_df, indicator=True, how='outer')


#diff_df = comparison_df[comparison_df['_merge'] != 'both']

"""
Case 1: A [contest, reportingunit] pair in both the files and same.  Good
Case 2: A [contest, reportingunit] pair in both the files and count not same. Show differences.
Case 3: A [contest, reportingunit] pair in TypetotalMod_df not in Typetotal_df. Pair not analysed.
Case 4: A [contest, reportingunit] pair in Typetotal_df not in TypetotalMod_df. Check the downloaded results file. 
"""


#Case 1
matching_df = comparison_df[comparison_df['_merge'] == 'both']
#print ("Following contest are correctly analysed")
print()

#case 2
"""Define coulumns to group by and campare"""
grp_cols = ['Contest', 'ReportingUnit', 'Selection']
differences_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() > 1)

#remove duplicate contests and display one row
differences1_df = differences_df[differences_df ['_merge'] == 'left_only']
differences1_df ['_merge'] = 'Counts_Vary'
print ("Following contest are not correctly analysed")
print(differences1_df)

#case3
missing_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() == 1)

missing1_df = missing_df[missing_df ['_merge'] == 'right_only']
print ("Following contest are not analysed by the program")
print(missing1_df)
#case 4 - genearlly not possible

missing2_df = missing_df[missing_df ['_merge'] == 'left_only']
print ("Following contest are missing in downloaded results")
print(missing2_df)
