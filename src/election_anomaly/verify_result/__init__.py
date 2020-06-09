#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import user_interface as ui
import os
import numpy as np
import xlrd


if __name__ == '__main__':

    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 10)

    #read both the result files
    print("Select result file 1 for comparision")
    fpath = ui.pick_filepath()
    Typetotal_df = pd.read_csv(fpath, sep='\t')

    print("Select result file 2 for comparision")
    fpath = ui.pick_filepath()
    TypetotalMod_df = pd.read_csv(fpath,sep='\t')



    #Find rows which are different between two DataFrames.
    comparison_df = Typetotal_df.merge(TypetotalMod_df, indicator=True, how='outer')


    """
    Case 1: A [contest, reportingunit, Selection] pair in both the files and same.  Good
    Case 2: A [contest, reportingunit, Selection] pair in both the files and count not same. Show differences.
    Case 3: A [contest, reportingunit, Selection] pair in in result file 2 but not in result file 1
    Case 4: A [contest, reportingunit, Selection] pair in result file 1 but not in result file 2 
    """


    #case 2
    #Define coulumns to group by and campare
    grp_cols = ['Contest', 'ReportingUnit', 'Selection']
    differences_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() > 1)


    # remove duplicate contests and display one row
    describers = {'_merge': {'left_only': 'Vote Count in Result file 1', 'right_only': 'Vote Count in file 2'}}
    differences_df = differences_df.replace(describers)
    differences_df =  differences_df.set_index(['contest_type', 'Contest', 'contest_district_type', 'Selection','ReportingUnit', 'CountItemType', '_merge'])
    differences_df = differences_df.unstack()


    count = len(differences_df.index)
    print("Vote counts do not match in {} rows".format(count))

    interact = input('Do you want to look at these rows  (y/n)?\n')
    if interact == 'y':
        print(differences_df)

    interact = input('Do you want to export the unmatched rows to a file  (y/n)?\n')
    if interact == 'y':
        p_root = os.getcwd()
        print("The deafault folder for the file is {}".format(p_root))
        interact2 = input('Do you want to select a different folder(y/n)?\n')
        if interact2 == 'y':
            p_root = input(f'Enter absolute path of project root.\n')

        fpath = os.path.join(p_root, 'result_differences.csv')
        differences_df.to_csv(fpath, index=True)


    #case3
    missing_df = comparison_df.groupby(grp_cols).filter(lambda x: x.Count.count() == 1)

    interact = input('Do you want to see the rows in file 2 missing from file 1.(y/n)?\n')
    if interact == 'y':
        missing1_df = missing_df[missing_df ['_merge'] == 'right_only']
        print(missing1_df)


    #case 4
    interact = input('Do you want to see the rows in file 1 missing from file 2.(y/n)?\n')
    if interact == 'y':
        missing2_df = missing_df[missing_df ['_merge'] == 'left_only']
        print(missing2_df)

