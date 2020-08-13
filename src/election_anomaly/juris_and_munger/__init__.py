import os.path

from election_anomaly import db_routines as dbr
import pandas as pd
from pandas.api.types import is_numeric_dtype
from election_anomaly import munge_routines as mr
from election_anomaly import user_interface as ui
import re
import numpy as np
from pathlib import Path


class Jurisdiction:
    def load_juris_to_db(self,session,project_root):
        """Load info from each element in the Jurisdiction's directory into the db"""
        # for element in Jurisdiction directory (except dictionary, remark)
        juris_elements = [
            x[:-4] for x in os.listdir(self.path_to_juris_dir)
            if x != 'remark.txt' and x != 'dictionary.txt' and x[0] != '.']
        # reorder juris_elements for efficiency
        leading = ['ReportingUnit','Office','Party','CandidateContest']
        trailing = ['ExternalIdentifier']
        juris_elements = leading + [
            x for x in juris_elements if x not in leading and x not in trailing
        ] + trailing
        error = {}
        for element in juris_elements:
            # read df from Jurisdiction directory
            load_juris_dframe_into_cdf(session,element,self.path_to_juris_dir,project_root,error)
        if error:
            for element in juris_elements:
                dbr.truncate_table(session, element)
            return error
        return None

    def __init__(self,path_to_juris_dir):
        self.short_name = Path(path_to_juris_dir).name
        self.path_to_juris_dir = path_to_juris_dir


class Munger:
    def get_aux_data(self, aux_data_dir, err, project_root=None) -> dict:
        """creates dictionary of dataframes, one for each auxiliary datafile.
       DataFrames returned are (multi-)indexed by the primary key(s)"""
        aux_data_dict = {}  # will hold dataframe for each abbreviated file name

        field_list = list(set([x[0] for x in self.auxiliary_fields()]))
        for abbrev in field_list:
            # get munger for the auxiliary file
            aux_mu = Munger(os.path.join(self.path_to_munger_dir, abbrev), project_root=project_root)

            # find file in aux_data_dir whose name contains the string <afn>
            aux_filename_list = [x for x in os.listdir(aux_data_dir) if abbrev in x]
            if len(aux_filename_list) == 0:
                e = f'No file found with name containing {abbrev} in the directory {aux_data_dir}'
                if 'datafile' in err.keys():
                    err['datafile'].append(e)
                else:
                    err['datafile'] = [e]
            elif len(aux_filename_list) > 1:
                e = f'Too many files found with name containing {abbrev} in the directory {aux_data_dir}'
                if 'datafile' in err.keys():
                    err['datafile'].append(e)
                else:
                    err['datafile'] = [e]
            else:
                aux_path = os.path.join(aux_data_dir, aux_filename_list[0])

            # read and clean the auxiliary data file, including setting primary key columns as int
            df, err = ui.read_single_datafile(aux_mu, aux_path, err)

            # cast primary key(s) as int if possible, and set as (multi-)index
            primary_keys = self.aux_meta.loc[abbrev, 'primary_key'].split(',')
            df = mr.cast_cols_as_int(df,primary_keys,error_msg=f'In dataframe for {abbrev}')
            df.set_index(primary_keys, inplace=True)

            aux_data_dict[abbrev] = df

        return aux_data_dict, err

    def auxiliary_fields(self):
        """Return set of [file_abbrev,field] pairs, one for each
        field in <self>.cdf_elements.fields referring to auxilliary files"""
        pat = re.compile('([^\\[]+)\\[([^\\[\\]]+)\\]')
        all_set = set().union(*list(self.cdf_elements.fields))
        aux_field_list = [re.findall(pat,x)[0] for x in all_set if re.findall(pat,x)]
        return aux_field_list

    def check_against_self(self):
        """check that munger is internally consistent"""
        problems = []

        # every source is either row or column
        bad_source = [x for x in self.cdf_elements.source if x not in ['row','column']]
        if bad_source:
            b_str = ','.join(bad_source)
            problems.append(f'''At least one source in cdf_elements.txt is not recognized: {b_str} ''')

        # formulas have good syntax
        bad_formula = [x for x in self.cdf_elements.raw_identifier_formula.unique() if not mr.good_syntax(x)]
        if bad_formula:
            f_str = ','.join(bad_formula)
            problems.append(f'''At least one formula in cdf_elements.txt has bad syntax: {f_str} ''')

        # for each column-source record in cdf_element, contents of bracket are numbers in the header_rows
        p_not_just_digits = re.compile(r'<.*\D.*>')
        p_catch_digits = re.compile(r'<(\d+)>')
        bad_column_formula = set()
        for i,r in self.cdf_elements[self.cdf_elements.source == 'column'].iterrows():
            if p_not_just_digits.search(r['raw_identifier_formula']):
                bad_column_formula.add(r['raw_identifier_formula'])
            else:
                integer_list = [int(x) for x in p_catch_digits.findall(r['raw_identifier_formula'])]
                bad_integer_list = [x for x in integer_list if (x > self.header_row_count-1 or x < 0)]
                if bad_integer_list:
                    bad_column_formula.add(r['raw_identifier_formula'])
        if bad_column_formula:
            cf_str = ','.join(bad_column_formula)
            problems.append(f'''At least one column-source formula in cdf_elements.txt has bad syntax: {cf_str} ''')

        # TODO if field in formula matches an element self.cdf_element.index,
        #  check that rename is not also a column
        if problems:
            error = {}
            error["munger_internal_consistency"] = ", ".join(problems)
            return error
        else:
            return None

          
    def __init__(self,dir_path,aux_data_dir=None,project_root=None,check_files=True):
        """<dir_path> is the directory for the munger. If munger deals with auxiliary data files,
        <aux_data_dir> is the directory holding those files."""
        self.name= os.path.basename(dir_path)  # e.g., 'nc_general'
        self.path_to_munger_dir = dir_path

        # create dir if necessary
        if not os.path.isdir(dir_path):
            Path(dir_path).mkdir(parents=True,exist_ok=True)

        if check_files:
            ensure_munger_files(dir_path,project_root=project_root)
        [self.cdf_elements,self.header_row_count,self.field_name_row,self.field_names_if_no_field_name_row,self.count_columns,
         self.file_type,self.encoding,self.thousands_separator,self.aux_meta] = read_munger_info_from_files(
            self.path_to_munger_dir,project_root=project_root)

        if aux_data_dir:
            self.aux_data = self.get_aux_data(aux_data_dir,project_root=project_root)
        else:
            self.aux_data = {}
        self.aux_data_dir = aux_data_dir

        # used repeatedly, so calculated once for convenience
        self.field_list = set()
        for t,r in self.cdf_elements.iterrows():
            self.field_list = self.field_list.union(r['fields'])


def read_munger_info_from_files(dir_path,project_root=None,aux_data_dir=None):
    """<aux_data_dir> is required if there are auxiliary data files"""
    # create auxiliary dataframe
    if 'aux_meta.txt' in os.listdir(dir_path):
        # if some elements are reported in separate files per auxilliary.txt file, read from file
        aux_meta = pd.read_csv(os.path.join(dir_path, 'aux_meta.txt'),sep='\t',index_col='abbreviated_file_name')
    else:
        # set auxiliary dataframe to empty
        aux_meta = pd.DataFrame([[]])

    # read cdf_element info
    cdf_elements = pd.read_csv(
        os.path.join(dir_path,'cdf_elements.txt'),sep='\t',index_col='name',encoding='iso-8859-1').fillna('')
    # add column for list of fields used in formulas
    cdf_elements['fields'] = [[]]*cdf_elements.shape[0]
    for i,r in cdf_elements.iterrows():
        text_field_list,last_text = mr.text_fragments_and_fields(cdf_elements.loc[i,'raw_identifier_formula'])
        cdf_elements.loc[i,'fields'] = [f for t,f in text_field_list]

    # read formatting info
    format_info = pd.read_csv(os.path.join(dir_path,'format.txt'),sep='\t',index_col='item',encoding='iso-8859-1')
    try:
        # if field_name_row can be interpreted as an integer, use it
        field_name_row = int(format_info.loc['field_name_row','value'])
        field_names_if_no_field_name_row = None
    except:
        # otherwise assume no field_name_row
        field_name_row = None
        field_names_if_no_field_name_row = format_info.loc['field_names_if_no_field_name_row','value'].split(',')

    header_row_count = int(format_info.loc['header_row_count','value'])
    if format_info.loc['count_columns','value'] == 'None' or (
            type(format_info.loc['count_columns','value']) == float
            and np.isnan(format_info.loc['count_columns','value'])
    ) or format_info.loc['count_columns','value'] == '':
        count_columns = []
    else:
        count_columns = [int(x) for x in format_info.loc['count_columns','value'].split(',')]
    file_type = format_info.loc['file_type','value']
    encoding = format_info.loc['encoding','value']
    thousands_separator = format_info.loc['thousands_separator','value']
    if thousands_separator in ['None','',np.nan]:
        thousands_separator = None
    # TODO warn if encoding not recognized

    return [cdf_elements,header_row_count,field_name_row,field_names_if_no_field_name_row,count_columns,file_type,
            encoding,thousands_separator,aux_meta]

# TODO combine ensure_jurisdiction_dir with ensure_juris_files
def ensure_jurisdiction_dir(juris_path,project_root,ignore_empty=False):
    path_output = None
    # create jurisdiction directory
    try:
        Path(juris_path).mkdir(parents=True)
    except FileExistsError:
        pass
    else:
        path_output = f'Directory {juris_path} created.'

    # ensure the contents of the jurisdiction directory are correct
    juris_file_error = ensure_juris_files(juris_path,project_root,ignore_empty=ignore_empty)
    if path_output:
        juris_file_error["directory_status"] = path_output
    if juris_file_error:
        return juris_file_error
    else:
        return None


def ensure_juris_files(juris_path,project_root,ignore_empty=False):
    """Check that the jurisdiction files are complete and consistent with one another.
    Check for extraneous files in Jurisdiction directory.
    Assumes Jurisdiction directory exists. Assumes dictionary.txt is in the template file"""

    #package possible errors from this function into a dictionary and return them
    error_ensure_juris_files = {}

    templates_dir = os.path.join(project_root,'templates/jurisdiction_templates')
    # notify user of any extraneous files
    extraneous = [f for f in os.listdir(juris_path) if
                  f != 'remark.txt' and f not in os.listdir(templates_dir) and f[0] != '.']
    if extraneous:
        error_ensure_juris_files["extraneous_files_in_juris_directory"] = extraneous
        extraneous = []

    template_list = [x[:-4] for x in os.listdir(templates_dir)]

    # reorder template_list, so that first things are created first
    ordered_list = ['dictionary','ReportingUnit','Office','CandidateContest']
    template_list = ordered_list + [x for x in template_list if x not in ordered_list]

    file_empty = []
    column_errors =[]
    null_columns_dict = {}
    duplicate_rows = []

    # ensure necessary all files exist
    for juris_file in template_list:
        #a list of file empty errors
        cf_path = os.path.join(juris_path,f'{juris_file}.txt')
        created = False
        # if file does not already exist in jurisdiction directory, create from template and invite user to fill
        try:
            temp = pd.read_csv(os.path.join(templates_dir,f'{juris_file}.txt'),sep='\t',encoding='iso-8859-1')
        except pd.errors.EmptyDataError:
            if not ignore_empty:
                file_empty.append('Template file {'+juris_file+'}.txt has no contents')
                # print(f'Template file {juris_file}.txt has no contents')
            temp = pd.DataFrame()
        if not os.path.isfile(cf_path):
            temp.to_csv(cf_path,sep='\t',index=False)
            file_empty.append('File {'+juris_file+'}.txt has just been created. Enter information in the file')
            created = True

        # if file exists, check format against template
        if not created:
            cf_df = pd.read_csv(os.path.join(juris_path,f'{juris_file}.txt'), sep='\t', encoding='iso=8859-1')
            if set(cf_df.columns) != set(temp.columns):
                cols = '\t'.join(temp.columns.to_list())
                column_errors.append(f'Columns of {juris_file}.txt need to be (tab-separated):\n '
                                    f' {cols}\n')

            if juris_file == 'ExternalIdentifier':
                d, dupe =  dedupe(cf_path)
            elif juris_file == 'dictionary':
                d, dupe = dedupe(cf_path)
            else:
                # run dupe check
                d, dupe = dedupe(cf_path)
                # check for problematic null entries
                null_columns = check_nulls(juris_file,cf_path,project_root)
                if null_columns:
                    null_columns_dict[juris_file] = null_columns

            if dupe != '':
                duplicate_rows.append(dupe)

            if column_errors:
                error_ensure_juris_files["column_errors"] = column_errors
            if null_columns_dict:
                error_ensure_juris_files["null_columns"] = null_columns_dict
            if duplicate_rows:
                error_ensure_juris_files["duplicate_rows"] = duplicate_rows

        if file_empty:
            error_ensure_juris_files["file_empty_errors"] = file_empty

    # check dependencies
    dependency_error = []
    for juris_file in [x for x in template_list if x != 'remark' and x != 'dictionary']:
        # check dependencies
        d, dep_error = check_dependencies(juris_path,juris_file)
        if dep_error:
            dependency_error.append(dep_error)
    if dependency_error:
        error_ensure_juris_files["failed_dependencies"] = dependency_error
    if error_ensure_juris_files:
        return error_ensure_juris_files
    else:
        return {}


def ensure_munger_files(munger_path,project_root=None):
    """Check that the munger files are complete and consistent with one another.
    Assumes munger directory exists. Assumes dictionary.txt is in the template file.
    <munger_path> is the path to the directory of the particular munger"""
    if not project_root:
        project_root = ui.get_project_root()

    # ensure all files exist
    created = []
    if not os.path.isdir(munger_path):
        created.append(munger_path)
        os.makedirs(munger_path)
    templates = os.path.join(project_root,'templates/munger_templates')
    template_list = [x[:-4] for x in os.listdir(templates)]


    error = {}
    # create each file if necessary
    for munger_file in template_list:
        # TODO create optional template for auxiliary.txt
        cf_path = os.path.join(munger_path,f'{munger_file}.txt')
        # if file does not already exist in munger dir, create from template and invite user to fill
        file_exists = os.path.isfile(cf_path)
        if not file_exists:
            temp = pd.read_csv(os.path.join(templates,f'{munger_file}.txt'),sep='\t',encoding='iso-8859-1')
            created.append(f'{munger_file}.txt')
            temp.to_csv(cf_path,sep='\t',index=False)

        # if file exists, check format against template
        if file_exists:
            err = check_munger_file_format(munger_path, munger_file, templates)
            if err:
                error[f'{munger_file}.txt'] = err

    # check contents of each file if they were not newly created and
    # if they have successfully been checked for the format
    if file_exists and not error:
        err = check_munger_file_contents(munger_path,project_root=project_root)
        if err:
            error["contents"] = err

    if created:
        created = ', '.join(created)
        error["newly_created"] = created
    if error:
        return error
    return None


def check_munger_file_format(munger_path, munger_file, templates):
    cf_df = pd.read_csv(os.path.join(munger_path,f'{munger_file}.txt'),sep='\t',encoding='iso-8859-1')
    temp = pd.read_csv(os.path.join(templates,f'{munger_file}.txt'),sep='\t',encoding='iso-8859-1')
    problems = []
    # check column names are correct
    if set(cf_df.columns) != set(temp.columns):
        cols = '\t'.join(temp.columns.to_list())
        problems.append(f'Columns of {munger_file}.txt need to be (tab-separated):\n'
            f' {cols}\n')

    # check first column matches template
    #  check same number of rows
    elif cf_df.shape[0] != temp.shape[0]:
        first_col = '\n'.join(list(temp.iloc[:,0]))
        problems.append(
            f'Wrong number of rows in {munger_file}.txt. \nFirst column must be exactly:\n{first_col}')
    elif set(cf_df.iloc[:,0]) != set(temp.iloc[:,0]):
        first_error = (cf_df.iloc[:,0] != temp.iloc[:,0]).index.to_list()[0]
        first_col = '\n'.join(list(temp.iloc[:,0]))
        problems.append(f'First column of {munger_file}.txt must be exactly:\n{first_col}\n'
                        f'First error is at row {first_error}: {cf_df.loc[first_error]}')

    if problems:
        problems = ', '.join(problems)
        error = {}
        error["format_problems"] = problems
    else:
        error = None
    return error

def check_munger_file_contents(munger_name,project_root):
    """check that munger files are internally consistent; offer user chance to correct"""
    # define path to munger's directory
    munger_dir = os.path.join(project_root,'mungers',munger_name)

    problems = []
    warns = []

    # read cdf_elements and format from files
    cdf_elements = pd.read_csv(os.path.join(munger_dir,'cdf_elements.txt'),sep='\t',encoding='iso-8859-1').fillna('')
    format_df = pd.read_csv(
        os.path.join(munger_dir,'format.txt'),sep='\t',index_col='item',encoding='iso-8859-1').fillna('')
    template_format_df = pd.read_csv(
        os.path.join(
            project_root,'templates/munger_templates/format.txt'),sep='\t',index_col='item',encoding='iso-8859-1'
    ).fillna('')

    # format.txt has the required items
    req_list = template_format_df.index
    missing_items = [x for x in req_list if x not in format_df.index]
    if missing_items:
        item_string = ','.join(missing_items)
        problems.append(f'Format file is missing some items: {item_string}')

    # Either field_name_row is a number, or field_names_if_no_field_name_row is not the empty string
    if (not format_df.loc['field_name_row','value'].isnumeric()) and \
        len(format_df.loc['field_names_if_no_field_name_row','value']) == 0:
        problems.append(f'In format file, field_name_row is not an integer, '
                        f'but no field names are give in field_names_if_no_field_name_row.')

    # other entries in format.txt are of correct type
    try:
        int(format_df.loc['header_row_count','value'])
    except:
        problems.append(f'In format file, header_row_count must be an integer'
                        f'({format_df.loc["header_row_count","value"]} is not.)')

    if not format_df.loc['encoding','value'] in ui.recognized_encodings:
        warns.append(f'Encoding {format_df.loc["field_name_row","value"]} in format file is not recognized.')

    # every source is either row, column or other
    bad_source = [x for x in cdf_elements.source if x not in ['row','column']]
    if bad_source:
        b_str = ','.join(bad_source)
        problems.append(f'''At least one source in cdf_elements.txt is not recognized: {b_str} ''')

    # formulas have good syntax
    bad_formula = [x for x in cdf_elements.raw_identifier_formula.unique() if not mr.good_syntax(x)]
    if bad_formula:
        f_str = ','.join(bad_formula)
        problems.append(f'''At least one formula in cdf_elements.txt has bad syntax: {f_str} ''')

    # for each column-source record in cdf_element, contents of bracket are numbers in the header_rows
    p_not_just_digits = re.compile(r'<.*\D.*>')
    p_catch_digits = re.compile(r'<(\d+)>')
    bad_column_formula = set()

    # problems found above may cause this block of code to error out, so this is
    # wrapped in a try block since it returns a general error message
    for i,r in cdf_elements[cdf_elements.source == 'column'].iterrows():
        if p_not_just_digits.search(r['raw_identifier_formula']):
            bad_column_formula.add(r['raw_identifier_formula'])
        else:
            integer_list = [int(x) for x in p_catch_digits.findall(r['raw_identifier_formula'])]
            bad_integer_list = [
                x for x in integer_list if (x > int(format_df.loc['header_row_count','value'])-1 or x < 0)]
            if bad_integer_list:
                bad_column_formula.add(r['raw_identifier_formula'])
    if bad_column_formula:
        cf_str = ','.join(bad_column_formula)
        problems.append(f'''At least one column-source formula in cdf_elements.txt has bad syntax: {cf_str} ''')

    # TODO if field in formula matches an element self.cdf_element.index,
    #  check that rename is not also a column
    error = {}
    if problems:
        error['problems'] = '\n\t'.join(problems)
    if warns:
        error['warnings'] = '\n\t'.join(warns)

    if error:
        return error
    return None


def dedupe(f_path,warning='There are duplicates'):
    # TODO allow specification of unique constraints
    df = pd.read_csv(f_path,sep='\t',encoding='iso-8859-1')
    dupe=''
    dupes_df,df = ui.find_dupes(df)
    if not dupes_df.empty:
        dupe = f'Edit {f_path} to remove the duplication, then hit return to continue'
        df = pd.read_csv(f_path,sep='\t',encoding='iso-8859-1')
    return df,dupe


def check_nulls(element,f_path,project_root):
    # TODO write description
    # TODO automatically drop null rows
    nn_path = os.path.join(
        project_root,'election_anomaly/CDF_schema_def_info/elements',element,'not_null_fields.txt')
    not_nulls = pd.read_csv(nn_path,sep='\t',encoding='iso-8859-1')
    df = pd.read_csv(f_path,sep='\t',encoding='iso-8859-1')

    problem_columns = []

    for nn in not_nulls.not_null_fields.unique():
        # if nn is an Id, name in jurisdiction file is element name
        if nn[-3:] == '_Id':
            nn = nn[:-3]
        n = df[df[nn].isnull()]
        if not n.empty:
            problem_columns.append(nn)
            # drop offending rows
            df = df[df[nn].notnull()]

    return problem_columns



def check_dependencies(juris_dir,element):
    """Looks in <juris_dir> to check that every dependent column in <element>.txt
    is listed in the corresponding jurisdiction file. Note: <juris_dir> assumed to exist.
    """
    d = juris_dependency_dictionary()
    f_path = os.path.join(juris_dir,f'{element}.txt')
    assert os.path.isdir(juris_dir)
    element_df = pd.read_csv(f_path,sep='\t',index_col=None,encoding='iso-8859-1')
    unmatched_error = []

    # Find all dependent columns
    dependent = [c for c in element_df if c in d.keys()]
    changed_elements = set()
    report = [f'In {element}.txt:']
    for c in dependent:
        target = d[c]
        ed = pd.read_csv(os.path.join(
            juris_dir,f'{element}.txt'),sep='\t',header=0,encoding='iso-8859-1').fillna('').loc[:,c].unique()

        # create list of elements, removing any nulls
        ru = list(
            pd.read_csv(
                os.path.join(
                    juris_dir,f'{target}.txt'),sep='\t',
                encoding='iso-8859-1').fillna('').loc[:,dbr.get_name_field(target)])
        try:
            ru.remove(np.nan)
        except ValueError:
            pass

        missing = [x for x in ed if x not in ru]
        if len(missing) == 0:
            report.append(f'Every {c} in {element}.txt is a {target}.')
        elif len(missing) == 1 and missing == ['']:  # if the only missing is null or blank
            # TODO some dependencies are ok with null (eg. PrimaryParty) and some are not
            report.append(f'Some {c} are null, and every non-null {c} is a {target}.')
        else:
            changed_elements.add(element)
            changed_elements.add(target)
            unmatched_error.append(f'Every {c} must be a {target}. This is not optional!!')

    # if dependent:
    #     print('\n\t'.join(report))

    return changed_elements, unmatched_error


def juris_dependency_dictionary():
    """Certain fields in jurisdiction files refer to other jurisdiction files.
    E.g., ElectionDistricts are ReportingUnits"""
    d = {'ElectionDistrict':'ReportingUnit','Office':'Office','PrimaryParty':'Party','Party':'Party',
         'Election':'Election'}
    return d


# TODO before processing jurisdiction files into db, alert user to any duplicate names.
#  Enforce name change? Or just suggest?
def load_juris_dframe_into_cdf(session,element,juris_path,project_root,error,load_refs=True):
    """ TODO
    """
    cdf_schema_def_dir = os.path.join(project_root,'election_anomaly/CDF_schema_def_info')
    element_fpath = os.path.join(juris_path,f'{element}.txt')
    if not os.path.exists(element_fpath):
        error[f'{element}.txt'] = "file not found"
        return
    df = pd.read_csv(element_fpath,sep='\t',encoding='iso-8859-1') \
        .fillna('none or unknown')
    # TODO check that df has the right format

    # TODO deal with duplicate 'none or unknown' records
    if element != 'ExternalIdentifier':
        # add 'none or unknown' line to df
        d = {c:'none or unknown' for c in df.columns}
        fields_file = os.path.join(cdf_schema_def_dir,'elements',element,'fields.txt')
        fields = pd.read_csv(fields_file,sep='\t',index_col='fieldname')
        for f in fields.index:
            # change any non-string, non-foreign-id fields to default values
            t = fields.loc[f,'datatype']
            if t == 'Date':
                d[f]='1000-01-01'
            elif t == 'Integer':
                d[f] = -1
            elif t != 'String':
                raise TypeError(f'Datatype {t} not recognized')
        df = df.append([d])

    # dedupe df
    dupes,df = ui.find_dupes(df)
    if not dupes.empty:
        print(f'WARNING: duplicates removed from dataframe, may indicate a problem.\n')
        #ui.show_sample(dupes,f'lines in {element} source data','are duplicates')
        if not element in error:
            error[element] = {}
        error[element]["found_duplicates"] = True

    # replace nulls with empty strings
    df.fillna('',inplace=True)

    # replace plain text enumerations from file system with id/othertext from db
    enum_file = os.path.join(cdf_schema_def_dir,'elements',element,'enumerations.txt')
    if os.path.isfile(enum_file):  # (if not, there are no enums for this element)
        enums = pd.read_csv(enum_file,sep='\t')
        # get all relevant enumeration tables
        for e in enums['enumeration']:  # e.g., e = "ReportingUnitType"
            cdf_e = pd.read_sql_table(e,session.bind)
            # for every instance of the enumeration in the current table, add id and othertype columns to the dataframe
            if e in df.columns:
                df = mr.enum_col_to_id_othertext(df,e,cdf_e)
        # TODO skipping assignment of CountItemStatus to ReportingUnit for now,
        #  since we can't assign an ReportingUnit as ElectionDistrict to Office
        #  (unless Office has a CountItemStatus; can't be right!)
        #  Note CountItemStatus is weirdly assigned to ReportingUnit in NIST CDF.
        #  Note also that CountItemStatus is not required, and a single RU can have many CountItemStatuses

    # TODO somewhere, check that no CandidateContest & Ballot Measure share a name; ditto for other false foreign keys

    # get Ids for any foreign key (or similar) in the table, e.g., Party_Id, etc.
    fk_file_path = os.path.join(
            cdf_schema_def_dir,'elements',element,'foreign_keys.txt')
    if os.path.isfile(fk_file_path):
        foreign_keys = pd.read_csv(fk_file_path,sep='\t',index_col='fieldname')

        for fn in foreign_keys.index:
            refs = foreign_keys.loc[fn,'refers_to'].split(';')

            try:
                df = get_ids_for_foreign_keys(session,df,element,fn,refs,load_refs,error)
            except ForeignKeyException as e:
                if load_refs:
                    for r in refs:
                        load_juris_dframe_into_cdf(session,r,juris_path,project_root,error)
                    # try again to load main element (but don't load referred-to again)
                    load_juris_dframe_into_cdf(session,element,juris_path,project_root,error,load_refs=False)
            except Exception as e:
                if not element in error:
                    error[element] = {}
                error[element]["jurisdiction"] = \
                    f"""{e}\nThere may be something wrong with the file {element}.txt.
                    You may need to make changes to the Jurisdiction directory and try again."""

    # commit info in df to corresponding cdf table to db
    data, err = dbr.dframe_to_sql(df,session,element)
    if err:
        if not element in error:
            error[element] = {}
        error[element]["database"] = err
    return


def get_ids_for_foreign_keys(session,df1,element,foreign_key,refs,load_refs,error):
    """ TODO <fn> is foreign key"""
    df = df1.copy()
    # append the Id corresponding to <fn> from the db
    foreign_elt = f'{foreign_key[:-3]}'
    interim = f'{foreign_elt}_Name'

    target_list = []
    for r in refs:
        ref_name_field = dbr.get_name_field(r)

        r_target = pd.read_sql_table(r,session.bind)[['Id',ref_name_field]]
        r_target.rename(columns={'Id':foreign_key,ref_name_field:interim},inplace=True)
        if element == 'ExternalIdentifier':
            # add column for cdf_table of referent
            r_target.loc[:,'cdf_element'] = r

        target_list.append(r_target)

    target = pd.concat(target_list)

    if element == 'ExternalIdentifier':
        # join on cdf_element name as well
        df = df.merge(
            target,how='left',left_on=['cdf_element','internal_name'],right_on=['cdf_element',interim])
        # rename 'Foreign_Id' to 'Foreign' for consistency in definition of missing
        # TODO why is ExternalIdentifier special in this regard?
        #  Is it that ExternalIdentifier doesn't have a name field?
        df.rename(columns={foreign_key:foreign_elt},inplace=True)
    else:
        df = df.merge(target,how='left',left_on=foreign_elt,right_on=interim)

    missing = df[(df[foreign_elt].notnull()) & (df[interim].isnull())]
    if missing.empty:
        df.drop([interim],axis=1)
    else:
        if load_refs:
            # Always try to handle/fill in the missing IDs
            raise ForeignKeyException(f'For some {element} records, {foreign_elt} was not found')
        else:
            if not element in error:
                error[element] = {}
            error[element]["foreign_key"] = \
            f"For some {element} records, {foreign_elt} was not found"
    return df


def check_results_munger_compatibility(mu: Munger, df: pd.DataFrame, error: dict) -> dict:
    # check that count columns exist
    missing = [i for i in mu.count_columns if i >= df.shape[1]]
    if missing:
        e = f'Only {df.shape[1]} columns read from file. Check file_type in format.txt'
        if 'datafile' in error.keys():
            error['datafile'].append(e)
        else:
            error['datafile'] = e
    else:
        # check that count cols are numeric
        for i in mu.count_columns:
            if not is_numeric_dtype(df.iloc[:,i]):
                try:
                    df.iloc[:, i]= df.iloc[:,i].astype(int)
                except ValueError as ve:
                    e = f'Column {i} ({df.columns[i]}) cannot be parsed as an integer.\n{ve}'
                    if 'datafile' in error.keys():
                        error['datafile'].append(e)
                    else:
                        error['datafile'] = e
    return error


def check_element_against_raw_results(el,results_df,munger,numerical_columns,d,err=None):
    mode = munger.cdf_elements.loc[el,'source']

    # restrict to element in question; add row for 'none or unknown'
    none_series = pd.Series(
        {'cdf_internal_name':'none or unknown','raw_identifier_value':'none or unknown'},name=el)
    d_restricted = d[d.index == el].append(none_series)

    translatable = [x for x in d_restricted['raw_identifier_value']]

    if mode == 'row':
        # add munged column
        raw_fields = [f'{x}_SOURCE' for x in munger.cdf_elements.loc[el,'fields']]
        try:
            relevant = results_df[raw_fields].drop_duplicates()
        except KeyError:
            formula = munger.cdf_elements.loc[el,"raw_identifier_formula"]
            raise mr.MungeError(
                f'Required column from formula {formula} not found in file columns:\n{results_df.columns}')
        mr.add_munged_column(relevant,munger,el,err,mode=mode)
        # check for untranslatable items
        missing = relevant[~relevant[f'{el}_raw'].isin(translatable)]

    elif mode == 'column':
        # add munged column
        formula = munger.cdf_elements.loc[el,'raw_identifier_formula']
        text_field_list,last_text = mr.text_fragments_and_fields(formula)
        # create raw element column from formula applied to num col headers
        val = {}  # holds evaluations of fields
        raw_val = {}  # holds the raw value for column c
        for c in numerical_columns:
            raw_val[c] = ''
            # evaluate f-th entry in the column whose 0th entry is c
            for t,f in text_field_list:
                if int(f) == 0:  # TODO make prettier, assumes first line is col header in df
                    val[f] = c
                else:
                    val[f] = results_df.loc[f - 1,c]
                raw_val[c] += t + val[f]
        # TODO check that numerical cols are correctly identified even when more than one header row
        relevant = pd.DataFrame(pd.Series([raw_val[c] for c in numerical_columns],name=f'{el}_raw'))
        # check for untranslatable items # TODO code repeated from above
        if el in d.index:
            # if there are any items in d corresponding to the element <el>, do left merge and identify nulls
            # TODO more natural way to do this, without taking cases?
            relevant = relevant.merge(
                d.loc[[el]],how='left',left_on=f'{el}_raw',right_on='raw_identifier_value')
            missing = relevant[relevant.cdf_internal_name.isnull()]
        else:
            missing = relevant

    else:
        missing = pd.DataFrame([])
        print(f'Not checking {el}, which is not read from the records in the results file.')
    return missing


class ForeignKeyException(Exception):
    pass


if __name__ == '__main__':
    print('Done (juris_and_munger)!')
