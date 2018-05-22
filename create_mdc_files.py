import csv
import os
import subprocess


def create_mdc_from_input():
    """
    Create a create_mdc.csv from user input. Program will ask for all information needed
    to populate the csv including where to save the data.
    :return: pathway to the created csv
    """
    while True:
        mdc_csv_name = input('What do you want to call the mdc file? ')
        if mdc_csv_name is not "":
            break
    while True:
        dir_to_save_mdc_file = input('Where do you want to save the file? ')
        if dir_to_save_mdc_file is not "" and os.path.isdir(dir_to_save_mdc_file):
            break
    mdc_file_path = os.path.join(dir_to_save_mdc_file, mdc_csv_name)
    # Create the create_mdc_.csv file at the given directory
    with open(mdc_file_path, 'w', newline="") as csv_outfile:
        # Headers for the create_mdc.csv teamplate
        field_names = ['dataset', 'patids', 'protseg', 'field_to_mdc', 'visno', 'other_key_fields']
        csv_writer = csv.writer(csv_outfile)
        csv_writer.writerow(field_names)
        datasets = input('What data sets to include(comma separated)? ').split(",")
        patids = input('What patids to include(comma separated)? (Leave empty to search for all patids ')
        if patids is not '':
            # Wrap them in "" because they are characters
            patids = ",".join(['"{}"'.format(patid)
                               for patid in patids.split(",")
                               ])
        protseg = input('What protseg to include? (Leave empty for none) ')
        visno = input('What VISNO to include? (Leave empty for none ')
        other_key_fields = input('What other key fields to include(comma separated)? (Leave empty if N/A)')
        while True:
            field_to_mdc = input('What field to MDC? (Cannot be empty) ')
            if field_to_mdc is not "":
                break
        # Create a row in the create_mdc file for each data set to search
        for dataset in datasets:
            row = [dataset, patids, protseg, field_to_mdc, visno, other_key_fields]
            csv_writer.writerow(row)
        print("Wrote MDC file to {}".format(mdc_file_path))
    return mdc_file_path


def create_sas_files_from_create_mdc_csv(create_mdc_csv_file):
    """
    Create an mdc sas file from the create_mdc file
    :param create_mdc_csv_file: pathway to create_mdc.csv
    :return: No return
    """

    # Open the create_mdc.csv file to get the data for the sas file
    with open(create_mdc_csv_file, 'r') as csv_infile:
        create_mdc_csv_reader = csv.DictReader(csv_infile)
        clinical_data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS'
        an_data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS\an'
        pm_data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS\pm'
        mdc_folder_path = os.path.dirname(create_mdc_csv_file)

        # Get data from create_mdc.csv to create the sas file
        for row in create_mdc_csv_reader:
            dataset = row['dataset']
            patids = row['patids'].split(",")
            protseg = row['protseg']
            visno = row['visno']
            other_key_fields = row['other_key_fields']
            other_key_fields_lenth = len(other_key_fields)
            if other_key_fields_lenth == 0:
                other_key_fields_lenth = 2  # Account for NA
            key_fields_lenth = ' OTHER_KEY_FIELDS ${}.'.format(other_key_fields_lenth)
            other_key_fields = other_key_fields.split(",")
            field_to_mdc = row['field_to_mdc']
            sas_file_path = os.path.join(mdc_folder_path, 'Create_{}_MDC.sas'.format(dataset))

            mdc_info = dict()
            mdc_info['clinical_data_path'] = clinical_data_path
            mdc_info['an_data_path'] = an_data_path
            mdc_info['pm_data_path'] = pm_data_path
            mdc_info['mdc_path'] = mdc_folder_path
            mdc_info['dataset'] = dataset
            mdc_info['patids'] = patids
            mdc_info['protseg'] = protseg
            mdc_info['visno'] = visno
            mdc_info['other_key_fields'] = other_key_fields
            mdc_info['key_fields_length'] = key_fields_lenth
            mdc_info['field_to_mdc'] = field_to_mdc

            create_mdc_sas_file(mdc_info, sas_file_path)


def create_mdc_sas_file(mdc_info: dict, sas_file_path: str):
    """
    Creates a sas file from the create_mcd.csv file
    :param mdc_info: info to populate the sas file
    :param sas_file_path: pathway to create the sas file
    :return: pathway to the created sas file
    """
    filter_data = []  # Holds the variables we want to filter on
    # Holds the variables we want to keep
    keep_data = ['DATASET', 'SITE', 'PROTOCOL', 'PATID', 'PROJID', 'OTHER_KEY_FIELDS', 'VARIABLE_NAME', 'OLD_VALUE']

    # Main field to MDC. If Updating or Deleting an entire record
    # We don't need to keep the field name in the final keep statement
    # If it is not All or DELETE then we will keep the field name in the
    # final keep statement for output.
    field_to_mdc = mdc_info['field_to_mdc'].upper()
    if field_to_mdc not in ['ALL', 'DELETE']:
        keep_data.append(field_to_mdc)  # Add to list of variables to keep

    # PATIIDS to search sas tables for if we are searching for all
    # IDs under a given condintion then leave blank and it will not
    # be added to the filter conditions
    patids = mdc_info['patids']
    if patids != ['']:
        patid_string = 'PATID in ({})'.format(",".join(patids))
        filter_data.append(patid_string)  # Add to filter conditions

    # PROTSEG to search by. If the table has no PROTSEG key
    # This will not be added to the filter string
    protseg = mdc_info['protseg']
    if protseg != "":
        protseg_string = 'AND PROTSEG = "{}"'.format(protseg)
        filter_data.append(protseg_string)  # Add to filter conditions
        keep_data.append('PROTSEG')  # Add to list of variables to keep

    # VISNO to search by. If the table has no VISNO key
    # This will not be added to the filter string
    visno = mdc_info['visno']
    if visno != "":
        visno_string = 'AND VISNO = "{}"'.format(visno)
        filter_data.append(visno_string)  # Add to filter conditions
        keep_data.append('VISNO')  # Add to list of variables to keep
    # OTHER_KEY_FIELDS to search by. If the table has no other
    # key fields this will not be added to the filter string.
    # each key field will be kept in the table output
    # will show all values for other key fields and user can remove those not needed
    temp_other_key_fields = mdc_info['other_key_fields']
    if temp_other_key_fields != ['']:
        # other_key_fields_template = 'AND {key_field} = "{key_field}"'
        for key_field in temp_other_key_fields:
            keep_data.append(key_field)
            mdc_info['other_key_fields'] = ','.join(temp_other_key_fields)
    else:
        mdc_info['other_key_fields'] = "NA"
        # TODO: Create a new variable to hold the other key field data in the keep

    filter_string = " ".join(filter_data)  # Final list of conditions to filter on
    mdc_info['filter_string'] = filter_string
    keep_string = " ".join(keep_data)  # Final list of variables tokeep
    mdc_info['keep_string'] = keep_string

    #  Create the sas file with the data generated from the create_mdc.csv file
    with open(sas_file_path, 'w', newline="") as mdc_outfile:
        template = """
/* Get data for mdc from by checking main pm and an tables
*/

LIBNAME ndcdata "{clinical_data_path}";
LIBNAME andata "{an_data_path}";
LIBNAME pmdata "{pm_data_path}";

data {dataset};
	length PROTOCOL $4. DATASET $6.{key_fields_length};
	set {clinical_data_path}.{dataset};
	if {filter_string};
	DATASET = "{dataset}";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	PROTOCOL = substr(PROT,1,4);
	OTHER_KEY_FIELDS = "{other_key_fields}";
	keep {keep_string};
run;

data {dataset}_an;
	length PROTOCOL $4. DATASET $6.{key_fields_length};
	set {an_data_path}.{dataset}_an;
	if {filter_string};
	DATASET = "{dataset}_an";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	PROTOCOL = substr(PROT,1,4);
	OTHER_KEY_FIELDS = "{other_key_fields}";
	keep {keep_string};
run;

data {dataset}_pm;
	length PROTOCOL $4. DATASET $6.{key_fields_length};
	set {pm_data_path}.{dataset}_pm;
	if {filter_string};
	DATASET = "{dataset}_pm";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	PROTOCOL = substr(PROT,1,4);
	OTHER_KEY_FIELDS = "{other_key_fields}";
	keep {keep_string};
run;

proc sort data={dataset}; by DATASET SITE PROTOCOL PATID;run;
proc sort data={dataset}_an; by DATASET SITE PROTOCOL PATID;run;
proc sort data={dataset}_pm; by DATASET SITE PROTOCOL PATID;run;

data {dataset}_mdc;
	merge {dataset} {dataset}_an {dataset}_pm;
	by DATASET SITE PROTOCOL PATID;
run;

proc export data={dataset}_mdc
    outfile='{mdc_path}\{dataset}_mdc.csv'
    dbms=csv
    replace;
run;""".format(**mdc_info)

        mdc_outfile.write(template)
        return sas_file_path


def run_mdc(mdc_sas_file):
    """
    Runs the sas file created by the program
    :param mdc_sas_file: pathway to sas file
    :return: No return
    """
    subprocess.call([r'S:\SAS 9.4\x86\SASFoundation\9.4\sas.exe', mdc_sas_file])


def create_single_mdc(mdc_file_folder):
    """
    Creates a single csv file out of the multiple csv files created by the program so that
    all data found for each table is in one csv.
    :param mdc_file_folder: folder that contains the csvs
    :return: No return
    """
    # Get the list of files from the folder that are the mdc csvs
    mdc_files = [item for item in os.listdir(mdc_file_folder)
                 if item.endswith('.csv')
                 if item.find('create') == -1
                 ]

    # Create the main csv file give it the same name as the base folder the csv files are in
    with open(os.path.join(mdc_file_folder, '{}_mdc.csv'.format(mdc_file_folder.split("\\")[-1])), 'w', newline="") as \
            mdc_outfile:
        # Headers for the MDC template
        headers = ['PROTOCOL', 'DATASET', 'SITE', 'PATID', 'PROJID', 'PROTSEG', 'VISNO', 'OTHER_KEY_FIELDS',
                   'VARIABLE_NAME', 'OLD_VALUE'
                   ]
        csv_writer = csv.DictWriter(mdc_outfile, fieldnames=headers)
        csv_writer.writeheader()
        # Write contents of each file to the main csv file
        for mdc_file in mdc_files:
            with open(os.path.join(mdc_file_folder, mdc_file), 'r') as mdc_infile:
                csv_reader = csv.DictReader(mdc_infile)
                for row in csv_reader:
                    csv_writer.writerow(row)


def main():
    # mdc_file_path = create_mdc_from_input()
    mdc_file_path = r'G:\NIDADSC\spitts\MDCs\visno_00I\create_mdc.csv'
    mdc_file_dir = os.path.dirname(mdc_file_path)
    create_sas_files_from_create_mdc_csv(mdc_file_path)
    create_single_mdc(mdc_file_dir)


if __name__ == '__main__':
    main()
