import csv
import os
import subprocess


def create_mdc_from_input():
    """
    Create a create_mdc.csv from user input. Program will ask for all information needed
    to populate the csv including where to save the data.
    :return: pathway to the created csv
    """
    mdc_csv_name = 'create_mdc.csv'
    while True:
        dir_to_save_mdc_file = input('Where do you want to save the file? ')
        if dir_to_save_mdc_file is not "" and os.path.isdir(dir_to_save_mdc_file):
            break
    mdc_file_path = os.path.join(dir_to_save_mdc_file, mdc_csv_name)
    # Create the create_mdc_.csv file at the given directory
    with open(mdc_file_path, 'w', newline="") as csv_outfile:
        # Headers for the create_mdc.csv teamplate
        field_names = ['dataset', 'patids', 'protocols', 'protseg', 'field_to_mdc', 'visno', 'other_key_fields']
        csv_writer = csv.writer(csv_outfile)
        csv_writer.writerow(field_names)
        datasets = input('What data sets to include(comma separated)? ').split(",")
        patids = input('What patids to include(comma separated)? (Leave empty to search for all patids) ')
        if patids is not '':
            # Wrap them in "" because they are characters
            patids = ",".join(['"{}"'.format(patid)
                               for patid in patids.split(",")
                               ])
        protocols = input('What protocols to include(comma separated)? (Leave empty if searching by PATID) ')
        if protocols is not '':
            # Wrap them in "" because they are characters
            protocols = ",".join(['"{}"'.format(protocol)
                                  for protocol in protocols.split(",")
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
            row = [dataset, patids, protocols, protseg, field_to_mdc, visno, other_key_fields]
            csv_writer.writerow(row)
        print("Wrote MDC file to {}".format(mdc_file_path))
    return other_key_fields, mdc_file_path


def create_sas_files_from_create_mdc_csv(create_mdc_csv_file):
    """
    Create an mdc sas file from the create_mdc file
    :param create_mdc_csv_file: pathway to create_mdc.csv
    :return: No return
    """

    created_sas_files = []
    # Open the create_mdc.csv file to get the data for the sas file
    with open(create_mdc_csv_file, 'r') as csv_infile:
        create_mdc_csv_reader = csv.DictReader(csv_infile)
        clinical_data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS'
        an_data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS\an'
        pm_data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS\pm'
        mdc_folder_path = os.path.dirname(create_mdc_csv_file)

        # Get data from create_mdc.csv to create the sas file
        # TODO: Check for valide headers in the csv file
        final_keep_statement = []
        for row in create_mdc_csv_reader:
            dataset = row['dataset']
            patids = row['patids'].split(",")
            protocols = row['protocols'].split(",")
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
            mdc_info['protocols'] = protocols
            mdc_info['protseg'] = protseg
            mdc_info['visno'] = visno
            mdc_info['other_key_fields'] = other_key_fields
            mdc_info['key_fields_length'] = key_fields_lenth
            mdc_info['field_to_mdc'] = field_to_mdc

            sas_file, final_keep_statement = create_mdc_sas_file(mdc_info, sas_file_path)
            created_sas_files.append(sas_file)
        return created_sas_files, final_keep_statement


def create_mdc_sas_file(mdc_info: dict, sas_file_path: str):
    """
    Creates a sas file from the create_mcd.csv file
    :param mdc_info: info to populate the sas file
    :param sas_file_path: pathway to create the sas file
    :return: pathway to the created sas file
    """
    filter_data = []  # Holds the variables we want to filter on
    # Holds the variables we want to keep
    keep_data = ['DATASET', 'SITE', 'PROTOCOL', 'PATID', 'PROJID', 'MDC_VARIABLE_NAME', 'OLD_VALUE']

    # Main field to MDC. If Updating or Deleting an entire record
    # We don't need to keep the field name in the final keep statement
    # If it is not All or DELETE then we will keep the field name in the
    # final keep statement for output.
    field_to_mdc = mdc_info['field_to_mdc'].upper()
    # Don't need to keep field to mdc as that is captured in MDC Variable name and OLD Value
    # if field_to_mdc not in ['ALL', 'DELETE']:
    #     keep_data.append(field_to_mdc)  # Add to list of variables to keep

    # PATIIDS to search sas tables for if we are searching for all
    # IDs under a given condintion then leave blank and it will not
    # be added to the filter conditions
    patids = mdc_info['patids']
    if patids != ['']:
        patids = ['"{}"'.format(patid) for patid in patids]
        patid_string = 'PATID in ({})'.format(",".join(patids))
        filter_data.append(patid_string)  # Add to filter conditions
    else:
        patid_string = 'PATID'
        filter_data.append(patid_string)

    # PROTOCOLs to search sas tables for. If we do not need to search
    # by Protocol this will not be added to the filter conditions
    # PROT will always be included in the keep statement In NDC SAS tables
    # PROTO ranges from Len 4 to 5 so we will use substr to identify the prot
    protocols = mdc_info['protocols']
    # TODO: Determine why some numbers are incrementing multiple times
    if protocols != ['']:
        substr_template = 'substr(PROT,1,4) = {}'
        protocols = [substr_template.format(protocol) for protocol in protocols]
        protocol_string = ' AND ({})'.format(" OR ".join(protocols))
        filter_data.append(protocol_string)  # Add to filter conditions

    # PROTSEG to search by. If the table has no PROTSEG key
    # This will not be added to the filter string
    protseg = mdc_info['protseg']
    if protseg != "":
        protseg_string = 'AND PROTSEG = "{}"'.format(protseg)
        filter_data.append(protseg_string)  # Add to filter conditions
    keep_data.append('PROTSEG')  # Always keep protseg for when searching without PATID

    # VISNO to search by. If the table has no VISNO key
    # This will not be added to the filter string
    visno = mdc_info['visno']
    if visno != "":
        visno_string = 'AND VISNO = "{}"'.format(visno)
        filter_data.append(visno_string)  # Add to filter conditions
    keep_data.append('VISNO')  # Alwas keep VISNO
    # OTHER_KEY_FIELDS to search by. If the table has no other
    # key fields this will not be added to the filter string.
    # each key field will be kept in the table output
    # will show all values for other key fields and user can remove those not needed
    temp_other_key_fields = mdc_info['other_key_fields']
    other_key_fields_name = []
    if temp_other_key_fields != ['']:
        # other_key_fields_template = 'AND {key_field} = "{key_field}"'
        other_key_field_data = []
        for key_field in temp_other_key_fields:
            other_key_field_name = 'Other_Key_Field_{key_field}'.format(key_field=key_field)
            other_key_field_string = '{key_field_name} = {key_field};'.format(key_field_name=other_key_field_name,
                                                                              key_field=key_field)
            other_key_field_data.append(other_key_field_string)
            keep_data.append(other_key_field_name)
        # mdc_info['other_key_fields'] = ','.join(temp_other_key_fields)
        mdc_info['other_key_fields_string'] = "\n\t".join(other_key_field_data)
    else:
        # mdc_info['other_key_fields'] = "NA"
        mdc_info['other_key_fields_string'] = ""
        # TODO: Create a new variable to hold the other key field data in the keep

    filter_string = " ".join(filter_data)  # Final list of conditions to filter on
    mdc_info['filter_string'] = filter_string
    keep_string = " ".join(keep_data)  # Final list of variables tokeep
    mdc_info['keep_string'] = keep_string

    #  Create the sas file with the data generated from the create_mdc.csv file
    with open(sas_file_path, 'w', newline="") as mdc_outfile:
        # Check if dataset exist in an and pm folders
        an_dataset_path = os.path.join(mdc_info['an_data_path'], mdc_info['dataset'] + "_an.sas7bdat")
        pm_dataset_path = os.path.join(mdc_info['pm_data_path'], mdc_info['dataset'] + "_pm.sas7bdat")
        if os.path.exists(an_dataset_path):
            mdc_info['an_dataset_string'] = """
data {dataset}_an;
	length PROTOCOL $4. DATASET $10.{key_fields_length};
	set andata.{dataset}_an;
	if {filter_string};
	DATASET = "{dataset}_an";
	MDC_VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = put({field_to_mdc},hex4.);
	PROTOCOL = substr(PROT,1,4);
	{other_key_fields_string}
	keep {keep_string};
run;

proc sort data={dataset}_an; by DATASET SITE PROTOCOL PATID;run;

""".format(**mdc_info)

            mdc_info['an_merge_dataset'] = """{dataset}_an""".format(**mdc_info)

        else:
            mdc_info['an_dataset_string'] = ""
            mdc_info['an_merge_dataset'] = ""
            print(an_dataset_path, "- No AN table for this dataset")

        if os.path.exists(pm_dataset_path):
            mdc_info['pm_dataset_string'] = """
data {dataset}_pm;
	length PROTOCOL $4. DATASET $10.{key_fields_length};
	set pmdata.{dataset}_pm;
	if {filter_string};
	DATASET = "{dataset}_pm";
	MDC_VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = put({field_to_mdc},hex4.);
	PROTOCOL = substr(PROT,1,4);
	{other_key_fields_string}
	keep {keep_string};
run;

proc sort data={dataset}_pm; by DATASET SITE PROTOCOL PATID;run;

""".format(**mdc_info)

            mdc_info['pm_merge_dataset'] = """{dataset}_pm""".format(**mdc_info)
        else:
            mdc_info['pm_dataset_string'] = ""
            print(pm_dataset_path, "- No pm table for this dataset")
            mdc_info['pm_merge_dataset'] = ""

        template = """
/* Get data for mdc from by checking main pm and an tables
*/

LIBNAME ndcdata "{clinical_data_path}";
LIBNAME andata "{an_data_path}";
LIBNAME pmdata "{pm_data_path}";

data {dataset};
	length PROTOCOL $4. DATASET $10.{key_fields_length};
	set ndcdata.{dataset};
	if {filter_string};
	DATASET = "{dataset}";
	MDC_VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = put({field_to_mdc},hex4.);
	PROTOCOL = substr(PROT,1,4);
	{other_key_fields_string}
	keep {keep_string};
run;

{an_dataset_string}

{pm_dataset_string}

proc sort data={dataset}; by DATASET SITE PROTOCOL PATID;run;

data {dataset}_mdc;
	merge {dataset} {an_merge_dataset} {pm_merge_dataset};
	by DATASET SITE PROTOCOL PATID;
run;

proc export data={dataset}_mdc
    outfile='{mdc_path}\{dataset}_mdc.csv'
    dbms=csv
    replace;
run;""".format(**mdc_info)

        mdc_outfile.write(template)
        return sas_file_path, keep_data


def run_mdc(mdc_sas_file):
    """
    Runs the sas file created by the program
    :param mdc_sas_file: pathway to sas file
    :return: No return
    """
    subprocess.call([r'S:\SAS 9.4\x86\SASFoundation\9.4\sas.exe', mdc_sas_file])


def create_single_mdc(mdc_file_folder, keep_statement: list):
    """
    Creates a single csv file out of the multiple csv files created by the program so that
    all data found for each table is in one csv.
    :param mdc_file_folder: folder that contains the csvs
    :param keep_statement: keep statment used to create the sas data sets will be the headers of the csv
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
        headers = keep_statement
        csv_writer = csv.DictWriter(mdc_outfile, fieldnames=headers)
        csv_writer.writeheader()
        # Write contents of each file to the main csv file
        for mdc_file in mdc_files:
            with open(os.path.join(mdc_file_folder, mdc_file), 'r') as mdc_infile:
                csv_reader = csv.DictReader(mdc_infile)
                for row in csv_reader:
                    csv_writer.writerow(row)


def main():
    # other_key_fields, mdc_file_path = create_mdc_from_input()
    mdc_file_path = r'G:\NIDADSC\spitts\MDCs\roi_form\create_mdc.csv'
    mdc_file_dir = os.path.dirname(mdc_file_path)
    created_sas_files, final_keep_statement = create_sas_files_from_create_mdc_csv(mdc_file_path)
    for sas_file in created_sas_files:
        run_mdc(sas_file)
    create_single_mdc(mdc_file_dir, final_keep_statement)


if __name__ == '__main__':
    main()
