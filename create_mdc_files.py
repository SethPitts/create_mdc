import csv
import subprocess
import os


def create_mdc_sas_file(mdc_info: dict, mdc_file_path: str):
    filter_data = []
    keep_data = ['DATASET', 'SITE', 'PROTOCOL', 'PATID', 'PROJID', 'VARIABLE_NAME', 'OLD_VALUE']

    field_to_mdc = mdc_info['field_to_mdc'].upper()
    if field_to_mdc not in ['ALL', 'DELETE']:
        keep_data.append(field_to_mdc)

    patids = mdc_info['patids']
    if patids != ['']:
        patid_string = 'PATID in ({})'.format(",".join(patids))
        filter_data.append(patid_string)

    protseg = mdc_info['protseg']
    if protseg != "":
        protseg_string = 'AND PROTSEG = "{}"'.format(protseg)
        filter_data.append(protseg_string)
        keep_data.append('PROTSEG')

    visno = mdc_info['visno']
    if visno != "":
        visno_string = 'AND VISNO = "{}"'.format(visno)
        filter_data.append(visno_string)
        keep_data.append('VISNO')

    temp_other_key_fields = mdc_info['other_key_fields']
    if temp_other_key_fields != ['']:
        other_key_fields_template = 'AND {key_field} = "{key_field}"'
        for key_field in temp_other_key_fields:
            filter_data.append(other_key_fields_template.format(**{'key_field': key_field}))
            keep_data.append(key_field)

    filter_string = " ".join(filter_data)
    mdc_info['filter_string'] = filter_string
    keep_string = " ".join(keep_data)
    mdc_info['keep_string'] = keep_string

    with open(mdc_file_path, 'w', newline="") as mdc_outfile:
        template = """
/* Get data for mdc from by checking main pm and an tables
*/

LIBNAME ndcdata "{data_path}";
LIBNAME andata "{an_path}";
LIBNAME pmdata "{pm_path}";

data {dataset};
	length PROTOCOL $4. DATASET $6.;
	set ndcdata.{dataset};
	if {filter_string};
	DATASET = "{dataset}";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	PROTOCOL = substr(PROT,1,4);
	keep {keep_string};
run;

data {dataset}_an;
	length PROTOCOL $4. DATASET $6.;
	set andata.{dataset}_an;
	if {filter_string};
	DATASET = "{dataset}_an";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	PROTOCOL = substr(PROT,1,4);
	keep {keep_string};
run;

data {dataset}_pm;
	length PROTOCOL $4. DATASET $6.;
	set pmdata.{dataset}_pm;
	if {filter_string};
	DATASET = "{dataset}_pm";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	PROTOCOL = substr(PROT,1,4);
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
        return mdc_file_path


def run_mdc(mdc_file):
    subprocess.call([r'S:\SAS 9.4\x86\SASFoundation\9.4\sas.exe', mdc_file])


def create_single_mdc(mdc_file_folder):
    mdc_files = [item for item in os.listdir(mdc_file_folder)
                 if item.endswith('.csv')
                 ]

    with open(os.path.join(mdc_file_folder, '{}_mdc.csv'.format(mdc_file_folder.split("\\")[-1])), 'w', newline="") as \
            mdc_outfile:
        headers = ['PROTOCOL', 'DATASET', 'SITE', 'PATID', 'PROJID', 'PROTSEG', 'VISNO', 'VARIABLE_NAME', 'OLD_VALUE']
        csv_writer = csv.DictWriter(mdc_outfile, fieldnames=headers)
        csv_writer.writeheader()
        for mdc_file in mdc_files:
            with open(os.path.join(mdc_file_folder, mdc_file), 'r') as mdc_infile:
                csv_reader = csv.DictReader(mdc_infile)
                for row in csv_reader:
                    csv_writer.writerow(row)
def main():
    with open('mdc.csv', 'r') as csv_infile:
        csv_reader = csv.DictReader(csv_infile)
        data_path = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS'
        andata = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS\an'
        pmdata = r'G:\NIDADSC\NDC\SAS\PROD_SAS_CUP\SAS\pm'
        mdc_path = r'G:\NIDADSC\spitts\MDCs\visno_00I'
        for row in csv_reader:
            dataset = row['dataset']
            patids = row['patids'].split(",")
            protseg = row['protseg']
            visno = row['visno']
            other_key_fields = row['other_key_fields'].split(",")
            field_to_mdc = row['field_to_mdc']
            mdc_file_path = 'Create_{}_MDC.sas'.format(dataset)

            mdc_info = dict()
            mdc_info['data_path'] = data_path
            mdc_info['an_path'] = andata
            mdc_info['pm_path'] = pmdata
            mdc_info['mdc_path'] = mdc_path
            mdc_info['dataset'] = dataset
            mdc_info['patids'] = patids
            mdc_info['protseg'] = protseg
            mdc_info['visno'] = visno
            mdc_info['other_key_fields'] = other_key_fields
            mdc_info['field_to_mdc'] = field_to_mdc

            run_mdc(create_mdc_sas_file(mdc_info, mdc_file_path))

    create_single_mdc(r'G:\NIDADSC\spitts\MDCs\visno_00I')


if __name__ == '__main__':
    main()
