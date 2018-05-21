
def create_mdc_sas_file(mdc_info: dict, mdc_file_path: str):

    patids = mdc_info['patids']
    patid_string = '"PATID in ("{}")'.format(",".join(patids))
    protseg = mdc_info['protseg']
    if protseg is None:
        protseg_string = ""
    else:
        protseg_string = 'AND PROTSEG = "{}"'.format(protseg)
    visno = mdc_info['visno']
    if visno is None:
        visno_string = ""
    else:
        visno_string = 'AND VISNO in = "{}"'.format(visno)
    temp_other_key_fields = mdc_info['other_key_fields']
    if temp_other_key_fields is None:
        temp_other_key_fields = [""]
    other_key_fields = []
    other_key_fields_template = 'AND {key_field} = "{key_field}"'
    for key_field in temp_other_key_fields:
        other_key_fields.append(other_key_fields_template.format(**{'key_field': key_field}))
    other_key_field_string = " ".join(other_key_fields)
    filter_data = [patid_string, protseg_string, visno_string, other_key_field_string]
    filter_string = " ".join(filter_data)
    mdc_info['filter_string'] = filter_string

    with open(mdc_file_path, 'w', newline="") as mdc_outfile:
        template = """
/* Get data for mdc from by checking main pm and an tables
*/

LIBNAME ndcdata "{data_path}";
LIBNAME andata "{an_path}";
LIBNAME pmdata "{pm_path}";

data {dataset};
	set ndcdata.{dataset};
	if {filter_string};
	DATASET = "{dataset}";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	keep DATASET SITE PROT PATID PROJID  PROTSEG VISNO OTHR_KEY_FLD VARIABLE_NAME OLD_VALUE;
run;

data {dataset}_an;
	set andata.{dataset}_an;
	if {filter_string};
	DATASET = "{dataset}";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	keep DATASET SITE PROT PATID PROJID  PROTSEG VISNO OTHR_KEY_FLD VARIABLE_NAME OLD_VALUE;
run;

data {dataset}_pm;
	set pmdata.{dataset}_pm;
	if {filter_string};
	DATASET = "{dataset}";
	VARIABLE_NAME = "{field_to_mdc}";
	OLD_VALUE = {field_to_mdc};
	keep DATASET SITE PROT PATID PROJID  PROTSEG VISNO OTHR_KEY_FLD VARIABLE_NAME OLD_VALUE;
run;

proc sort {dataset} by DATASET SITE PROT PATID;run;
proc sort {dataset}_an by DATASET SITE PROT PATID;run;
proc sort {dataset}_pm by DATASET SITE PROT PATID;run;

data {dataset}_mdc;
	merge {dataset} {dataset}_an {dataset}_pm;
	by DATASET SITE PROT PATID;
run;

proc export data={dataset}_mdc
    outfile={mdc_path}\{dataset}_mdc.csv
    dbms=csv
    replace;
run;""".format(**mdc_info)

        mdc_outfile.write(template)


def main():
    data_path = 'DataPath'
    andata = 'Anomly_Data_Path'
    pmdata = 'PM_Data_Path'
    mdc_path = 'MDC_Path'
    mdc_file_path = 'MDC_out.sas'
    dataset = 'DTX'
    patids = ['0001', '0002', '0003']
    protseg = 'A'
    visno = '00I'
    other_key_fields = ['other_key_1', 'other_key_2']
    field_to_mdc = 'DTX_SEQ_NUM'

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

    create_mdc_sas_file(mdc_info, mdc_file_path)

if __name__ == '__main__':
    main()
