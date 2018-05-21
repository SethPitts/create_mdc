
/* Get data for mdc from by checking main pm and an tables
*/

LIBNAME ndcdata "DataPath";
LIBNAME andata "Anomly_Data_Path";
LIBNAME pmdata "PM_Data_Path";

data DTX;
	set ndcdata.DTX;
	if PATID in ('0001','0002','0003') AND PROTSEG = "A" AND VISNO = "00I" AND other_key_1 = "other_key_1" AND other_key_2 = "other_key_2";
	DATASET = "DTX";
	VARIABLE_NAME = "DTX_SEQ_NUM";
	OLD_VALUE = DTX_SEQ_NUM;
	keep DATASET SITE PROT PATID PROJID VARIABLE_NAME OLD_VALUE PROTSEG VISNO other_key_1 other_key_2;
run;

data DTX_an;
	set andata.DTX_an;
	if PATID in ('0001','0002','0003') AND PROTSEG = "A" AND VISNO = "00I" AND other_key_1 = "other_key_1" AND other_key_2 = "other_key_2";
	DATASET = "DTX_an";
	VARIABLE_NAME = "DTX_SEQ_NUM";
	OLD_VALUE = DTX_SEQ_NUM;
	keep DATASET SITE PROT PATID PROJID VARIABLE_NAME OLD_VALUE PROTSEG VISNO other_key_1 other_key_2;
run;

data DTX_pm;
	set pmdata.DTX_pm;
	if PATID in ('0001','0002','0003') AND PROTSEG = "A" AND VISNO = "00I" AND other_key_1 = "other_key_1" AND other_key_2 = "other_key_2";
	DATASET = "DTX_pm";
	VARIABLE_NAME = "DTX_SEQ_NUM";
	OLD_VALUE = DTX_SEQ_NUM;
	keep DATASET SITE PROT PATID PROJID VARIABLE_NAME OLD_VALUE PROTSEG VISNO other_key_1 other_key_2;
run;

proc sort DTX by DATASET SITE PROT PATID;run;
proc sort DTX_an by DATASET SITE PROT PATID;run;
proc sort DTX_pm by DATASET SITE PROT PATID;run;

data DTX_mdc;
	merge DTX DTX_an DTX_pm;
	by DATASET SITE PROT PATID;
run;

proc export data=DTX_mdc
    outfile=MDC_Path\DTX_mdc.csv
    dbms=csv
    replace;
run;