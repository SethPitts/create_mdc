/* Get data for mdc from by checking main pm and an tables
*/

LIBNAME ndcdata "ndc_path";

data dtx;
	set ndcdata.dtx;
	if PATID in ("PATIDS") AND PROTSEG = "PROTSEG" AND VISNO in ("VISNO")
	AND OTHR_KEY_FLD = "Other Key Field";
	DATASET = "dtx";
	VARIABLE_NAME = "variable_to_change";
	OLD_VALUE = FIELD_TO_MDC;
	keep DATASET SITE PROT PATID PROJID  PROTSEG VISNO OTHR_KEY_FLD VARIABLE_NAME OLD_VALUE;
run;

data dtx_an;
	set ndcdata.dtx;
	if PATID in ("PATIDS") AND PROTSEG = "PROTSEG" AND VISNO in ("VISNO")
	AND OTHR_KEY_FLD = "Other Key Field";
	DATASET = "dtx_an";
	VARIABLE_NAME = "variable_to_change";
	OLD_VALUE = FIELD_TO_MDC;
	keep DATASET SITE PROT PATID PROJID  PROTSEG VISNO OTHR_KEY_FLD VARIABLE_NAME OLD_VALUE;
run;

data dtx_pm;
	set ndcdata.dtx;
	if PATID in ("PATIDS") AND PROTSEG = "PROTSEG" AND VISNO in ("VISNO")
	AND OTHR_KEY_FLD = "Other Key Field";
	DATASET = "dtx_pm";
	VARIABLE_NAME = "variable_to_change";
	OLD_VALUE = FIELD_TO_MDC;
	keep DATASET SITE PROT PATID PROJID  PROTSEG VISNO OTHR_KEY_FLD VARIABLE_NAME OLD_VALUE;
run;

proc sort dtx by DATASET SITE PROT PATID;run;
proc sort dtx_an by DATASET SITE PROT PATID;run;
proc sort dtx_pm by DATASET SITE PROT PATID;run;

data dtx_mdc;
	merge dtx dtx_an dtx_pm;
	by DATASET SITE PROT PATID;
run;
