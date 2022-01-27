libname JACK '\\BCSPARSAS01\sasdata\SAS Data\Jack\SASdata';

proc import out = SJA_2015_2016
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Debtload Files\2015-2016.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Source";
RUN;

data SJA_2015_2016;
	set SJA_2015_2016;
	format client_reference_number q $char30.;
	client_reference_number = 'Invoice #'n;
	q = title;
	drop 'Invoice #'n title;
	rename q = title;
run;

proc import out = SJA_2015_2016_PN
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Debtload Files\2015-2016.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Possible Phone Numbers";
RUN;

data SJA_2015_2016_PN;
	set SJA_2015_2016_PN;
	format q $char30.;
	length q $30.;
	q = client_reference_number;
	drop client_reference_number;
	rename q = client_reference_number;
run;

proc import out = SJA_2015_2016_EA
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Debtload Files\2015-2016.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Possible Emails";
RUN;

data SJA_2015_2016_EA;
	set SJA_2015_2016_EA;
	format q $char30.;
	format r $char100.;
	length q $30.;
	q = client_reference_number;
	r = email;
	drop client_reference_number email;
	rename q = client_reference_number r = email;
run;

proc import out = SJA_2017_2018
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Debtload Files\2017-2018.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Source";
RUN;

data SJA_2017_2018;
	set SJA_2017_2018;
	format client_reference_number q $char30.;
	client_reference_number = 'Invoice #'n;
	q = title;
	drop 'Invoice #'n title;
	rename q = title;
run;

proc import out = SJA_2017_2018_PN
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Debtload Files\2017-2018.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Possible Phone Numbers";
RUN;

data SJA_2017_2018_PN;
	set SJA_2017_2018_PN;
	format q $char30.;
	length q $30.;
	q = client_reference_number;
	drop client_reference_number;
	rename q = client_reference_number;
run;

proc import out = SJA_2017_2018_EA
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Debtload Files\2017-2018.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Possible Emails";
RUN;

data SJA_2017_2018_EA;
	set SJA_2017_2018_EA;
	format q $char30.;
	format r $char100.;
	length q $30.;
	q = client_reference_number;
	r = email;
	drop client_reference_number email;
	rename q = client_reference_number r = email;
run;

data Debtload;
	set SJA_2015_2016 (where=('Contact Notes'n='1') drop=z aa ab ac ad ah ai aj ak al am an ao ap aq ar as at au av aw ax ay az ba bb bc bd be bf bg bh bi bj bk bl bm bn) 
	SJA_2017_2018 (where=('Contact Notes'n='1') drop=z aa ab ac ad ah ai aj ak al am an ao ap aq ar as at au av aw ax ay az ba bb bc bd be bf bg bh bi bj bk bl bm bn);
	post_code_1 = input(post_code,8.);
	loaded_amount = input('Outstanding Value'n,8.);
	date_of_service_1 = input(strip(date_of_service),ddmmyy10.);
	DOB = input(strip(patient_DOB),ddmmyy10.);
	pickup_address = prxchange('s/^,|,$//',-1,strip('Pickup/Service address continued 'n));
	if pickup_address = Patient_Address then pickup_flag = 'Y';
		else if pickup_address ne "" then pickup_flag = 'N';
		else pickup_address = '?';
	if UPCASE(title) in ('MR','MSTR') then gender='M';
		else if UPCASE(title) in ('MRS','MS','MIS','MISS') then gender='F';
		else gender='?';
	/*See if debtor has health insurance*/
	if find(dossier, 'Insurance', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Medibank', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' Bupa ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' NIB ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' HBF ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Australian Unity', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'GMHBA', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Defence Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'CBHS', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' AHM ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'HCF', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Teachers Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'ACA Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Allianz', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' Apia ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Budget Direct', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Cessnock District', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' CUA ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Doctors Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Emergency Services Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' HBF ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Australian Unity', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'GMHBA', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'GMF', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Grand United', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' HBA ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Health Partners', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' HIF ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' IMAN ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Latrobe Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Mildura Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Navy Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Nurses & Midwives', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'onemedifund', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Peoplecare', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Phoemix Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Police Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Qantas Assure', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Reserve Bank Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'RT Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Transport Health', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, ' TUH ', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'UniHealth', 'i',1) ge 1 then insurance_flag = 'Y';
		else if find(dossier, 'Westfund', 'i',1) ge 1 then insurance_flag = 'Y';
		else insurance_flag = 'N';
	format date_of_service_1 DOB ddmmyy10.;
	drop date_of_service;
	rename date_of_service_1=date_of_service;
	format client_reference_number $char30.;
	if client_reference_number = '' then delete;
run;

/*Check to see how many accounts from the debtload files are missing from au_account*/
/*proc sort data=Debtload;*/
/*	by client_reference_number;*/
/*run;*/
/**/
/*proc sort data=sdata.au_account (keep= client_ledger_number client_reference_number loaded_date where=(client_ledger_number in (6000170,6000171,6000222) and  loaded_date ge '01Jan2015'd and loaded_date le '31Dec2018'd)) out=r;*/
/*	by client_reference_number;*/
/*run;*/
/**/
/*data q;*/
/*	merge Debtload (in=t1) r (in=t2 keep=client_ledger_number client_reference_number loaded_date where=(client_ledger_number in (6000170,6000171,6000222) and loaded_date ge '01Jan2015'd and loaded_date le '31Dec2018'd));*/
/*	by client_reference_number;*/
/*	if t1 and t2;*/
/*run;*/

data email;
	set SJA_2015_2016_EA (where=(email ne '@')) SJA_2017_2018_EA (where=(email ne '@'));
	if find (UPCASE(email), '@STJOHNAMBULANCE.COM.AU', 'i',1) ge 1 then delete;
		else if find (UPCASE(email), '@SMS', 'i',1) ge 1 then delete;
	format client_reference_number $char30.;
run;

data phone;
	set SJA_2015_2016_PN SJA_2017_2018_PN;
	format client_reference_number $char30.;
run;

proc sort data=Debtload nodupkey;
	by client_reference_number;
run;

proc sort data=email nodupkey;
	by client_reference_number email;
run;

proc sort data=phone nodupkey;
	by client_reference_number phone_number;
run;

proc summary data=phone nway missing;
	class client_reference_number;
	output out=phone2 (drop=_TYPE_ rename=(_FREQ_=Phone_Count));
run;

proc summary data=email nway missing;
	class client_reference_number;
	output out=email2 (drop=_TYPE_ rename=(_FREQ_=Email_Count));
run;

data Debtload2;
	merge Debtload (in=t1) email2 (in=t2) phone2 (in=t3);
	by client_reference_number;
	if t1;
	confirmed_phone_number = compress(compress(confirmed_phone_number, ,'A'));
run;

data accts;
	set sdata.au_account (where=(client_ledger_number in (6000170,6000171,6000222) and loaded_date ge '01Jan2015'd and loaded_date le '31Dec2017'd) keep=client_ledger_number account_number debtor_number loaded_date client_reference_number loaded_amount next_expected_event_type debt_to_date debtors_linked);
	if client_ledger_number = 6000170 then area = 'Metro';
		else if client_ledger_number = 6000171 then area = 'Rural';
		else area = '?';
	if next_expected_event_type in (931,932,933,934,935,956,963,969) then delete; 
	format q $char30.;
	q = client_reference_number;
	drop client_reference_number ;
	rename q = client_reference_number;
run;

/*Get Out of Time Data January 2018 to March 2018*/
/*data accts;*/
/*	set sdata.au_account (where=(client_ledger_number in (6000170,6000171,6000222) and loaded_date ge '01Jan2018'd and loaded_date le '31Mar2018'd) keep=client_ledger_number account_number debtor_number loaded_date client_reference_number loaded_amount debt_to_date debtors_linked);*/
/*	if client_ledger_number = 6000170 then area = 'Metro';*/
/*		else if client_ledger_number = 6000171 then area = 'Rural';*/
/*		else area = '?';*/
/*	if next_expected_event_type in (931,932,933,934,935,956,963,969) then delete; */
/*	format q $char30.;*/
/*	q = client_reference_number;*/
/*	drop client_reference_number ;*/
/*	rename q = client_reference_number;*/
/*run;*/

proc sort data=accts;
	by debtor_number;
run;

/*Flag Debtors with multiple SJA accounts*/
data dup;
	set sdata.au_account (where=(client_ledger_number in (6000170,6000171,6000222) and loaded_date le '31Dec2017'd) keep=debtor_number client_ledger_number account_number loaded_date);
run;

proc sort data=dup;
	by debtor_number;
run;

data dup2;
	set dup (where=(loaded_date between '01Jan2015'd and '31Mar2018'd));
	format loaded_date2 ddmmyy10.;
	loaded_date2 = loaded_date;
	drop loaded_date;
run;

proc sql;
create table dup3 as select * from dup2 a,
	dup b where a.debtor_number = b.debtor_number and
	a.loaded_date2 > b.loaded_date
	order by a.debtor_number, a.loaded_date2, b.loaded_date;
quit;  

proc sort data=dup3 (keep=client_ledger_number account_number) nodupkey;
	by client_ledger_number account_number;
run;

data duplicates;
	set dup3;
	multiple_account_flag = 1;
run;

/*Get dimension from date of record that was valid 30 days after load*/
/*data q;*/
/*	merge dmart.RM_account_Debtor (in= t1 where= client_ledger_number in (6000170,6000171,6000222) and Dimension_record_latest_version_ = 1 keep= client_ledger_number debtor_number Dimension_record_latest_version_) rm_debtor (in=t2 keep=debtor_number dimension_record_valid_from_date dimension_record_valid_to_date country_code);*/
/*	by debtor_number;*/
/*	if t1;*/
/*run;*/
/**/
/*data Interval;*/
/*	merge accts (in=t1 keep=debtor_number) dmart.rm_debtor (in=t2 keep=debtor_number dimension_record_valid_from_date dimension_record_valid_to_date country_code);*/
/*	by debtor_number;*/
/*	if t1;*/
/*	if first.debtor_number then output;*/
/*run;*/
/**/
/*data first_debtor_record;*/
/*	set Interval (rename=);*/
/*	if substrn(dimension_record_valid_from_date,5,2) ne 12 then dimension_record_valid_to_date=dimension_record_valid_from_date+100;*/
/*		else dimension_record_valid_to_date=dimension_record_valid_from_date+8900;*/
/*run;*/
/**/
/*proc sql;*/
/*	create table thirty as*/
/*	select a.*,b.dimension_record_valid_from_date*/
/*	from first_debtor_record a left join Interval b*/
/*	on(a.debtor_number = b.debtor_number and b.dimension_record_valid_from_date between a.dimension_record_valid_from_date and a.dimension_record_valid_to_date)*/
/*	order by*/
/*	a.debtor_number,a.dimension_record_valid_from_date*/
/*	;*/
/*quit;*/
/**/
/*data thirty;*/
/*	set thirty;*/
/*	if last.debtor_number then output;*/
/*run;*/

/*Get first record of debtor info*/
data debtor_info;
	merge dmart.rm_account_debtor (in=t1 where=(client_ledger_number in (6000170,6000171,6000222) and Dimension_record_latest_version_ = 1) keep= client_ledger_number account_number debtor_number Dimension_record_latest_version_) dmart.rm_debtor (in=t2 where=(country_code='A' and dimension_record_valid_from_date ge 20150101) keep=debtor_number debtor_first_name debtor_middle_name birth_date employer_name occupation_name home_phone_number home_mobile_number work_phone_number dimension_record_valid_from_date dimension_record_valid_to_date country_code email_address debtor_title_name gender Postal_street_number Postal_street_name Postal_suburb_name Postal_city_name Postal_state_name Postal_post_code 
	Residential_street_number Residential_street_name Residential_suburb_name Residential_city_name Residential_state_name Residential_post_code);
	by debtor_number;
	if t1;
	drop client_ledger_number account_number Dimension_record_latest_version_;
run;

proc sort data=debtor_info;
	by debtor_number dimension_record_valid_from_date;
run;

/*Use equals option to make sure only first records are retained*/
proc sort data=debtor_info nodupkey equals;
	by debtor_number;
run;
	
/*Only  around 8 email adddresses recorded @ load*/
data accts2;
	merge accts (in=t1) debtor_info (in=t2);
	by debtor_number;
	if t1;
	format dob ddmmyy10. employment_status client_reference_number $char30.;
	phone_count=0;
	if occupation_name in ('UNKNOWN','') then employment_status='?';
		else if find(occupation_name, 'CENTRE', 'i',1) ge 1 or find(occupation_name, 'C/LINK', 'i',1) ge 1 or find(occupation_name, 'NEWSTART', 'i',1) ge 1 or find(occupation_name, 'DISABILITY', 'i',1) ge 1 or find(occupation_name, 'PENSION', 'i',1) ge 1 then employment_status='CENTRELINK';
	if Home_mobile_number not in (.,0) then phone_count=phone_count+1;
	if Home_phone_number not in (.,0) then phone_count=phone_count+1;
	if Work_phone_number not in (.,0) then phone_count=phone_count+1;
	mobile_flag=0; fixed_flag=0;
	if phone_count > 0 then do;
		if Home_phone_number ne "" then do; if substr(Home_phone_number,1,2) in ('04','05') then mobile_flag=1; else fixed_flag=1; end;
		if Home_mobile_number ne "" then do; if substr(Home_mobile_number,1,2) in ('04','05') then mobile_flag=1; else fixed_flag=1; end;
		if Work_phone_number ne "" then do; if substr(Work_phone_number,1,2) in ('04','05') then mobile_flag=1; else fixed_flag=1; end;
	end;
	if gender = "" and debtor_title_name ne "" then do;
		if debtor_title_name = 'MR' then gender='M';
		else if debtor_title_name in ('MRS','MS','MIS') then gender='F';
		else gender='?';
	end;
	if gender = "" and debtor_title_name = "" then gender='?';
	/*Birth Date*/
	dob =input(put(birth_date,best8.),yymmdd8.);
	if dob ne . then Debtor_Age_At_Load = intck('year',dob,Loaded_Date,'C');
	/*Address Flag*/
	if residential_street_name ne "" and (residential_city_name ne "" or residential_suburb_name ne "") then residential_address_flag = "Y";
		else residential_address_flag = "N";
	if postal_street_name ne "" and (postal_city_name ne "" or postal_suburb_name ne "") then postal_address_flag = "Y";
		else postal_address_flag = "N";
	debtor_first_name=UPCASE(debtor_first_name);
	debtor_middle_name=UPCASE(debtor_middle_name);
	rename debtor_first_name=first_name debtor_middle_name=middle_name;
	drop dob birth_date occupation_name employer_name Home_mobile_number Home_phone_number Work_phone_number debtor_title_name country_code;
run;

proc sort data=accts2;
	by client_reference_number;
run;

data accts3;
	merge accts2 (in=t1 rename=(debt_to_date=date_of_service)) Debtload2 (in=t2 keep=client_reference_number confirmed_phone_number pickup_flag post_code_1 gender email_count phone_count insurance_flag rename=(phone_count=phone_count_1 gender=gender_1));
	by client_reference_number;
	if t1;
run;

proc sort data=accts3;
	by First_Name;
run;

/*Gender Generator*/

data gender;
	set sdata.gender;
		First_Name=upcase(Debtor_Name);
		SData_Gender1=Gender;
	keep First_Name SData_Gender1;	
run;

data rr;
	merge accts3 (in=t1) gender (in=t2);
	by First_Name;
	if t1;
run;

proc sort data=rr;
	by Middle_Name;
run;

data gender2;
	set sdata.gender;
		Middle_Name=upcase(Debtor_Name);
		SData_Gender2=Gender;
	keep Middle_Name SData_Gender2;	
run;

data ss;
	merge rr (in=t1) gender2 (in=t2);
	by Middle_Name;
	if t1;
run;

data ss;
	set ss;
	if gender in ('?', '') then do;
		if SData_Gender1='M' then Gender='M';
		else if SData_Gender1='F' then Gender='F';
		else if SData_Gender1 in ('', '?') and SData_Gender2='M' then Gender='M';
		else if SData_Gender1 in ('', '?') and SData_Gender2='F' then Gender='F';
		else Gender='?';
	end;
	drop SData_Gender1 SData_Gender2;
	if residential_post_code not in (.,0) then post_code = residential_post_code;
		else if postal_post_code not in (.,0) then post_code = postal_post_code;
		else if post_code_1 not in (.,0) then post_code = post_code_1;
run;

/*Post Code Scores (2016 version)*/

proc import out= post_code_scores
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\St. John's Ambulance\Post Code Scores.xlsx"
	DBMS=xlsx REPLACE; 
RUN;

proc sort data=ss;
	by post_code;
run;

proc sort data=post_code_scores;
	by post_code;
run;

data accts4;
	merge ss (in=t1) post_code_scores (in=t2 keep=post_code Economic_Resource_Decile Occupation_Decile Relative_Disadvantage_Decile Relative_Advantage_Decile);
	by post_code;
	if t1;
	format Address_type $char15.;
	if residential_city_name in ('NFA','NFI') or residential_street_name in ('NO FIXED ADDRESS','NFA','NFI') then NFA_Flag = 'Y';
		else NFA_Flag = 'N';
	if Economic_Resource_Decile = . then Economic_Resource_Decile = 0;
	if Occupation_Decile = . then Occupation_Decile = 0; 
	if Relative_Disadvantage_Decile = . then Relative_Disadvantage_Decile = 0;
	if Relative_Advantage_Decile = . then Relative_Advantage_Decile = 0; 
	if residential_city_name in ('NFA','NFI') or residential_street_name in ('NO FIXED ADDRESS','NFA','NFI') then Address_type = 'NFA';
		else if Economic_Resource_Decile in (.,0) and Occupation_Decile in (.,0) and post_code not in (.,0) and (find(Residential_street_name,'PO BOX', 'i',1) ge 1 or find(Residential_street_number,'PO BOX', 'i',1) ge 1) then Address_type = 'PO Box';
		else if Economic_Resource_Decile in (.,0) and Occupation_Decile in (.,0) and post_code not in (.,0) and Residential_state_name in ('WA','NSW','VIC','SA','NT','TAS','QLD','ACT') then Address_type = 'Special PC';
		else if Economic_Resource_Decile in (.,0) and Occupation_Decile in (.,0) and Residential_state_name = '' then Address_type = 'Overseas';
		else if Economic_Resource_Decile in (.,0) and Occupation_Decile in (.,0) and post_code in (.,0) then Address_type = 'No PC';
		else Address_type = 'Residential';
	if pickup_flag = '' then pickup_flag = '?';
run;

proc sort data=accts4 nodupkey;
	by Client_Ledger_Number Account_Number;
run;

/*Add payment flag in first 6 months*/
/*Need to run first half of "Locate Rate & Pay Rate Analysis Code" first*/
data resp;
	set SJA_pay_rate (where=(pay_6mth=1) keep=Client_Ledger_Number Account_Number pay_6mth);
	rename pay_6mth=resp_var;
run;

proc format library=Work;
	value f_dtrage Low-18 = '1: <=18' 
					19-24 = '2: 19-24' 
					25-43 = '3: 25-43' 
	    		    44-47 = '4: 44-47'
					48-51 = '5: 48-51'
					52-61 = '6: 52-61'
	    		  62-high = '7: 62+'
	    		    other = '3: 25-43';
	value f_load  Low-200 = '1: <=200' 
			   200.01-350 = '2: 200-350'
			   350.01-450 = '3: 350-450'
			   450.01-850 = '4: 450-850'
/*			   550.01-850 = '5: 550-850'*/
			   850.01-900 = '6: 850-900'
			  900.01-high = '7: 900+'
	    		    other = '8: Not Specified';
	value f_service Low-1 = '1: 1' 
						2 = '2: 2'
					    3 = '3: 3'
/*				        4 = '4: 4'*/
/*				        5 = '5: 5'*/
				   4-high = '4: 4+'
	    		    other = '8: Not Specified';
	value f_phone       0 = '1: 0' 
					    1 = '2: 1'
				   2-high = '3: 2+'
	    		    other = '4: Not Specified';
	value f_occup     /*1-1 = '1: 1'*/ 
	    		      1-4 = '2: 1-4,6-7' 
					  5-5 = '3: 5'
	    		      6-7 = '2: 1-4,6-7'
/*					  7-7 = '5: 7'*/
	    		     8-10 = '6: 8-10'
	    		    other = '2: 1-4,6-7';
	value f_occupa    1-4 = '1: 1-4' 
/*	    		      2-2 = '2: 2' */
/*					  3-4 = '3: 3-4' */
/*					  4-4 = '4: 4' */
					  5-5 = '5: 5'
	    		      6-6 = '6: 6'
					  7-7 = '7: 7'
	    		     8-10 = '8: 8-10'
	    		    other = '1: 1-4';
	value f_econ      1-3 = '1: 1-3' 
	    		      4-4 = '2: 4'
					  5-7 = '3: 5-7'
	    		        8 = '4: 8'
					    9 = '5: 9'
					   10 = '6: 10'
					 	0 = '1: 1-3'
	    		    other = '11: Not Specified';
	value f_dis       1-3 = '1: 1-3'
					  4-5 = '2: 4-5'
	    		      6-8 = '3: 6-8'
					  9-10 = '4: 9-10'
					 	0 = '2: 4-5'
	    		    other = '11: Not Specified';
	value f_disa      1-1 = '1: 1'
					  2-5 = '2: 2-5'
/*					  3-3 = '3: 3'*/
/*					  4-5 = '4: 4-5'*/
/*	    		      5-5 = '5: 5'*/
					  6-8 = '6: 6-8'
/*					  7-7 = '7: 7'*/
/*					  8-8 = '8: 8'*/
					  9-9 = '9: 9'
					10-10 = '10: 10'
					 	0 = '2: 2-5'
	    		    other = '11: Not Specified';
	value f_adv       1-1 = '1: 1'
 					  2-2 = '2: 2'
					  3-3 = '3: 3' 
	    		      4-5 = '4: 4-5'
	    		     6-10 = '5: 6-10'
					 	0 = '5: 6-10'
	    		    other = '11: Not Specified';
run;

data accts5;
	merge accts4 (in=t1) resp (in=t2) duplicates (in=t3);
	by Client_Ledger_Number Account_Number;
	if t1;
	if multiple_account_flag = .  then multiple_account_flag = 0;
	if resp_var = . then resp_var = 0;
	if email_count = . then email_count = 0;
	if phone_count_1 = . then phone_count_1 = 0;
	if insurance_flag = '' then insurance_flag = 'N';
	if pickup_flag = '' then pickup_flag = '?';
	if area = '?' then area = 'Rural';
	if email_count ge 1 then email_flag = 1;
		else email_flag = 0;
	if address_type in ('Special PC') then address_type = 'NFA';
		else if address_type in ('No PC') then address_type = 'Residential';
	if debtors_linked in (4,5) then debtors_linked = 1;
	if phone_count = 3 then phone_count = 2;
	Loaded_Amount_Band = put(Loaded_amount,f_load.);
	service_to_load = intck('MONTH',date_of_service,loaded_date,'C');
	service_to_load_band = put(service_to_load,f_service.);
	Debtor_Age_At_Load_Band = put(Debtor_Age_At_Load,f_dtrage.);
	Occupation_Decile_Band = put(Occupation_Decile,f_occupa.);
	Economic_Resource_Decile_Band = put(Economic_Resource_Decile,f_econ.);
	Disadvantage_Decile = put(Relative_Disadvantage_Decile,f_disa.);
	Advantage_Decile = put(Relative_Advantage_Decile,f_adv.);
	phone_count_1_band = put(phone_count_1,f_phone.);
	drop Occupation_Decile phone_count_1 Economic_Resource_Decile Relative_Disadvantage_Decile Relative_Advantage_Decile;
	rename Occupation_Decile_Band = Occupation_Decile phone_count_1_band = phone_count_1 Economic_Resource_Decile_Band = Economic_Resource_Decile;
run;




/*data accts6;*/
/*	set accts5 (where=(loaded_date between '01Jan2015'd and '31Dec2017'd));*/
/*run;*/

/*Out of Time Validation Data Set*/
/*data accts7;*/
/*	set accts5 (where=(loaded_date between '01Jan2018'd and '30Jun2018'd));*/
/*run;*/

/*data jack.sja_time_validate;*/
/*	set accts5;*/
/*run;*/

/*Partitioning of dataset to train and validation datasets*/


/*proc surveyselect data=accts5 out=pay_model seed=103662 samprate=0.70 outall method=srs noprint; run;*/
/*data jack.sja_model_train; set pay_model; where selected =1; run;*/
/*data jack.sja_model_validate; set pay_model; where selected =0; run;*/

/*proc summary data=jack.sja_model_train nway missing;*/
/*	class service_to_load_band;*/
/*	var resp_var;*/
/*	output out=q (drop = _TYPE_) sum=;*/
/*run;*/



/*Look for payments made by debtors to other non-SJA accounts*/

/*proc sort data=sdata.au_account (keep=client_ledger_number account_number debtor_number where=(client_ledger_number not in (6000170,6000171,6000222) and debtor_number ne .)) out=No_SJA;*/
/*	by debtor_number;*/
/*run;*/
/**/
/*data linked_accounts;*/
/*	merge debtor_info (in=t1 keep=debtor_number) No_SJA (in=t2);*/
/*	by debtor_number;*/
/*	if t1 and t2;*/
/*run;*/
/**/
/*proc sort data=linked_accounts;*/
/*	by client_ledger_number account_number;*/
/*run;*/
/**/
/*data payment_flag;*/
/*	merge linked_accounts (in=t1) sdata.au_payments (in=t2 keep=client_ledger_number account_number payment_amount);*/
/*	by client_ledger_number account_number;*/
/*	if t1;*/
/*run;*/
/**/
/*proc summary data=payment_flag nway missing;*/
/*	class client_ledger_number account_number debtor_number;*/
/*	var payment_amount;*/
/*	output out=q sum=payment_amount;*/
/*run;*/








