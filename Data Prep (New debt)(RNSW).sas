libname JACK '\\BCSPARSAS01\sasdata\SAS Data\Jack\SASdata';

data accts;
	set sdata.au_account (where=(client_ledger_number = 2000966 and loaded_date ge '01Jan2015'd and loaded_date le '28Apr2019'd) keep=client_ledger_number account_number debtor_number next_expected_event_type loaded_date client_reference_number loaded_amount original_debt_amount debt_from_date debt_to_date debtors_linked client_last_payment_date client_last_payment_amount);
	if next_expected_event_type in (931,932,933,934,935,956,963,969) then delete; 
run;

proc sort data=accts;
	by debtor_number;
run;

/*Get first record of debtor info*/
data debtor_info;
	merge dmart.rm_account_debtor (in=t1 where=(client_ledger_number =2000966 and Dimension_record_latest_version_ = 1) keep= client_ledger_number account_number debtor_number Dimension_record_latest_version_) dmart.rm_debtor (in=t2 where=(country_code='A' and dimension_record_valid_from_date ge 20150101) keep=debtor_number debtor_first_name debtor_middle_name debtor_family_name birth_date drivers_licence email_address employer_name occupation_name home_phone_number home_mobile_number work_phone_number dimension_record_valid_from_date dimension_record_valid_to_date country_code email_address debtor_title_name gender Postal_street_number Postal_street_name Postal_suburb_name Postal_city_name Postal_state_name Postal_post_code 
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

data accts2;
	merge accts (in=t1) debtor_info (in=t2);
	by debtor_number;
	if t1;
	format dob ddmmyy10. employment_status client_reference_number $char30.;
	phone_count=0;
	if find(employer_name, 'CENTRE', 'i',1) ge 1 or find(employer_name, 'CLINK', 'i',1) ge 1 or find(employer_name, 'DISABILITY', 'i',1) ge 1 or find(employer_name, 'PENSION', 'i',1) ge 1 or find(employer_name, 'not work', 'i',1) ge 1 or find(employer_name, 'student', 'i',1) ge 1 or find(employer_name, 'allow', 'i',1) ge 1 or find(employer_name, 'unemp', 'i',1) ge 1 then employment_status='Unemployed';
		else if employer_name = '' then employment_status = 'Unknown';
		else if find(employer_name, 'empl', 'i',1) ge 1 and find(employer_name, 'self', 'i',1) ge 1 then employment_status = 'Self-employed';
		else employment_status = 'Employed';
	if Home_mobile_number not in (.,0) then phone_count=phone_count+1;
	if Home_phone_number not in (.,0) then phone_count=phone_count+1;
	if Work_phone_number not in (.,0) then phone_count=phone_count+1;
	mobile_flag=0; fixed_flag=0; mobile_count=0; fixed_count=0;
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
	if drivers_licence ne "" then drivers_licence_flag = "Y";
		else drivers_licence_flag = "N";
	if email_address ne "" then email_flag = "Y";
		else email_flag = "N";
	/*Birth Date*/
	dob =input(put(birth_date,best8.),yymmdd8.);
	if residential_state_name = '' then do;
		if find(residential_city_name,'NSW','i',1) ge 1 then residential_state_name = 'NSW';
			else if find(residential_city_name,'QLD','i',1) ge 1 then residential_state_name = 'QLD';
			else if find(residential_city_name,' VIC','i',1) ge 1 then residential_state_name = 'VIC';
			else if find(residential_city_name,' NT','i',1) ge 1 then residential_state_name = 'NT';
			else if find(residential_city_name,' SA','i',1) ge 1 then residential_state_name = 'SA';
			else if find(residential_city_name,' TAS','i',1) ge 1 then residential_state_name = 'TAS';
			else if find(residential_city_name,' ACT','i',1) ge 1 then residential_state_name = 'ACT';
			else if find(residential_city_name,' WA','i',1) ge 1 then residential_state_name = 'WA';
	end;
	/*Address Flag*/
	if residential_street_name ne "" and (residential_city_name ne "" or residential_suburb_name ne "") then residential_address_flag = "Y";
		else residential_address_flag = "N";
	if postal_street_name ne "" and (postal_city_name ne "" or postal_suburb_name ne "") then postal_address_flag = "Y";
		else postal_address_flag = "N";
	debtor_first_name=UPCASE(debtor_first_name);
	debtor_middle_name=UPCASE(debtor_middle_name);
	rename debtor_first_name=first_name debtor_middle_name=middle_name debtor_family_name=last_name;
	drop birth_date email_address occupation_name employer_name Home_mobile_number Home_phone_number Work_phone_number debtor_title_name country_code drivers_licence;
run;

proc sort data=accts2 nodupkey equals;
	by client_ledger_number account_number;
run;

proc sort data=accts2;
	by client_reference_number;
run;


/*Check for duplicates*/
/*proc sort data=accts2 nodupkey out=q;*/
/*	by client_ledger_number account_number;*/
/*run;*/

/*Get Debtload File Info -  Start*/

proc import out = E2015
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2015\2015 E File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = E2016
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2016\2016 E File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = E2017
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2017\2017 E File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = E2018
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2018\2018 E File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = E2019
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2019\2019 E File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

data E_File;
	set E2015 E2016 E2017 E2018 E2019;
	row = _n_;
run;

/*Get Date of Debtload File*/
data edate (keep=date row);
	set E_File;
	format date ddmmyy10.;
	if substr(llll,1,1) = 'H' then date = input(put(substr(llll,19,8)*1,z8.),yymmdd8.);
	if date ne '' then output;
run;

data edate2 additional_row;
	set edate nobs = nobs;
	format date2 ddmmyy10.;
	first_row = lag(row);
	last_row = row - 1;
	date2 = lag(date);
	if _n_ = nobs then output additional_row;
	if _n_ ge 2 then output edate2;
run;

proc sql noprint;
	select count(*) into :final_row trimmed from E_File;
quit;

data additional_row2;
	set additional_row;
	first_row = row;
	last_row = &final_row;
	date2 = date;
run;

data edate3;
	set edate2 additional_row2;
	drop row date;
	rename date2 = date;
run;

proc sql;
	create table E_File2 as
	select a.llll,b.date
	from E_File a 
	left join edate3 b on(b.first_row <= a.row <= b.last_row)
	order by a.row;
quit;

/*Remove duplicate fines caused by record updates (that are unrelated to the E file)*/
proc sort data=E_File2;
	by date llll;
run;

proc sort data=E_File2 nodupkey equals;
	by llll;
run;



data E_File3;
	set E_File2;
	format word1 crime_type $50. Client_Reference_Number crime_type crime_severity $20.;
	delims = ' ,.!';
	count = countw(llll,delims);
	word1 = scan(llll, 1,' ');
	Client_Reference_Number = substr(llll,2,8);
	EO_Number = substr(llll,10,9);
	if length(word1) = 26 then issue_date = substr(word1,max(1,length(word1)-7))*1;
	fine_referral = strip(scan(llll, 2,' '));
/*	fine_referral = strip(substr(llll,27,30));*/
	call scan(llll,3,pos,len,' ');
	leftover = substr(llll,pos);
	crime = scan(leftover, 1,'|');
	if find(crime, 'Park', 'i',1) ge 1 then crime_type = 'Parking';
		else if find(crime, 'intimidat', 'i',1) ge 1 or find(crime, 'threat', 'i',1) ge 1 or find(crime, 'harass', 'i',1) ge 1 or find(crime, 'stalk', 'i',1) ge 1 then crime_type = 'Intimidation';
		else if find(crime, 'Assault', 'i',1) ge 1 or find(crime, 'Bodily', 'i',1) ge 1 or find(crime, 'Wound', 'i',1) ge 1 or find(crime, 'Affray', 'i',1) ge 1 or find(crime, 'armed', 'i',1) ge 1 or find(crime, 'violen', 'i',1) ge 1 then crime_type = 'Assault';
		else if find(crime, 'Firearm', 'i',1) ge 1 or find(crime, 'Pistol', 'i',1) ge 1 or find(crime, 'Gun', 'i',1) ge 1 or find(crime, 'Knife', 'i',1) ge 1 or find(crime, 'offensive implement', 'i',1) ge 1 or find(crime, 'weapon', 'i',1) ge 1 then crime_type = 'Weapon Possession';
		else if find(crime, 'Travel', 'i',1) ge 1 or find(crime, 'Ticket', 'i',1) ge 1 or find(crime, 'Concession', 'i',1) ge 1 or find(crime, ' Train', 'i',1) ge 1 or find(crime, 'Bus', 'i',1) ge 1 or find(crime, ' Ferry', 'i',1) ge 1 or find(crime, ' fare', 'i',1) ge 1 then crime_type = 'Public Transport';
		else if find(crime, 'vote', 'i',1) ge 1 or find(crime, ' jury', 'i',1) ge 1 then crime_type = 'Not Voting/Jury';
		else if find(crime, 'standard', 'i',1) ge 1 and find(crime, 'time', 'i',1) ge 1 then crime_type = 'Overwork';
		else if find(crime, 'false', 'i',1) ge 1 or find(crime, 'fraud', 'i',1) ge 1 or find(crime, 'impersonate', 'i',1) ge 1 then crime_type = 'Fraud';
		else if find(crime, 'Litter', 'i',1) ge 1 then crime_type = 'Littering';
		else if find(crime, 'Drink', 'i',1) ge 1 or find(crime, 'intoxicat', 'i',1) ge 1 or find(crime, 'liquor', 'i',1) ge 1 or find(crime, 'Smoke', 'i',1) ge 1 then crime_type = 'Drinking/Smoking';
		else if find(crime, 'drug', 'i',1) ge 1 or find(crime, 'prohibited plant', 'i',1) ge 1 or find(crime, 'restricted substance', 'i',1) ge 1 or find(crime, 'cultivate', 'i',1) ge 1 or find(crime, 'cannabis', 'i',1) ge 1 then crime_type = 'Drug';
		else if find(crime, 'resist', 'i',1) ge 1 then crime_type = 'Resist';
		else if find(crime, 'steal', 'i',1) ge 1 or find(crime, 'stolen', 'i',1) ge 1 or find(crime, 'shoplift', 'i',1) ge 1 or find(crime, 'robbery', 'i',1) ge 1 or find(crime, 'deception', 'i',1) ge 1 or 
		find(crime, 'larceny', 'i',1) ge 1 or find(crime, 'embezzle', 'i',1) ge 1 or find(crime, 'proceeds of crime', 'i',1) ge 1 or find(crime, 'unlawfully obtain', 'i',1) ge 1 then crime_type = 'Theft';
		else if find(crime, 'Disclose', 'i',1) ge 1 or find(crime, 'records', 'i',1) ge 1 or find(crime, 'Change', 'i',1) ge 1 and find(crime, 'Notify', 'i',1) ge 1 then crime_type = 'Non-disclosure';				
		else if find(crime, 'Tax', 'i',1) ge 1 or (find(crime, 'Pay', 'i',1) ge 1 and (find(crime, 'Charge', 'i',1) ge 1 or find(crime, 'Fee', 'i',1) ge 1 or find(crime, 'Toll', 'i',1) ge 1)) then crime_type = 'Fee/Taxes';
		else if find(crime, 'Dog', 'i',1) ge 1 or find(crime, 'Animal', 'i',1) ge 1 then crime_type = 'Animal';
		else if find(crime, 'sex', 'i',1) ge 1 or find(crime, 'prostitut', 'i',1) ge 1 then crime_type = 'Sexual';
		else if find(crime, 'property', 'i',1) ge 1 and (find(crime, 'damage', 'i',1) ge 1 or find(crime, 'destroy', 'i',1) ge 1) then crime_type = 'Property Damage';
		else if find(crime, 'speed', 'i',1) ge 1 or find(crime, 'traffic', 'i',1) ge 1 or find(crime, 'drive', 'i',1) ge 1 or find(crime, 'vehicle', 'i',1) ge 1 or find(crime, 'not stop', 'i',1) ge 1 or find(crime, 'keep left', 'i',1) ge 1 or find(crime, 'learner', 'i',1) ge 1 or find(crime, 'exceed', 'i',1) ge 1 or find(crime, 'Pedestrian', 'i',1) ge 1 or
		find(crime, 'Passenger', 'i',1) ge 1 or find(crime, 'P1', 'i',1) ge 1 or find(crime, 'P2', 'i',1) ge 1 or find(crime, 'Driv', 'i',1) ge 1 or find(crime, 'U-Turn', 'i',1) ge 1 or find(crime, 'Car ', 'i',1) ge 1 or find(crime, 'disobey', 'i',1) ge 1 or find(crime, 'motor', 'i',1) ge 1 or
		find(crime, 'Stop', 'i',1) ge 1 or find(crime, 'Stand ', 'i',1) ge 1 or find(crime, 'Remain', 'i',1) ge 1 or find(crime, 'ride', 'i',1) ge 1 or find(crime, 'Bike', 'i',1) ge 1 then crime_type = 'Traffic Offence';
		else if find(crime, 'possess', 'i',1) ge 1 or find(crime, 'dealer', 'i',1) ge 1 /*or find(crime, 'prohibited', 'i',1) ge 1*/ then crime_type = 'Possession';
		else if find(crime, 'Bail', 'i',1) ge 1 or find(crime, 'appear', 'i',1) ge 1 then crime_type = 'Bail';
		else if find(crime, 'Waste', 'i',1) ge 1 or find(crime, 'Pollut', 'i',1) ge 1 then crime_type = 'Pollution';
		else if find(crime, 'Offensive', 'i',1) ge 1 or find(crime, 'Noise', 'i',1) ge 1 or find(crime, 'Interfere', 'i',1) ge 1 then crime_type = 'Nuisance';
		else if find(crime, 'restricted area', 'i',1) ge 1 or find(crime, 'premises', 'i',1) ge 1 or find(crime, 'enter', 'i',1) ge 1 or find(crime, 'entry', 'i',1) ge 1 or find(crime, 'trespass', 'i',1) ge 1 then crime_type = 'Trespassing';
		else if find(crime, 'comply', 'i',1) ge 1 or find(crime, 'refuse', 'i',1) ge 1 or find(crime, 'contraven', 'i',1) ge 1 or find(crime, 'without insurance', 'i',1) ge 1 or (find(crime, 'development', 'i',1) ge 1 and find(crime, 'consent', 'i',1) ge 1) or find(crime, 'fish', 'i',1) ge 1 or find(crime, 'sell', 'i',1) ge 1 or find(crime, 'lodge', 'i',1) ge 1 or
		find(crime, 'licen', 'i',1) ge 1 or find(crime, 'Approv', 'i',1) ge 1 or find(crime, 'regist', 'i',1) ge 1 or find(crime, 'certificate', 'i',1) ge 1 or find(crime, 'report', 'i',1) ge 1 or find(crime, 'unreg', 'i',1) ge 1 or find(crime, 'name', 'i',1) ge 1 or find(crime, 'operat', 'i',1) ge 1 or
		find(crime, 'statement', 'i',1) ge 1 or find(crime, 'fail', 'i',1) ge 1 then crime_type = 'Non-compliance';
		else if find(crime, 'signal', 'i',1) ge 1 or find(crime, 'roundabout', 'i',1) ge 1 or find(crime, 'light', 'i',1) ge 1 then crime_type = 'Traffic Offence';
		else crime_type = 'Other'; 
	if crime_type in ('Not Voting/Jury','Nuisance','Overwork','Littering','Non-compliance','Fee/Taxes','Public Transport','Parking','Non-disclosure') then crime_severity = '1. Rare';
		else if crime_type in ('Drinking/Smoking','Animal','Bail','Fraud','Possession','Pollution','Traffic Offence') then crime_severity = '2. Medium Rare';
		else if crime_type in ('Theft','Trespassing','Resist','Drug') then crime_severity = '3. Medium';
		else if crime_type in ('Intimidation','Property Damage','Sexual','Weapon Possession') then crime_severity = '4. Medium Well';
		else if crime_type in ('Assault') then crime_severity = '5. Well Done';
		else crime_severity = '6. Other';
/*	if crime_type = 'Traffic Offence' and find(crime, 'comply', 'i',1) ge 1 then to_type = '';*/
/*		else if crime_type = 'Traffic Offence' and find(crime, 'comply', 'i',1) ge 1 then to_type = '';*/
	crime_address = scan(leftover, 2,'|');
	leftover2 = scan(leftover, 3,'|');
	pos2 = index(leftover2,'  ');
	k = substr(leftover2,1,pos2-1);
	number_plate = substr(k, 1, length(k)-8);
	offence_date = substr(k,max(1,length(k)-7))*1;
	fine_type = strip(substr(leftover2,pos2+2));
	if number_plate not in ('','NO PLATE NUMBER') then number_plate = 'Y';
		else number_plate = 'N';
	if count le 2 then delete;
	drop delims count pos pos2 len leftover leftover2 k word1 llll;
run;

proc summary data=E_File3 nway missing;
	class crime_type;
	output out=q;
run;

proc summary data=E_File3 (where=(crime_type = 'Traffic Offence')) nway missing;
	class crime;
	output out=q;
run;

/*Use Fine_referral number (verified wtih Oliver Calman) to differentiate between different fines for the same person*/
proc sort data=E_File3 nodupkey equals;
	by client_reference_number fine_referral;
run;


/*proc sort data=E_File3 nodupkey equals out=r;*/
/*	by client_reference_number issue_date crime crime_address offence_date fine_type;*/
/*run;*/





proc import out = M2015
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2015\2015 M File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = M2016
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2016\2016 M File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = M2017
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2017\2017 M File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = M2018
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2018\2018 M File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

proc import out = M2019
	DATAFILE = "\\bcssydfs01\shared\JZ\Adel\Revenue NSW\Debtload Files\2019\2019 M File.xlsx"
	DBMS=xlsx REPLACE;
	sheet="Compile";
RUN;

data M_File;
	set M2015 M2016 M2017 (drop= b) M2018 M2019;
	row = _n_;
run;


/*Get Date of Debtload File*/
data mdate (keep=date row);
	set M_File;
	format date ddmmyy10.;
	if substr(llll,1,1) = 'H' then date = input(put(substr(llll,19,8)*1,z8.),yymmdd8.);
	if date ne '' then output;
run;

data mdate2 additional_row;
	set mdate nobs = nobs;
	format date2 ddmmyy10.;
	first_row = lag(row);
	last_row = row - 1;
	date2 = lag(date);
	if _n_ = nobs then output additional_row;
	if _n_ ge 2 then output mdate2;
run;

proc sql noprint;
		select count(*) into :lr trimmed from M_File;
quit;

data additional_row2;
	set additional_row;
	first_row = row;
	last_row = &lr;
	date2 = date;
run;

data mdate3;
	set mdate2 additional_row2;
	drop row date;
	rename date2 = date;
run;

proc sql;
	create table M_File2 as
	select a.llll,b.date
	from M_File a 
	left join mdate3 b on(b.first_row <= a.row <= b.last_row)
	order by a.row;
quit;
	

data new_referral (keep=Client_Reference_Number date) person_flag(keep=Client_Reference_Number date person_flag ) bank(keep=Client_Reference_Number date bank) employer(keep=Client_Reference_Number date employer)
	offence_severity(keep=Client_Reference_Number date offence_severity) bday (keep=Client_Reference_Number date bday) balance (keep=Client_Reference_Number date balance) /*filename (keep=llll count)*/;
	set M_File2;
	format Client_Reference_Number $20. bday ddmmyy10.;
	delims = ' ,.!';
	count = countw(llll,delims);
	Client_Reference_Number = substr(llll,2,8);
	if find(substr(llll,1957,32),'referred','i',1) ge 1 then type = 1;
		else type = 0;
	person_flag = substr(llll,35,1);
	bank = substr(llll,619,32);
	employer = substr(llll,683,32);
	offence_severity = substr(llll,2031,1);
	bday = input(put(substr(llll,176,8)*1,z8.),yymmdd8.);
	balance = substr(llll,2021,10)/100;
/*	if count le 2 then output filename;*/
	if count le 2 then delete;
	if type not in (.,0) then output new_referral;
	if person_flag ne '' then output person_flag;
	if bank ne '' and type not in (.,0) then output bank;
	if employer ne '' then output employer;
	if offence_severity ne '' then output offence_severity;
	if bday ne '' then output bday;
	if balance ne '' then output balance;
	drop delims;
run;

/*Check*/
/*data q;*/
/*	set filename;*/
/*	if substr(llll,1,1) not in ('H','T');*/
/*run;*/

/*Keep the first 'initial' transaction*/
proc sort data=new_referral;
	by Client_Reference_Number date;
run;

proc sort data=new_referral nodupkey equals;
	by Client_Reference_Number;
run;

proc sort data=person_flag;
	by Client_Reference_Number date;
run;

proc sort data=person_flag nodupkey equals;
	by Client_Reference_Number;
run;

proc sort data=bank;
	by Client_Reference_Number date;
run;

proc sort data=bank nodupkey equals;
	by Client_Reference_Number;
run;

proc sort data=employer;
	by Client_Reference_Number date;
run;

proc sort data=employer nodupkey equals;
	by Client_Reference_Number;
run;

proc sort data=offence_severity;
	by Client_Reference_Number date;
run;

proc sort data=offence_severity nodupkey equals;
	by Client_Reference_Number;
run;

proc sort data=bday;
	by Client_Reference_Number date;
run;

proc sort data=bday nodupkey equals;
	by Client_Reference_Number;
run;

proc sort data=balance;
	by Client_Reference_Number date;
run;

proc sort data=balance nodupkey equals;
	by Client_Reference_Number;
run;

proc sql;
	create table M_File3 as
	select a.Client_Reference_Number,a.date,b.person_flag,c.bank,d.employer,e.offence_severity,f.bday,g.balance
	from new_referral a 
	left join person_flag b on(a.Client_Reference_Number = b.Client_Reference_Number and a.date <= b.date <= a.date+7)
	left join bank c on(a.Client_Reference_Number = c.Client_Reference_Number and a.date <= c.date <= a.date+7)
	left join employer d on(a.Client_Reference_Number = d.Client_Reference_Number and a.date <= d.date <= a.date+7)
	left join offence_severity e on(a.Client_Reference_Number = e.Client_Reference_Number and a.date <= e.date <= a.date+7)
	left join bday f on(a.Client_Reference_Number = f.Client_Reference_Number and a.date <= f.date <= a.date+7)
	left join balance g on(a.Client_Reference_Number = g.Client_Reference_Number and a.date <= g.date <= a.date+7)
	order by a.Client_Reference_Number,a.date;
quit;

proc sort data=M_File3 nodupkey;
	by Client_Reference_Number;
run;

proc sql;
	create table E_File4 as
	select a.Client_Reference_Number,a.date as mdate,b.*
	from M_File3 a 
	left join E_File3 b on(a.Client_Reference_Number = b.Client_Reference_Number and a.date-7 <= b.date <= a.date+7)
	order by a.Client_Reference_Number,a.date;
quit;

/*Testing for date difference between accounts in the E and M Files - Start*/

/*proc sort data=E_File3;*/
/*	by Client_Reference_Number date;*/
/*run;*/

/*data data_test (keep=Client_Reference_Number date);*/
/*	merge E_File3 (in=t1) M_File3 (in=t2);*/
/*	by Client_Reference_Number date;*/
/*	if t1 and not t2;*/
/*	if t1 and t2;*/
/*	if t2 and not t1;*/
/*run;*/

/*data q;*/
/*	merge data_test (in=t1) E_File3 (in=t2 rename=(date=date2));*/
/*	by Client_Reference_Number;*/
/*	if t1 and t2;*/
/*	datedif = date2 - date;*/
/*run;*/

/*proc summary data=q nway missing;*/
/*	class datedif;*/
/*	output out=qq;*/
/*run;*/

/*data qqq;*/
/*	set q (where=(datedif = -6));*/
/*run;*/

/*data qqqq;*/
/*	set E_File3 (where=(Client_Reference_Number = '00240596'));*/
/*run;*/

/*data qq;*/
/*	set sdata.au_account (where=(Client_Reference_Number = '00240596'));*/
/*run;*/

/*Testing for date difference between accounts in the E and M Files - End*/





data debtload;
	merge E_File4 (in=t1 rename=(date=edate)) M_File3 (in=t2 drop=date);
	by Client_Reference_Number;
	if t1 and t2;
	if number_plate = 'Y' then number_plate_f = 1;
	if person_flag = 'N' then company_f = 1;
	if bank ne '' then bank_f = 1;
/*	if crime_type = 'Traffic Offence' then traffic_offence = 1;*/
/*		else if crime_type = 'Public Transport' then Public_Transport = 1;*/
/*		else if crime_type = 'Parking' then Parking = 1;*/
	if crime_severity = '1. Rare' then a = 1;
		else if crime_severity = '2. Medium Rare' then b = 1;
		else if crime_severity = '3. Medium' then c = 1;
		else if crime_severity = '4. Medium Well' then d = 1;
		else if crime_severity = '5. Well Done' then e = 1;
		else if crime_severity = '6. Other' then f = 1;
	if crime_type = 'Not Voting/Jury' then vote = 1;
		else if crime_type = 'Nuisance' then nuisance = 1;
		else if crime_type = 'Overwork' then overwork = 1;
		else if crime_type = 'Littering' then littering = 1;
		else if crime_type = 'Non-compliance' then comply = 1;
		else if crime_type = 'Fee/Taxes' then fee = 1;
		else if crime_type = 'Public Transport' then public_transport = 1;
		else if crime_type = 'Parking' then parking = 1;
		else if crime_type = 'Non-disclosure' then disclose = 1;
		else if crime_type = 'Drinking/Smoking' then drink = 1;
		else if crime_type = 'Animal' then animal = 1;
		else if crime_type = 'Bail' then bail = 1;
		else if crime_type = 'Fraud' then fraud = 1;
		else if crime_type = 'Possession' then possess = 1;
		else if crime_type = 'Pollution' then pollution = 1;
		else if crime_type = 'Traffic Offence' then traffic_offence = 1;
		else if crime_type = 'Theft' then theft = 1;
		else if crime_type = 'Trespassing' then trespass = 1;
		else if crime_type = 'Resist' then resist = 1;
		else if crime_type = 'Drug' then drug = 1;
		else if crime_type = 'Intimidation' then intimidation = 1;
		else if crime_type = 'Property Damage' then damage = 1;
		else if crime_type = 'Sexual' then sexual = 1;
		else if crime_type = 'Weapon Possession' then Wpossession = 1;
		else if crime_type = 'Assault' then assault = 1;
		else Other = 1;
	fine_count = sum(a,b,c,d,e,f);
	if fine_type = 'Penalty Notice' then Penalty_Notice = 1;
		else if fine_type = 'Courts' then Courts = 1;
		else if fine_type = 'Electoral Office' then Electoral_Office = 1;
		else if fine_type = 'Historical' then Historical = 1;
		else if fine_type = 'Other Matter' then Other_Matter = 1;
		else if fine_type = 'Sheriff Office' then Sheriff_Office = 1;
run;

proc summary data=debtload nway missing;
	class Client_Reference_Number bday;
	var balance a b c d e f nuisance overwork littering comply fee fine_count Public_Transport Parking disclose drink animal bail fraud possess pollution traffic_offence theft trespass resist drug intimidation damage sexual Wpossession assault other Penalty_Notice Courts Electoral_Office Historical Other_Matter Sheriff_Office bank_f company_f number_plate_f;
	output out=crime (drop=_TYPE_ _FREQ_) sum=;
run;

/*Get most common crime type - Start*/
proc sort data=debtload;
  by Client_Reference_Number crime_type;
run;

data most_common_crime(drop=freq maxfreq crime_type rename=(mode = most_common_crime));
  do until (last.Client_Reference_Number);
    set debtload (keep=Client_Reference_Number crime_type);
    by Client_Reference_Number crime_type;
    if first.crime_type then freq=0;
    freq+1;
    maxfreq=max(freq,maxfreq);
    if freq=maxfreq then mode=crime_type;
  end;
  do until (last.Client_Reference_Number);
    set debtload (keep=Client_Reference_Number crime_type);
    by Client_Reference_Number;
    output;
  end;
run;

proc sort data=most_common_crime nodupkey;
	by Client_Reference_Number;
run;

/*Get most common crime type - End*/

data crime2;
	set crime;
	drop /*a b c d e f*/ bank_f company_f number_plate_f;
	if e ne . then worst_crime = 5;
		else if d ne . then worst_crime = 4;
		else if c ne . then worst_crime = 3;
		else if b ne . then worst_crime = 2;
		else if a ne . then worst_crime = 1;
		else worst_crime = 0;
	if bank_f not in (.,0) then bank_flag = 'Y';
		else bank_flag = 'N';
	if company_f not in (.,0) then company_flag = 'Y';
		else company_flag = 'N';
	if number_plate_f not in (.,0) then number_plate_flag = 'Y';
		else number_plate_flag = 'N';
run;

proc summary data=debtload nway missing;
	class Client_Reference_Number;
	var issue_date offence_date;
	output out=dates (drop=_TYPE_ _FREQ_) min=issue_min offence_min max=issue_max offence_max;
run;

data debtload2;
	merge dates (in=t1) crime2 (in=t2) most_common_crime (in=t3);
	by Client_Reference_Number;
	if t1 or t2;

run;

/*Get Debtload File Info - End*/

data accts3;
	merge accts2 (in= t1) debtload2 (in=t2);
	by Client_Reference_Number;
	/*Need to do t1 and t2 instead of t1 to excluded accounts that are missing from the debtload file due to the 8 missing 2018 M files*/
	if t1 and t2;
	format first_issue_date last_issue_date first_offence_date last_offence_date ddmmyy10.;
	/*Use debtload bday instead where dob from datamart is missing*/
	if dob = . then dob = bday;
	if dob ne . then Debtor_Age_At_Load = intck('year',dob,Loaded_Date,'C');
	first_issue_date = input(put(issue_min,best8.),yymmdd8.);
	last_issue_date = input(put(issue_max,best8.),yymmdd8.);
	first_offence_date = input(put(offence_min,best8.),yymmdd8.);
	last_offence_date = input(put(offence_max,best8.),yymmdd8.);
	drop issue_min offence_min issue_max offence_max;
run;

/*Check for discrepancies between M files and sdata.au_account - Start*/

/*data q;*/
/*	merge accts2 (in=t1) new_referral (in=t2);*/
/*	by Client_Reference_Number;*/
/*	if t1 and not t2;*/
/*run;*/

/*Check which M files are missing*/
/*Only 8 Files/Dates with this discrepancy*/
/*proc summary data=q nway missing;*/
/*	class loaded_date;*/
/*	output out=qq;*/
/*run;*/

/*proc summary data=sdata.au_account (where=(client_ledger_number = 2000966 and next_expected_event_type not in (931,932,933,934,935,956,963,969) and loaded_date in ('08Aug2017'd,'09Aug2017'd,'04Sep2017'd,'05Sep2017'd,'16Oct2017'd,'17Oct2017'd,'08Nov2017'd,'20Nov2017'd) )) nway missing;*/
/*	class loaded_date;*/
/*	output out=q;*/
/*run;*/

/*Check for discrepancies between M files and sdata.au_account - End*/

/*Gender Generator - Start*/

proc sort data=accts3;
	by First_Name;
run;

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
run;

/*Gender Generator - End*/

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
		else if Economic_Resource_Decile in (.,0) and Occupation_Decile in (.,0) and post_code in (.,0) then Address_type = 'No PC';
		else Address_type = 'Residential';
run;

proc sort data=accts4 nodupkey;
	by Client_Ledger_Number Account_Number;
run;


/*Add payment flag in first 3 months*/
/*Need to run first half of "Locate Rate & Pay Rate Analysis Code" first*/
data resp;
	set RNSW_pay_rate (where=(pay_1mth=1) keep=Client_Ledger_Number Account_Number pay_1mth);
	rename pay_1mth=resp_var;
run;

proc sort data=resp nodupkey;
	by Client_Ledger_Number Account_Number;
run;


proc format library=Work;
	value f_dtrage Low-20 = '1: <=20' 
					21-22 = '2: 21-22'
					23-31 = '3: 23-31'
					32-39 = '4: 32-39'
	    		    40-51 = '5: 40-51'
				  52-high = '6: 52+'
	    		    other = '1: <=20';
	value f_load  Low-250 = '1: <=250'
			   250.01-600 = '2: 250-600'
			  600.01-1000 = '3: 600-1000'
			 1000.01-3000 = '4: 1000-3000'
			 3000.01-high = '5: 3000+'
	    		    other = '6: Not Specified';
	value f_pen       0-0 = '1: 0'
	    		      1-3 = '2: 1-3' 
					  4-6 = '3: 4-6'
	    		      7-9 = '4: 7-9'
					10-12 = '5: 10-12'
	    		    13-16 = '6: 13-16'
				  17-high = '7: 17+'
	    		    other = '2: 1-3';
	value f_issue     0-2 = '1: 0-3'
	    		      3-3 = '2: 3-4' 
					  4-6 = '3: 4-7'
	    		     7-10 = '4: 7-11'
					11-28 = '5: 11-29'
				  29-high = '6: 29+'
	    		    other = '2: 3-4';
	value f_offence   0-5 = '1: 0-5'
	    		      6-6 = '2: 6-7' 
					 7-11 = '3: 7-12'
					12-14 = '4: 12-15'
	    		  15-high = '5: 15+'
	    		    other = '3: 7-12';
	value f_offenceb  0-5 = '1: 0-6'
	    		      6-6 = '2: 6-7' 
					 7-10 = '3: 7-11'
					11-59 = '4: 11-60'
				  60-high = '5: 60+'
	    		    other = '3: 7-11';
	value f_fine      1-1 = '1: 1' 
	    		      2-2 = '2: 2' 
					  3-3 = '3: 3' 
					  4-4 = '4: 4' 
					  5-6 = '5: 5-6'
	    		     7-10 = '6: 7-10'
				  11-high = '7: 11+'
	    		    other = '1: 1';
	value f_econ        0 = '2: 2-5' 
					    1 = '1: 1'
					  2-5 = '2: 2-5'
					  6-8 = '3: 6-8'
					 9-10 = '4: 9-10'
	    		    other = '5: Missing';
	value f_dis       1-3 = '1: 1-3'
					  4-5 = '2: 4-5'
	    		      6-8 = '3: 6-8'
					 9-10 = '4: 9-10'
					 	0 = '2: 4-5'
	    		    other = '11: Not Specified';
run;

data accts5;
	merge accts4 (in=t1) resp (in=t2);
	by Client_Ledger_Number Account_Number;
	if t1;
	format State $char10. fine_type $char20. phone_type $char4.;
	if resp_var = . then resp_var = 0;
	fines = put(fine_count,f_fine.);
	if traffic_offence = . then traffic_offence = 0;
		else traffic_offence = 1;
	if Public_Transport = . then Public_Transport = 0;
		else Public_Transport = 1;
	if Parking = . then Parking = 0;
		else Parking = 1;
	if email_count = . then email_count = 0;
	if phone_count = . then phone_count = 0;
	if phone_count in (2,3) then phone_count = 1;
	if mobile_flag = 1 then phone_type = 'Mob';
		else if fixed_flag = 1 then phone_type = 'Fix';
		else phone_type = 'None';	
	if most_common_crime = '' then most_common_crime = 'Other';
	if most_common_crime in ('Intimidation','Non-compliance','Non-disclosure','Sexual') then crime = 'Intimidation/Disclosure/Comply/Sexual';
		else if most_common_crime in ('Possession','Other','Overwork','Trespassing','Not Voting/Jury','Littering','Animal') then crime = 'P/O/O/T/NVJ/L/A';
		else if most_common_crime in ('Assault','Drug','Fee/Taxes','Fraud','Parking','Weapon Possession') then crime = 'A/D/FT/F/P/WP';
		else if most_common_crime in ('Traffic Offence','Pollution','Nuisance','Property Damage') then crime = 'Traffic Offence';
		else if most_common_crime in ('Theft','Public Transport','Bail','Drinking/Smoking','Resist') then crime = 'T/PT/B/DS/R';
		else crime = most_common_crime;
	if worst_crime in (3,4,5) then serious_crime = 1;
		else serious_crime = 0;
	if residential_state_name = '' then residential_state_name = postal_state_name;
	if residential_state_name = '' then residential_state_name = 'Mis';
	if residential_state_name in ('ACT','TAS','NT') then residential_state_name = 'ANT';
		else if residential_state_name in ('QLD','VIC') then residential_state_name = 'Q/V';
		else if residential_state_name in ('SA','WA') then residential_state_name = 'S/W';
		else if residential_state_name in ('Mis','NSW') then residential_state_name = 'N/M';
	state = residential_state_name;
	if employment_status = 'Unknown' then employment_status = 'Unemployed';
		else if employment_status = 'Self-employed' then employment_status = 'Employed';
	issue = intck('MONTH',first_issue_date,loaded_date,'C');
	issue_to_load = put(issue,f_issue.);
	offence = intck('MONTH',first_offence_date,loaded_date,'C');
	offence_to_load = put(offence,f_offence.);
	offence2 = intck('MONTH',first_offence_date,first_issue_date,'C');
	offence_to_issue = put(offence,f_offenceb.);
	if courts not in (0,.) then court_flag = 'Y';
		else court_flag = 'N';
	if penalty_notice not in (0,.) then penalty_flag = 'Y';
		else penalty_flag = 'N';
	if penalty_notice = . then penalty_notice = 0;
	if courts ge 1 and penalty_notice in (.,0) and electoral_office in (.,0) and historical in (.,0) then fine_type = 'Court';
		else if courts ge 1 and penalty_notice ge 1 then fine_type = 'Court + Penalty';
		else if penalty_notice ge 1 and electoral_office in (.,0) and historical in (.,0) and other_matter in (.,0) and sheriff_office in (.,0) then fine_type = 'Penalty';
		else fine_type = 'Other';
	penalty_count = put(penalty_notice,f_pen.);
	Loaded_Amount_Band = put(Loaded_amount,f_load.);
	Debtor_Age = put(Debtor_Age_At_Load,f_dtrage.);
	ER_Score = put(Economic_Resource_Decile,f_econ.);
	DA_Score = Relative_Disadvantage_Decile;
	drop residential_state_name bday;
run;


proc sort data=accts5 nodupkey;
	by Client_Ledger_Number Account_Number;
run;

/*Split Out of Time and In Time Data Sets*/

data jack.rnsw_in_time;
	set accts5 (where=(loaded_date between '01Jan2015'd and '30Jun2018'd));
run;

data jack.rnsw_out_time;
	set accts5 (where=(loaded_date between '01Jul2018'd and '31Dec2018'd));
run;

data jack.rnsw_out_time2;
	set accts5 (where=(loaded_date between '01Feb2019'd and '28Apr2019'd));
run;

/*Partitioning of dataset to train and validation datasets*/


proc surveyselect data=jack.rnsw_in_time out=pay_model seed=103662 samprate=0.70 outall method=srs noprint; run;
data jack.rnsw_model_train; set pay_model; where selected =1; run;
data jack.rnsw_model_validate; set pay_model; where selected =0; run;


