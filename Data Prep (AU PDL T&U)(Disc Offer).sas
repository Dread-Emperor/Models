libname JACK '\\BCSPARSAS01\sasdata\SAS Data\Jack\SASdata';
libname Veda '\\BCSPARSAS01\sasdata\SAS Data\Datawash\OutputFiles';

%let Start_Date= '01Jan2015'd;
%let End_Date= '31Mar2019'd;
%let Filter_Date= '31May2019'd;
%let Load_date = '01Jan2010'd;
%let test_ledgers = (0,2000001,2000003,2000004,2000005,1000014,2000049,2000466,2000412,2000217,2000756,2000687,2000803,2000996,2001270,2001271,
					4000152,9000104,9000113,9000114,9000131,9000134,9000147,9000148,6000056,6000073,6100236);

/*Get data from base table jack.discounts9*/


proc format library=Work;
	value f_bal   Low-500 = '1: <=500'
			   500.01-700 = '2: 500-700'
			  700.01-1000 = '3: 700-1000'
			 1000.01-1500 = '4: 1000-1500'
			 1500.01-2800 = '5: 1500-2800'
			 2800.01-4000 = '6: 2800-4000'
			 4000.01-5000 = '7: 4000-5000'
			5000.01-10000 = '8: 6000-10000'
			10000.01-high = '9: 10000+'
	    		    other = '9: 10000+';
	value f_econ    1-1 = '1: 1'
					2-3 = '2: 2-3'
					4-6 = '3: 4-6'
					7-9 = '4: 7-9'
				     10 = '5: 10'
	    		  other = '2: 2-3';
	value f_change low--0.5 = '1: <-50%'
				 -0.49--0.2 = '2: -50%--20%'
				 -0.2--0.01 = '3: -20%-0%'
				   		  0 = '4: 0%'
				  0.01-high = '5: 0%+'
				      Other = '5: 0%+';
	value f_disc    low-10 = '1: <10%'
				     11-20 = '2: 10%-20%'
				     21-30 = '3: 20%-30%'
					 31-40 = '4: 30%-40%'
				     41-60 = '5: 40%-60%'
				   61-high = '6: 60%+'
				     Other = '7: Missing';
	value f_drs     low--1 = '6: Missing'
				     0-400 = '2: 0-400'
				   401-500 = '3: 400-500'
				   501-550 = '4: 500-550'
				  551-high = '5: 550+'
				     Other = '6: Missing';
	value f_add    low--21 = '1: <-20%'
				   -20--11 = '2: -20%--10%'
					-10-10 = '3: -10%-10%'
				     11-30 = '4: 10%-30%'
				   31-high = '5: 30%+'
				     Other = '6: Missing';
	value f_tslp      0 = '1: <1 Mth'
					1-2 = '2: 1-3 Mths'
					3-5 = '3: 3-6 Mths'
				 6-high = '4: 6+ Mths'
	    		  other = '5: No Pay';
	value f_tslr      0 = '1: <1 Mth'
					1-2 = '2: 1-3 Mths'
					3-5 = '3: 3-6 Mths'
				   6-11 = '4: 6-12 Mths'
				  12-23 = '5: 1-2 Yrs'
				  24-35 = '6: 2-3 Yrs'
				36-high = '7: 3+ Yrs'
	    		  other = '8: No RPC';
	value f_tsl       0 = '1: <1 Mth'
					1-5 = '2: 1-6 Mths'
				   6-23 = '3: 6-24 Mths'
				  24-47 = '4: 2-4 Yrs'
				  48-59 = '5: 4-5 Yrs'
				60-high = '6: 5+ Yrs'
	    		  other = '7: ?';
	value f_numb    0-1 = '1: 0-1'
					  2 = '2: 2'
					  3 = '3: 3'
					  4 = '4: 4'
				 5-high = '5: 5+'
	    		  other = '7: Missing';
	value f_pay       0 = '1: 0'
					1-5 = '2: 1-5'
				   6-10 = '3: 6-10'
				  11-16 = '4: 11-16'
				17-high = '5: 17+'
	    		  other = '6: Missing';
run;

/*Need to include last rpc and number of previous rpc's*/

data discounts10;
	merge jack.discounts9 (in=t1 where=(classification = 'T&U')) drs6 (in=t2 drop=process_date rename=(drs=veda_score));
	by client_ledger_number account_number discount_date;
	if t1;
	format State $char10.;
	if paid_flag = . then paid_flag = 0;
	if phone_count = . then phone_count = 0;
	if mobile_flag = 1 and fixed_flag = 1 then phone_type = 'M + F';
		else if fixed_flag = 1 then phone_type = 'Fix';
		else if mobile_flag = 1 then phone_type = 'Mob';
		else phone_type = 'None';
	if campaign_medium  = '' then campaign_medium = 'Agent';
	if campaign  = '' then campaign = 'None';
	state = residential_state_name;
	if state in ('NSW','Mis','SA','NT','VIC') then state = 'NSV';
		else if state in ('QLD','ACT','WA') then state = 'QAW';
/*		else if state in ('QLD','Mis') then state = 'QM';*/
	new_balance = put(balance,f_bal.);
	if pay_count = . then pay_count = 0;
	payments = put(pay_count,f_pay.);
	if last_payment_date ne . then LP = intck('month',last_payment_date,discount_date,'c');
/*		else LP = 999;*/
	tslp = put(LP,f_tslp.);
	tslr = put(intck('month',call_date,discount_date,'c'),f_tslr.);
	tsl = put(intck('month',loaded_date,discount_date,'c'),f_tsl.);
	tsld = put(intck('month',last_discount_date,discount_date,'c'),f_tsl.);
	debt_change = put(round(balance_change,0.01),f_change.);
	disc = put(discount,f_disc.);
	prev_disc = put(previous_discount,f_numb.);
	last_discount = put(last_discount_rate,f_disc.);
	if add_discount = . then add_discount = discount - last_discount_rate;
	add_disc = put(add_discount,f_add.);
	if ctype = '' or ctype = 'Outbound' then ctype = 'None';
/*	if prod = 'Business Loan' then prod = 'Credit Card';*/
/*		else if prod = 'Transaction Acct' then prod = 'Personal Loan';*/
	if employment_status in ('Self-employed','Employed') then employment_status = 'Unknown';
/*	last_pay_amt = put(last_payment_amount,f_cash.);*/
/*	total_pay_amt = put(pay,f_cash.);*/
/*	Load_Age = put(Age_At_Load,f_dtrage.);*/
/*	Age_AD = put(Age_At_Disc,f_dtrage.);*/
	drs=put(veda_score,f_drs.);
	ER_Score = put(Economic_Resource_Decile,f_econ.);
	DA_Score = put(Relative_Disadvantage_Decile,f_econ.);
	ADV_Score = Relative_Advantage_Decile;
	OC_Score = Occupation_Decile;
	/*Reminders are automatically sent via email or SMS to all acounts that haven't settled or been put on an arrangement
	so don't include this variable. Extensions are just not statistically signficant*/
	/*Remove null values for random forest*/
/*	debt_change = round(balance_change,0.01);*/
/*	tslr = intck('month',call_date,discount_date,'c');*/
/*	tsl = intck('month',loaded_date,discount_date,'c');*/
/*	tsld = intck('month',last_discount_date,discount_date,'c');*/
/*	if discount = . then discount = 0;*/
/*	if add_discount = . then add_discount = 0;*/
/*	if reduced_settlement_amount = . then reduced_settlement_amount = -9999;*/
/*	if arrangement_flag = . then arrangement_flag = 0;*/
/*	if campaign_flag = . then campaign_flag = 0;*/
/*	if reminder_flag = . then reminder_flag = 0;*/
/*	if extension_flag = . then extension_flag = 0;*/
/*	if client_last_payment_amount = . then client_last_payment_amount = 0;*/
/*	if total_payment = . then total_payment = 0;*/
/*	if tslr = . then tslr = 999;*/
/*	if tsl = . then tsl = 999;*/
/*	if tsld = . then tsld = 999;*/
	/*Remove null values for random forest*/
	if campaign_medium = 'Letter' then letter = 1;
		else if campaign_medium = 'Agent' then agent = 1;
		else if campaign_medium = 'SMS' then sms = 1;
		else if campaign_medium = 'Email' then email = 1;
	drop residential_state_name;
run;

proc sort data=discounts10 nodupkey;
	by Client_Ledger_Number Account_Number discount_date;
run;

/*Show number of different types of discounts (email, SMS, etc.)*/
proc summary data=discounts10 nway missing;
	class Client_Ledger_Number Account_Number;
	var letter agent sms email;
	output out=types (drop=_TYPE_ _FREQ_) sum=;
run;

/*Get Last Discount Offered to increase response rate*/

proc sort data=discounts10;
	by Client_Ledger_Number Account_Number paid_flag discount_date;
run;

/*Include only last discount that occurred before 30th of Sept 2018 for in time data set. 
Make sure that Paid flag is 0 if another discount occurs after 'final' discount*/
data discounts10a;
	set discounts10;
	by Client_Ledger_Number Account_Number paid_flag discount_date;
	if not last.Account_Number then paid_flag = 0;
run;

proc sort data=discounts10a;
	by Client_Ledger_Number Account_Number discount_date;
run;

/*In Time*/
data discounts10b;
	set discounts10a (where=(discount_date between '01Jan2015'd and '30Sep2018'd));
	by Client_Ledger_Number Account_Number discount_date;
	if last.Account_Number then output;
run;

/*Out of Time*/
data discounts10c;
	set discounts10a (where=(discount_date between '01Oct2018'd and '31Mar2019'd));
	by Client_Ledger_Number Account_Number discount_date;
	if last.Account_Number then output;
run;

data discounts11;
	set discounts10b discounts10c;
run;

proc sort data=discounts11 nodupkey;
	by Client_Ledger_Number Account_Number discount_date;
run;

data discounts12;
	merge discounts11 (in=t1) types (in=t2);
	by Client_Ledger_Number Account_Number;
	if t1;
	if letter = . then letter = 0;
	if agent = . then agent = 0;
	if sms = . then sms = 0;
	if email = . then email = 0;
/*	if previous_discount in (0) then contact = 'single contact';*/
	if letter ne 0 and sms ne 0 and email ne 0 then contact = 'All 3';
		else if letter ne 0 and sms ne 0 then contact = 'LS';
		else if letter ne 0 and email ne 0 then contact = 'LE';
		else if sms ne 0 and email ne 0 then contact = 'SE';
		else if letter ne 0 then contact = 'L';
		else if sms ne 0 then contact = 'S';
		else if email ne 0 then contact = 'E';
		else contact = 'None';
run;

proc sort data=discounts12 nodupkey;
	by Client_Ledger_Number Account_Number;
run;
/*Split Out of Time and In Time Data Sets*/

data jack.discount_in_time_tu;
	set discounts12 (where=(discount_date between '01Jan2015'd and '30Sep2018'd));
run;

/*data random_forest;*/
/*	set jack.discount_in_time (keep=discount client_last_payment_amount economic_resource_decile gender lp loaded_amount occupation_decile reduced_settlement_amount relative_advantage_decile Relative_Disadvantage_Decile state add_discount age_at_disc age_at_load arrangement_flag balance balance_change campaign campaign_flag campaign_medium classification contact debt_change drivers_licence_flag email_flag extension_flag reminder_flag phone_count phone_type paid_flag postal_address_flag residential_address_flag previous_discount total_payment tsl tsld tslr);*/
/*run;*/

/*proc export data=random_forest*/
/*	OUTFILE= "\\bcssydfs01\shared\JZ\Adel\Discount Offer\AU PDL\R\In Time.csv"*/
/*	dbms=csv label REPLACE;*/
/*run;	*/

data jack.discount_out_time_tu;
	set discounts12 (where=(discount_date between '01Oct2018'd and '31Mar2019'd));
run;


/*Partitioning of dataset to train and validation datasets*/

proc surveyselect data=jack.discount_in_time_tu out=settle_model seed=103662 samprate=0.70 outall method=srs noprint; run;
data jack.discount_model_train_tu; set settle_model; where selected =1; run;
data jack.discount_model_validate_tu; set settle_model; where selected =0; run;