libname JACK '\\BCSAKLSAS01\userdata\SAS Data\Jack\SASdata';
/*libname Veda '\\BCSPARSAS01\sasdata\SAS Data\Datawash\OutputFiles';*/

%let Start_Date= '01Jan2015'd;
%let End_Date= '30Jun2019'd;
%let Filter_Date= '30Sep2019'd;
%let Load_date = '01Jan2010'd;

proc sql noprint;
	select distinct client_ledger_number into :test_ledgers separated by ',' from sdata.nz_portlist where portfolio = 'Delete';
quit;

/*Get Discount work requests*/

data discounts;
	set dmart.rm_account_event_fact (where=(event_class= 'O' and event_type_ID = 4251 and created_calendar_ID ge 20100101 and created_calendar_ID le 20190831 and client_ledger_number not in (&test_ledgers))
	keep=client_ledger_number account_number created_calendar_ID created_time_ID reduced_settlement_amount reduced_settlement_expires variable_data account_current_balance_amount event_class event_type_ID);
	format discount_date settlement_end_date ddmmyy10. discount_time time8.;
	discount_date = input(put(created_calendar_ID,8.),yymmdd10.);
	discount_time = input(put(created_time_ID,6.),hhmmss6.);
	settlement_end_date = input(put(reduced_settlement_expires,8.),yymmdd10.);
	drop created_calendar_ID created_time_ID event_type_ID event_class reduced_settlement_expires;
run;


/*Remove discounts that occur on the same day*/
proc sort data=discounts nodupkey;
	by client_ledger_number account_number discount_date discount_time;
run;

/*First and last discounts of the day are the same so it doesn't matter whether the first or last one is kept*/
proc sort data=discounts nodupkey equals;
	by client_ledger_number account_number discount_date;
run;


/*Letters, SMS' and Emails*/
/*801/107 events include both discount and non-discount SMS campaigns*/
proc sql;
	create table SMS as
	select a.client_ledger_number,a.account_number,a.created_calendar_id,a.created_time_id,a.account_current_balance_amount,b.transaction_type_code,b.sequence_number
	from dmart.rm_account_event_fact a left join dmart.event_type b
	on(a.event_type_ID = b.event_type_ID)
	where a.client_ledger_number not in (&test_ledgers) and a.event_class = 'F' and a.Work_request_status ne 'X' and a.created_calendar_id ge 20100101 and a.created_calendar_id le 20190831 and
	b.transaction_type_code = 801 and b.sequence_number in (115/*,107*/)
	order by a.client_ledger_number,a.account_number,a.created_calendar_id;
quit;

proc sql;
	create table letters as
	select a.client_ledger_number,a.account_number,a.created_calendar_id,a.created_time_id,a.account_current_balance_amount,b.transaction_type_code,b.sequence_number
	from dmart.rm_account_event_fact a left join dmart.event_type b
	on(a.event_type_ID = b.event_type_ID)
	where a.client_ledger_number not in (&test_ledgers) and a.event_class = 'L' and a.Work_request_status ne 'X' and a.created_calendar_id ge 20100101 and a.created_calendar_id le 20190831 and
	b.transaction_type_code = 55 and b.sequence_number in (23,40)
	order by a.client_ledger_number,a.account_number,a.created_calendar_id;
quit;

proc sort data=letters nodupkey equals;
	by client_ledger_number account_number created_calendar_id;
run;

data letters;
	set letters;
	format discount_date ddmmyy10.;
	discount_date = input(put(created_calendar_ID,8.),yymmdd10.);
run;

/*Differentiate between letter and email discounts*/
proc sql;
	create table letter_flag as
	select a.client_ledger_number,a.account_number,a.created_calendar_id,a.created_time_id,a.variable_data,b.transaction_type_code,b.sequence_number
	from dmart.rm_account_event_fact a left join dmart.event_type b
	on(a.event_type_ID = b.event_type_ID)
	where a.client_ledger_number not in (&test_ledgers) and a.event_class = 'X' and a.created_calendar_id ge 20160101 and a.created_calendar_id le 20190831 and
	b.transaction_type_code = 675 and b.sequence_number = 0
	order by a.client_ledger_number,a.account_number,a.created_calendar_id;
quit;

data letter_flag;
	set letter_flag (where=(find(variable_data,'55/23','i',1) or find(variable_data,'55/40','i',1) ));
	format cancel_date ddmmyy10.;
	email_fail_flag = 1;
	cancel_date = input(put(created_calendar_ID,8.),yymmdd10.);
	drop variable_data Created_time_ID;
run;

proc sql;
	create table letters2 as
	select a.*,b.*
	from letters a left join letter_flag b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date <= b.cancel_date and a.discount_date >= b.cancel_date - 7)
	order by a.client_ledger_number,a.account_number,a.discount_date;
quit;

proc sort data=SMS;
	by client_ledger_number account_number created_calendar_id created_time_id;
run;

proc sort data=SMS nodupkey equals;
	by client_ledger_number account_number created_calendar_id;
run;


proc sort data=letters2;
	by client_ledger_number account_number created_calendar_id created_time_id;
run;

proc sort data=letters2 nodupkey equals;
	by client_ledger_number account_number created_calendar_id;
run;

/*Need to do the merge in two steps as discount date will default to the first value for SMS*/
data campaign;
	set SMS letters2;
run;

data campaign;
	set campaign;
	format discount_date ddmmyy10. discount_time time8.;
	if discount_date = . then discount_date = input(put(created_calendar_ID,8.),yymmdd10.);
	discount_time = input(put(created_time_ID,6.),hhmmss6.);
	campaign_flag = 1;
	if transaction_type_code = 55 and email_fail_flag ne 1 then campaign_medium = 'Letter/Email';
		else if transaction_type_code = 55 then campaign_medium = 'Letter';
		else campaign_medium = 'SMS';
	drop transaction_type_code sequence_number created_calendar_ID created_time_ID account_current_balance_amount cancel_date email_fail_flag;
run;

/*Add in campaign discounts from event_fact*/

proc sql;
	create table discounts2 as
	select a.*,b.*
	from discounts a left join campaign b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date = b.discount_date)
	order by a.client_ledger_number,a.account_number,a.discount_date;
quit;

data discounts2b;
	format gday ddmmyy10.;
	set discounts2;
	/*yday used later for discount to debtor info merge*/
	gday = discount_date - 1;
	yday = input(put(gday,yymmddn8.),8.);
	if settlement_end_date = . then settlement_end_date = intnx('month',discount_date,1,'s');
	if campaign_medium = '' and campaign_flag ne 1 then campaign_medium = 'Agent';
	drop gday variable_data;
run;

proc sort data=discounts2b nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

/*Differentiate between Email and Letter discounts by checking whether a debtor had an email address at the time of discount*/

data debtors;
	set discounts2b (keep=yday discount_date client_ledger_number account_number);
run;

data debtors;
	merge debtors (in=t1) sdata.nz_account (keep=client_ledger_number account_number debtor_number);
	by client_ledger_number account_number;
	if t1;
run;

proc sql;
	create table debtor_email as
	select a.*,b.email_address ,b.dimension_record_valid_from_date ,b.dimension_record_valid_to_date,b.dimension_record_latest_version_
	from debtors a left join dmart.rm_debtor b
	on(a.debtor_number = b.debtor_number and a.yday ge b.dimension_record_valid_from_date and (a.yday le b.dimension_record_valid_to_date or dimension_record_valid_to_date = 0))
	order by a.client_ledger_number,a.account_number,a.discount_date,b.dimension_record_valid_from_date;
quit;

/*Get rid of records that appear to be open but have actually been closed and replaced with new records*/
data debtor_email;
	set debtor_email;
	if dimension_record_valid_to_date = 0 and dimension_record_latest_version_ = 0 then delete;
	if dimension_record_valid_to_date = 0 then dimension_record_valid_to_date = 40000101;
	;
run;

/*Use equals to make sure records remain in the same relative order to get the first record in the day*/
proc sort data=debtor_email equals;
	by client_ledger_number account_number debtor_number discount_date dimension_record_valid_from_date dimension_record_valid_to_date;
run;

proc sort data=debtor_email nodupkey equals;
	by client_ledger_number account_number debtor_number discount_date;
run;



/*Get the first record when there are multiple debtor records in a day*/
proc sort data=debtor_email nodupkey equals;
	by debtor_number discount_date client_ledger_number account_number;
run;

proc sort data=debtor_email;
	by client_ledger_number account_number discount_date debtor_number;
run;

/*Apparently discount emails weren't used prior to July 2017. Every account with an email address will be emailed (if there's no 675 WR)*/
data discounts2c;
	merge discounts2b (in=t1) debtor_email (in=t2 keep=client_ledger_number account_number discount_date email_address);
	by client_ledger_number account_number discount_date;
	if t1;
	if campaign_medium = 'Letter/Email' and email_address ne '' and discount_date ge '01Jul2017'd then campaign_medium = 'Email';
		else if campaign_medium = 'Letter/Email' then campaign_medium = 'Letter'; 
	if email_address ne '' then email_flag = 1;
	drop email_address;
run;

proc summary data=discounts2c nway missing;
	class campaign_medium;
	output out=q;
run;




/*Remove agent-initiated discounts that occur within the discount period of a strategy discount as debtors are simply calling in*/

data ver1;
	set discounts2b (where=(campaign_medium = 'Agent') keep=client_ledger_number account_number discount_date campaign_medium account_current_balance_amount reduced_settlement_amount);
	rename client_ledger_number = cln account_number = an discount_date = inbound_date account_current_balance_amount = inbound_bal reduced_settlement_amount = inbound_settle;
	drop campaign_medium;
run;

data ver2;
	set discounts2b (where=(campaign_medium ne 'Agent'));
run;

proc sql;
	create table ver3 as
	select a.*,b.*
	from ver2 a inner join ver1 b
	on(a.client_ledger_number = b.cln and a.account_number = b.an and b.inbound_date ge a.discount_date and b.inbound_date le a.settlement_end_date)
	order by a.client_ledger_number,a.account_number,a.discount_date;
quit;

data ver4;
	set ver3 (keep=client_ledger_number account_number discount_date inbound_bal inbound_settle inbound_date);
	debtor_contact_flag = 1;
run;

proc sort data=ver4 nodupkey;
	by client_ledger_number account_number discount_date;
run;

proc sort data=ver3 nodupkey;
	by client_ledger_number account_number inbound_date;
run;

data discounts3;
	merge discounts2b (in=t1) ver3 (in=t2 keep=client_ledger_number account_number inbound_date rename=(inbound_date=discount_date)) ver4 (in=t3);
	by client_ledger_number account_number discount_date;
	if t1 and not t2;
run;

/*Many of these duplicates are the same discount being resent (maybe the first attempt failed)*/
proc sort data=discounts3 nodupkey;
	by client_ledger_number account_number discount_date;
run;



/*Getting the RPC Rate for each Loaded Month - Start*/

*** look at the telephone over life of the accounts ***;
/*proc sql; */
/*  connect to odbc(&ODBC_DataMart_B);*/
/*  create table vcalls as select * from connection to odbc( */
/*	select client_ledger_number,*/
/*           account_number,*/
/*	       created_calendar_id,*/
/*		   Result_Code,*/
/*		   event_type_id,*/
/*		   event_class*/
/*    from rm_account_event_fact */
/*    where event_class = 'V' and country_code = 'B' and created_calendar_id >= 20100101*/
/*	order by client_ledger_number, account_number*/
/*  ); */
/*  disconnect from odbc; */
/*quit;*/

/*proc sql; */
/*  connect to odbc(UID=sasadm pwd={sas001}JGFzYWRtUFcx dsn=datamart);*/
/*  create table frequest as select * from connection to odbc( */
/*	select client_ledger_number,*/
/*           account_number,*/
/*		   created_calendar_id,*/
/*	       work_request_status, */
/*           work_request_completed,*/
/*		   attempts_number,*/
/*		   Result_Code,*/
/*		   status_ind,*/
/*		   event_type_id,*/
/*		   event_class*/
/*    from rm_account_event_fact */
/*    where event_class = 'F' and country_code = 'B' and created_calendar_id >= 20100101*/
/*	order by client_ledger_number, account_number*/
/*  ); */
/*  disconnect from odbc; */
/*quit;*/

/*data Calls;*/
/*  set vcalls(in=in1 rename=(created_calendar_id=work_request_completed)) frequest(in=in2);*/
/*  length ctype $8.;*/
/*  if in1 then ctype = 'Base';*/
/*  if in2 then ctype = 'Outbound';*/
/*  if upcase(work_request_status) = 'X' then delete;*/
/*  if client_ledger_number = . then delete;*/
/*  if work_request_completed = 0 then delete;*/
/*run;*/
/*proc sort; by event_type_id; run;*/


/*data calls;*/
/*    merge calls(in=in1) sdata.scard_event_nz(in=in2);*/
/*    by event_type_id;*/
/*    drop created_calendar_id work_request_completed;*/
/*    format call_date ddmmyy10. call_type $8.;*/
/*    if event_class = 'V' and transaction_type_code not in (988,989,992,993) then delete;*/
/*    if ctype = 'Outbound' and transaction_type_code not in (747,748,750,751,752,753,754,757,780,781,815,830,852,862,868) then delete;*/
/*    call_date = input(put(work_request_completed, z8.), yymmdd8.);*/
/*	call=1; rpc=0; */
/*	if transaction_type_code in (988,747,748,750,751,752,753,754,780,781,815,830,852,862,868) and result_code in (8,9,10,11,12,13,15,16,17,29,49,60,106) then rpc=1;*/
/*	if transaction_type_code in (757) and result_code in (7,8,9,10,11,12,14,19,23,24,25) then rpc=1;*/
/*    if transaction_type_code = 989 and result_code in (4,5,6,7,8,9,10,11,12,14,60) then rpc=1;*/
/*    if transaction_type_code = 992 and result_code in (1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,19,20,21,22,23,30,40,42,43,44,46,60) then rpc=1;*/
/*    if transaction_type_code = 993 and result_code in (1,2,3,4,5,6,7,8,9,10,11,13,15,16,17,21,22,31,32,34,35,60) then rpc=1;*/
/*	if event_class = 'V' and transaction_type_code in (992,993) then call_type = 'Inbound';*/
/*	if event_class = 'V' and transaction_type_code in (988,989) then call_type = 'Base';*/
/*	if event_class = 'F' then call_type = 'Outbound';*/
/*	if in1 and in2 then output;*/
/*run;*/
/*proc sort; by client_ledger_number account_number call_date rpc; run;*/

/*data jack.discount_rpc;*/
/*	set calls (where=(rpc = 1) keep=client_ledger_number account_number call_date call_type rpc);*/
/*run;*/

proc sql;
	create table latest_rpc as
	select a.client_ledger_number,a.account_number,a.discount_date,b.call_date,b.call_type
	from discounts3 a left join jack.discount_rpc b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date > b.call_date)
	order by a.client_ledger_number,a.account_number,a.discount_date,b.call_date;
quit;

data latest_rpc;
	set latest_rpc (where=(call_date ne .));
	by client_ledger_number account_number discount_date call_date;
	if last.discount_date then output;
run;

/*Adjust current balance using payments and charges to change to balance as at beginning of day*/
/*Note account current balance is at end of day not beginning of day*/
data charge_balance_adjustment;
	merge discounts2b (in=t1 keep=client_ledger_number account_number discount_date rename=(discount_date = charge_date)) sdata.nz_charges (in=t2 where=(charge_date ge &Start_Date) keep=client_ledger_number account_number charge_date charge_amount);
	by client_ledger_number account_number charge_date;
	if t1 and t2;
	rename charge_date = discount_date;
run;

proc summary data=charge_balance_adjustment nway missing;
	class client_ledger_number account_number discount_date;
	var charge_amount;
	output out=charge_balance_adjustment2 (drop=_TYPE_ _FREQ_) sum=charge_adjustment;
run;

data payment_balance_adjustment;
	merge discounts2b (in=t1 keep=client_ledger_number account_number discount_date rename=(discount_date = payment_date)) sdata.nz_payments (in=t2 where=(payment_date ge &Start_Date) keep=client_ledger_number account_number payment_date payment_amount);
	by client_ledger_number account_number payment_date;
	if t1 and t2;
	rename payment_date = discount_date;
run;

proc summary data=payment_balance_adjustment nway missing;
	class client_ledger_number account_number discount_date;
	var payment_amount;
	output out=payment_balance_adjustment2 (drop=_TYPE_ _FREQ_) sum=payment_adjustment;
run;

data discounts4;
	merge discounts3 (in=t1) charge_balance_adjustment2 (in=t2) payment_balance_adjustment2 (in=t3) latest_rpc (in=t4);
	by client_ledger_number account_number discount_date;
	if t1;
		balance = sum(account_current_balance_amount,payment_adjustment,-charge_adjustment); 
		discount = round(1-(reduced_settlement_amount/balance),0.01)*100; 
	drop account_current_balance_amount payment_adjustment charge_adjustment;
run;

proc sort data=discounts4 nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

/*Adjust balance of debtors calling in*/
proc sort data=discounts4;
	by client_ledger_number account_number inbound_date;
run;

data discounts4a;
	merge discounts4 (in=t1) charge_balance_adjustment2 (in=t2 rename=(discount_date=inbound_date)) payment_balance_adjustment2 (in=t3 rename=(discount_date=inbound_date));
	by client_ledger_number account_number inbound_date;
	if t1;
	if inbound_bal ne . then inbound_balance = sum(inbound_bal,payment_adjustment,-charge_adjustment);
	drop inbound_bal payment_adjustment charge_adjustment;
run;

proc sort data=discounts4a nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

/*Get discounts for extra and special*/
/*data extra_discount (keep=client_ledger_number account_number discount_date discount2);*/
/*	set discounts4a (where=(campaign_medium ne 'Agent'));*/
/*	lag_nd = lag(discount);*/
/*	lag2_nd = lag2(discount);*/
/*	lag3_nd = lag3(discount);*/
/*	lag4_nd = lag4(discount);*/
/*	lag5_nd = lag5(discount);*/
/*	if find(narrative,'Settlement on file','i',1) ge 1 and client_ledger_number = lag(client_ledger_number) and account_number = lag(account_number) then discount = lag_nd;*/
/*	if find(narrative,'Settlement on file','i',1) ge 1 and client_ledger_number = lag2(client_ledger_number) and account_number = lag2(account_number) and discount = . then discount = lag2_nd;*/
/*	if find(narrative,'Settlement on file','i',1) ge 1 and client_ledger_number = lag3(client_ledger_number) and account_number = lag3(account_number) and discount = . then discount = lag3_nd;*/
/*	if find(narrative,'Settlement on file','i',1) ge 1 and client_ledger_number = lag4(client_ledger_number) and account_number = lag4(account_number) and discount = . then discount = lag4_nd;*/
/*	if find(narrative,'Settlement on file','i',1) ge 1 and client_ledger_number = lag5(client_ledger_number) and account_number = lag5(account_number) and discount = . then discount = lag5_nd;*/
/*	if ((campaign = 'Extra' and discount = .) or (campaign = 'Special' and find(narrative,'extra','i',1) ge 1)) and client_ledger_number = lag(client_ledger_number) and account_number = lag(account_number) then discount = lag_nd + add_discount;*/
/*	if ((campaign = 'Extra' and discount = .) or (campaign = 'Special' and find(narrative,'extra','i',1) ge 1)) and client_ledger_number = lag2(client_ledger_number) and account_number = lag2(account_number) then discount = lag2_nd + add_discount;*/
/*	if ((campaign = 'Extra' and discount = .) or (campaign = 'Special' and find(narrative,'extra','i',1) ge 1)) and client_ledger_number = lag3(client_ledger_number) and account_number = lag3(account_number) then discount = lag3_nd + add_discount;*/
/*	if ((campaign = 'Extra' and discount = .) or (campaign = 'Special' and find(narrative,'extra','i',1) ge 1)) and client_ledger_number = lag4(client_ledger_number) and account_number = lag4(account_number) then discount = lag4_nd + add_discount;*/
/*	if ((campaign = 'Extra' and discount = .) or (campaign = 'Special' and find(narrative,'extra','i',1) ge 1)) and client_ledger_number = lag5(client_ledger_number) and account_number = lag5(account_number) then discount = lag5_nd + add_discount;*/
/*	discount2 = discount;*/
/*	if campaign in ('Extra','Special') or find(narrative,'Settlement on file','i',1) ge 1 then output;*/
/*run;*/

/*data discounts4b;*/
/*	merge discounts4a (in=t1) extra_discount (in=t2);*/
/*	by client_ledger_number account_number discount_date;*/
/*	if t1;*/
/*	if discount = . then discount = discount2;*/
/*	if balance = . and reduced_settlement_amount ne . and discount ne . then balance = reduced_settlement_amount/(discount/100);*/
/*	if campaign_medium = 'Letter' then letter = 1;*/
/*		else if campaign_medium = 'Agent' then agent = 1;*/
/*		else if campaign_medium = 'SMS' then sms = 1;*/
/*		else if campaign_medium = 'Email' then email = 1;*/
/*	if last.account_number then final_discount = 1;*/
/*	drop discount2;*/
/*run;*/

data discounts4b;
	merge discounts4a;
	by client_ledger_number account_number discount_date;
	if last.account_number then final_discount = 1;
run;

proc sort data=discounts4b nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

/*Show number of different types of discounts (email, SMS, etc.)*/
proc summary data=discounts4b (where=(final_discount ne 1)) nway missing;
	class Client_Ledger_Number Account_Number;
	var letter agent sms email;
	output out=types (drop=_TYPE_ _FREQ_) sum=;
run;


/*Get number of previous discounts + previous discount date*/
/*Try to get last discount rate*/
data discounts5;
	set discounts4b;
	format last_discount_date ddmmyy10.;
	by client_ledger_number account_number discount_date;
	if first.account_number then previous_discount = -1;
		previous_discount + 1;
	last_discount_date = lag(discount_date);
	last_discount_rate = lag(discount);
	if previous_discount in (.,0) then last_discount_date = .;
	/*	previous_discount = ^(first.account_number and last.account_number);*/
	/*	if previous_discount = 1 and first.account_number then previous_discount = 0;*/
	if discount_date < &Start_Date then delete;
run;

proc sort data=discounts5 nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

/*Start account selection (PDL/Cont, B&F/T&U etc.*/

data discounts6;
	merge discounts5 (in=t1) sdata.nz_account (in=t2 where=(company_type = 'Cont' and next_expected_event_type not in (931,932,933,934,935,956,963,969) and loaded_date ge &Load_date and loaded_date le &End_Date and client_ledger_number not in (&test_ledgers)) keep=client_ledger_number account_number debtor_number next_expected_event_type loaded_date loaded_amount debt_from_date debt_to_date debtors_linked client_last_payment_date client_last_payment_amount company_type product prod_desc port_desc);
	by client_ledger_number account_number;
	/*Get only PDL accounts*/
	if t1 and t2;
	format settlement_window ddmmyy10.;
	settlement_window = intnx('month',discount_date,3,'s');
	balance_change = (balance/loaded_amount)-1;
/*	if product in ('Telco','Utility') or port_desc = 'Sensis' then classification = 'T&U';*/
/*		else classification = 'B&F';*/
/*	if product = 'Banking' and prod_desc ne 'Business' then prod = 'Transaction Acct';*/
/*		else if product = 'Banking' then prod = 'Business Loan';*/
/*		else if product = 'Flexi Loan' then prod = 'Personal Loan';*/
/*		else if product = 'Finance' and port_desc not in ('Lombard','Lombard FF','Lombard Inv','MSA - PDL') then prod = 'Rental';*/
/*		else if product = 'Finance' then prod = 'Credit Card';*/
/*		else prod = product;*/
/*	if classification = 'T&U' then do;*/
/*		if prod_desc not in ('','Business','Kenan')  then prod = prod_desc;*/
/*			else prod = product;*/
/*	end;*/
	drop company_type product prod_desc port_desc;
run;

proc sort data=discounts6 nodupkey;
	by client_ledger_number account_number discount_date;
run;

/*proc summary data=discounts6 (where=(classification = 'T&U')) nway missing;*/
/*	class product prod_desc;*/
/*	output out=q;*/
/*run;*/

/*Get payments - Start*/

proc sort data=discounts6 (keep=client_ledger_number account_number loaded_date next_expected_event_type) nodupkey out=discount_accts;
	by client_ledger_number account_number;
run;

proc sql;
create table payment_data as
select * from
(
select 
a.client_ledger_number as client_ledger_number
,a.account_number as account_number
,a.loaded_date as loaded_date
,a.next_expected_event_type as next_expected_event_type
,b.payment_month as payment_month
,b.payment_date as payment_date
,b.payment_amount as payment_amount
,b.conv_pay_flag as conv_pay_flag
,b.reversal as reversal
from 
(select client_ledger_number,account_number,loaded_date,next_expected_event_type
from discount_accts
/*-- Filter for current forecast cut-off date --*/
where 
/*-- Filter Out records that do have a PDL Number --*/
next_expected_event_type not in (931,932,933,934,935,956,963,969)
) as a left join sdata.nz_payments as b
on a.client_ledger_number=b.client_ledger_number and
a.account_number=b.account_number and
/*-- Filter for duration from the last forecast cutoff date and present forecast cutoff date --*/
b.payment_date ge &Load_date and 
b.payment_date le &Filter_Date 
) as table1
where payment_month is not null and payment_amount >= 1 and reversal = 0 
;
quit;

/*Get first payment in discount window*/

proc sql;
	create table payment as
	select a.client_ledger_number,a.account_number,a.discount_date,a.settlement_end_date,b.payment_date,b.payment_amount
	from discounts6 a left join payment_data b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date le b.payment_date and a.settlement_end_date ge b.payment_date)
	order by a.client_ledger_number,a.account_number,a.discount_date,b.payment_date;
quit;

proc sort data=payment nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

/*Get payments before discount offer*/

proc sql;
	create table payments_before_discount as
	select a.client_ledger_number,a.account_number,a.discount_date,b.payment_date,b.payment_amount
	from discounts6 a left join payment_data b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date > b.payment_date)
	order by a.client_ledger_number,a.account_number,a.discount_date,b.payment_date;
quit;

proc summary data=payments_before_discount (where=(payment_date ne .)) nway missing;
	class client_ledger_number account_number discount_date;
	var payment_amount;
	output out=payment_summary (drop=_TYPE_ rename=(_FREQ_=pay_count)) sum=total_payment;
run;

data last_payment;
	set payments_before_discount (where=(payment_date ne .));
	by client_ledger_number account_number discount_date payment_date;
	if last.discount_date then output;
	rename payment_date = last_payment_date;
run;

/*Get payments - End*/

/*Get Settlements after discount - Start*/
/*Need to verify Event type id for settlements*/

data settlements;
	set dmart.rm_account_event_fact (where=(event_class= 'E' and event_type_ID in (7358,7362,/*7368,*/7372,7375) and created_calendar_ID ge 20150101 and created_calendar_ID le 20190831 and client_ledger_number not in (&test_ledgers))
	keep=client_ledger_number account_number created_calendar_ID account_current_balance_amount event_class event_type_ID);
	format paid_date ddmmyy10.;
	paid_date = input(put(created_calendar_ID,8.),yymmdd10.);
	rename account_current_balance_amount = final_balance;
	drop event_class created_calendar_ID;
run;

/*Get first settlement for each account*/
proc sort data=settlements;
	by client_ledger_number account_number paid_date;
run;

proc sort data=settlements nodupkey equals;
	by client_ledger_number account_number;
run;

proc sql;
	create table settlements2 as
	select a.client_ledger_number,a.account_number,a.discount_date,b.paid_date,b.final_balance,b.event_type_ID
	from discounts6 a left join settlements b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date le b.paid_date and b.paid_date le a.settlement_window)
	order by a.client_ledger_number,a.account_number,a.discount_date,b.paid_date;
quit;

/*Get Settlements after discount - End*/

data discounts7;
	merge discounts6 (in=t1) payment (in=t2 keep=client_ledger_number account_number discount_date payment_date payment_amount) settlements2 (in=t3) payment_summary (in=t4) last_payment (in=t5);
	by client_ledger_number account_number discount_date;
	if t1;
	if payment_date ne . then pay_flag = 1;
	if paid_date ne . then paid_flag = 1;
	drop event_type_id;
run;

proc sort data=discounts7 nodupkey;
	by client_ledger_number account_number discount_date;
run;

/*Check which accounts have paid in the window and which ones have paid off their debt within 3 months*/
/*proc summary data= discounts7 nway missing;*/
/*	class pay_flag paid_flag;*/
/*	output out=q;*/
/*run;*/







/*Get debtor record as at discount*/
/*Need to get unmasked first name and date of birth from a NZ account*/

data debtors;
	set discounts7 (keep=debtor_number yday discount_date client_ledger_number account_number where=(debtor_number ne .));
run;

proc sql;
	create table debtor_info as
	select a.*,b.dimension_record_latest_version_,b.debtor_first_name ,b.debtor_middle_name ,b.debtor_family_name ,b.debtor_title_name ,b.gender ,b.debtor_type_code ,b.birth_date ,b.drivers_licence ,b.email_address ,b.occupation_name ,b.employer_name ,b.home_phone_number ,b.home_mobile_number ,b.work_phone_number ,b.postal_post_code ,b.postal_street_name ,b.postal_city_name ,b.postal_suburb_name ,b.residential_post_code ,b.residential_street_name ,b.residential_city_name ,b.residential_suburb_name ,b.dimension_record_valid_from_date ,b.dimension_record_valid_to_date
	from debtors a left join dmart.rm_debtor b
	on(a.debtor_number = b.debtor_number and a.yday ge b.dimension_record_valid_from_date and (a.yday le b.dimension_record_valid_to_date or dimension_record_valid_to_date = 0))
	order by a.client_ledger_number,a.account_number,a.discount_date,b.dimension_record_valid_from_date;
quit;

/*Get rid of records that appear to be open but have actually been closed and replaced with new records*/
data debtor_info;
	set debtor_info;
	if dimension_record_valid_to_date = 0 and dimension_record_latest_version_ = 0 then delete;
	if dimension_record_valid_to_date = 0 then dimension_record_valid_to_date = 40000101;
	;
run;

/*Use equals to make sure records remain in the same relative order to get the first record in the day*/
proc sort data=debtor_info equals;
	by client_ledger_number account_number debtor_number discount_date dimension_record_valid_from_date dimension_record_valid_to_date;
run;

proc sort data=debtor_info nodupkey equals;
	by client_ledger_number account_number debtor_number discount_date;
run;




/*Get the first record when there are multiple debtor records in a day*/
proc sort data=debtor_info nodupkey equals;
	by debtor_number discount_date client_ledger_number account_number;
run;

proc sort data=debtor_info;
	by client_ledger_number account_number discount_date debtor_number;
run;

proc sort data=discounts7;
	by client_ledger_number account_number discount_date debtor_number;
run;

data discounts8;
	merge discounts7 (in=t1) debtor_info (in=t2);
	by client_ledger_number account_number discount_date debtor_number;
	if t1;
	format dob ddmmyy10. employment_status $char20.;
	phone_count=0;
	if find(employer_name, 'VOA','i',1) or find(employer_name, 'LOA','i',1) then employer_name = '';
	if find(employer_name, 'CENTRE', 'i',1) ge 1 or find(employer_name, 'CLINK', 'i',1) ge 1 or find(employer_name, 'DISABILITY', 'i',1) ge 1 or find(employer_name, 'PENSION', 'i',1) ge 1 or find(employer_name, 'not work', 'i',1) ge 1 or find(employer_name, 'student', 'i',1) ge 1 or find(employer_name, 'allow', 'i',1) ge 1 or find(employer_name, 'unemp', 'i',1) ge 1 then employment_status='Unemployed';
		else if employer_name = '' then employment_status = 'Unknown';
		else if find(employer_name, 'empl', 'i',1) ge 1 and find(employer_name, 'self', 'i',1) ge 1 then employment_status = 'Self-employed';
		else employment_status = 'Employed';
	if find(occupation_name,'UNEMP','i',1) or find(occupation_name,'retire','i',1) or find(occupation_name,'pension','i',1) or occupation_name in ('U/E','HOME DUTIES') or (find(occupation_name,'link','i',1) and (find(occupation_name,'C-','i',1) or find(occupation_name,'C/','i',1) or find(occupation_name,'Cent','i',1) or find(occupation_name,'clink','i',1))) then employment_status = 'Unemployed';
		else if employer_name = '' and find(occupation_name,'self','i',1) then employment_status = 'Self-employed';
		else if employer_name = '' and (find(occupation_name,'student','i',1) or find(occupation_name,'study','i',1)) then employment_status = 'Unemployed';
	if Home_mobile_number not in (.,0) then phone_count=phone_count+1;
	if Home_phone_number not in (.,0) then phone_count=phone_count+1;
	if Work_phone_number not in (.,0) then phone_count=phone_count+1;
	mobile_flag=0; fixed_flag=0;
	if phone_count > 0 then do;
		if Home_phone_number ne "" then do; if substr(Home_phone_number,1,2) = '02' or substr(Home_phone_number,1,1) = '2' then mobile_flag=1; else fixed_flag=1; end;
		if Home_mobile_number ne "" then do; if substr(Home_mobile_number,1,2) = '02' or substr(Home_mobile_number,1,1) = '2' then mobile_flag=1; else fixed_flag=1; end;
		if Work_phone_number ne "" then do; if substr(Work_phone_number,1,2) = '02' or substr(Work_phone_number,1,1) = '2' then mobile_flag=1; else fixed_flag=1; end;
	end;
	if debtor_type_code = "C" then gender = "C";
	if gender = "" and debtor_title_name ne "" then do;
		if debtor_title_name = 'MR' then gender='M';
		else if debtor_title_name in ('MRS','MS','MIS') then gender='F';
		else gender='?';
	end;
	/*Splitting email/letter*/
/*	if campaign_medium = 'Email/Letter' and email_flag = 'Y' then campaign_medium = 'Email';*/
/*		else if campaign_medium = 'Email/Letter' and email_flag = 'N' then campaign_medium = 'Letter';*/
	if gender in ("","U") then gender='?';
	if drivers_licence ne "" then drivers_licence_flag = "Y";
		else drivers_licence_flag = "N";
	if email_address ne "" then email_flag = "Y";
		else email_flag = "N";
	/*Birth Date*/
	dob =input(put(birth_date,best8.),yymmdd8.);
	age_at_load = intck('year',dob,loaded_date,'c');
	age_at_disc = intck('year',dob,discount_date,'c');
	/*Address Flag*/
	if residential_street_name ne "" and (residential_city_name ne "" or residential_suburb_name ne "") then residential_address_flag = "Y";
		else residential_address_flag = "N";
	if postal_street_name ne "" and (postal_city_name ne "" or postal_suburb_name ne "") then postal_address_flag = "Y";
		else postal_address_flag = "N";
	if residential_post_code not in (.,0) then post_code = residential_post_code;
		else if postal_post_code not in (.,0) then post_code = postal_post_code;
	debtor_first_name=UPCASE(debtor_first_name);
	debtor_middle_name=UPCASE(debtor_middle_name);
	rename debtor_first_name=first_name debtor_middle_name=middle_name;
	drop employer_name occupation_name dob event_type_id yday dimension_record_latest_version_ dimension_record_valid_from_date dimension_record_valid_to_date birth_date email_address Home_mobile_number Home_phone_number Work_phone_number debtor_title_name drivers_licence postal_street_name postal_city_name postal_suburb_name postal_state_name residential_post_code postal_post_code residential_street_name residential_city_name residential_suburb_name;
run; 


/*proc summary data=discounts8 nway missing;*/
/*	class residential_state_name;*/
/*	output out=q;*/
/*run;*/

proc sort data=discounts8 nodupkey equals;
	by client_ledger_number account_number discount_date;
run;




/*Gender Generator - Start*/

proc sort data=discounts8;
	by First_Name;
run;

data gender;
	set sdata.gender;
		First_Name=upcase(Debtor_Name);
		SData_Gender1=Gender;
	keep First_Name SData_Gender1;	
run;

data rr;
	merge discounts8 (in=t1) gender (in=t2);
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
run;

/*Gender Generator - End*/

/*Deprivation Scores (2013 version)*/

proc sort data=ss;
	by post_code;
run;

proc sort data=jack.post_code_scores (rename=(postcode=post_code));
	by post_code;
run;

data jack.discounts9;
	merge ss (in=t1) jack.post_code_scores (in=t2 drop=deprivation_score);
	by post_code;
	if t1;
	if deprivation_score2 = . then deprivation_score2 = 0; 
	drop first_name middle_name family_name post_code;
run;

proc sort data=jack.discounts9 nodupkey equals;
	by Client_Ledger_Number Account_Number discount_date;
run;

proc summary data=jack.discounts9 nway missing;
	class pay_flag paid_flag;
	output out=q;
run;

/*Get DRS Score - Start*/

/*data drs;*/
/*	set veda.au_eqfx_amsnew_combined (keep=Client_Ledger_Number Account_Number debtor_number process_date score_consumer_negative_1) veda.au_eqfx_amsold_combined (keep=Client_Ledger_Number Account_Number debtor_number process_date score_consumer_negative_1) */
/*	veda.au_eqfx_mthnew_combined (keep=Client_Ledger_Number Account_Number debtor_number process_date score_consumer_negative_1) veda.au_eqfx_mthold_combined (keep=Client_Ledger_Number Account_Number debtor_number process_date DRS rename=(DRS=score_consumer_negative_1));*/
/*	if score_consumer_negative_1 = . then delete;*/
/*	rename score_consumer_negative_1 = drs;*/
/*run;*/

/*proc sort data=drs;*/
/*	by Client_Ledger_Number Account_Number;*/
/*run;*/

/*proc sort data=jack.discounts9 (keep=Client_Ledger_Number Account_Number) nodupkey out=drs_list;*/
/*	by Client_Ledger_Number Account_Number;*/
/*run;*/

/*data drs2;*/
/*	merge drs_list (in=t1) drs (in=t2);*/
/*	by Client_Ledger_Number Account_Number;*/
/*	if t1 and t2;*/
/*run;*/

/*proc sql noprint;*/
/*	select distinct client_ledger_number into :varlist_ledger separated by ','*/
/*	from drs;*/
/*quit;*/

/*proc sql;*/
/*	create table account_debtor_01 as select client_ledger_number, account_number, Debtor_Number, Debtor_Relation_Type_Code*/
/*	from dmart.rm_account_debtor*/
/*	where client_ledger_number in (&varlist_ledger.) and dimension_record_latest_version_=1 and debtor_relation_type_code in ('', 'O', 'R');*/
/*quit;*/

/*proc sort data=drs2; by client_ledger_number account_number debtor_number; run;*/
/*proc sort data=account_debtor_01; by client_ledger_number account_number debtor_number; run;*/

/*data drs3;*/
/*	merge drs2 (in=t1) account_debtor_01 (in=t2);*/
/*	by client_ledger_number account_number debtor_number;*/
/*	if t1;*/
/*run;*/


/*proc sort data=drs3 nodupkey out=primary_debtor_list; by client_ledger_number account_number debtor_number; run;*/

/* Output first record - this should always pull in order of Primary->Guarantor->Related since file has been sorted */
/*proc sort data=primary_debtor_list;*/
/*	by client_ledger_number account_number debtor_relation_type_code;*/
/*run;*/

/*data primary_debtor_list;*/
/*	set primary_debtor_list;*/
/*	by client_ledger_number account_number;*/
/*	if first.account_number then output;*/
/*	drop debtor_relation_type_code process_date drs;	*/
/*run;*/

/*data drs4;*/
/*	merge primary_debtor_list (in=t1) drs3 (in=t2);*/
/*	by client_ledger_number account_number debtor_number;*/
/*	if t1;*/
/*run;*/

/*proc sql;*/
/*	create table drs5 as*/
/*	select a.client_ledger_number,a.account_number,a.discount_date,b.drs,b.process_date*/
/*	from jack.discounts9 a left join drs4 b*/
/*	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.discount_date > b.process_date)*/
/*	where b.drs ne .*/
/*	order by a.client_ledger_number,a.account_number,a.discount_date,b.process_date;*/
/*quit;*/

/*Get latest DRS score for each discount*/
/*data drs6;*/
/*	set drs5;*/
/*	by client_ledger_number account_number discount_date process_date;*/
/*	if last.discount_date then output;*/
/*run;*/


/*Get DRS Score - End*/

proc format library=Work;
	value f_dtrage Low-18 = '1: <=18' 
					19-26 = '2: 19-26'
					27-28 = '3: 27-28'
					29-40 = '4: 29-40'
	    		    41-46 = '5: 41-46'
					47-51 = '6: 47-51'
				    52-53 = '7: 52-53'
				    54-65 = '8: 54-65'
				  66-high = '9: 66+'
	    		    other = '10: Missing';
	value f_bal   Low-300 = '1: <=300'
			   300.01-600 = '2: 300-600'
			  600.01-1000 = '3: 600-1000'
			 1000.01-3200 = '4: 1000-3200'
			 3200.01-6000 = '5: 3200-6000'
			6000.01-10000 = '6: 6000-10000'
		   10000.01-16000 = '7: 10000-16000'
			16000.01-high = '8: 16000+'
	    		    other = '8: 16000+';
	value f_econ    1-5 = '1: 1-5'
					6-7 = '2: 6-7'
					8-8 = '3: 8'
				   9-10 = '4: 9-10'
	    		  other = '1: 1-5';
	value f_change low--0.5 = '1: <-50%'
				 -0.49--0.2 = '2: -50%--20%'
				   -0.2-0.6 = '3: -20%-60%'
					 0.61-1 = '4: 60%-100%'
				     1.01-2 = '5: 100%-200%'
				  2.01-high = '6: 200%+'
				      Other = '6: 200%+';
	value f_disc    low-10 = '1: <10%'
				     11-20 = '2: 10%-20%'
				     21-40 = '3: 20%-40%'
					 41-70 = '4: 40%-70%'
				   71-high = '5: 70%+'
				     Other = '6: Missing';
	value f_drs     low--1 = '1: <0'
				     0-400 = '2: 0-400'
				   401-500 = '3: 400-500'
				   501-600 = '4: 500-600'
				   601-700 = '4: 600-700'
				  701-high = '5: 700+'
				     Other = '6: Missing';
	value f_add    low--21 = '1: <-20%'
					-20--1 = '2: -20%-0%'
					  0-10 = '3: 0%-10%'
					 11-20 = '5: 10%-20%'
				     21-40 = '6: 20%-40%'
				   41-high = '7: 40%+'
				     Other = '8: Missing';
	value f_tslp      0 = '1: <1 Mth'
					1-2 = '2: 1-3 Mths'
					3-5 = '3: 3-6 Mths'
				   6-23 = '4: 6-24 Mths'
				  24-47 = '6: 2-4 Yrs'
				48-high = '7: 4+ Yrs/No Pay'
	    		  other = '7: 4+ Yrs/No Pay';
	value f_tslr      0 = '1: <1 Mth'
					1-2 = '2: 1-3 Mths'
					3-5 = '3: 3-6 Mths'
				   6-11 = '4: 6-12 Mths'
				  12-47 = '5: 1-4 Yrs'
				48-high = '6: 4+ Yrs'
	    		  other = '6: 4+ Yrs';
	value f_tsl       0 = '1: <1 Mth'
					1-2 = '2: 1-3 Mths'
					3-5 = '3: 3-6 Mths'
				   6-11 = '4: 6-12 Mths'
				  12-23 = '5: 1-2 Yrs'
				  24-47 = '6: 2-4 Yrs'
				48-high = '7: 4+ Yrs'
	    		  other = '8: No RPC';
	value f_numb    0-1 = '1: 0-1'
					  2 = '2: 2'
					  3 = '3: 3'
					4-6 = '4: 4-6'`
					7-8 = '5: 7-8'
				   9-11 = '6: 9-11'
				  12-19 = '7: 12-19'
				20-high = '8: 20+'
	    		  other = '9: Missing';
	value f_pay       0 = '1: 0'
					1-5 = '2: 1-5'
				   6-10 = '3: 6-10'
				  11-16 = '4: 11-16'
				17-high = '5: 17+'
	    		  other = '6: Missing';
run;

/*Need to include last rpc and number of previous rpc's*/

data discounts10;
	set jack.discounts9 (in=t1 /*where=(classification = 'B&F')*/);
	format State $char10.;
	if paid_flag = . then paid_flag = 0;
	if phone_count = . then phone_count = 0;
	if mobile_flag = 1 and fixed_flag = 1 then phone_type = 'M + F';
		else if fixed_flag = 1 then phone_type = 'Fix';
		else if mobile_flag = 1 then phone_type = 'Mob';
		else phone_type = 'None';
	if campaign_medium  = '' then campaign_medium = 'Agent';
	if campaign  = '' then campaign = 'None';
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
	if call_type = '' then call_type = 'None';
/*	if prod = 'Business Loan' then prod = 'Credit Card';*/
/*		else if prod = 'Transaction Acct' then prod = 'Personal Loan';*/
	if employment_status = 'Self-employed' then employment_status = 'Unknown';
/*	last_pay_amt = put(last_payment_amount,f_cash.);*/
/*	total_pay_amt = put(pay,f_cash.);*/
/*	Load_Age = put(Age_At_Load,f_dtrage.);*/
/*	Age_AD = put(Age_At_Disc,f_dtrage.);*/
/*	drs=put(veda_score,f_drs.);*/
run;

proc sort data=discounts10 nodupkey;
	by Client_Ledger_Number Account_Number discount_date;
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
data jack.discount_in_time_bf;
	set discounts12 (where=(discount_date between '01Jan2015'd and '30Sep2018'd));
run;

/*data random_forest;*/
/*	set jack.discount_in_time (keep=discount client_last_payment_amount economic_resource_decile gender lp loaded_amount occupation_decile reduced_settlement_amount relative_advantage_decile Relative_Disadvantage_Decile state add_discount age_at_disc age_at_load arrangement_flag balance balance_change campaign campaign_flag campaign_medium classification contact debt_change drivers_licence_flag email_flag extension_flag reminder_flag phone_count phone_type paid_flag postal_address_flag residential_address_flag previous_discount total_payment tsl tsld tslr);*/
/*run;*/

/*proc export data=random_forest*/
/*	OUTFILE= "\\bcssydfs01\shared\JZ\Adel\Discount Offer\AU PDL\R\In Time.csv"*/
/*	dbms=csv label REPLACE;*/
/*run;	*/

data jack.discount_out_time_bf;
	set discounts12 (where=(discount_date between '01Oct2018'd and '31Mar2019'd));
run;


/*Partitioning of dataset to train and validation datasets*/

proc surveyselect data=jack.discount_in_time_bf out=settle_model seed=103662 samprate=0.70 outall method=srs noprint; run;
data jack.discount_model_train_bf; set settle_model; where selected =1; run;
data jack.discount_model_validate_bf; set settle_model; where selected =0; run;