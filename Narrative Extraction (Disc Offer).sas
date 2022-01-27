libname JACK '\\BCSPARSAS01\sasdata\SAS Data\Jack\SASdata';

data acct_narr_2011;
	infile '\\bcsparsas01\sasdata\SAS Data\Jack\SASdata\Discount Narrative\2011.txt'
	delimiter= ',' dsd missover firstobs=1;
	input client_ledger_number :7. account_number :7. date :7. time :6. type :7. narrative :$100. opid :5.;
	format narrative_date ddmmyy10. narrative_time time.;
	narrative_date=input(put(date+19000000,8.),yymmdd8.);
	narrative_time=input(put(time,z6.),B8601TM.);
	drop date time;
run;

data acct_narr_2015;
	infile '\\bcsparsas01\sasdata\SAS Data\Jack\SASdata\Discount Narrative\2015.txt'
	delimiter= ',' dsd missover firstobs=1;
	input client_ledger_number :7. account_number :7. date :7. time :6. type :7. narrative :$100. opid :5.;
	format narrative_date ddmmyy10. narrative_time time.;
	narrative_date=input(put(date+19000000,8.),yymmdd8.);
	narrative_time=input(put(time,z6.),B8601TM.);
	drop date time;
run;

data acct_narr_2016;
	infile '\\bcsparsas01\sasdata\SAS Data\Jack\SASdata\Discount Narrative\2016.txt'
	delimiter= ',' dsd missover firstobs=1;
	input client_ledger_number :7. account_number :7. date :7. time :6. type :7. narrative :$100. opid :5.;
	format narrative_date ddmmyy10. narrative_time time.;
	narrative_date=input(put(date+19000000,8.),yymmdd8.);
	narrative_time=input(put(time,z6.),B8601TM.);
	drop date time;
run;

data acct_narr_2017;
	infile '\\bcsparsas01\sasdata\SAS Data\Jack\SASdata\Discount Narrative\2017.txt'
	delimiter= ',' dsd missover firstobs=1;
	input client_ledger_number :7. account_number :7. date :7. time :6. type :7. narrative :$100. opid :5.;
	format narrative_date ddmmyy10. narrative_time time.;
	narrative_date=input(put(date+19000000,8.),yymmdd8.);
	narrative_time=input(put(time,z6.),B8601TM.);
	drop date time;
run;

data acct_narr_2018;
	infile '\\bcsparsas01\sasdata\SAS Data\Jack\SASdata\Discount Narrative\2018.txt'
	delimiter= ',' dsd missover firstobs=1;
	input client_ledger_number :7. account_number :7. date :7. time :6. type :7. narrative :$100. opid :5.;
	format narrative_date ddmmyy10. narrative_time time.;
	narrative_date=input(put(date+19000000,8.),yymmdd8.);
	narrative_time=input(put(time,z6.),B8601TM.);
	drop date time;
run;

data acct_narr_2019;
	infile '\\bcsparsas01\sasdata\SAS Data\Jack\SASdata\Discount Narrative\2019.txt'
	delimiter= ',' dsd missover firstobs=1;
	input client_ledger_number :7. account_number :7. date :7. time :6. type :7. narrative :$100. opid :5.;
	format narrative_date ddmmyy10. narrative_time time.;
	narrative_date=input(put(date+19000000,8.),yymmdd8.);
	narrative_time=input(put(time,z6.),B8601TM.);
	drop date time;
run;

/*Exclude years: 2010,2012,2013,2014 (they have no discount campaigns)*/

data acct_narr;
	set acct_narr_2011 acct_narr_2015 acct_narr_2016 acct_narr_2017 acct_narr_2018 acct_narr_2019;
run;

proc sort data= acct_narr; by client_ledger_number account_number narrative_date narrative_time; run;

data acct_narr_disc;
	set acct_narr (where=(find(narrative,'***','i',1) = 1 and find(narrative,'******','i',1) = 0 and
	(find(narrative,'discou','i',1) ge 1 or find(narrative,'settle','i',1) ge 1 or find(narrative,'offer','i',1) ge 1 or find(narrative,' deal ','i',1) ge 1 or find(narrative,'extra','i',1) ge 1)
	and (find(narrative,'Experian','i',1) = 0 and find(narrative,'Veda','i',1) = 0 and find(narrative,'D&B','i',1) = 0 and find(narrative,'Data wash','i',1) = 0 and find(narrative,'employer','i',1) = 0) 
	and find(narrative,'instalment offer','i',1) = 0 ));
run;

/*Remove duplicates caused by campaigns failing to run which are rerun*/
proc sort data=acct_narr_disc nodupkey equals;
	by client_ledger_number account_number narrative_date narrative_time;
run;

data acct_narr2;
	merge acct_narr_disc (in=t1 keep=client_ledger_number account_number narrative_date narrative_time opid) acct_narr (in=t2 keep=client_ledger_number account_number narrative narrative_date narrative_time);
	by client_ledger_number account_number narrative_date narrative_time;
	if t1;
run;

proc sort data=acct_narr2 nodupkey equals out=a dupout=b;
	by client_ledger_number account_number narrative_date narrative_time;
run;

proc sort data=b nodupkey equals out=b dupout=c;
	by client_ledger_number account_number narrative_date narrative_time;
run;

proc sort data=c nodupkey equals out=c dupout=d;
	by client_ledger_number account_number narrative_date narrative_time;
run;

proc sort data=d nodupkey equals out=d dupout=e;
	by client_ledger_number account_number narrative_date narrative_time;
run;

proc sort data=e nodupkey equals out=e dupout=f;
	by client_ledger_number account_number narrative_date narrative_time;
run;

proc sort data=f nodupkey equals out=f dupout=g;
	by client_ledger_number account_number narrative_date narrative_time;
run;

/*May add settlement end date, doesn't apply to all campaign discounts*/
data acct_narr3;
	merge a (in=t1 rename=(narrative = a)) b (in=t2 rename=(narrative = b)) c (in=t3 rename=(narrative = c)) d (in=t4 rename=(narrative = d)) e (in=t5 rename=(narrative = e)) f (in=t6 rename=(narrative = f));
	by client_ledger_number account_number narrative_date narrative_time;
	if t1;
	format narrative $char400. discount_type campaign_medium $char20. email_check1 email_check2 $char50.;
	a = strip(a);b = strip(b);c = strip(c);d = strip(d);e = strip(e);f = strip(f);
	narrative = cats(a,b,c,d,e,f);
	campaign_flag = 1;
	if find(narrative,'$','i',1) ne 0 then foo = substr(narrative,find(narrative,'$','i',1)+1,20);
	if find(narrative,'If Customer is unable to make full','i',1) ge 1 then arrangement_flag = 1;
	if find(narrative,'Extension','i',1) ge 1 and find(narrative,'Requests Extension','i',1) = 0 then discount_type = 'Extension';
		else if find(narrative,'reminder','i',1) ge 1 or find(narrative,'follow up','i',1) ge 1 then discount_type = 'Reminder';
		else if find(narrative,'NOT delivered successfully','i',1) ge 1 or find(narrative,'was not sent','i',1) ge 1 then discount_type = 'Fail';
		else if find(narrative,'Tax Time','i',1) ge 1 then discount_type = 'Tax Time';
		else if find(narrative,'payer','i',1) ge 1 then discount_type = 'Payer';
		else if find(narrative,'Special','i',1) ge 1 then discount_type = 'Special';
		else if find(narrative,'Barred','i',1) ge 1 or find(narrative,'aged','i',1) ge 1 then discount_type = 'Statute Barred';
		else if find(narrative,'extra ','i',1) ge 1 then discount_type = 'Extra';
		else if find(narrative,'dispute','i',1) ge 1 and find(narrative,'resolutions settlement','i',1) ge 1 then discount_type = 'Dispute';
		else if find(narrative,'Broken','i',1) ge 1 then discount_type = 'Broken Arrangement';
		else if find(narrative,'Make a Deal','i',1) ge 1 or find(narrative,'Make an Offer','i',1) ge 1 then discount_type = 'Deal';
		else discount_type = 'Other';
	if find(narrative,'New Discount','i',1) ge 1 and find(narrative,'New Discount Amount Due','i',1) = 0 then narrative_discount = substr(narrative,anydigit(narrative,find(narrative,'New Discount','i',1)),2)*1;
		else if discount_type ne 'Extra' then narrative_discount = substr(narrative,find(narrative,'%','i',1)-2,2)*1;
		else narrative_discount = substr(narrative,anydigit(narrative,find(narrative,'Increased to','i',1)),2)*1;
	if ((discount_type = 'Extra' and narrative_discount = .) or (discount_type = 'Special' and find(narrative,'extra','i',1) ge 1) and count(narrative,'%') le 2) and find(narrative,'%','i',1) ge 1 then add_discount = substr(narrative,find(narrative,'%','i',1)-2,2)*1;
	if narrative_discount = 0 and find(narrative,'%','i',1) ge 1 then narrative_discount = substr(narrative,find(narrative,'%','i',1)-3,2)*1;
	if discount_type = 'Special' and find(narrative,'extra','i',1) ge 1 and count(narrative,'%') le 1 then narrative_discount = .;
	if discount_type = 'Special' and narrative_discount in (5,10) then do; add_discount = narrative_discount; narrative_discount = .; end;
	if arrangement_flag ne 1 then narrative_settlement_amount = compress(scan(foo,1,' '),',|')*1;
		else narrative_settlement_amount = round(compress(scan(foo,1,' '),',')*2,0.1);
	if narrative_discount ne . and narrative_settlement_amount ne . then narrative_balance = narrative_settlement_amount/((100-narrative_discount)/100);
	if find(narrative,'SMS','i',1) ge 1 then campaign_medium = 'SMS';
		else if find(narrative,'Email','i',1) ge 1 then campaign_medium = 'Email';
		else if find(narrative,'Letter','i',1) ge 1 then campaign_medium = 'Letter';
		else campaign_medium = 'Other';
	if campaign_medium = 'Email' and find(narrative,'@','i',1) ge 1 then do; email_check1 = scan(scan(narrative, 1,'@'),-1,' '); 
		email_check2 = UPCASE(scan(scan(scan(narrative,2,'@'),1,' '),1,'.'));  end;
	if email_check2 in ('NO','NOMAIL','NOEMAIL','NONE','NOWHERE','O','F','FAKE','EMAIL','GAMAIL','GAMIL','GMIAL','HOHTMAIL','OHOTMAIL','HOMAIL','HOMTAIL','HOMTMAIL','HOYMAIL','HTMAIL','HTOMAIL','ICOULD','WINDOSLIVE','YAHOPO','ZILCH') 
	or 	find(email_check2,',','i',1) ge 1 or (substr(email_check2,length(email_check2)-2,3) = 'COM' and find(narrative,'com.com','i',1) = 0 and find(narrative,'com.net','i',1) = 0)
	or (find(email_check2,'BIGPO','i',1) ge 1 and email_check2 ne 'BIGPOND') or (find(email_check2,'GMA','i',1) ge 1 and email_check2 ne 'GMAIL') 
	or (find(email_check2,'HOTM','i',1) ge 1 and email_check2 ne 'HOTMAIL') or (find(email_check2,'ICLO','i',1) ge 1 and email_check2 ne 'ICLOUD') then fake_email_flag = 1;
	drop foo;
run;



/*proc import out=response*/
/*	datafile = "\\bcssydfs01\shared\JZ\Adel\Discount Offer\AU PDL\Reachtel Response Files\Baycorp-6September16-Email-GeneralDiscount.xlsx"*/
/*	dbms=xlsx replace;*/
/*run;*/

/*data response2;*/
/*	set response (keep=uniqueid status sent track hardbounce softbounce removed duplicate current_balance cust_full_name 'Discount Due'n 'Due Date :'n);*/
/*	client_ledger_number = substr(uniqueid,1,7)*1;*/
/*	account_number = substr(uniqueid,9,7)*1;*/
/*	if status = 'ABANDONED' or hardbounce = 'YES' or softbounce = 'YES' then fail_flag = 1;*/
/*	if track ne . then link_flag = 1;*/
/*	rename 'Discount Due'n = samt 'Due Date :'n = sdate;*/
/*	drop uniqueid status track hardbounce softbounce removed duplicate;*/
/*run;*/

/*%macro import(text,e);*/
/**/
/*%let n = 1;*/
/**/
/*%do n = 1 %to &e;*/
/**/
/*proc import out=r&n*/
/*	datafile = "\\bcssydfs01\shared\JZ\Adel\Discount Offer\AU PDL\Reachtel Response Files\&text.\&n..csv"*/
/*	dbms=csv replace;*/
/*	GUESSINGROWS=max;*/
/*run;*/
/**/
/*data r&n;*/
/*	set r&n (keep=uniqueid status sent track hardbounce softbounce removed duplicate current_balance cust_full_name 'Due Date:'n date extension);*/
/*run;*/
/**/
/*%end;*/
/**/
/*%mend;*/
/**/
/*%import(2019\Email,1);*/
/**/
/**/
/*data response2;*/
/*	set response (keep=uniqueid status sent track hardbounce softbounce removed duplicate current_balance cust_full_name DiscountBalance 'Due Date:'n);*/
/*	client_ledger_number = substr(uniqueid,1,7)*1;*/
/*	account_number = substr(uniqueid,9,7)*1;*/
/*	if status in ('ABANDONED','READY') or hardbounce = 'YES' or softbounce = 'YES' then fail_flag = 1;*/
/*	if track ne . then link_flag = 1;*/
/*	rename DiscountBalance = samt 'Due Date:'n = sdate;*/
/*	drop uniqueid status track hardbounce softbounce removed duplicate;*/
/*run;*/
/**/
/*proc summary data=response2 nway missing;*/
/*	class fail_flag link_flag;*/
/*	output out=q;*/
/*run;*/



/*Remove extensions and reminders and replace them with a flag*/
data reminder (drop=extension_flag fail_flag) extension (drop=reminder_flag fail_flag) fail (drop=extension_flag reminder_flag);
	set acct_narr3 (where=(discount_type in ('Reminder','Extension','Fail')) keep=client_ledger_number account_number narrative_date discount_type);
	if discount_type = 'Reminder' then reminder_flag = 1;
		else if discount_type = 'Extension' then extension_flag = 1;
		else fail_flag = 1;
	drop discount_type;
	if discount_type = 'Reminder' then output reminder;
	else if discount_type = 'Extension' then output extension;
	else output fail;
run;

/*Add in reminder and extension flags*/

data tack;
	set acct_narr3 (where=(discount_type not in ('Reminder','Extension','Fail')) keep=client_ledger_number account_number narrative_date discount_type);
	rename narrative_date = discount_date;
	drop discount_type;
run;

proc sql;
	create table reminder2 as
	select a.*,b.*
	from reminder a left join tack b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.narrative_date ge b.discount_date)
	order by a.client_ledger_number,a.account_number,a.narrative_date,b.discount_date;
quit;

/*According to Neerav the max difference is 31 days also no cmapaign reminders/extensions for agent-initiated discounts*/
data reminder3;
	set reminder2;
	by client_ledger_number account_number narrative_date discount_date;
	diff = narrative_date - discount_date;
	if last.narrative_date and diff < 31 then output;
run;

proc sql;
	create table extension2 as
	select a.*,b.*
	from extension a left join tack b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.narrative_date ge b.discount_date)
	order by a.client_ledger_number,a.account_number,a.narrative_date,b.discount_date;
quit;

data extension3;
	set extension2;
	by client_ledger_number account_number narrative_date discount_date;
	diff2 = narrative_date - discount_date;
	if last.narrative_date and diff2 < 31 then output;
run;

proc sort data=acct_narr3 nodupkey equals;
	by client_ledger_number account_number narrative_date;
run;

data acct_narr3a;
	merge acct_narr3 (in=t1 rename=(narrative_date=discount_date)) reminder3 (in=t2);
	by client_ledger_number account_number discount_date;
	if t1;
run;

/*Remove duplicates resulting from the merge (multiple reminders/extensions for a discount)*/
proc sort data=acct_narr3a;
	by client_ledger_number account_number discount_date diff;
run;

proc sort data=acct_narr3a nodupkey equals;
	by client_ledger_number account_number discount_date;
run;

data acct_narr3b;
	merge acct_narr3a (in=t1) extension3 (in=t2);
	by client_ledger_number account_number discount_date;
	if t1;
	drop narrative_date;
	rename discount_date = narrative_date;
run;

/*Remove duplicates resulting from the merge (multiple reminders/extensions for a discount)*/
proc sort data=acct_narr3b;
	by client_ledger_number account_number narrative_date diff2;
run;

proc sort data=acct_narr3b nodupkey equals;
	by client_ledger_number account_number narrative_date;
run;



/*Check to see if Balance Due is missing from any narrative with $ sign*/
/*data q;*/
/*	set acct_narr3 (where=(find(narrative,'$','i',1) ge 1 and (find(narrative,'Balance Due','i',1) = 0 and find(narrative,'Amount Due','i',1) = 0 and */
/*	find(narrative,'AmountDue','i',1) = 0 and find(narrative,'Amount to','i',1) = 0 and find(narrative,'Settlement Due','i',1) = 0 and find(narrative,'New Settlement','i',1) = 0 and*/
/*	find(narrative,'Due','i',1) = 0 and find(narrative,'Amount','i',1) = 0)*/
/*	and arrangement_flag = . and discount_type ne 'Deal'));*/
/*run;*/

/*data q;*/
/*	set acct_narr3 (where=(0<narrative_settlement_amount<25 and find(narrative,'instalment offer','i',1) = 0));*/
/*run;*/

/*Check to see if Discount is missing from any narrative with % sign*/


/*Get campaign medium for 'Other'. If it has an L class event on the same day it is a letter, else it is an SMS */

data match;
	set acct_narr3b (where=(campaign_medium = 'Other' and arrangement_flag = .) keep=client_ledger_number account_number narrative_date narrative_time campaign_medium arrangement_flag);
	created_calendar_id = input(put(narrative_date,yymmddn8.),8.);
run;

proc sql noprint;
	select distinct client_ledger_number into :ledgers separated by ',' from match;
quit;

proc sql;
	create table letters as
		select client_ledger_number, account_number, created_calendar_id, event_class, account_current_balance_amount
		from dmart.rm_account_event_fact
		where client_ledger_number in (&ledgers) and event_class = 'L' and created_calendar_id ge 20150101;
quit;

proc sort data=letters;
	by client_ledger_number account_number created_calendar_id;
run;

data match2;
	merge match (in=t1 keep=client_ledger_number account_number created_calendar_id narrative_date narrative_time) letters (in=t2);
	by client_ledger_number account_number created_calendar_id;
	if t1;
run;

/*Need to include both extra and special discounts discount calculation after all discounts are put together (excluding agent-initiated discounts)*/

data jack.narrative_discount;
	merge acct_narr3b (in=t1 where=(discount_type not in ('Reminder','Extension','Fail'))) match2 (in=t2);
	by client_ledger_number account_number narrative_date narrative_time;
	if t1;
	if find(narrative,'NOT delivered successfully','i',1) ge 1 then delete;
	if campaign_medium = 'Other' and event_class = 'L' then campaign_medium = 'Letter';
		else if campaign_medium = 'Other' then campaign_medium = 'SMS';
	if account_current_balance_amount ne . then narrative_balance = account_current_balance_amount;
	if narrative_settlement_amount = . and narrative_discount ne . and narrative_balance ne . then narrative_settlement_amount = (1-(narrative_discount/100))*narrative_balance;
	if narrative_balance = . and narrative_discount ne . and narrative_settlement_amount ne . then narrative_balance = narrative_settlement_amount/(1-(narrative_discount/100));
	drop a b c d e f created_calendar_id event_class account_current_balance_amount opid diff diff2 lag_nd lag2_nd lag3_nd lag4_nd lag5_nd email_check1 email_check2;
run;

/*Add in balance to accounts that don't have discount rate or settlement amount*/

data narrative_balance;
	set jack.narrative_discount (where=(narrative_balance = .) keep=client_ledger_number account_number narrative_date narrative_time narrative_balance);
	zday = input(put(narrative_date,yymmddn8.),8.);
	drop narrative_balance;
run;

/*proc sql noprint;*/
/*	select distinct client_ledger_number into :ledgers2 separated by ',' from narrative_balance;*/
/*quit;*/

%gitemn(narrative_balance,narrative_balance2,client_ledger_number,dmart.rm_account_event_fact,and created_calendar_id ge 20150101,
keep client_ledger_number account_number account_current_balance_amount event_class created_calendar_id created_time_id);

proc sql;
	create table narrative_balance3 as
	select a.*,b.*
	from narrative_balance a left join narrative_balance2 b
	on(a.client_ledger_number = b.client_ledger_number and a.account_number = b.account_number and a.zday > b.created_calendar_id)
	order by a.client_ledger_number,a.account_number,a.narrative_date,a.narrative_time,b.created_calendar_id,b.created_time_id;
quit;

data jack.narrative_balance4;
	set narrative_balance3;
	by client_ledger_number account_number narrative_date narrative_time created_calendar_id created_time_id;
	drop event_class zday created_calendar_id created_time_id;
	if last.narrative_date then output;
run;

data jack.narrative_discount2;
	merge jack.narrative_discount (in=t1) jack.narrative_balance4 (in=t2);
	by client_ledger_number account_number narrative_date narrative_time;
	if t1 and t2;
	if narrative_balance = . then narrative_balance = account_current_balance_amount;
	if narrative_discount = . and narrative_balance ne . and narrative_settlement_amount ne . then narrative_discount = round((narrative_balance - narrative_settlement_amount)/narrative_balance,0.01)*100;
	drop account_current_balance_amount;
	rename campaign_flag = campaign_flag2 narrative_date = campaign_date2 campaign_medium = campaign_medium2 narrative_discount = campaign_discount2 discount_type = campaign2 client_ledger_number = client_ledger_number2 account_number = account_number2;
run;

/*Delete intermediate steps (about 125GB)*/
proc datasets memtype=all nolist library=work;
  delete narrative_balance3 narrative_balance2 narrative_balance;
run;
quit;
