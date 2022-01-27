/*Logistic Regression*/

proc logistic data=jack.sja_model_train descending outmodel=jack.sja_model_test; 

/*Put under class section all the categorical variables*/
/*Model creates dummy variables for these variables*/
/*You can specify inside "ref" the base category for each variable. */

class 	
/*Occupation_Decile(ref='2: 1-4,6-7')*/
/*Occupation_Decile(ref='1: 1-4')*/
fixed_flag(ref='0')
area(ref='Metro')
Address_type(ref='Residential')
email_flag(ref='0')
Economic_Resource_Decile(ref='1: 1-3')
Debtor_Age_At_Load_Band(ref='3: 25-43')
phone_count_1(ref='1: 0')
Loaded_Amount_Band(ref='7: 900+')
/*service_to_load_band(ref='2: 2')*/
insurance_flag(ref='N')
phone_count(ref='0')
mobile_flag(ref='0')
/*Advantage_Decile(ref='5: 6-10')*/
/*Disadvantage_Decile(ref='2: 4-5')*/
/*Disadvantage_Decile(ref='2: 2-5')*/
/*pickup_flag(ref='Y')*/
/*gender(ref='M')*/
/*debtors_linked(ref='1')*/

/ param=ref;

/*Logistic Regression Equation -Stepwise method*/

model resp_var = 
/*Occupation_Decile*/
fixed_flag
area
Address_type
email_flag
Economic_Resource_Decile
Debtor_Age_At_Load_Band
phone_count_1
Loaded_Amount_Band
/*service_to_load_band*/
insurance_flag
phone_count
mobile_flag
/*Advantage_Decile*/
/*Disadvantage_Decile*/
/*pickup_flag*/
/*gender*/
/*debtors_linked*/

/ selection=stepwise sle=0.05 sls=0.1 outroc=troc; 


/*score data=jack.sja_model_validate out=jack.pay_test outroc=vroc;*/
score data=jack.sja_time_validate out=jack.pay_test_time outroc=vroc;
roccontrast;
;
run;

/*proc freq data=jack.sja_model_train;*/
/*	table Disadvantage_Decile*Advantage_Decile /cellchi2 chisq;*/
/*	output out=q pchi;*/
/*run;*/




/* Validation of Test (Out of Sample) Data*/
proc rank data=jack.pay_test out=pay_test_p ties=low descending groups=10;
	var p_1;
	ranks p_score;
run;
proc sort data=pay_test_p; by p_score; run;
proc summary data=pay_test_p; var resp_var p_1; by p_score; output out=q sum=; run;


/* Validation of Test (Out of Time) Data*/
proc rank data=jack.pay_test_time out=pay_test_time_p ties=low descending groups=10;
	var p_1;
	ranks p_score;
run;
proc sort data=pay_test_time_p; by p_score; run;
proc summary data=pay_test_time_p; var resp_var p_1; by p_score; output out=s sum=; run;


/* Validation of Train Data*/
proc logistic inmodel=/*jack.sja_model_test*/qqqqq;
	score data=jack.sja_model_train out=jack.sja_model_train_test; run;

proc rank data=jack.sja_model_train_test out=jack.sja_model_train_test_p ties=low descending groups=10;
	var P_1;
	ranks p_score;
run;
proc sort data=jack.sja_model_train_test_p; by p_score; run;
proc summary data=jack.sja_model_train_test_p; var resp_var P_1; by p_score; output out=r sum=; run;

