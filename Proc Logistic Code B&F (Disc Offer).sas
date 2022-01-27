/*Logistic Regression*/

proc logistic data=jack.discount_model_train_bf descending outmodel=jack.discount_model_bf; 

/*Put under class section all the categorical variables*/
/*Model creates dummy variables for these variables*/
/*You can specify inside "ref" the base category for each variable. */

class
email_flag(ref='N')
ER_Score(ref='1: 1-3')
phone_count(ref='0')
phone_type(ref='None')
employment_status(ref='Unknown')
postal_address_flag(ref='N')
prod(ref='Credit Card')
new_balance(ref='7: 16000+')

tslp(ref='3: No Pay')
tsl(ref='6: 4+ Yrs')
tslr(ref='5: 4+ Yrs')

drs(ref='4: 600-700')
disc(ref='3: 30%-70%')
add_disc(ref='2: -20%-20%')
prev_disc(ref='1: 0-2')

campaign_medium(ref='Letter')
contact(ref='L')

/*State(ref='NSW')*/
/*ctype(ref='None')*/
/*debt_change(ref='3: -20%-20%')*/
/*tsld(ref='8: No RPC')*/


/ param=ref;

/*Logistic Regression Equation -Stepwise method*/

model paid_flag = 
/*email_flag*/
ER_Score
phone_count
phone_type
employment_status
postal_address_flag
prod
new_balance

tslp
tsl
tslr

drs
disc
add_disc
prev_disc

campaign_medium
contact

/*State*/
/*ctype*/
/*debt_change*/
/*tsld*/



/ selection=stepwise sle=0.05 sls=0.1 outroc=troc; 


score data=jack.discount_model_validate_bf (where=(campaign_medium ne 'Agent')) out=jack.discount_out_validate_bf outroc=vroc;
/*score data=jack.discount_out_time_bf (where=(campaign_medium ne 'Agent')) out=jack.discount_time_validate_bf outroc=vroc;*/
roccontrast;

run;

/*proc freq data=jack.sja_model_train;*/
/*	table Disadvantage_Decile*Advantage_Decile /cellchi2 chisq;*/
/*	output out=q pchi;*/
/*run;*/




/* Validation of Test (Out of Sample) Data*/
proc rank data=jack.discount_out_validate_bf out=validate_test ties=low descending groups=10;
	var p_1;
	ranks p_score;
run;
proc sort data=validate_test; by p_score; run;
proc summary data=validate_test; var paid_flag p_1; by p_score; output out=q sum=; run;


/* Validation of Test (Out of Time) Data*/
/*proc rank data=jack.discount_time_validate_bf out=time_test ties=low descending groups=10;*/
/*	var p_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=time_test; by p_score; run;*/
/*proc summary data=time_test; var paid_flag p_1; by p_score; output out=s sum=; run;*/


/* Validation of Train (In Sample) Data*/
/*proc logistic inmodel=jack.discount_model_bf;*/
/*	score data=jack.discount_model_train_bf (where=(campaign_medium ne 'Agent')) out=jack.discount_model_train_test_bf; run;*/
/*proc rank data=jack.discount_model_train_test_bf out=jack.discount_model_train_test_p_bf ties=low descending groups=10;*/
/*	var P_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=jack.discount_model_train_test_p_bf; by p_score; run;*/
/*proc summary data=jack.discount_model_train_test_p_bf; var paid_flag P_1; by p_score; output out=r sum=; run;*/

