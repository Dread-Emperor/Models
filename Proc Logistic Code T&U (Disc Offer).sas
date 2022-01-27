/*Logistic Regression*/

proc logistic data=jack.discount_model_train_tu descending outmodel=jack.discount_model_tu; 

/*Put under class section all the categorical variables*/
/*Model creates dummy variables for these variables*/
/*You can specify inside "ref" the base category for each variable. */

class
email_flag(ref='N')
phone_count(ref='0')
phone_type(ref='None')
postal_address_flag(ref='N')
ctype(ref='None')

prod(ref='Electricity')
drs(ref='6: Missing')
employment_status(ref='Unknown')
ER_Score(ref='3: 4-6')

debt_change(ref='4: 0%')
new_balance(ref='1: <=500')
tslr(ref='8: No RPC')
tslp(ref='5: No Pay')
tsl(ref='4: 2-4 Yrs')

disc(ref='4: 30%-40%')
add_disc(ref='3: -10%-10%')
prev_disc(ref='1: 0-1')

/*State(ref='NSV')*/
/*tsld(ref='8: No RPC')*/
/*contact(ref='L')*/
/*Loaded_Amount_Band(ref='7: 3000+')*/
/*payments(ref='1: 0')*/

/ param=ref;

/*Logistic Regression Equation -Stepwise method*/

model paid_flag = 
email_flag
phone_count
phone_type
postal_address_flag
ctype

prod
drs
employment_status
ER_Score

debt_change
new_balance
tslr
tslp
tsl

disc
add_disc
prev_disc

/*State*/
/*tsld*/
/*contact*/
/*Loaded_Amount_Band*/
/*payments*/


/ selection=stepwise sle=0.05 sls=0.1 outroc=troc; 


score data=jack.discount_model_validate_tu out=jack.discount_out_validate_tu outroc=vroc;
/*score data=jack.discount_out_time_tu out=jack.discount_time_validate_tu outroc=vroc;*/
roccontrast;

run;

/*proc freq data=jack.sja_model_train;*/
/*	table Disadvantage_Decile*Advantage_Decile /cellchi2 chisq;*/
/*	output out=q pchi;*/
/*run;*/




/* Validation of Test (Out of Sample) Data*/
/*proc rank data=jack.discount_out_validate_tu out=validate_test ties=low descending groups=10;*/
/*	var p_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=validate_test; by p_score; run;*/
/*proc summary data=validate_test; var paid_flag p_1; by p_score; output out=q sum=; run;*/


/* Validation of Test (Out of Time) Data*/
/*proc rank data=jack.discount_time_validate_tu out=time_test ties=low descending groups=10;*/
/*	var p_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=time_test; by p_score; run;*/
/*proc summary data=time_test; var paid_flag p_1; by p_score; output out=s sum=; run;*/


/* Validation of Train (In Sample) Data*/
/*proc logistic inmodel=jack.discount_model_tu;*/
/*	score data=jack.discount_model_train_tu out=jack.discount_model_train_test_tu; run;*/
/*proc rank data=jack.discount_model_train_test_tu out=jack.discount_model_train_test_p_tu ties=low descending groups=10;*/
/*	var P_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=jack.discount_model_train_test_p_tu; by p_score; run;*/
/*proc summary data=jack.discount_model_train_test_p_tu; var paid_flag P_1; by p_score; output out=r sum=; run;*/

