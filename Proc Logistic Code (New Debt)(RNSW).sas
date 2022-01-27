/*Logistic Regression*/

proc logistic data=jack.rnsw_model_train descending outmodel=jack.rnsw_model_new; 

/*Put under class section all the categorical variables*/
/*Model creates dummy variables for these variables*/
/*You can specify inside "ref" the base category for each variable. */

class 	
phone_type(ref='None')
fixed_flag(ref='0')
email_flag(ref='N')
ER_Score(ref='2: 2-5')
Loaded_Amount_Band(ref='2: 250-600')
issue_to_load(ref='2: 3-4')
offence_to_load(ref='3: 7-12')
phone_count(ref='0')
mobile_flag(ref='0')
employment_status(ref='Unemployed')
company_flag(ref='N')
bank_flag(ref='N')
postal_address_flag(ref='N')
State(ref='N/M')
court_flag(ref='N')
number_plate_flag(ref='N')
drivers_licence_flag(ref='N')
traffic_offence(ref='1')
Public_Transport(ref='0')
Parking(ref='0')
serious_crime(ref='0')

/ param=ref;

/*Logistic Regression Equation -Stepwise method*/

model resp_var = 
phone_type
fixed_flag
email_flag
ER_Score
Loaded_Amount_Band
issue_to_load
offence_to_load
phone_count
mobile_flag
employment_status
company_flag
bank_flag
postal_address_flag
State
court_flag
number_plate_flag
drivers_licence_flag
traffic_offence
Public_Transport
Parking
serious_crime

/ selection=stepwise sle=0.05 sls=0.1 outroc=troc; 


/*score data=jack.rnsw_model_validate out=jack.rnsw_out_validate outroc=vroc;*/
score data=jack.rnsw_out_time2 out=jack.rnsw_time_validate outroc=vroc;
roccontrast;

run;


/* Validation of Test (Out of Sample) Data*/
/*proc rank data=jack.rnsw_out_validate out=validate_test ties=low descending groups=10;*/
/*	var p_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=validate_test; by p_score p_0; run;*/
/*proc summary data=validate_test; var resp_var p_1; by p_score; output out=q sum=; run;*/


/* Validation of Test (Out of Time) Data*/
proc rank data=jack.rnsw_time_validate out=time_test ties=low descending groups=10;
	var p_1;
	ranks p_score;
run;
proc sort data=time_test; by p_score; run;
proc summary data=time_test; var resp_var p_1; by p_score; output out=s sum=; run;


/* Validation of Train (In Sample) Data*/
/*proc logistic inmodel=jack.rnsw_model_new;*/
/*	score data=jack.rnsw_model_train out=jack.rnsw_model_train_test; run;*/
/*proc rank data=jack.rnsw_model_train_test out=jack.rnsw_model_train_test_p ties=low descending groups=10;*/
/*	var P_1;*/
/*	ranks p_score;*/
/*run;*/
/*proc sort data=jack.rnsw_model_train_test_p; by p_score; run;*/
/*proc summary data=jack.rnsw_model_train_test_p; var resp_var P_1; by p_score; output out=r sum=; run;*/

