################### to create acces token #################
# salesforce/oAuth/SHM
# salesforce/sqldb/SHM
import json
import boto3
import pyodbc
import requests
import pandas as pd
from datetime import datetime
from colorama import Fore, Back, Style

client = boto3.client('secretsmanager', region_name='us-east-1')

shm_sf_secret_name = 'salesforce/oAuth/SHM'
originalresponse = client.get_secret_value(SecretId=shm_sf_secret_name)
data = json.loads(originalresponse['SecretString'])
TOKENURL = data['tokenurl']
params = {
	"grant_type": "password",
	"client_id": data['client_id'],
	"client_secret": data['client_secret'],
	"username": data['username'],
	"password": data['password']
}

r = requests.post(TOKENURL, params=params)
response = r.json()
access_token = response["access_token"]
instanceurl = response["instance_url"]
# print(access_token)
# print(instanceurl)

################### sql#################
shm_sf__sql_secret_name = 'salesforce/sqldb/SHM'
sql_response = client.get_secret_value(SecretId=shm_sf__sql_secret_name)
data = json.loads(sql_response['SecretString'])

conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
					  "Server=" + data['server'] + ';'
					  "Database=" + data['database'] + ';'
					  "UID=" + data['uid'] + ';'
					  "PWD=" + data['pwd'] + ';'
					  "autocommit= True;")
cursor = conn.cursor()

######################## Accounts Object (Account) ####################################
### 1a. Truncate Table (TRUNCATE TABLE [TableName]) ###
# cursor.execute('TRUNCATE TABLE FL_Accounts_Update_arahman')
cursor.execute('TRUNCATE TABLE FL_Accounts_Update')

# 1b. Calculate Max_LastModifiedDate & Max_LastModifiedDate
create_modified_date_query = cursor.execute(
    '''SELECT Max(CreatedDate) AS Max_CreateDate, MAX(LastModifiedDate) AS Max_LastModifiedDate FROM [Anywhere_Salesforce].dbo.[FL_Accounts] ''')

rows = [x for x in create_modified_date_query]
cols = [x[0] for x in create_modified_date_query.description]
create_modified_date_query_data = []
for row in rows:
    data1 = {}
    for k, v in zip(cols, row):
        data1[k] = v
    create_modified_date_query_data.append(data1)

Max_CreateDate_c = create_modified_date_query_data[0]['Max_CreateDate']
Max_LastModifiedDate_c = create_modified_date_query_data[0]['Max_LastModifiedDate']

Max_CreateDate_account = Max_CreateDate_c.strftime("%Y-%m-%dT%H:%M:%SZ")
# print(Max_CreateDate_account)

Max_LastModifiedDate_account = Max_LastModifiedDate_c.strftime("%Y-%m-%dT%H:%M:%SZ")
# print(Max_LastModifiedDate_account)



account_url = f'''{instanceurl}/services/data/v56.0/query/?q=SELECT
	Id, IsDeleted, MasterRecordId, Name, Type, RecordTypeId, ParentId, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingStateCode, BillingCountryCode, BillingLatitude, BillingLongitude, BillingGeocodeAccuracy, BillingAddress, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, ShippingStateCode, ShippingCountryCode, ShippingLatitude, ShippingLongitude, ShippingGeocodeAccuracy, ShippingAddress, Phone, Fax, Website, PhotoUrl, Industry, NumberOfEmployees, Description, CurrencyIsoCode, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate,  LastViewedDate,  LastReferencedDate,  Jigsaw,  JigsawCompanyId,  AccountSource,  SicDesc,  is_active__c,  txt_internal_id__c,  txt_msa_number__c,  Absence_Time_Survey_Resume_Date__c,  txt_country_code__c,  txt_address_line_1__c,  Absence_Time__c,  txt_zip_code__c,  txt_addressee__c,  txt_billing_addressee__c,  txt_county__c,  txt_email__c,  int_total_employees__c,  int_teachers__c,  int_nces_operational_schools__c,  txt_nces_agency_id__c,  int_staff__c,  txt_nces_county_number__c,  txt_nces_state_agency_id__c,  txt_nces_supervisory_union_id__c,  int_sped_students__c,  int_nces_operational_charter_schools__c,  txt_attention__c,  txt_billing_attention__c,  int_lep_ell_students__c,  txt_tx_region__c,  txt_nces_school_id__c,  txt_csm_region__c,  int_504_students__c,  int_rti_students__c,  int_gifted_students__c,  txt_agile_detailed_grade__c,  txt_agile_district_type__c,  txt_agile_file_type__c,  txt_agile_grade_level__c,  txt_agile_status__c,  txt_agile_main_parent_name__c,  txt_agile_nces_stateid__c,  txt_agile_parent_name__c,  txt_agile_record_type__c,  txt_agile_school_type__c,  int_agile_students_building__c,  int_agile_students_district__c,  int_agile_teachers_building__c,  int_agile_teachers_district__c,  primary_currency__c,  tier__c,  renewal_tier__c,  Territory_Type__c,  qualtrics__NPS_Date__c,  Territory_ID__c,  txt_locale__c,  Vertical__c,  Vertical_Sub_Type__c,  qualtrics__Net_Promoter_Score__c,  netsuite_url__c,  Netsuite_Account_Name__c,  BDR__c,  Bill_To_ARR__c,  CSM__c,  Frontline_Central_Survey_Resume_Date__c,  Frontline_Central__c,  Integration_Error__c,  MSA_Number__c,  Ownership_Summary__c,  Partner_ARR__c,  Pause_Communication_Until__c,  Professional_Growth_Survey_Resume_Date__c,  Professional_Growth__c,  Recruiting_Hiring_Survey_Resume_Date__c,  Recruiting_Hiring__c,  SEI_Sales_Rep__c,  Special_Ed_Intervention__c,  Special_Ed_Interventions_Survey_Resume__c,  Sync_Status__c,  Uplift__c,  User_ARR__c,  FIPS_Code__c,  int_agile_district_school_admin__c,  Platform__c,  Phone_Agile__c,  Website_Agile__c,  Business_Solutions__c,  platformID__c,  SIS_Used__c,  Agile_Building_Charter_Status__c,  Platform_Tools__c,  Test_Account__c,  X18_Character_ID__c,  Third_Party_Platform_ID__c,  Third_Party_Platform_Tools__c,  First_Signed_Date__c,  Partner_Account__c,  Aspex_Client_Code__c,  SSO_Integration_Applications_Platform__c,  Existing_Opportunities__c,  Total_Weighted_Score__c,  Count_of_Products__c,  Founded__c,  Mission__c,  Leadership__c,  Number_of_Customers__c,  Acquisition_Information__c,  Partnership_Information__c,  Awards__c,  Strengths__c,  Weaknesses__c,  Frontline_Opportunities__c,  Frontline_Threats__c,  Account__c,  CSM_First_Assigned__c,  Open_NCC_Opps__c,  Platform_Project_Status__c,  Platform_Live_Date__c,  Open_Implementation_Projects__c,  Platform_Stalled_Reason__c,  emp_staf__c,  Healthmaster_Sales_Overlay__c,  Contract_Assigned_Accelify__c,  Contract_Assigned_Healthmaster__c,  Nurses_in_District_Agile__c,  Nurses_in_School_Agile__c,  NetSuite_Link__c,  NCES_FTE_Count__c,  Sales_Employee_Override__c,  Total_Employees_new__c,  cpqAnniversaryDate__c,  resellerType__c,  HM_Intacct_ID__c,  Contract_Assignment_Accelify__c,  Contract_Assignment_Healthmaster__c,  ERP_Sales_Rep__c,  SalesLoft1__Active_Account__c,  ProgressBook_Sales_Rep__c,  NCES_Total_Students__c,  NCES_IEP_Students__c,  NCES_ELL_Students__c,  Consortium_Member__c,  SSO_Identity_Provider__c,  Health_Management__c,  Has_Bill_To_ARR__c
 FROM
	Account
 WHERE
    CreatedDate > %s OR LastModifiedDate > %s''' % (Max_CreateDate_account, Max_LastModifiedDate_account)

account_done = False
account_page_no= 1
while account_done is False:
	cursor.execute('TRUNCATE TABLE FL_Accounts_Update')
	print(Fore.RED + 'Truncated table "FL_Accounts_Update"')
	print(Fore.GREEN + f'Account table data paging number: {account_page_no}')
	account_response = requests.get(account_url, headers={'Authorization': 'Bearer {}'.format(access_token)})
	account_response_json = account_response.json()
	# print(account_response_json)
	# print(account_url)
	account_page_no = account_page_no + 1

	if account_response_json['done'] is False:
		account_done = False
		nextRecordsUrl = account_response_json['nextRecordsUrl']
		account_url = f'''{instanceurl}{nextRecordsUrl}'''
		# print(nextRecordsUrl)

	else:
		account_url = None
		account_done = True

	totalSize = account_response_json['totalSize']
	print(Fore.GREEN + f'Total number of records: {totalSize}')

	if len(account_response_json) != 0:
		with open('C:/Users/admarahman/Desktop/account_response_json.json', 'w') as o:
			json.dump(account_response_json, o, indent=4)
		print(Fore.GREEN + '"account_response_json.json" file created')

		account_response_df = pd.DataFrame(account_response_json.get('records'))
		del account_response_df['attributes']
		# print(account_response_df)

		account_col = '''Id, IsDeleted, MasterRecordId, Name, Type, RecordTypeId, ParentId, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingStateCode, BillingCountryCode, BillingLatitude, BillingLongitude, BillingGeocodeAccuracy, BillingAddress, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, ShippingStateCode, ShippingCountryCode, ShippingLatitude, ShippingLongitude, ShippingGeocodeAccuracy, ShippingAddress, Phone, Fax, Website, PhotoUrl, Industry, NumberOfEmployees, Description, CurrencyIsoCode, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate,  LastViewedDate,  LastReferencedDate,  Jigsaw,  JigsawCompanyId,  AccountSource,  SicDesc,  is_active__c,  txt_internal_id__c,  txt_msa_number__c,  Absence_Time_Survey_Resume_Date__c,  txt_country_code__c,  txt_address_line_1__c,  Absence_Time__c,  txt_zip_code__c,  txt_addressee__c,  txt_billing_addressee__c,  txt_county__c,  txt_email__c,  int_total_employees__c,  int_teachers__c,  int_nces_operational_schools__c,  txt_nces_agency_id__c,  int_staff__c,  txt_nces_county_number__c,  txt_nces_state_agency_id__c,  txt_nces_supervisory_union_id__c,  int_sped_students__c,  int_nces_operational_charter_schools__c,  txt_attention__c,  txt_billing_attention__c,  int_lep_ell_students__c,  txt_tx_region__c,  txt_nces_school_id__c,  txt_csm_region__c,  int_504_students__c,  int_rti_students__c,  int_gifted_students__c,  txt_agile_detailed_grade__c,  txt_agile_district_type__c,  txt_agile_file_type__c,  txt_agile_grade_level__c,  txt_agile_status__c,  txt_agile_main_parent_name__c,  txt_agile_nces_stateid__c,  txt_agile_parent_name__c,  txt_agile_record_type__c,  txt_agile_school_type__c,  int_agile_students_building__c,  int_agile_students_district__c,  int_agile_teachers_building__c,  int_agile_teachers_district__c,  primary_currency__c,  tier__c,  renewal_tier__c,  Territory_Type__c,  qualtrics__NPS_Date__c,  Territory_ID__c,  txt_locale__c,  Vertical__c,  Vertical_Sub_Type__c,  qualtrics__Net_Promoter_Score__c,  netsuite_url__c,  Netsuite_Account_Name__c,  BDR__c,  Bill_To_ARR__c,  CSM__c,  Frontline_Central_Survey_Resume_Date__c,  Frontline_Central__c,  Integration_Error__c,  MSA_Number__c,  Ownership_Summary__c,  Partner_ARR__c,  Pause_Communication_Until__c,  Professional_Growth_Survey_Resume_Date__c,  Professional_Growth__c,  Recruiting_Hiring_Survey_Resume_Date__c,  Recruiting_Hiring__c,  SEI_Sales_Rep__c,  Special_Ed_Intervention__c,  Special_Ed_Interventions_Survey_Resume__c,  Sync_Status__c,  Uplift__c,  User_ARR__c,  FIPS_Code__c,  int_agile_district_school_admin__c,  Platform__c,  Phone_Agile__c,  Website_Agile__c,  Business_Solutions__c,  platformID__c,  SIS_Used__c,  Agile_Building_Charter_Status__c,  Platform_Tools__c,  Test_Account__c,  X18_Character_ID__c,  Third_Party_Platform_ID__c,  Third_Party_Platform_Tools__c,  First_Signed_Date__c,  Partner_Account__c,  Aspex_Client_Code__c,  SSO_Integration_Applications_Platform__c,  Existing_Opportunities__c,  Total_Weighted_Score__c,  Count_of_Products__c,  Founded__c,  Mission__c,  Leadership__c,  Number_of_Customers__c,  Acquisition_Information__c,  Partnership_Information__c,  Awards__c,  Strengths__c,  Weaknesses__c,  Frontline_Opportunities__c,  Frontline_Threats__c,  Account__c,  CSM_First_Assigned__c,  Open_NCC_Opps__c,  Platform_Project_Status__c,  Platform_Live_Date__c,  Open_Implementation_Projects__c,  Platform_Stalled_Reason__c,  emp_staf__c,  Healthmaster_Sales_Overlay__c,  Contract_Assigned_Accelify__c,  Contract_Assigned_Healthmaster__c,  Nurses_in_District_Agile__c,  Nurses_in_School_Agile__c,  NetSuite_Link__c,  NCES_FTE_Count__c,  Sales_Employee_Override__c,  Total_Employees_new__c,  cpqAnniversaryDate__c,  resellerType__c,  HM_Intacct_ID__c,  Contract_Assignment_Accelify__c,  Contract_Assignment_Healthmaster__c,  ERP_Sales_Rep__c,  SalesLoft1__Active_Account__c,  ProgressBook_Sales_Rep__c,  NCES_Total_Students__c,  NCES_IEP_Students__c,  NCES_ELL_Students__c,  Consortium_Member__c,  SSO_Identity_Provider__c,  Health_Management__c,  Has_Bill_To_ARR__c'''
		account_val = '''?, ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?'''

		account_response_df = account_response_df.astype(object).where(pd.notnull(account_response_df), None)
		for index, row in account_response_df.iterrows():
			row.BillingAddress = str(row.BillingAddress)
			row.ShippingAddress = str(row.ShippingAddress)

	# CreatedDate
			if row.CreatedDate != None:
				row.CreatedDate_sf = row.CreatedDate.split('.')
				row.CreatedDate_sf = row.CreatedDate_sf[0] + 'z'
				row.CreatedDate_sf = datetime.strptime(row.CreatedDate_sf,"%Y-%m-%dT%H:%M:%SZ")
				row.CreatedDate = row.CreatedDate_sf.strftime("%Y, %m, %d, %H, %M, %S")
				row.CreatedDate = datetime.strptime(row.CreatedDate, "%Y, %m, %d, %H, %M, %S")

	# LastModifiedDate
			if row.LastModifiedDate != None:
				row.LastModifiedDate_sf = row.LastModifiedDate.split('.')
				row.LastModifiedDate_sf = row.LastModifiedDate_sf[0] + 'z'
				row.LastModifiedDate_sf = datetime.strptime(row.LastModifiedDate_sf, "%Y-%m-%dT%H:%M:%SZ")
				row.LastModifiedDate = row.LastModifiedDate_sf.strftime("%Y-%m-%d %H:%M:%S")
				row.LastModifiedDate = datetime.strptime(row.LastModifiedDate, "%Y-%m-%d %H:%M:%S")

	# SystemModstamp
			if row.SystemModstamp != None:
				row.SystemModstamp_sf = row.SystemModstamp.split('.')
				row.SystemModstamp_sf = row.SystemModstamp_sf[0] + 'z'
				row.SystemModstamp_sf = datetime.strptime(row.SystemModstamp_sf, "%Y-%m-%dT%H:%M:%SZ")
				row.SystemModstamp = row.SystemModstamp_sf.strftime("%Y, %m, %d, %H, %M, %S")
				row.SystemModstamp = datetime.strptime(row.SystemModstamp, "%Y, %m, %d, %H, %M, %S")

	# LastViewedDate
			if row.LastViewedDate != None:
				row.LastViewedDate_sf = row.LastViewedDate.split('.')
				row.LastViewedDate_sf = row.LastViewedDate_sf[0] + 'z'
				row.LastViewedDate_sf = datetime.strptime(row.LastViewedDate_sf, "%Y-%m-%dT%H:%M:%SZ")
				row.LastViewedDate = row.LastViewedDate_sf.strftime("%Y, %m, %d, %H, %M, %S")
				row.LastViewedDate = datetime.strptime(row.LastViewedDate, "%Y, %m, %d, %H, %M, %S")

	# LastReferencedDate
			if row.LastReferencedDate != None:
				row.LastReferencedDate_sf = row.LastReferencedDate.split('.')
				row.LastReferencedDate_sf = row.LastReferencedDate_sf[0] + 'z'
				row.LastReferencedDate_sf = datetime.strptime(row.LastReferencedDate_sf, "%Y-%m-%dT%H:%M:%SZ")
				row.LastReferencedDate = row.LastReferencedDate_sf.strftime("%Y, %m, %d, %H, %M, %S")
				row.LastReferencedDate = datetime.strptime(row.LastReferencedDate, "%Y, %m, %d, %H, %M, %S")

# 1c. Query for data via Salesforce API and insert it into the "update" table
			cursor.execute(f'INSERT INTO FL_Accounts_Update({account_col}) values({account_val})', row.Id, row.IsDeleted, row.MasterRecordId, row.Name, row.Type, row.RecordTypeId, row.ParentId, row.BillingStreet, row.BillingCity, row.BillingState, row.BillingPostalCode, row.BillingCountry, row.BillingStateCode, row.BillingCountryCode, row.BillingLatitude, row.BillingLongitude, row.BillingGeocodeAccuracy, row.BillingAddress, row.ShippingStreet, row.ShippingCity, row.ShippingState, row.ShippingPostalCode, row.ShippingCountry, row.ShippingStateCode, row.ShippingCountryCode, row.ShippingLatitude, row.ShippingLongitude, row.ShippingGeocodeAccuracy, row.ShippingAddress, row.Phone, row.Fax, row.Website, row.PhotoUrl, row.Industry, row.NumberOfEmployees, row.Description, row.CurrencyIsoCode, row.OwnerId, row.CreatedDate, row.CreatedById, row.LastModifiedDate, row.LastModifiedById, row.SystemModstamp, row.LastActivityDate, row. LastViewedDate, row. LastReferencedDate, row. Jigsaw, row. JigsawCompanyId, row. AccountSource, row. SicDesc, row. is_active__c, row. txt_internal_id__c, row. txt_msa_number__c, row. Absence_Time_Survey_Resume_Date__c, row. txt_country_code__c, row. txt_address_line_1__c, row. Absence_Time__c, row. txt_zip_code__c, row. txt_addressee__c, row. txt_billing_addressee__c, row. txt_county__c, row. txt_email__c, row. int_total_employees__c, row. int_teachers__c, row. int_nces_operational_schools__c, row. txt_nces_agency_id__c, row. int_staff__c, row. txt_nces_county_number__c, row. txt_nces_state_agency_id__c, row. txt_nces_supervisory_union_id__c, row. int_sped_students__c, row. int_nces_operational_charter_schools__c, row. txt_attention__c, row. txt_billing_attention__c, row. int_lep_ell_students__c, row. txt_tx_region__c, row. txt_nces_school_id__c, row. txt_csm_region__c, row. int_504_students__c, row. int_rti_students__c, row. int_gifted_students__c, row. txt_agile_detailed_grade__c, row. txt_agile_district_type__c, row. txt_agile_file_type__c, row. txt_agile_grade_level__c, row. txt_agile_status__c, row. txt_agile_main_parent_name__c, row. txt_agile_nces_stateid__c, row. txt_agile_parent_name__c, row. txt_agile_record_type__c, row. txt_agile_school_type__c, row. int_agile_students_building__c, row. int_agile_students_district__c, row. int_agile_teachers_building__c, row. int_agile_teachers_district__c, row. primary_currency__c, row. tier__c, row. renewal_tier__c, row. Territory_Type__c, row. qualtrics__NPS_Date__c, row. Territory_ID__c, row. txt_locale__c, row. Vertical__c, row. Vertical_Sub_Type__c, row. qualtrics__Net_Promoter_Score__c, row. netsuite_url__c, row. Netsuite_Account_Name__c, row. BDR__c, row. Bill_To_ARR__c, row. CSM__c, row. Frontline_Central_Survey_Resume_Date__c, row. Frontline_Central__c, row. Integration_Error__c, row. MSA_Number__c, row. Ownership_Summary__c, row. Partner_ARR__c, row. Pause_Communication_Until__c, row. Professional_Growth_Survey_Resume_Date__c, row. Professional_Growth__c, row. Recruiting_Hiring_Survey_Resume_Date__c, row. Recruiting_Hiring__c, row. SEI_Sales_Rep__c, row. Special_Ed_Intervention__c, row. Special_Ed_Interventions_Survey_Resume__c, row. Sync_Status__c, row. Uplift__c, row. User_ARR__c, row. FIPS_Code__c, row. int_agile_district_school_admin__c, row. Platform__c, row. Phone_Agile__c, row. Website_Agile__c, row. Business_Solutions__c, row. platformID__c, row. SIS_Used__c, row. Agile_Building_Charter_Status__c, row. Platform_Tools__c, row. Test_Account__c, row. X18_Character_ID__c, row. Third_Party_Platform_ID__c, row. Third_Party_Platform_Tools__c, row. First_Signed_Date__c, row. Partner_Account__c, row. Aspex_Client_Code__c, row. SSO_Integration_Applications_Platform__c, row. Existing_Opportunities__c, row. Total_Weighted_Score__c, row. Count_of_Products__c, row. Founded__c, row. Mission__c, row. Leadership__c, row. Number_of_Customers__c, row. Acquisition_Information__c, row. Partnership_Information__c, row. Awards__c, row. Strengths__c, row. Weaknesses__c, row. Frontline_Opportunities__c, row. Frontline_Threats__c, row. Account__c, row. CSM_First_Assigned__c, row. Open_NCC_Opps__c, row. Platform_Project_Status__c, row. Platform_Live_Date__c, row. Open_Implementation_Projects__c, row. Platform_Stalled_Reason__c, row. emp_staf__c, row. Healthmaster_Sales_Overlay__c, row. Contract_Assigned_Accelify__c, row. Contract_Assigned_Healthmaster__c, row. Nurses_in_District_Agile__c, row. Nurses_in_School_Agile__c, row. NetSuite_Link__c, row. NCES_FTE_Count__c, row. Sales_Employee_Override__c, row. Total_Employees_new__c, row. cpqAnniversaryDate__c, row. resellerType__c, row. HM_Intacct_ID__c, row. Contract_Assignment_Accelify__c, row. Contract_Assignment_Healthmaster__c, row. ERP_Sales_Rep__c, row. SalesLoft1__Active_Account__c, row. ProgressBook_Sales_Rep__c, row. NCES_Total_Students__c, row. NCES_IEP_Students__c, row. NCES_ELL_Students__c, row. Consortium_Member__c, row. SSO_Identity_Provider__c, row. Health_Management__c, row. Has_Bill_To_ARR__c)
			print(Fore.WHITE + f"{index}: Records inserted into FL_Accounts_Update.")

	else:
		print('No Data')
	conn.commit()


# 1d. Update primary table. (Delete/Insert Records from "Update" table)
	cursor.execute(' DELETE FROM [Anywhere_Salesforce].[dbo].FL_Accounts WHERE Id IN (SELECT ID FROM [Anywhere_Salesforce].[dbo].Fl_Accounts_Update)')
	cursor.execute('INSERT INTO [Anywhere_Salesforce].[dbo].FL_Accounts SELECT * FROM [Anywhere_Salesforce].[dbo].FL_Accounts_Update')
	conn.commit()
	print(Fore.GREEN + f'\n Updated primary table "FL_Accounts"')
