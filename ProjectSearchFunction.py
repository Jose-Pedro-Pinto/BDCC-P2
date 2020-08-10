import pandas as pd
import google.cloud.bigquery as bq
from Project2_CloudAuth import google_cloud_authenticate


# print a summary bigQuery table
def print_summary(tableName):
    ds_id = '%s.%s' % (dataset_id, tableName)
    query = BQ_CLIENT.query(
        '''
        SELECT * FROM `%s` 
        LIMIT %s
        ''' % (ds_id, '20'))
    df = query.to_dataframe()
    print(df)


# set project parameters to use in google cloud
PROJECT_ID = "bigdata-269209"
google_cloud_authenticate(PROJECT_ID)
BQ_CLIENT = bq.Client(PROJECT_ID)

# set pandas display (print) parameters
pd.set_option("display.max_rows", 101)
pd.set_option("display.max_columns", 101)

dataset_id = PROJECT_ID

print(
    '''
    **************
    ** ICU INFO **
    **************
    ''')

print_summary("icu_manager.icu")

dataset_id = PROJECT_ID + ".bdcc1920_project_datasets"

print(
    '''
    **********************
    ** ICU YEAR SUMMARY **
    **********************
    ''')

print_summary("ICUYearSummaryView")

print(
    '''
    ***********************
    ** ICU MONTH SUMMARY **
    ***********************
    ''')

print_summary("ICUMonthSummaryView")

print(
    '''
    *********************
    ** ICU DAY SUMMARY **
    *********************
    ''')

print_summary("ICUDateSummaryView")

print(
    '''
    *************************
    ** ICU YEARLY PATIENTS **
    *************************
    ''')

print_summary("YearlyICUPatients")

print(
    '''
    **************************
    ** ICU MONTHLY PATIENTS **
    **************************
    ''')

print_summary("MonthlyICUPatients")

print(
    '''
    ************************
    ** ICU DAILY PATIENTS **
    ************************
    ''')

print_summary("DailyICUPatients")

print(
    '''
    ***************************
    ** ICU YEARLY ADMISSIONS **
    ***************************
    ''')

print_summary("YearlyICUHAdmissions")

print(
    '''
    ****************************
    ** ICU MONTHLY ADMISSIONS **
    ****************************
    ''')

print_summary("MonthlyICUHAdmissions")

print(
    '''
    **************************
    ** ICU DAILY ADMISSIONS **
    **************************
    ''')

print_summary("DailyICUHAdmissions")

print(
    '''
    ***************************
    ** ICU YEARLY CAREGIVERS **
    ***************************
    ''')

print_summary("YearlyICUCaregivers")

print(
    '''
    ****************************
    ** ICU MONTHLY CAREGIVERS **
    ****************************
    ''')

print_summary("MonthlyICUCaregivers")

print(
    '''
    **************************
    ** ICU DAILY CAREGIVERS **
    **************************
    ''')

print_summary("DailyICUCaregivers")

print(
    '''
    ******************************************
    ** ICU DAYS SPENT PER HOSPITAL ADMISSION**
    ******************************************
    ''')

print_summary("DaysSpentICU")

print(
    '''
    ************************
    ** ICU TOTAL WARNINGS **
    ************************
    ''')

print_summary("ICUWarnings")

print(
    '''
    **********************
    ** ICU TOTAL ERRORS **
    **********************
    ''')

print_summary("ICUErrors")

print(
    '''
    ****************************
    ** ICU MONTHLY ERROR INFO **
    ****************************
    ''')

print_summary("MonthlyICUErrors")

print(
    '''
    *******************
    ** Processed ICU **
    *******************
    ''')

print_summary("OneHotEncodedICU")