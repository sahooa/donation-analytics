import numpy as np
import datetime
import math
from decimal import Decimal

##Stream Data from the file one record at a time
def read_file(file_obj):
    while True:
        data = file_obj.readline()
        if not data:
            break

        yield data

##Parse date to check if the date is valid or not
def date_parse(date_str):
    try:
        datetime.datetime.strptime(date_str, '%m%d%Y')
    except ValueError:
        return False

##Get the Percentile value from the percentile file
def get_percentile():
    f = open('../input/percentile.txt', 'r')
    percentile = 0
    for value in f:
        percentile = value
    f.close()
    return percentile

#Open the Source file and output file
f = open('../input/itcont.txt', 'r')
file_write = open('../output/repeat_donors.txt', 'w')

#Create a Dictionary of customers and zip as KEY and VALUES as Year and Transcation Amount
# to capture the first transaction amount to be able to retrieve it when you get a repeat customer
unique_cust_zip_dict = {}

#Create a Dictionary of Campaign,Zip and Date as KEY and VALUES as Transaction Amount in a list format
campaign_zip_dt_dict = {}

#Variable to keep track of the record count to know the progress
count = 0

#Loop through each records in the file
for data in read_file(f):
    #Increment the recound count tracker
    count += 1
    print count
    #Get the Percentile value
    percentile = get_percentile()
    # Split the records
    row_val = data.split('|')
    #Map the records to the values
    CMTE_ID = row_val[0]
    ZIP_CODE = row_val[10][:5]
    NAME = row_val[7]
    TRANSACTION_DT = row_val[13]
    TRANSACTION_AMT = int(row_val[14])
    OTHER = row_val[15]
    #Discard records which doesnt fall in the criteria defined in requirements
    if (OTHER) or not(TRANSACTION_DT) or date_parse(TRANSACTION_DT) == False or not(ZIP_CODE) or len(ZIP_CODE) < 5 or not(NAME) or not(CMTE_ID) or not(TRANSACTION_AMT):
        print "Record Discarded"
        continue

    #Create a KEY combining ZIP_CODE and NAME to be used as KEY for the dictionary unique_cust_zip_dict
    zip_name = ZIP_CODE + NAME


    #Create a KEY combining ZIP_CODE,CMTE_ID and TRANSACTION_YEAR to be used for the dictionary campaign_zip_dt_dict
    campaign_zip_value = ZIP_CODE + CMTE_ID + TRANSACTION_DT[-4:]
    #Check if the ZIP_CODE and NAME exsist in the unique_cust_zip_dict and if not then load it to unique_cust_zip_dict
    if zip_name not in unique_cust_zip_dict:


        unique_cust_zip_dict[zip_name] = TRANSACTION_DT[-4:]
    #campaign_zip_dt_dict keeps track of the dollar amounts by ZIP_CODE,CMTE_ID and TRANSACTION_YEAR
        if campaign_zip_value in campaign_zip_dt_dict:
            campaign_zip_dt_dict[campaign_zip_value].append(TRANSACTION_AMT)
        else:
            campaign_zip_dt_dict[campaign_zip_value] = [TRANSACTION_AMT]

    #If the ZIP_CODE and NAME already exist then its a repeat customer which could lead to 2 scenarios as below
    #If the ZIP_CODE,CMTE_ID and TRANSACTION_YEAR exist in the campaign_zip_dt_dict then append the TRANSACTION_AMT from the current transaction to the list VALUE
    #If the ZIP_CODE,CMTE_ID and TRANSACTION_YEAR doesnt exist
        #Then we initialize it to the TRANSACTION_AMT
    elif zip_name in unique_cust_zip_dict:
        if campaign_zip_value in campaign_zip_dt_dict:
            campaign_zip_dt_dict[campaign_zip_value].append(TRANSACTION_AMT)
        else:
            campaign_zip_dt_dict[campaign_zip_value]= [TRANSACTION_AMT]



    # Calculate the percentile of the list values of the Dcitionary with Transaction amount
    #There is a Numpy function to calculate Percentile but wasnt sure that was working since i could see -ve values so wasnt sure so took the approach to write my own

        length_list = len(campaign_zip_dt_dict[campaign_zip_value])
        ordinal_rank = int(math.ceil((int(percentile) / 100.0) * length_list))
        percentile_no = sorted(campaign_zip_dt_dict[campaign_zip_value])[ordinal_rank - 1]
        #percentile_no=np.percentile(sorted(campaign_zip_dt_dict[campaign_zip_value]), percentile, interpolation='nearest')

        file_write.write("%s|%s|%s|%s|%s|%s\n" % (
        CMTE_ID, ZIP_CODE, TRANSACTION_DT[-4:],int(round(percentile_no)),int(round(reduce(lambda x,y: x+y,[float(j) for j in campaign_zip_dt_dict[campaign_zip_value]]))),
        len(campaign_zip_dt_dict[campaign_zip_value])))


f.close()
file_write.close()

test

