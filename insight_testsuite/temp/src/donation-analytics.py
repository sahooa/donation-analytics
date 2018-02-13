'''src_file = open('sample_file_raw','r')
line = src_file.readline()
while line:
    print line
    line = src_file.readline()'''
import datetime,math
from decimal import Decimal

def read_file(file_obj):
    while True:
        data = file_obj.readline()
        row_val = data.split('|')
        CMTE_ID = row_val[0]
        if not data:
            break


        yield data

def date_parse(date_str):
    try:
        datetime.datetime.strptime(date_str, '%m%d%Y')
    except ValueError:
        return False

def get_percentile():
    f = open('../input/percentile.txt','r')
    percentile = 0
    for value in f:
        percentile = value
    f.close()
    return percentile

f = open('../input/itcont.txt','r')
file_write = open('../output/repeat_donors.txt','w')
unique_cust_zip_dict = {}
campaign_zip_dt_dict = {}
count = 0
for data in read_file(f):
    count += 1
    print count
    percentile = get_percentile()
    row_val = data.split('|')
    CMTE_ID = row_val[0]
    ZIP_CODE = row_val[10][:5]
    NAME = row_val[7]
    TRANSACTION_DT = row_val[13]
    TRANSACTION_AMT = row_val[14]
    OTHER = row_val[15]

    if (OTHER) or not(TRANSACTION_DT) or date_parse(TRANSACTION_DT)==False or not(ZIP_CODE) or len(ZIP_CODE) < 5 or not(NAME) or not(CMTE_ID) or not(TRANSACTION_AMT):
        print "Record Discarded"
        continue

    ZIP_NAME = ZIP_CODE + NAME
    campaign_zip_value = ZIP_CODE + CMTE_ID + TRANSACTION_DT[-4:]
    if ZIP_NAME not in unique_cust_zip_dict :

        unique_cust_zip_dict[ZIP_NAME] = [TRANSACTION_DT[-4:],TRANSACTION_AMT]
    elif ZIP_NAME in unique_cust_zip_dict:
        #print " I am in"
        if campaign_zip_value in campaign_zip_dt_dict:

            campaign_zip_dt_dict[campaign_zip_value].append(TRANSACTION_AMT)
        else:
            if unique_cust_zip_dict[ZIP_NAME][0]==TRANSACTION_DT[-4:]:
                campaign_zip_dt_dict[campaign_zip_value]= [unique_cust_zip_dict[ZIP_NAME][1],TRANSACTION_AMT]
            else:
                campaign_zip_dt_dict[campaign_zip_value]=[TRANSACTION_AMT]


        length_list = len(campaign_zip_dt_dict[campaign_zip_value])
        ordinal_rank = int(math.ceil((int(percentile)/100.0)*length_list))
        percentile_no = campaign_zip_dt_dict[campaign_zip_value][ordinal_rank-1]
        file_write.write("%s|%s|%s|%s|%s|%s|%s \n" %(NAME,CMTE_ID,ZIP_CODE,TRANSACTION_DT[-4:],percentile_no,reduce(lambda x,y :x+y ,[int(j) for j in campaign_zip_dt_dict[campaign_zip_value]]),len(campaign_zip_dt_dict[campaign_zip_value])))

f.close()
file_write.close()