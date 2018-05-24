# -*- coding: utf-8 -*-
"""
Created on Thu May 24 12:08:16 2018

@author: Sandman
"""

#  kiva_loans.csv file contains comma (,) in the column field value due to this Pyspark CSV reader package is unable to read data correctly

#  Thus, first load the data using the python pandas library and select the appropriate columns, handle the missing values and create new Spark data from Pandas dataframe

#  Operations are as below
# Input File
#kiva_loans.csv -- (id,funded_amount,loan_amount,activity,sector,use,country_code,country,region,currency,partner_id,posted_time,disbursed_time,funded_time,term_in_months,lender_count,tags,borrower_genders,repayment_interval,date)

# output File in gunzip format
# /user/sahilbhange/output/kiva/formated_output/part-00000.gz
#(id,funded_amount,loan_amount,activity,sector,country,region,currency,partner_id,posted_time,disbursed_time,term_in_months,lender_count,borrower_genders,repayment_interval,date)

# Spark parameter setting for execution

# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 3 --executor-memory 1G src/main/python/kiva_code/kiva_loans_data_preprocessing.py


import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#from pyspark.sql import *
#from pyspark.sql.functions import *

conf=SparkConf().setAppName("kiva-loan-file-cleaning").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

# load csv file data using pandas
kiva_loan_pdf=pd.read_csv("/home/sahilbhange/kiva_loan_data/kiva_loans.csv", encoding='utf-8',delimiter=',')

''' 
#pandas data pre-processing
#Null values in borrower_genders column
#>>> kiva_loan_pdf['borrower_genders'].isnull().sum()
#4221

#There are 4221 records with NULL value for field 'borrower_genders'
#Thus default NULL value as "NotAvailable"
'''

kiva_loan_pdf['borrower_genders']=kiva_loan_pdf['borrower_genders'].fillna("NotAvailable")
'''
# normalize the values in borrower_genders columns as below
# male - male
# female - female
# if male and female - group 
'''
kiva_loan_pdf['borrower_genders']=[elem if elem in ['female','male'] else 'group' for elem in kiva_loan_pdf['borrower_genders'] ]

'''
# There are 2396 records with NULL value for field 'disbursed_time'
# thus default missing disbursed_time with '1900-01-01 00:00:00+00:00'
# We can filter out default value records while querying the data
#>>> kiva_loan_pdf['disbursed_time'].isnull().sum()
#2396
# Default the missing values for disbursed_time with '1900-01-01 00:00:00+00:00'
'''

kiva_loan_pdf['disbursed_time']=kiva_loan_pdf['disbursed_time'].fillna("1900-01-01 00:00:00+00:00")

'''
#>>> kiva_loan_pdf['country_code'].isnull().sum()
#8

# 8 values for country_code field are NULL

# Find the corresponding coutry for NULL country_code value
#>>> kiva_loan_pdf[kiva_loan_pdf['country_code'].isnull()][['country','country_code']]
#        country country_code
#202537  Namibia          NaN

# Country Namibia has null values for country field
# Fill Namibia coutry code value as 'NA'
'''

kiva_loan_pdf['country_code']=kiva_loan_pdf['country_code'].fillna("NA")

# Select only required fields and create new pandas data frame
# Exclude the country code in new file as coutry and coutry code give same information
kivaLoan_req_fields = kiva_loan_pdf[['id','funded_amount','loan_amount','activity','sector','country','currency','partner_id','posted_time','disbursed_time','term_in_months','lender_count','borrower_genders','repayment_interval','date']]

sqlc=SQLContext(sc)

# Convert Pandas dataframe to Spark data frame
kivaLoan_SDF=sqlc.createDataFrame(kivaLoan_req_fields)


# Save the Spark data frame with the required fields to HDFS in gunzip format
# Save in overwrite mode in case of rerun
kivaLoan_SDF.repartition(1).write.format('com.databricks.spark.csv').option("codec", "org.apache.hadoop.io.compress.GzipCodec").save('/user/sahilbhange/output/kiva/formated_output/',header = 'true',mode='overwrite')


