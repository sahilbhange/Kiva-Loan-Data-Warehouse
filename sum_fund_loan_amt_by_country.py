# -*- coding: utf-8 -*-
"""
Created on Tue May 22 22:23:02 2018

@author: Sandman
"""

# Get the sum funded_amount and loan_amount for each country and sort it by total funded amout in descending order and save data as csv file

# Input File
#kiva_loans_formated.csv -- (id,funded_amount,loan_amount,activity,sector,use,country_code,country,region,currency,partner_id,posted_time,disbursed_time,funded_time,term_in_months,lender_count,tags,borrower_genders,repayment_interval,date)

# Spark parameter setting for execution

# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 4 --executor-memory 1GB --packages com.databricks:spark-csv_2.10:1.4.0 src/main/python/kiva_code/sum_fund_loan_amt_by_country.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *

conf=SparkConf().setAppName("no-loan-by-cntry").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

kivaLoanDF = sqlContext.read.format('com.databricks.spark.csv').options(header='true',  inferschema='true').load('/user/sahilbhange/data/kiva/stg/part-00000')


# Load Spark data frame as Spark SQL table
kivaLoanDF.registerTempTable("kivaLoanTable")


# Spark SQL to find the to total_funded_amount and total_loan_amount by country

sqlRes = sqlContext.sql("select cast(sum(funded_amount) as  DECIMAL(18,2)) as total_funded_amount,cast(sum(loan_amount) as  DECIMAL(12,2))as total_loan_amount,country from  kivaLoanTable group by country order by total_funded_amount desc")



# Save the result file to the HDFS 
sqlRes.repartition(1).write.format('com.databricks.spark.csv').save('/user/sahilbhange/output/kiva/sum_fund_loan_amt',header = 'true')


