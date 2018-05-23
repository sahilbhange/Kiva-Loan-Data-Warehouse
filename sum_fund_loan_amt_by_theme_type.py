# -*- coding: utf-8 -*-
"""
Created on Wed May 23 00:31:55 2018

@author: Sandman
"""
# Loan total funded amount and loan amount based on Loan Theme Type


# Input File
#kiva_loans_formated.csv -- (id,funded_amount,loan_amount,activity,sector,use,country_code,country,region,currency,partner_id,posted_time,disbursed_time,funded_time,term_in_months,lender_count,tags,borrower_genders,repayment_interval,date)

# Spark parameter setting for execution

# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 4 --executor-memory 1GB --packages com.databricks:spark-csv_2.10:1.4.0 src/main/python/kiva_code/sum_fund_loan_amt_by_theme_type.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *

conf=SparkConf().setAppName("sum-fund-loan-theme-type").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

# Load Kiva_loan.csv data
kivaLoanDF = sqlContext.read.format('com.databricks.spark.csv').options(header='true',  inferschema='true').load('/user/sahilbhange/data/kiva/stg/part-00000')


# Load Spark data frame as Spark SQL table
kivaLoanDF.registerTempTable("kivaLoanTable")

# Load Loan_theme_ids.csv data
loanThemeidsDf = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sahilbhange/data/kiva/loan_theme_ids.csv')

loanThemeidsDf.registerTempTable("loanThemeidsTable")

# Spark SQL to find the to total_funded_amount and total_loan_amount by laon theme type

sqlRes = sqlContext.sql("select cast(sum(funded_amount) as  DECIMAL(18,2)) as total_funded_amount,cast(sum(loan_amount) as  DECIMAL(12,2)) as total_loan_amount,lt.`Loan Theme Type` as Loan_Theme_Type  from kivaLoanTable kl left join loanThemeidsTable lt on kl.id=lt.id group by lt.`Loan Theme Type` order by total_funded_amount desc")

# Save the result file to the HDFS 
sqlRes.repartition(1).write.format('com.databricks.spark.csv').save('/user/sahilbhange/output/kiva/sum_fund_loan_themy_type',header = 'true')


