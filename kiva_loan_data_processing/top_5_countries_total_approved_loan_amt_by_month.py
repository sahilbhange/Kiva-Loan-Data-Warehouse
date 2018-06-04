# -*- coding: utf-8 -*-
"""
@author: Sandman
"""

# Get the top 5 countries for each month by total loan amount
# Which 5 coutries given the highest loan amount each month


# Input File
#/user/sahilbhange/output/kiva/formated_output/ -- (id,funded_amount,loan_amount,activity,sector,country,currency,partner_id,posted_time,disbursed_time,term_in_months,lender_count,borrower_genders,repayment_interval,date)

# Spark parameter setting for execution

# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 4 --executor-memory 1GB --packages com.databricks:spark-csv_2.10:1.4.0 src/main/python/kiva_code/top_5_countries_total_approved_loan_amt_by_month.py

from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *

conf=SparkConf().setAppName("top-5-cntry-loan-amt-mth").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)


kivaLoanDF = sqlContext.read.format('com.databricks.spark.csv').options(header='true',  inferschema='true').load('/user/sahilbhange/data/kiva/stg/part-00000')


# Load Spark data frame as Spark SQL table
kivaLoanDF.registerTempTable("kivaLoanTable")


# Spark SQL to find the top 5 countries for each month by total loan amount

sqlRes = sqlContext.sql('select country,mnth,total_loan_amt from (select country,mnth,total_loan_amt, rank() over (partition by mnth order by total_loan_amt desc) as rnk from (select country,mnth,sum(loan_amount) as total_loan_amt from (select country,concat(year(date),SUBSTRING(CAST(date AS VARCHAR(11)),6,2)) as mnth,loan_amount from kivaLoanTable) as liva_mnth group by country,mnth order by mnth,total_loan_amt desc) loan_amount_rnk order by mnth, total_loan_amt desc ) kiva_rank where rnk <=5')


# Save the result file to the HDFS 
sqlRes.repartition(1).write.format('com.databricks.spark.csv').save('/user/sahilbhange/output/kiva/top_5_countries_total_approved_loan_amt_by_month',header = 'true')
