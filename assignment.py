import sys

import pyspark.sql.functions as func
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import abs
import matplotlib.pyplot as plt

filename = 'getstat_com_serp_report_201707.csv.gz'
url = 'https://stat-ds-test.s3.amazonaws.com/' + filename
spark = None
sqlContext = None

def printTitle(title):
	line1 = "****************************************************************\n"
	line2 = "*                                                              *\n"
	title = "*" + title.center(len(line1)-3) + "*\n"
	print line1 + line2 + title + line2 + line1

def startSparkSession():
	global spark, sqlContext

	printTitle("Opening Spark Connection")

	spark = SparkSession \
		.builder \
		.appName("GETSTAT") \
		.getOrCreate()

	sqlContext = SQLContext(spark)

def stopSparkSession():
	printTitle("Closing Spark Connection")
	spark.stop()

def convertCsvToParquet(url, filename):
	printTitle("Downloading CSV file... (may take a while)")
	spark.sparkContext.addFile(url)
	df = spark.read.csv(SparkFiles.get(filename), header=True)
	df = df.withColumnRenamed('Crawl Date', 'Crawl_Date')
	printTitle("Downloaded. Printing schema")
	df.printSchema()

	printTitle("Write to Parquet file")
	df.write.mode('overwrite').parquet("getstat.parquet")

def createTempView():
	printTitle("Read from Parquet file")
	parquetFile = spark.read.parquet("getstat.parquet")
	# Parquet files can also be used to create a temporary view and then used in SQL statements.
	parquetFile.createOrReplaceTempView("tempView")

def question1():
	question = """Which URL has the most ranks in the top 10 across all keywords over the period?"""
	printTitle("Question 1")
	print("Q: " + question)

	result = spark.sql("""
		SELECT URL,sum(case when rank<=10 then 1 else 0 end) as top_sum
		FROM tempView
		group by URL
		order by top_sum desc limit 1
	""")

	print("A: " + result.take(1)[0]['URL'])

def question2():
	question = """Provide the set of keywords (keyword information) where the rank 1 URL changes the most over
the period. A change, for the purpose of this question, is when a given keyword's rank 1 URL is
different from the previous day's URL."""
	printTitle("Question 2")
	print("Q: " + question)

	result = spark.sql("""
		SELECT keyword,market,location,device,
		case when URL != (lead(URL,1,null) over(partition by keyword,market,location,device
		   order by crawl_date)) then 1 else 0 end as change
		from tempView
		where Rank = 1
	""")

	result2 = result.groupBy(['keyword','market','location','device'])\
			   .agg(func.sum("change").alias("change_sum"))\
			   .orderBy("change_sum",ascending=False)
	print("A:")
	result2.show(10)

def question3():
	question = """We would like to understand how similar the results returned for the same keyword, market, and
location are across devices. For the set of keywords, markets, and locations that have data for
both desktop and smartphone devices, please devise a measure of difference to indicate how
similar the URLs and ranks are, and please use this to show how the mobile and desktop results
in our provided data set converge or diverge over the period."""
	printTitle("Question 3")
	print("Q: " + question)

	#dataset that have both desktop and smartphone intersect
	result = spark.sql("""
		SELECT a.* from tempView a
			inner join 
		(SELECT keyword,market,location
				FROM tempView
				WHERE device = 'desktop'
				intersect 
				SELECT keyword,market,location
				FROM tempView
				WHERE device = 'smartphone') b
		on a.keyword = b.keyword and a.market = b.market
	""")

	df_desktop = result.filter(col("device").isin(['desktop']))
	df_mobile = result.filter(col("device").isin(['smartphone'])).withColumnRenamed("Rank","Mobile_Rank")

	merged_df = df_desktop.join(df_mobile,['Keyword','Market','Crawl_Date','URL'])

	# Similarity score, absolute value of difference (diff column). Other similairty score could be used here to more accuracy
	merged_df = merged_df.withColumn('diff', abs(merged_df['Rank'] - merged_df['Mobile_Rank']))
	merged_df = merged_df.groupBy('Crawl_Date').mean('diff').sort('Crawl_Date')

	merged_df = merged_df.toPandas()
	x = merged_df['Crawl_Date']
	y = merged_df['avg(diff)']

	fig = plt.figure(figsize=(32, 8))
	plt.plot(x, y)
	plt.legend()
	fig.savefig('chart.png')
	print("A: Saved to chart.png")

if __name__ == '__main__':
	if len(sys.argv) != 2 or sys.argv[1] not in ['etl', 'q1', 'q2', 'q3', 'q123']:
		print("Usage: python assignment.py [etl|q123|q1|q2|q3]")
		sys.exit(-1)
	else:
		startSparkSession()
		command = sys.argv[1]
		if command == 'etl':
			convertCsvToParquet(url, filename)
		else:
			createTempView()
			if command == 'q1':
				question1()
			elif command == 'q2':
				question2()
			elif command == 'q3':
				question3()
			elif command == 'q123':
				question1()
				question2()
				question3()
		stopSparkSession()