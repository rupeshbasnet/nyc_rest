import pyspark

if __name__ == '__main__':
	sc = pyspark.SparkContext()
	RES_Data = 'hdfs:///data/share/bdm/nyc_restaurants.csv'
	df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(RES_Data)
	cusines = df.select('CUISINE DESCRIPTION')
	cusines = cusines.groupBy('CUISINE DESCRIPTION').count().orderBy('count', ascending=False)
	cusines.saveAsTextFile('output2')
