from pyspark.sql import SparkSession

sparkSession = SparkSession \
	.builder \
	.appName('Simple accumulator')\
	.getOrCreate()

total_acc = sparkSession.sparkContext.accumulator(0)

def main():
	
	sparkSession.sparkContext.setLogLevel('ERROR')

	rdd = sparkSession.sparkContext.parallelize([2, 4, 6, 8])

	def sum_fn(x):
		global total_acc
		total_acc += x
		print(x, type(total_acc))

	
	rdd.foreach(sum_fn)

	print('Result is:', total_acc.value)



if __name__ == '__main__':
	main()