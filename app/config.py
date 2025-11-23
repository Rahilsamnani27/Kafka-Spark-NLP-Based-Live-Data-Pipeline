import os
import pyspark
from pyspark.sql import SparkSession



class Config:
	KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
	SPARK_MASTER_URL = "spark://spark-master:7077"
	KAFKA_TOPIC = "data-pipeline"
	STORAGE_PATH = "./clean_data.parquet"

	@staticmethod
	def get_spark_session():
		spark_session = SparkSession.builder.master(Config.SPARK_MASTER_URL) \
										.appName('DataPipeline') \
										.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.4.1') \
										.config('spark.streaming.stopGracefullyOnShutdown', 'true') \
										.config("spark.driver.extraJavaOptions", "-Dhadoop.rpc.max.data.length=200000000") \
										.config("spark.driver.memory", "3g") \
										.config("spark.executor.memory", "3g") \
										.getOrCreate()

		return spark_session

