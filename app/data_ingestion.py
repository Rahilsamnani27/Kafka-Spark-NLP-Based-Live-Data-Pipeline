from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
from config import Config


class DataIngestion:
	def __init__(self, spark_session):
		self.spark = spark_session
		self.schema = StructType([
						StructField("url", StringType(), True),
						StructField("html_content", StringType(), True)
						])

	def read_stream(self):
		print('DataIngestion is Starting ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
		df = self.spark.readStream.format("kafka")\
								.option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS)\
								.option("subscribe", Config.KAFKA_TOPIC)\
								.option("startingOffsets", "earliest")\
								.option("consumerGroup", "Data-APP")\
								.load()
		
		df = df.selectExpr("CAST(value AS STRING) as json_str") \
							.select(F.from_json(F.col("json_str"), self.schema).alias("data")) \
							.select("data.*")

		return df
