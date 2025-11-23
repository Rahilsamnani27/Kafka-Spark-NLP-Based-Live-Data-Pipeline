from data_ingestion import DataIngestion
from data_processing import DataProcessing
from config import Config



class Pipeline:
	def __init__(self):
		self.spark = Config.get_spark_session()
		self.data_ingestion = DataIngestion(self.spark)
		self.data_processing = DataProcessing()


	def process_batch(self, batch_df, batch_id):
		try:
			batch_df.write.format("parquet").mode("append").save(Config.STORAGE_PATH)
			print(f"Batch {batch_id} processed successfully")

		except Exception as e:
			print(f"Error in processing batch {batch_id}: {e}")


	def run(self):
		data = self.data_ingestion.read_stream()
		print(data)
		processed_data = self.data_processing.transform(data)

		query = processed_data.writeStream.outputMode("append") \
								.foreachBatch(self.process_batch) \
								.option("checkpointLocation", "checkpoint") \
								.start()
