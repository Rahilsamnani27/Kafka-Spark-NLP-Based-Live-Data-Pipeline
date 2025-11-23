import pyspark
import pyspark.sql.functions as F
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline



class DataProcessing:
	def __init__(self):
		self.pipeline = self._build_pipeline()


	def _build_pipeline(self):
		document_assembler = DocumentAssembler().setInputCol("html_content") \
												.setOutputCol("document")

		document_normalizer = DocumentNormalizer().setInputCols(["document"]) \
												.setOutputCol("text") \
												.setAction("clean") \
												.setPatterns(["<[^>]*>"]) \
												.setReplacement(" ") \
												.setPolicy("pretty_all") \
												.setLowercase(True)

		title_extractor = RegexMatcher().setInputCols(["document"]) \
										.setOutputCol("title")\
										.setStrategy("MATCH_ALL") \
										.setRules(["<title>(.*?)</title>,TITLE"]) \
										.setDelimiter(",")

		paragraph_extractor = RegexMatcher().setInputCols(["document"]) \
											.setOutputCol("paragraphs") \
											.setStrategy("MATCH_ALL") \
											.setRules(["<p>(.*?)</p>,PARAGRAPH"]) \
											.setDelimiter(',')

		sentence_detector = SentenceDetector().setInputCols(["text"]) \
											.setOutputCol("sentences") \
											.setCustomBounds([","]) \
											.setExplodeSentences(False)

		tokenizer = Tokenizer().setInputCols(["sentences"]) \
								.setOutputCol("tokens")

		word_embeddings = WordEmbeddingsModel.pretrained("glove_100d", "en") \
											.setInputCols(["document", "tokens"]) \
											.setOutputCol("embeddings")

		tokenClassifier = BertForTokenClassification.pretrained("bert_ner_bert_ner_i2b2", "en") \
													.setInputCols(["text", "tokens"]) \
													.setOutputCol("bert")

		ner_converter = NerConverter().setInputCols(["text", "tokens", "bert"]) \
									.setOutputCol("ner_chunks")

		stages = [
				document_assembler, 
				document_normalizer, 
				title_extractor, 
				paragraph_extractor,
				sentence_detector, 
				tokenizer, 
				word_embeddings, 
				tokenClassifier, 
				ner_converter
			]

		return Pipeline(stages=stages)


	def transform(self, parsed_df):
		parsed_df = self.pipeline.fit(parsed_df) \
								.transform(parsed_df)

		clean_data = parsed_df.withColumn("title_cleaned",
									F.expr("transform(title.result, x -> regexp_replace(x, '<title>|</title>', ''))")
							).withColumn("paragraphs_cleaned",
									F.expr("transform(paragraphs.result, x -> regexp_replace(x, '<p>|</p>', ''))")
							).select(F.col("text.result")[0].alias("cleaned_text"),
								   F.col("ner_chunks.result").alias("ner_chunks"),
								   F.col("paragraphs_cleaned").alias("paragraphs"),
								   F.col("title_cleaned")[0].alias("title"),
								   F.col("url")
							).dropDuplicates(["cleaned_text"])

		print("in transform phase 1 ***************************************************************")

		clean_data = clean_data.withColumn("text",
									F.expr("aggregate(ner_chunks, cleaned_text, (acc, phrase) -> regexp_replace(acc, phrase, ''))")
								).withColumn("paragraphs_pii", 
									F.expr("transform(paragraphs, t -> aggregate(ner_chunks, t, (acc, phrase) -> regexp_replace(acc, concat('(?i)', phrase), '')))")
								)

		print("in transform phase 2 ***************************************************************")

		clean_data = clean_data.withColumn("ArticleMetadata",
									F.struct(F.col("url").alias("url"), 
											F.col("title").alias("title"))
								).withColumn('Article',
									F.struct(F.col("text").alias("text"), 
									F.col("paragraphs_pii").alias("paragraphs")
									)
								).select(F.to_json(F.struct(F.col("ArticleMetadata"), 
													F.col("Article"))).alias("value"))

		print("in transform phase 3 ***************************************************************")

		return clean_data
