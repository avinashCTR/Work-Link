package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToCSV,CSVToDF,DFToParquet,ParquetToDF}
import ai.couture.obelisk.commons.io.HdfsUtils.{getListOfFiles, copy, rename}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

object ExtractExternalVocabularyWords extends BaseBlocks {

  var synonyms, external: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    external = synonyms
      .filter(col("variant").isin(Seq("DEVNAGRI", "EXPANSION", "NUMERICAL", "SYMBOL"):_*))
      .withColumn("synonyms", explode(split(col("synonyms"), "\\|")))
      .select(lower(col("keyword")).as("externalword"), 
              lower(col("synonyms")).as("rightword"),
              lit(null).cast(FloatType).as("score"),
              lit(null).cast(IntegerType).as("rightword_count"),
              lit(null).cast(StringType).as("Drop"),
              col("variant")
              )
      .coalesce(1)
  }

  def save(): Unit = {
    DFToParquet.putDF(setOutputPath("external_vocabulary_words", "ExternalVocabularyWords"), external)
  }
}