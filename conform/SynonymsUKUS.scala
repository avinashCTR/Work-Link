package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToParquet, ParquetToDF}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}

object SynonymsUKUS extends BaseBlocks {

  var synonyms, ukus: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    ukus = synonyms
      .filter(col("variant")==="UK TO US")
      .select(lower(col("keyword")).as("uk"), lower(col("synonyms")).as("american"))
      .coalesce(1)
  }

  def save(): Unit = {
    DFToParquet.DFToParquet(ukus, setOutputPath("ukus"))
  }
}