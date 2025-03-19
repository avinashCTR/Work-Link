package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToParquet, ParquetToDF}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}

object Hinglish extends BaseBlocks {

  var synonyms,hinglish: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    hinglish=synonyms
      .filter(col("variant")==="Hinglish")
      .select(lower(col("synonyms")).as("Word"),
              col("keyword").as("Hinglish Translation"), 
              lit("1").cast(StringType).as("is_hinglish")
             )
      .coalesce(1)
  }

  def save(): Unit = {
    DFToParquet.DFToParquet(hinglish, setOutputPath("hinglish"))
  }
}