package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToParquet, ParquetToDF}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}

object SynonymsExternal extends BaseBlocks {

  var synonyms, external: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    external = synonyms
    .filter(col("variant").isin(Seq("SYNONYMS", "HINGLISH SYNONYMS", "SUBSTITUTE"):_*))
    .filter(col("keyword").isNull).count(s)
    .filter(col("keyword").isNotNull).select(col("keyword").as("token"),  col("synonyms")).union(
    .filter(col("keyword").isNull).select(col("synonyms").as("token"),  col("synonyms")))
    .coalesce(1)
  }

  def save(): Unit = {
    DFToParquet.DFToParquet(external, setOutputPath("synonyms_external"))
  }
}