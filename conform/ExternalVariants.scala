package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToParquet, ParquetToDF}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}

object ExternalVariants extends BaseBlocks {

  var synonyms, external: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {

    external = synonyms
      .filter(col("variant").isin(Seq("SPELL VARIANT", "STEM VARIANT", "BRAND EXPANSION"):_*))
      .withColumn("row_num", row_number().over(Window.partitionBy().orderBy("keyword")))
      .select(col("row_num"), lower(col("keyword")).as("word"),
              lower(col("synonyms")).as("rightword"), 
              lit(null).cast(StringType).as("_c3"),
              lit(null).cast(StringType).as("w_count"),
              lit(null).cast(StringType).as("r_count"),
              lit(null).cast(StringType).as("old_r_count"),
              lit(null).cast(StringType).as("soundex"),
              lit(null).cast(StringType).as("dropThese?"),
              lit(null).cast(StringType).as("RtoR"),
              lit(null).cast(StringType).as("Remarks"),
              lit("W2R").as("mapping_type")
              )
      .coalesce(1)

  }

  def save(): Unit = {
    DFToParquet.DFToParquet(external, setOutputPath("external_variants"))
  }
}