package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToCSV,CSVToDF,ParquetToDF }
import ai.couture.obelisk.commons.io.HdfsUtils.{getListOfFiles, copy, rename}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

object ExtractExternalVariants extends BaseBlocks {

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

    var base_path = setOutputPath("external_variants", "ExternalVariants")
    
    DFToCSV.putDF(base_path,external)

    var part_file_name = getListOfFiles(base_path)
    .filter(_.getName != "_SUCCESS").map(x=>x.toString.split("/").last).toSeq.head

    rename(base_path+"/"+part_file_name, base_path+"/"+"ExternalVariants.csv")
  }
}