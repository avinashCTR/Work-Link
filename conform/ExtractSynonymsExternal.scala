package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToCSV,CSVToDF,ParquetToDF,DFToParquet}
import ai.couture.obelisk.commons.io.HdfsUtils.{getListOfFiles, copy, rename}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

object ExtractSynonymsExternal extends BaseBlocks {

  var synonyms, external: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    external = synonyms
    .filter(col("variant").isin(Seq("SYNONYMS", "HINGLISH SYNONYMS", "SUBSTITUTE"):_*))

    external = external
    .filter(col("keyword").isNotNull)
    .select(col("keyword").as("token"),  col("synonyms"))
    .union(external.filter(col("keyword").isNull).select(col("synonyms").as("token"),  col("synonyms")))
    .withColumn("token", lower(col("token")))
    .withColumn("synonyms", lower(col("synonyms")))
    .coalesce(1)
  }

  def save(): Unit = {

    var base_path=setOutputPath("synonyms_external","SynonymsExternal")

    var base_synonyms = setOutputPath("synonyms_base_path","Synonyms") 

    DFToCSV.putDF(base_path,external)

    // rename the part file to synonyms_external.csv
    var part_file_name = getListOfFiles(base_path)
    .filter(_.getName != "_SUCCESS").map(x=>x.toString.split("/").last).toSeq.head

    rename(base_path+"/"+part_file_name, base_path+"/"+"External Synonyms.csv")

    // copy the file to Synonyms folder
    copy(base_path+"/"+"External Synonyms.csv",base_synonyms +"/"+"External Synonyms.csv")
  }
}