package ai.couture.obelisk.search.legos.history_jiomart

import ai.couture.obelisk.commons.io.{DFToParquet, HdfsUtils, ParquetToDF}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.commons.Constants.REGEX_PATTERN.IS_FLOAT
import ai.couture.obelisk.search.legos.history_jiomart.utils.HistoryUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.commons.text.StringEscapeUtils
import ai.couture.obelisk.commons.sql.UnionFunctions.union
import org.apache.spark.sql.types.IntegerType


/* 
 * This class is responsible for getting the top k queries from the history data with some transformations and cleaning.
 * 
 *  - input : history_data: DataFrame
 *  - output: cold_cache_queries: DataFrame ( save in cumulative and archive paths )
 * 
 *  - description:
 *   
 *   Cleanings done here:
 *   Filter only custom searches – Queries with "search_type" = "custom search" are retained.
 *   Decode HTML entities – Converts encoded characters like &#39; to '.
 *   Normalize spacing – Trims whitespace and replaces multiple spaces with a single space.
 *   Convert to lowercase – Ensures uniform case for better matching.
 *   Remove excessive character repetition – Queries with the same character repeated multiple times (e.g., "aaaaa" or "!!!!") are filtered out.
 *   Standardize punctuation – Replaces fancy quotes (“” → " and ‘’ → ').
 *   Ensure alphanumeric content – Queries must contain at least one letter or digit.
 *   Enforce length limits – Queries longer than 100 characters or that contain only numbers are removed.
 *   Split queries into words – Tokenizes the search term into a list of words.
 *   Limit word count – Queries with more than 8 words are filtered out.
 */
object GetColdCacheQueries extends BaseBlocks{

    var historyDataDF: DataFrame = _
    var topk: Int = _
    var searchTermColumn: String = _
    var freqColumn: String = _

    val decodeHtmlEntities = udf((query: String) => {
        if (query != null) StringEscapeUtils.unescapeHtml4(query) else null
    })

    override def setArguments(): Unit = {
        topk = setArgument("topk", 100)
        searchTermColumn = setArgument("search_term_column", "search_term")
        freqColumn = setArgument("freq_column", "total_searches")
    }

    def load(): Unit = {
        // "/data1/searchengine/rawdata/historydata/jiomart/dump/query_level_data"
        historyDataDF = ParquetToDF.getDF(setInputPath("history_data"))
    }

    def doTransformations(): Unit = {
        
        historyDataDF = historyDataDF
            .withColumn(searchTermColumn, decodeHtmlEntities(col(searchTermColumn)))
            .withColumn(searchTermColumn, regexp_replace(trim(col(searchTermColumn)), "\\s+", " "))
            .withColumn(searchTermColumn, lower(col(searchTermColumn)))
            .filter(!col(searchTermColumn).rlike("(.)\\1{3,}"))
            .withColumn(searchTermColumn, regexp_replace(col(searchTermColumn), "[“”]", "\""))
            .withColumn(searchTermColumn, regexp_replace(col(searchTermColumn), "[‘’]", "'"))
            .filter(col(searchTermColumn).rlike(".*[a-z0-9].*"))
            .filter(length(col(searchTermColumn)) <= 100 && !col(searchTermColumn).rlike(IS_FLOAT))
            .filter(!col(searchTermColumn).rlike("^\\d+(\\s\\d+)*$"))
            .withColumn("split_words", split(col(searchTermColumn), "\\W+"))
            .withColumn("size", size(col("split_words")))
            .filter(col("size") <= 8)
            .groupBy(searchTermColumn).agg(sum(freqColumn).as(freqColumn))
            .sort(desc(freqColumn))
            .limit(topk)
            .withColumnRenamed(searchTermColumn, "query")   // rename the column to "query"
            .withColumnRenamed(freqColumn, "freq")          // rename the column to "freq"
            .coalesce(1)

    }

    def save(): Unit = {

        /* 
        * directory structure:
        *   - Top100KQueriesJioMart
        *   - Top100KQueriesJioMart/Top100KQueriesJioMart_cummulative
        *   - Top100KQueriesJioMart/Top100KQueriesJioMart_archive/2021-06-01"
         */

        val date = setInputPath("history_data").split("/").reverse(1) // "2021-06-01"

        val baseOutputPath = setOutputPath("cold_cache_queries", "Top100KQueriesJioMart")
        val name = baseOutputPath.split("/").last
        val cummulativeOutputPath = baseOutputPath + "/" + name + "_cummulative"
        // "/data1/archive/tejkiran/Top100KQueriesJioMart/Top100KQueriesJioMart_cummulative"

        val archiveOutputPath =  baseOutputPath + "/archive" + "/coldCacheQueries_" + date

        HistoryUtils.saveWithCumulativeAndArchive(
        historyDataDF,
        cummulativeOutputPath,
        archiveOutputPath,
        Array("query"),
        Array("freq")
        )
    }

}