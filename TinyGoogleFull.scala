package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.io._
import org.apache.spark.rdd


object TinyGoogleFull {

  // class to use for counting words and storing in DataFrame
 case class WordCount(title: String, lineNum: Long, word: String)
 
/** Our main function where the action happens */
  def main(args: Array[String]) {
    //val dir = args(0) : String
     val dir = "/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books"

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    
    val sc = spark.sparkContext//new SparkContext("local[*]", "TinyGoogle")
    // Convert our csv file to a DataSet, using our case
    // class to infer the schema.
    import spark.implicits._

    /* fget list of files in a certain directory */
    // http://stackoverflow.com/questions/7425558/get-only-the-file-names-using-listfiles-in-scala
    //val files = getListOfFiles("/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books")

    //val dir = "/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books"
    val files = new File(dir).list
    // idea --> start here, and then go into the directory and look for that books
    // place its name in the first val of a tuple, every time
    
    /* get all files and concat into a DataFrame */
    val starter = sc.parallelize((Array(WordCount("N/A", -1, "starter_df"))))
    var accumulator = starter.toDF()
    var accumulatorGrouped = accumulator.groupBy('title, 'word).count()
    for (f <- files) {
      println(s"Filename is: $f")
      var path = dir + "/" + f
      println(s"Path to read is: $path")

      // function to read data in here, replacing my filename placeholder
      // with the actual filename

      /***************************
       * Core code to parse data
       * *************************/
      val book = "/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/DublinersbyJamesJoyce.txt"

      val lines = spark.sparkContext.textFile(book)
          // number each index
      val enumerated = lines.zipWithIndex.map{ case (e, i) => (i,e) }

      // map the file name over each
      val bookTitle = f : String
      val nameAndLine = enumerated.map { case (idx, line) => (bookTitle, idx, line) }

      // map lowercase and split over each line
      val splitWords = nameAndLine.map {
        case(fname, idx, line) => (fname, idx, line.toLowerCase().split("\\W+"))
      }

      // and do one more map to get: (fname, idx, word)
      val wordsAndLabels = splitWords.flatMap {
        case(fname, idx, wordList) => wordList.map{ w => WordCount(fname, idx, w) }
      }

      // transform into a DataFrame
      val df = wordsAndLabels.toDF()

      // groupby word and count
      val grouped = df.groupBy('title, 'word).count()

      /****************
       * End Parse file
       ****************/


      // concat the dataframes
      var intermediate  = accumulator.union(df)
      accumulator = intermediate

      var intermediateGrp = accumulatorGrouped.union(grouped)
      accumulatorGrouped = intermediateGrp
    }
    accumulator.show()
    accumulatorGrouped.show()
  }
}
