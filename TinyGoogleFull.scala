package com.you.package.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.io._
import scala.collection.mutable.ListBuffer
import scala.io._
import java.nio.charset.CodingErrorAction



object TinyGoogleFull {

  // class to use for counting words and storing in DataFrame
 case class WordCount(title: String, lineNum: Long, word: String)


  def main(args: Array[String]) {
        // Logger should only print ERROR level or higher
        Logger.getLogger("org").setLevel(Level.ERROR)

        // Use SparkSession interface (must have Spark 2.0+)
        val spark = SparkSession
          .builder
          .appName("SparkSQL")
          .master("local[*]")
          .getOrCreate()

        val sc = spark.sparkContext//new SparkContext("local[*]", "TinyGoogle")
        import spark.implicits._

        // handle command line input
        try{
                  // -i --> created the inverted index
                  // -s --> search for given search term
                  var command = args(0) : String

                  //get the directory of files from command line args
                  var dir = args(1) : String
                  if(command == "-i"){
                        val files = new File(dir).list
                        var num_of_files = files.length

                        /* get all files and concat into a DataFrame */
                        val starter = sc.parallelize((Array(WordCount("N/A", -1, "starter_df"))))
                        var accumulator = starter.toDF()
                        //var accumulatorGrouped = accumulator.groupBy('title, 'word).count()
                        for (f <- files) {
                          println(s"Filename is: $f")
                          var path = dir + "/" + f
                          println(s"Path to read is: $path")

                          /***************************
                           * Core code to parse data
                           * *************************/

                          var lines = spark.sparkContext.textFile(path)

                          // number each index
                          var enumerated = lines.zipWithIndex.map{ case (e, i) => (i,e) }

                          // map the file name over each
                          var bookTitle = f : String
                          var nameAndLine = enumerated.map { case (idx, line) => (bookTitle, idx, line) }

                          // map lowercase and split over each line
                          var splitWords = nameAndLine.map {
                            case(fname, idx, line) => (fname, idx, line.toLowerCase().split("\\W+"))
                          }

                          // and do one more FlatMap to get: WordCount(fname, idx, word)
                          var wordsAndLabels = splitWords.flatMap {
                            case(fname, idx, wordList) => wordList.map{ w => WordCount(fname, idx, w) }
                          }

                          // transform into a DataFrame
                          var df = wordsAndLabels.toDF()

                          // groupby word and count
                          var grouped = df.groupBy('title, 'word).count()

                          /****************
                           * End Parse file
                           ****************/

                          // concat the dataframes
                          var intermediate  = accumulator.union(df)
                          accumulator = intermediate

                        }

                        //save inverted_index
                        accumulator.write.json("inverted_index")


              }//end if -i statement
              if(command=="-s"){
                  try{
                      //read in the inverted index
                      val accumulator = spark.read.json("inverted_index")

                      //get the search term
                      var search = args(2) : String

                      //list of the search phrase
                      var search_list = search.split(" ")

                      var top_books = new ListBuffer[String]()


                      // for every word in the list of search terms
                      for(word <- search_list){
                        // query the count for each word in each file
                        val temp_data_count = accumulator.filter(accumulator("word") === word).groupBy('title,'word).count().sort('count.desc).take(3)

                        // query the line number for the word in each file
                        val temp_data_line = accumulator.filter(accumulator("word") === word).groupBy('title,'word).min()

                        for(rec <- temp_data_count){
                          // filter to get the line number for the specific file
                          var line_num = temp_data_line.filter(temp_data_line("title")===rec(0))

                          // create a string separated by colons, which has such format
                          // word : title : count : linenum
                          var temp_str = word +" : "+ rec(0).toString() + " : " + rec(2).toString() + " : " + (line_num.head()getLong(2)).toString

                          // add that to list of all the results
                          top_books += temp_str
                        }
                      }


                      /**************
                       *Print Results
                       **************/
                      var res = 1

                      for(record <- top_books){
                        var temp_list = record.split(" : ")

                        // print the search term
                        if(res == 1){
                            println("\n===================================")
                            print("SEARCH TERM-->" + "'" + temp_list(0) +"'")
                            println("\n===================================")
                        }

                        // tell what number the result is
                        println("RESULT " + res +" for search term --> " + "'" + temp_list(0) + "'" +  " : ")

                        print("TITLE : " + temp_list(1))
                        println("\t\t>>Word Occurences : " + temp_list(2) + "<<\n")

                        // find the context for the word in the book
                        val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
                        val lines = scala.io.Source.fromFile(dir+temp_list(1))(decoder).getLines

                        // get the lines from the book
                        if((temp_list(3).toInt)>=2){
                          var context = lines drop((temp_list(3).toInt)-2)
                            // print the context
                            println("Context : \n\n" + context.next())
                            println("\n" + context.next())
                            println("\n>>" + context.next())
                            println("\n" + context.next())
                            println("\n" + context.next())
                            println("\n------------------------------------")
                        }
                        else if(temp_list(3).toInt==1){

                          var context = lines drop((temp_list(3).toInt)-1)
                          // print the context
                            println("Context : \n\n" + context.next())
                            println("\n>>" + context.next())
                            println("\n" + context.next())
                            println("\n" + context.next())
                            println("\n------------------------------------")
                        }
                        else if(temp_list(3).toInt==0){

                          var context = lines drop((temp_list(3).toInt))
                          // print the context
                          println("Context : \n\n" + ">>" + context.next())
                          println("\n" + context.next())
                          println("\n" + context.next())
                          println("\n------------------------------------")
                        }




                        res+=1
                        // Stop after 3 iterations. We are only showing Top 3.
                        if(res==4){
                          res = 1
                        }
                      }
                  }//end try for whether the index is created or not
                  catch{
                    case e:  org.apache.spark.sql.AnalysisException => println("Please create an inverted index before you try to search for keywords\n by adding the -i command, and the path to books directory.")
                  }
                  finally{

                  }




              }//end if statement for -s command

        }//end try statement for command type
        catch{
          case e: java.lang.ArrayIndexOutOfBoundsException => println("Please specify command type, as the first argument in form: \n\t-i for the creation of inverted index\n\t-s to search for keyword. \nPlease also specify the path to the directory where the files are as the second argument.")
        }
  }//end main
}
