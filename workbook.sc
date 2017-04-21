// Plan:
// 1. make loading in all files in dir a function
// 2  make building df a function
// 3. concat all the df's
// 4. get to work. easier from there. and should be fast.


/* Read the file in, line by line */
// Create a SparkContext using the local machine
val sc = new SparkContext("local", "WordCountBetterSorted")

// Load each line of my book into an RDD
val book = "/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/DublinersbyJamesJoyce.txt"
val lines = spark.sparkContext.textFile(book)


// number each index
val enumerated = lines.zipWithIndex.map{ case (e, i) => (i,e) }
/* es8: Array[(Long, String)] =
Array((0,The Project Gutenberg EBook of Dubliners, by James Joyce),
(1,""), (2,This eBook is for the use of anyone anywhere at no cost and with almost),
(3,no restrictions whatsoever. You may copy it, give it away or re-use),
(4,it under the terms of the Project Gutenberg License included with this),
(5,eBook or online at www.gutenberg.org),
(6,""), (7,""), (8,Title: Dubliners), (9,"") */

// map the file name over each
val nameAndLine = enumerated.map { case (idx, line) => ("Filename", idx, line) }
/* Array[(String, Long, String)] =
Array((Filename,0,The Project Gutenberg EBook of Dubliners, by James Joyce),
(Filename,1,""),
(Filename,2,This eBook is for the use of anyone anywhere at no cost and with almost),
(Filename,3,no restrictions whatsoever. You may copy it, give it away or re-use),
(Filename,4,it under the terms of the Project Gutenberg License included with this),

(Filename,5,eBook or online at www.gutenberg.org) */

// map lowercase and split over each line
words = input.flatMap(x => x.split("\\W+"))

// this does it
var splitWords = nameAndLine.map {
  case(fname, idx, line) => (fname, idx, line.toLowerCase().split("\\W+"))
}
/*
res30: Array[(String, Long, Array[String])] =
Array((Filename,0,Array(the, project, gutenberg, ebook, of, dubliners, by, james, joyce)), */


// and do one more map to get: (fname, idx, word)

case class WordCount(title: String, count: Long, word: String)
var wordsAndLabels = splitWords.flatMap {
  case(fname, idx, wordList) => wordList.map{ w => WordCount(fname, idx, w) }
}
/*res31: Array[(String, Long, String)] = Array((Filename,0,the),
(Filename,0,project), (Filename,0,gutenberg), (Filename,0,ebook), (Filename,0,of),
(Filename,0,dubliners), (Filename,0,by), (Filename,0,james), (Filename,0,joyce)),
 Array((Filename,1,"")), Array((Filename,2,this), (Filename,2,ebook), (Filename,2,is),
 (Filename,2,for), (Filename,2,the), (Filename,2,use), (Filename,2,of),
 (Filename,2,anyone), (Filename,2,anywhere), (Filename,2,at), (Filename,2,no),
 (Filename,2,cost), (Filename,2,and), (Filename,2,with), (Filename,2,almost)),
 Array((Filename,3,no)*/

// get this to:
// (word (fname, idx))

// then
// (word (fname, [idxs]))
//
//var wordsAndLabelsWC = splitWords.map {
//  case(fname, idx, wordList) => wordList.map{ w => WordCount(fname, idx, w) }
//}
//wordsAndLabels.map{ case(title, count, word) => WordCount(title, count, word) }
/* transform to a dataframe */
// not quite right yet.  it's putting everything one column.
// remedy:: make a case class for this... then should be most of way there.
//   for an individual book. still need to read whole directory, one by one...
val df = wordsAndLabels.toDF()
df.head()
df.show()


// groupby word and count
scala> val grouped = df.groupBy('title, 'word).count()
/*
scala> grouped.show()
[Stage 13:>
+--------+----------+-----+
|   title|      word|count|
+--------+----------+-----+
|Filename| dubliners|    7|
|Filename|     hands|   50|
|Filename| stratagem|    1|
|Filename|    odours|    1|
|Filename|     treat|    4|
|Filename|  straight|    7|
|Filename|     notes|   10|
*/

// sort by most occurrences
rouped.sort('count.desc).show()
/*+--------+----+-----+
|   title|word|count|
+--------+----+-----+
|Filename| the| 4236|
|Filename|    | 2850|
|Filename| and| 2304|
|Filename|  of| 1986|
|Filename|  to| 1833|
|Filename|  he| 1714|
|Filename|   a| 1644|
|Filename| was| 1161|
|Filename| his| 1159|
|Filename|  in| 1112|
|Filename| had|  811|
|Filename| her|  793|
|Filename|   i|  784|
|Filename|said|  755|*/

/////////////////////////////////////////////
///// NEXT --> BUILD DF for all books ///////
/////////////////////////////////////////////

// Q:  Df.concat?
// Q:  How to make a function to pass to EACH file in DIR?
//     mapconcat?

/* fget list of files in a certain directory */
// http://stackoverflow.com/questions/7425558/get-only-the-file-names-using-listfiles-in-scala
val files = getListOfFiles("/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books")
val dir = "/Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books"
val files = new File(dir).list
// idea --> start here, and then go into the directory and look for that books
// place its name in the first val of a tuple, every time
