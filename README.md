# tinyGoogle-SparkScala

Created two DataFrames we can query for our searches.

Heere's expected output of `TinyGoogleFull.scala`

`
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Filename is: AdventuresOfHuckleberryFinnByMarkTwain.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/AdventuresOfHuckleberryFinnByMarkTwain.txt
Filename is: AliceAdventuresinWonderlandbyLewisCarroll.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/AliceAdventuresinWonderlandbyLewisCarroll.txt
Filename is: BeowulfbyJLesslieHall.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/BeowulfbyJLesslieHall.txt
Filename is: DublinersbyJamesJoyce.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/DublinersbyJamesJoyce.txt
Filename is: LesMiserablesbyVictorHugo.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/LesMiserablesbyVictorHugo.txt
Filename is: LeviathanbyThomasHobbes.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/LeviathanbyThomasHobbes.txt
Filename is: MetamorphisisByFranzKafka.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/MetamorphisisByFranzKafka.txt
Filename is: PeterPanbyJMBarrie.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/PeterPanbyJMBarrie.txt
Filename is: PrideandPrejudicebyJaneAusten.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/PrideandPrejudicebyJaneAusten.txt
Filename is: TheAdventuresofSherlockHolmesbyArthurConanDoyle.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/TheAdventuresofSherlockHolmesbyArthurConanDoyle.txt
Filename is: TheAdventuresOfTomSawyerByMarkTwain.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/TheAdventuresOfTomSawyerByMarkTwain.txt
Filename is: TheCompleteWorksofWilliamShakespearebyWilliamShakespeare.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/TheCompleteWorksofWilliamShakespearebyWilliamShakespeare.txt
Filename is: TheJungleBookbyRudyardKipling.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/TheJungleBookbyRudyardKipling.txt
Filename is: TheYellowWallpaperbyCharlotteGilman.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/TheYellowWallpaperbyCharlotteGilman.txt
Filename is: UlyssesbyJamesJoyce.txt
Path to read is: /Users/tony/Documents/_LEARNINGS/CLOUD/_spark_tinyGoogle/books/UlyssesbyJamesJoyce.txt
+--------------------+-------+----------+
|               title|lineNum|      word|
+--------------------+-------+----------+
|                 N/A|     -1|starter_df|
|AdventuresOfHuckl...|      0|       the|
|AdventuresOfHuckl...|      0|   project|
|AdventuresOfHuckl...|      0| gutenberg|
|AdventuresOfHuckl...|      0|     ebook|
|AdventuresOfHuckl...|      0|        of|
|AdventuresOfHuckl...|      0| dubliners|
|AdventuresOfHuckl...|      0|        by|
|AdventuresOfHuckl...|      0|     james|
|AdventuresOfHuckl...|      0|     joyce|
|AdventuresOfHuckl...|      1|          |
|AdventuresOfHuckl...|      2|      this|
|AdventuresOfHuckl...|      2|     ebook|
|AdventuresOfHuckl...|      2|        is|
|AdventuresOfHuckl...|      2|       for|
|AdventuresOfHuckl...|      2|       the|
|AdventuresOfHuckl...|      2|       use|
|AdventuresOfHuckl...|      2|        of|
|AdventuresOfHuckl...|      2|    anyone|
|AdventuresOfHuckl...|      2|  anywhere|
+--------------------+-------+----------+
only showing top 20 rows


[Stage 20:>                                                         (0 + 0) / 2]
[Stage 17:>   (0 + 2) / 2][Stage 18:>   (0 + 2) / 2][Stage 19:>   (0 + 0) / 2]
[Stage 19:>   (0 + 2) / 2][Stage 20:>   (0 + 2) / 2][Stage 21:>   (0 + 0) / 2]
[Stage 19:==> (1 + 1) / 2][Stage 20:>   (0 + 2) / 2][Stage 21:>   (0 + 1) / 2]
[Stage 21:>   (0 + 2) / 2][Stage 22:>   (0 + 2) / 2][Stage 23:>   (0 + 0) / 4]
[Stage 24:>   (0 + 2) / 2][Stage 25:>   (0 + 2) / 2][Stage 26:>   (0 + 0) / 2]
[Stage 24:==> (1 + 1) / 2][Stage 25:>   (0 + 2) / 2][Stage 26:>   (0 + 1) / 2]
[Stage 26:>   (0 + 2) / 2][Stage 27:>   (0 + 2) / 2][Stage 28:>   (0 + 0) / 2]
[Stage 26:==> (1 + 1) / 2][Stage 27:>   (0 + 2) / 2][Stage 28:>   (0 + 1) / 2]
[Stage 27:==> (1 + 1) / 2][Stage 28:==> (1 + 1) / 2][Stage 29:>   (0 + 2) / 2]
[Stage 28:==> (1 + 1) / 2][Stage 29:>   (0 + 2) / 2][Stage 30:>   (0 + 1) / 2]
[Stage 30:==> (1 + 1) / 2][Stage 31:>   (0 + 2) / 2][Stage 32:>   (0 + 1) / 2]
[Stage 31:=========>        (1 + 1) / 2][Stage 32:>                 (0 + 2) / 2]
                                                                                

[Stage 84:================================================>      (88 + 4) / 100]
                                                                                

[Stage 101:========>                                             (76 + 4) / 500]
[Stage 101:===========>                                         (110 + 4) / 500]
[Stage 101:===============>                                     (149 + 4) / 500]
[Stage 101:====================>                                (196 + 4) / 500]
[Stage 101:========================>                            (232 + 4) / 500]
[Stage 101:============================>                        (273 + 4) / 500]
[Stage 101:==================================>                  (327 + 4) / 500]
[Stage 101:=======================================>             (372 + 4) / 500]
[Stage 101:============================================>        (416 + 4) / 500]
[Stage 101:===============================================>     (452 + 8) / 500]
[Stage 101:====================================================>(494 + 4) / 500]
                                                                                +--------------------+-----------+-----+
|               title|       word|count|
+--------------------+-----------+-----+
|                 N/A| starter_df|    1|
|AdventuresOfHuckl...|        big|   25|
|AdventuresOfHuckl...|      above|   18|
|AdventuresOfHuckl...|      clock|   21|
|AdventuresOfHuckl...|      doing|    7|
|AdventuresOfHuckl...|    vaguely|    4|
|AdventuresOfHuckl...|  clambered|    2|
|AdventuresOfHuckl...|     sprang|    2|
|AdventuresOfHuckl...|    mystery|    5|
|AdventuresOfHuckl...|remembering|    4|
|AdventuresOfHuckl...|    belfast|    3|
|AdventuresOfHuckl...|   distress|    2|
|AdventuresOfHuckl...| university|    4|
|AdventuresOfHuckl...|   strolled|    1|
|AdventuresOfHuckl...|  obscurely|    1|
|AdventuresOfHuckl...|     elbows|    5|
|AdventuresOfHuckl...|appointment|    2|
|AdventuresOfHuckl...|   weathers|   12|
|AdventuresOfHuckl...|    trivial|    1|
|AdventuresOfHuckl...|    thinned|    1|
+--------------------+-----------+-----+
only showing top 20 rows
`
