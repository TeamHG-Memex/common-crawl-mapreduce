#!/bin/bash
hadoop jar target/common-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar  \
com.hyperiongray.s3wordcount.WordCountJob  data/input output data/keywords/keywords 100

#com.hyperiongray.s3wordcount.WordCountJob s3n://darpa-memex/data/input/ s3n://darpa-memex/output s3n://darpa-memex/data/keywords/keywords.txt 1000000000


#hadoop jar target/common-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar  \
#com.hyperiongray.s3wordcount.WordCountJob s3n://darpa-memex/data/input/ s3n://darpa-memex/output https://s3-us-west-2.amazonaws.com/darpa-#memex/data/keywords/keywords.txt 1 10



#s3://darpa-memex/jar/common-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar

EMR
com.hyperiongray.s3wordcount.WordCountJob s3://darpa-memex/data/input/ s3://darpa-memex/output https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt 1 1000000


#https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt

x2go


hadoop jar target/common-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar  \
com.hyperiongray.s3wordcount.WordCountJob s3n://darpa-memex/data/input/ s3n://darpa-memex/output https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt 10

hadoop jar target/common-crawler-0.1-SNAPSHOT-jar-with-dependencies.jar  \
com.hyperiongray.s3wordcount.WordCountJob  data/input output https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt 100


10x c8.large (second cluster) first run (segments from 11 to 40)
com.hyperiongray.s3wordcount.WordCountJob s3://darpa-memex/data/input-10/ s3://darpa-memex/output-10x https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt 35 10000000

....now

10x c8.large (second cluster) second run
com.hyperiongray.s3wordcount.WordCountJob s3://darpa-memex/data/input-41-1000/ s3://darpa-memex/output-41-1000 https://s3-us-west-2.amazonaws.com/darpa-memex/data/keywords/keywords.txt 1050 10000000

