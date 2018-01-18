//Ashika Avula 
//aavula@uncc.edu

Compile all java files in terminal. The following are the steps.
micro-wiki.out contains all output
micro-wiki_first100.out contains first 100 lines of output
--------------------------------------------------------------------------------------------------------------

First we have to create input and output paths in hdfs.

My input path is:
/home/cloudera/wordcount/input

My output path is:
/home/cloudera/wordcount/output

Now place input file one at each time of execuion in HDFS using following command.
Command:  hadoop fs -put /home/cloudera/graph1.txt /user/cloudera/wordcount/input
          hadoop fs -put /home/cloudera/graph2.txt /user/cloudera/wordcount/input
          hadoop fs -put /home/cloudera/wiki-micro.txt /user/cloudera/wordcount/input

We can check if the files are placed in our input hdfs location using the following command.
Command: hadoop fs -ls /user/cloudera/wordcount/input

In the same way we can check files in ouput by using following command.
Command: hadoop fs -ls /user/cloudera/wordcount/output
----------------------------------------------------------------------------------------------------------------

Google_PageRank.java execution as follows:

First Compile the java file and build class path	 
1. mkdir -p build
2. Java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/Google_PageRank.java -d build -Xlint

Next generate JAR file for that particular file.
3. jar -cvf google.jar -C build/ .

Everytime we execute we have to delete the output Folder.
4. hadoop fs -rm -r /user/cloudera/wordcount/output
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration0
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration1
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration2
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration3
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration4
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration5
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration6
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration7
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration8
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration9
   hadoop fs -rm -r /user/cloudera/wordcount/outputiteration10
   hadoop fs -rm -r /user/cloudera/wordcount/outputOutput_Sorted

Command for execution of JAR file
5. hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output

The final output is generated in this folder
6.  hadoop fs -cat /user/cloudera/wordcount/outputOutput_Sorted/*

To get the output into our local directory
7. hadoop fs -get /user/cloudera/wordcount/outputOutput_Sorted/part-r-00000 /home/cloudera/
part-r-00000 is the output file.

--------------------------------------------------------------------------------------------------------------------------------------


