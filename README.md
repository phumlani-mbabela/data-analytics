Sentiment Analysis of Tweets  
============================================================

Entailed in this project is a methodology for doing Data Analytics on twitter hashtags. All artifacts (code, algorithms and data) are released under Apache 2.
The artifacts are available at https://github.com/phumlani-mbabela/data-analytics .

The primary objective is to stream(download) tweets referencing the #ZumaMustFall hashtag, the secondary objective to is do data visualisation
 of the tweets. The traditional methods of processing such data are going to require a lot of coding(programming), as a result, 
 I choose to use the Cloudera Hadoop Ecosystem, the VM is available here. The cloudera VM comes equipped with the Hadoop Ecosystem. 
 This simplifies much of the work to be done, we need to use Flume to stream the tweets from twitter(Firehose)  and save the tweets to hdfs,
  after we are going to use Zeppelin do visualise the data using Hive.

In the second phase of this project we are going to use machine learning to determine a tweet’s sentiment, meaning if a tweet is 
positive or negative and apply statistical regression using Spark-ML.

The Hadoop Ecosystem(Cloudera)

Cloudera QuickStart virtual machines (VMs) includes “virtually” every application you need for Big Data or Machine Learning related tasks, 
cloudera is a our execution environment.

The Cloudera cluster comes pre-loaded with Spark, Impala, Crunch, Hive, Pig, Sqoop, Kafka, Flume, Kite, Hue, Oozie, DataFu, 
and many others (See a full list). In addition the cluster also comes with Python (2.6, 2.7, and 3.4), Perl 5.10, Elephant Bird, 
Cascading 2.6, Brickhouse, Hive Swarm, Scala 2.11, Scalding, IDEA, Sublime, Eclipse, and NetBeans. This is an ideal toolset for data hacking.


The articles defining the source code and data can be found at https://pmbabela.wordpress.com/big-data-articles/