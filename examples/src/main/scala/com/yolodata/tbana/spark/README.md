SplunkWordCountExample
======================

This is a basic word count example for using Spark against Splunk.

Version info
----------

* Scala 2.9.3
* Spark 0.7.3 (or Spark 0.8.0)
* Splunk 5.0.x
* Java 1.6.0

Later versions may or may not work. (as of Spark 0.8.0, Scala 2.10 is not supported) Earlier versions will likely not work.

##Getting Started

This section will walk you through how to run the example.

### Installing Splunk

You can download Splunk here:
> http://www.splunk.com/download

* Take the tgz, and unpack into your area of chosing. (aka $SPLUNK_HOME) 

* Go to $SPLUNK_HOME/bin, and run ./splunk start

* Accept the terms, then go to http://localhost:8000 to login

* Change your password for admin to "changeIt" (or whatever you'd like, but you need to change the code accordingly)

Splunk should now be ready for the example.

### Install Spark

You can download spark via information here:
> http://spark-project.org/docs/latest/index.html

Or you can clone the git repository:
> https://github.com/mesos/spark

Build spark, so you can execute:
> ./spark-shell

### Build tbana

You need to build tbana and the examples.

    gradle build
    gradle exampleJar

### Setup Environment

Before you do anything, it is *important* to ensure your environment is setup.

* Be sure to set $SPARK_HOME to your spark home location. For example: 

> export SPARK_HOME="/Users/dogbert/github/mesos/spark"

* Be sure to set $TBANA_HOME to your tbana home location. For example:

> export TBANA_HOME="/Users/dogbert/github/yolodata/tbana"

In <b>tbana/conf</b> there is a file <b>spark-env.sh</b>. To setup classpath:

> source spark-env.sh

### Running the example

To run the example via spark run:
> cd $TBANA_HOME;./spark-run com.yolodata.tbana.spark.SplunkWordCountExample

This will run in local mode. If you want to run against a cluster, then make the appropriate changes to the SparkContext constructor call.

### Using the spark-shell

You can also use the spark-shell repl by doing the following

* Go to $SPARK_HOME and run
> ./spark-shell

* Cut and paste the example code, everything except the main() method

* run by doing:
> scala> SplunkWordCountExample.run(sc)

### Resuts

* You should see something like this for the raw data:

    (26,[08-14-2013 16:51:52.160 -0700 INFO  Metrics - group=pipeline, name=typing, processor=regexreplacement, cpu_seconds=0.000000, executes=49, cumulative_hits=7996])
    (27,[08-14-2013 16:51:52.160 -0700 INFO  Metrics - group=pipeline, name=typing, processor=readerin, cpu_seconds=0.000000, executes=49, cumulative_hits=7996])
    (28,[08-14-2013 16:51:52.160 -0700 INFO  Metrics - group=pipeline, name=typing, processor=previewout, cpu_seconds=0.000000, executes=49, cumulative_hits=7996])
    ....


* Something like this for the word count tuples:

    (com,2)
    (mrsparkle_path,1)
    (520c0979161671ff0,50)
    (js,1)
    (quickdraw,1)
    (False,2)
    ....


