tbana
=====

tbana is a suite of connectors enabling several "big data" frameworks to acccess Splunk collected data.

With tbana, users of Hadoop, Cascading, Spark, and Storm can utilize data already collected in 
Splunk such as system log files, and other sources of "machine data," rather than setting up
completely new and parallel data integest systems.

tbana is designed for the non-Splunk system to perform the analytics, in contrast to other connectors that enable
Splunk to analyze data on non-Splunk systems.  In Hadoop, for instance, this is done by providing
an InputFormat that Hadoop MapReduce jobs can use. In effect, this opens up the data for
any higher-layer system that can utilize a Hadoop InputFormat such as Hive, Pig, etc. 

![My image](http://yolodata.github.io/images/tbana_architecture.png)

## Features

Currently, tbana supports the following:
* Apache Hadoop
* Cascading
* Apache Spark
* Twitter Storm

## Wish List

On the todo list are the following:
* Scalding
* Cascalog
* Spark Streaming
* Hive
* Pig

Some of these "should work" but are untested, or may need a thin wrapper.

## Data Retrieval 
Data from Splunk is retrieved in the following ways:
* Via a live search request using Splunk's REST API
* Via data from Splunk persisted in HDFS via Shuttl

Search requests to Splunk can be performed in the following ways:
* synchronous (via "export" mode)
* asynchronous (via "job" mode) 

### Splits and Parallel Transfers

When pulling live data from Splunk to MapReduce jobs, the InputFormat class has an option to split the data by indexer. 
Normally, a client to Splunk will access the cluster via the Search Head. The Search Head is responsible for
distributing searches to all the Indexers, and doing aggregating/reduce functions on the data before returning
to the client. In this case, the search head is bypassed in order to avoid a potential bottleneck in the search head.
Keep in mind that searches directly against the indexers may differ than from the search head, depending on how you've
configured Splunk. However, if you know that searches run directly against the individual Indexers are sufficient for 
your needs, you can use the Indexer Split scheme to do parallel transfers.


## Documentation

Visit the wiki for [build instructions and developer manual](https://github.com/yolodata/tbana/wiki)
