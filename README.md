# The Archive Monitor

**Use raw internet data to gauge your online media footprint**

  

[Google Slides Presentation for The Archive Monitor](https://docs.google.com/presentation/d/1o3R8Odrkqcwbpp6PAXOucX0fcK6NtDUzxhY4cG0X4Yc/edit#slide=id.g7c92e72691_0_132)

  

<hr/>

  

## How to install and get it up and running

This tool was tested on a computing cluster comprised of Elastic Compute Cloud (EC2) instances. Development utilized Python (v3.5.2) and execution required Apache Hadoop (v2.7.6) and Apache Spark 
(v2.4.0).  To start, clone the repository on the master node of your cluster, navigate to the `data-processing` directory, and type `python3 start.py`. You will be prompted to enter the master node 
Public DNS (IPv4), the term to be searched, and the time range of the search. Among other things, the program creates a bash script called `submit.sh` that can be executed to initiate the spark job.
  

<hr/>

  

## Introduction

This tool utilizes the distributed-processing capabilities of the Hadoop MapReduce paradigm via PySpark to compute the document frequency of a search term across the publicly accessible Common Crawl 
archive over a requested time range (restricted to the contiguous archive records from December 2016 to the present). For now, the search only supports and matches characters in the ASCII encoding 
standard.

The frequency analysis code was borrowed from the open-source cc-pyspark project, which is a collection of useful PySpark tools that process Common Crawl data.  The repository for cc-pyspark is located 
[here](https://github.com/commoncrawl/cc-pyspark).

## Architecture

The data pipeline links the Common Crawl archive on Amazon S3 to the PySpark processor, which, in turn, links to a PostgreSQL database that stores the output. This connects to Plotly's Falcon SQL 
Client, which visualizes the data. The result is then uploaded for display in a browser using Plotly Chart Studio.


![pipeline](images/pipeline.png)  

## Dataset

The dataset is the publicly accessible Common Crawl internet archive, stored on Amazon S3. It contains plain text information and metadata describing the HTTP responses from billions of webpages. The 
archive is stored in three formats and updated monthly. The monthly data is accessible as tens of thousands of compressed files (chunks), with the file paths collected in a master index.

The data is inconsistent. Monthly data is not contiguous prior to December 2016. The size of the collected data varies from month to month. The number of timestamps and number of files associated with 
a timestamp also vary over this period.



## Demonstration

I chose to analyze a small amount of archive data centered on the March 3, 2017 release of the critically acclaimed title *The Legend of Zelda: Breath of the Wild*, developed and distributed by Nintendo.
Approximately 10 GB of data was processed, and the runtime was about 52 minutes.

![Testcase Plot](images/testcase_plot.png)


## Engineering challenge

My challenge was a bottleneck that resulted from input scaling. The response was to first scale the cluster up (allocate more resources), but the improvements to the processing rate were marginal. Next 
I scaled the cluster out (added more workers), which proved to be more cost-effective.


## Cost Estimate

A conservative cost estimate was obtained for a 24-hour turnaround on the full data set available for the time range of the demo.

![130 TB Turnaround](images/130tb_turnaround.png)

Trends were discernible by processing only a fraction of the available data. Beyond modifications to the cluster, estimates were improved by filtering input, specifically, restricting the processing to 
1% of the available input (1.3 TB).

![1.3 TB Turnaround](images/1tb_turnaround.png)
