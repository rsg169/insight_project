# The Archive Monitor

**Use raw internet data to gauge your online media footprint**

  

[Google Slides Presentation for The Archive Monitor](https://docs.google.com/presentation/d/1o3R8Odrkqcwbpp6PAXOucX0fcK6NtDUzxhY4cG0X4Yc/edit#slide=id.g7c92e72691_0_132)

  

<hr/>

  

## How to install and get it up and running

This tool was tested on a computing cluster comprised of Elastic Compute Cloud (EC2) instances. Development utilized Python (v3.5.2) and execution used Apache Hadoop (v2.7.6) and Apache Spark (v2.4.0).  To start, clone the repository on your master node and type `python3 start.py`. Be sure to copy the master node Public DNS (IPv4) to your clipboard to respond to the prompt.

  

<hr/>

  

## Introduction

This tool utilizes the distributed-processing capabilities of Spark via the Python language to compute the term and document frequency of an entered search key across the publicly accessible Common Crawl archive over a time range requested by the user (limited from January 2017 to the present). For now, the search only supports characters in the ASCII encoding standard.

The baseline code was borrowed from the open-source cc-pyspark project, which is a collection of useful PySpark tools that process Common Crawl data.  The repository for cc-pyspark is located [here](https://github.com/commoncrawl/cc-pyspark).

## Architecture

The pipeline first links the compressed Common Crawl archives on Amazon S3 to the PySpark processor. The time and frequency output is stored in a PostgreSQL database, where it is retrieved by the Plotly Falcon SQL Client. From here, the data can be plotted and uploaded for display in a browser using Plotly Chart Studio.
  

## Dataset

Coming Soon

## Engineering challenges

Coming Soon

## Trade-offs

Coming Soon
