# Data Partitioning Lab

## Overview

This project includes a Python script designed to optimize data querying by partitioning data based on the `pitime` field. The primary goal is to enhance the performance of SQL-like queries on large datasets, demonstrated through two specific query examples. This script is part of a lab exercise aimed at showcasing the benefits of data partitioning.

## Features

- **Data Partitioning**: Partitions data by date using the `pitime` attribute.
- **Query Optimization**: Optimizes the execution time of specific queries by leveraging partitioned data.
- **Performance Comparison**: Measures and compares query execution times before and after partitioning.

## Queries

The script focuses on optimizing the following queries:

1. Selecting specific fields within a certain time range.
2. Calculating the average of a specific field within a time range.

## Requirements

- Python 3.x
- JSON module (included in standard Python library)
- OS module (included in standard Python library)
- Datetime module (included in standard Python library)

# Results

The script demonstrates a significant decrease in query execution time after partitioning:

Query 1 execution time decreased from 3.7 seconds to 2.6 seconds.
Query 2 execution time results are also provided, showcasing the efficiency of data partitioning.