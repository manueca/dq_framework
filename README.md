# scala-spark-projects

## Definition
---------------
This repository consists of all the artifacts related dq-framework code and related artifcats.

Define the process flow for the quality framework
Explain the technical stack and process flow   used.
Provide the Data models of the tables used in quality framework.

#Guiding priciples
Completeness

Accuracy

stability

consistency

tracability
## Purpose
Quality framework will be used to

capture data metrics and KPIS which is available in below environments
Hive,
Snowflake
Teradata
Athena
Based on the configuration provided in the config table for each metric below functionalities are achieved using the framework. 
Check if the metric is populated or not for a day. 
Calculate the average over a period of an year and check if the captured metric is within tolerance limit or not.
Calculate the standard deviation  over last year,last month,last quarter and last week  and check if the captured metric is within tolerance limit or not.
Calculate the forecasted value using SARIMA model and  check if the captured metric is within tolerance limit or not.
Tie back the metrics with source table using the concept of parent id.
