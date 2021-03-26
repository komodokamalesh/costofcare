# Cost of Care

This repo is dedicated to analyzing and modeling medical billing costs in Komodo Health data. It is maintained by Kamalesh Rao and is open for viewing to anyone in the company.

## General Overview

### Project Goals

The project is an analysis of cost of care for medical encounters. The idea is to use the data gathered by Komodo to create estimates for costs for all encounters in the Komodo Health map and perhaps healthcare as a whole. With such estimates, a more complete picture of the healthcare cost landscape could emerge along with a better understanding of drivers of cost. 

### Repo Organization

Currently, the repo has two parts: 

1) a [dbt pipeline](https://github.com/komodokamalesh/costofcare/tree/main/dbt) to pull allowed amounts data from Snowflake. Additional pipelines could be built to supplement the data. 

1) [analysis notebooks](https://github.com/komodokamalesh/costofcare/tree/main/analysis) that are the main tool for data exploration and modeling. 

### Notes

#### TO DO

-Rename model for readability
-Redo comments on deprecated file
-fix sampling?
-rewrite readme	
-new QA (counts, etc)
