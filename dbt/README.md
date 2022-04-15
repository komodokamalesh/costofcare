## Cost of Care DBT FLOW

### General Description
-This is the documentation for the dbt flow of the [Cost of Care project](https://github.com/komodokamalesh/costofcare). The pipelines built by this flow pull price data from Komodo Health's snowflake server, combine price data with other attributes and perform QA/Unit Tests on the data pipelines. The aim of the dbt is to produce a workable data set of healthcare billings for analysis and modeling on another platform like Jupyterhub. 


### Flow:

The pipeline flow can be visualized with the dbt UI (see [dbt documentation](http://csprojects.khinternal.net/cost_of_care/dbt/#!/overview) for more details.)

Briefly, the flow starts with pulling encounters from a given range (see schema.yml) . It then combines those results with claim date, procedure, setting of care and allowed amounts from various tables (allowed_amount_table_2020_enhanced). These models are generally contained in models/extract

The flow also introduces some sampling, generating random samples of encounter keys of various lengths. These models are generally found in models/sampling. The sampling of keys are then combined with the enhanced encounters pulls to generate sample of enhanced encounters.

There is an additional branch to allow for comparison of medicare fees to allowed amounts. It starts at the "extract/med_proc_fee_pivot" model, then over several steps merges allowed amounts and medicare data and provides a summary. The final tables include "ranges" (<.75x, <3x) around the medicare fees which bound acceptable allowed amounts. Percent of allowed amounts falling outside these ranges are calculated, but no filter is applied. The summary models can be found under models/summaries

The medicare fee data is then combined with samples of enhanced encounters to provide sample data for modeling (models/extract/random_1M_model_v1_data, models/extract/random_01pct_model_v1_data)

#### Note on 'limit throttling'/'limit_level' (DEPRECATED)
How much of allowed_amount data is pulled by the pipeline is set by the 'limit_level' variable in the dbt_project.yml file. The 'allowed_amounts_table_2020.sql' model reads this variable and sets a limit on the number of records pulled equal to 10^(limit_level * 2). This means that a limit level of 1 will pull 100 records, 2 will pull 10000 records etc. A limit_level of 0 means no limit is placed on the number of records. 

The limit_level was introduced to have an easy way to limit the data throughout the entire flow. Since the flow is used to produce analysis and models it was not clear how much data was needed and how much would be easy to work with. Hence a throttle.

