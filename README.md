# Google Analytics Data Unification

This dbt package is for the Google Analytics 4 data unification ingested by Native Google Analytics Connector that can be clubbed with other packages ingested through [Daton](https://sarasanalytics.com/daton/). 
[Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery

#### Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Google Analytics Native Connector Setup to Big Query
Daton Integrations for  
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your DBT project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/GoogleAnalytics
    version: v1.0.0
```

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_googleanalytics). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  bing_ads:
    +schema: custom_schema_extension
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the datetime columns from local timezone to given timezone, please mark the timezone_conversion_flag f as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours for each raw table

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "GoogleAnalytics.Brand_US_GoogleAnalytics_180494538_Events" : -7,
}

Note : Here, '-7' is given as the offset hour as per the time difference between UTC and PDT timezones. Provide the offset hour accordingly for each table based on your data.

```
### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
GoogleAnalyticsEvents: True
GoogleAnalyticsEventsEventParams: False
```

## Models

This package contains models from the Google Analytics 4 Native connector which includes reports {{at an event and event parameter level}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Performance | [GoogleAnalyticsEvents](models/BingAds/GoogleAnalyticsEvents.sql)  | This report provides long-term account performance and trends at an event level |
|Performance | [GoogleAnalyticsEventsEventParams](models/BingAds/GoogleAnalyticsEventsEventParams.sql)  | This report provides long-term account performance and trends at an event parameter level |


### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: GoogleAnalyticsEvents
    description: This report provides long-term account performance and trends at an event level
    config : 
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['event_timestamp','event_name','medium','user_pseudo_id','category','transaction_id']
      partition_by : { 'field': 'event_timestamp', 'data_type': int }
      cluster_by : ['event_timestamp','event_name']

  - name: GoogleAnalyticsEventsEventParams
    description: This report provides long-term account performance and trends at an event parameter level
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['event_timestamp','event_name','key','user_id','user_pseudo_id','string_value','int_value','double_value']
      partition_by : { 'field': 'event_timestamp', 'data_type': int }
      cluster_by : ['event_timestamp','event_name']


```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
