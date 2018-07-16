# Introduction
This project aims to provide a simple example of the data analysis architecture described in the following video series:

In short, it combines mongoDB with elasticsearch and kibana to deliver a scalable and easy end-to-end solution for general-purpose data exploration and analysis.

By way of example, this project will ingest monthly climate data published by NOAA here:
ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/v3/

**ghcn_to_mongo.py**: Though specific to this example, this may form a useful starting point for other simple ETL pipelines into mongo.  It uses a fairly young ETL library called bonobo (https://www.bonobo-project.org/).

**mongo_to_es.py**: This is a generic utility for synchronizing data from mongo to elasticsearch.  It is very simple but could be easily used in other situations.  It does modify the mongo objects (adds a few "es_*" fields to track progress and errors, otherwise leaves them as-is).  It handles automatic updates of the mongo objects provided that whatever updates the mongo objects sets the "es_status" field to "updated."

Note that for more advanced or high performance situations, you may want to check out mongo-connector:
https://github.com/mongodb-labs/mongo-connector.  Unfortunately, its ongoing support is in question and it doesn't claim support for the latest mongoDB/elasticsearch versions.  Partly for this reason, I've chosen to write my own, much simpler, script.

## Setup