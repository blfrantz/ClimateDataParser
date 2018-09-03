# Introduction
This project aims to provide a simple example of the data analysis architecture described in the following video series: (link coming soon)

In short, it combines mongoDB with elasticsearch and kibana to deliver a scalable and easy end-to-end solution for general-purpose data exploration and analysis.

By way of example, this project will ingest monthly climate data published by NOAA here:
ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/v3/
It expects the following files from that FTP location to be saved to a 'data' folder next to these scripts:
- country-codes
- contents of the 6 ghcnm.[...].tar.gz files (no subfolders, just the files)

**ghcn_to_mongo.py**: Parses the GHCN data files into dictionaries and inserts them into mongoDB.  Though specific to this example, this may form a useful starting point for other simple ETL pipelines into mongo.

**mongo_to_es.py**: This is a generic utility for synchronizing data from mongo to elasticsearch.  It is very simple but could be easily used in other situations.  It does modify the mongo objects (adds a few "es_*" fields to track progress and errors, otherwise leaves them as-is).  It handles automatic updates of the mongo objects provided that whatever updates the mongo objects sets the "es_status" field to "updated."

Note that for more advanced or high performance situations, you may want to check out mongo-connector:
https://github.com/mongodb-labs/mongo-connector.  Unfortunately, its ongoing support is in question and it doesn't claim support for the latest mongoDB/elasticsearch versions.  Partly for this reason, I've chosen to write my own, much simpler, script.