# parquet-manager
Helps to create Apache Parquet files from CSV without a full blown Hadoop cluster deployment. This looks like an ETL phase :)

On Windows OS, you'll need to install an Hadoop client; please check this thread for help: https://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path

Also, you'll have to define the HADOOP_HOME environment variable to make it work.

Usage:

```Bash
Syntax: parquet-manager [schema file] [data file in CSV format] [output file in Parquet format]

./parquet-manager chicagocrimes.avsc chicagocrimes.csv chicagocrimes.parquet
```

You may download the sample open data from here: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
(Portal: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2)
