#!/bin/sh
$JAVA_HOME/bin/java -Xmx512m -Xms512m -XX:+UseG1GC -jar parquet-manager-1.0.0-SNAPSHOT.jar $1 $2 $3
