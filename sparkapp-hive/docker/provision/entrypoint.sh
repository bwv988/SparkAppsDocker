#!/bin/bash

# FIXME: This should be auto-generated based on the project name.

export HIVEJARS=$(find /opt/hive/lib/ -name '*.jar' -print0 | sed 's/\x0/,/g')
exec spark-class org.apache.spark.deploy.SparkSubmit --jars ${HIVEJARS} --class com.warpbreaks.io.HiveApp /app/sparkapp-hive-1.0-SNAPSHOT.jar $@
