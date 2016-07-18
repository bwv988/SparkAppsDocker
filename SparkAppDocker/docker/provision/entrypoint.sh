#!/bin/bash

# FIXME: This should be auto-generated based on the project name.

exec spark-class org.apache.spark.deploy.SparkSubmit --class com.warpbreaks.io.WordCount /app/sparkapp-docker-1.0-SNAPSHOT.jar $@
