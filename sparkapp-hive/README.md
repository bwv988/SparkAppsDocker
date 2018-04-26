# Apache Spark + Hive example
 
## Prerequisites

This app demonstrates how to interact with the Hive container. 

A data set is loaded from HDFS and then stored into Hive as a table. 

## Data set
The data set in question is a list of Scarlatti keyboard sonatas and we are interested in figuring out how many sonatas were composed in a minor key vs. how many in a major key.

Again, I'm using my Data Science playground for Docker <https://github.com/bwv988/datascience-playground>.

```bash
# Clone and set up data science playground. 
git clone https://github.com/bwv988/datascience-playground.git
cd datascience-playground

# Start playground environment.
bin/playground.sh spark start

# In another terminal:
# Clone data set repo.
git clone https://github.com/bwv988/datasets/

cd datascience-playground
source bin/aliases.sh

# Create folder in HDFS & copy data into HDFS.
hadoop fs -mkdir /data

# Need to do this via exchange folder.
sudo cp ../datasets/scarlatti/scarlatti_sonatas.csv ~/ds-playground/workdir/
hadoop fs -put /workdir/scarlatti_sonatas.csv /data

# Check that all is good.
hadoop fs -ls /data
```

## Run app

```bash
ocker run --rm --net docker-compose_default --volumes-from spark-master sparkapp-hive  spark://spark-master:7077
```

## Notes
