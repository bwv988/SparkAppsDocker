# Apache Spark + Hive example
 
## Prerequisites

```bash
# Start playground environment.
cd datascience-docker-sandbox
bin/sandbox.sh spark start

# In another terminal:
cd datascience-docker-sandbox
source bin/aliases.sh

# Clone data sets repo.
cd ..
git clone https://github.com/bwv988/datasets.git
cd datasets/csv

# Create folder in HDFS & copy data into HDFS.
hadoop fs -mkdir /data
# Need to do this via exchange folder.
sudo cp scarlatti_sonatas.csv ~/datascience-sandbox/workdir/
hadoop fs -put /workdir/scarlatti_sonatas.csv /data
```


## Run app

```bash
docker run --rm --net dockercompose_default \ 
--volumes-from spark-master \
sparkapp-hive \
spark://spark-master:7077
```

## Notes
