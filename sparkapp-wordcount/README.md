# Apache Spark word count example
 
## Prerequisites
The below steps demonstrate how to run a dockerized Spark word count app using my Data Science playground for Docker <https://github.com/bwv988/datascience-playground>.
Please check there for further info and notes on required software packages.


```bash
# Clone and set up data science playground. 
git clone https://github.com/bwv988/datascience-playground.git
cd datascience-playground

# Start playground environment.
bin/playground.sh spark start

# In another terminal:
# Clone data set repo
git clone https://github.com/bwv988/datasets/

# Copy a data set file to the shared working directory.
cd datascience-playground
source bin/aliases.sh
sudo cp ../datasets/text/smaller.txt ~/ds-playground/workdir/
```


## Run app

```bash
docker run --rm --net dockercompose_default --volumes-from spark-master \
    sparkapp-wordcount \
    spark://spark-master:7077 \
    /workdir/smaller.txt \
    /workdir/wordcount-out
```

## Notes