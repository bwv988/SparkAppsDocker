# Apache Spark word count example
 
## Prerequisites

```bas
bin/sandbox.sh spark start
```
FIXME

## Run app

```bash
docker run --rm --net dockercompose_default --volumes-from spark-master \
    sparkapp-wordcount \
    spark://spark-master:7077 \
    /workdir/wordcount/smaller.txt \
    /workdir/wordcount-out
```

## Notes