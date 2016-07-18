# Running a Java-based Apache Spark application in a Docker container

This is a quick example that shows how to run a Java-based Apache Spark application in a docker container.

## Building

```bash
mvn clean package docker:build
```

## Running
```bash
docker run --rm --net dockercompose_default --volumes-from spark-master sparkapp-docker spark://spark-master:7077 /spark-scratch/input.txt /spark-scratch/out
```