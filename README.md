# Run Java-based Apache Spark applications with Docker

## Introduction

This repo contains a few examples that show how to develop and run Apache Spark applications in a Docker environment.

The code is organized into a number of Maven submodules; please consult the respective `README.md` files to learn more.

## List of examples

1. Word count in Apache Spark
2. Example with Spark SQL and Hive

## Building the examples
Firstly, let's build the maven modules & docker images:

```bash
mvn clean package
```

Then verify that images have been created:

```bash
docker images
```

## Implementation notes

* All examples will run without modification using my [Data Science playground for Docker](https://github.com/bwv988/datascience-playground).
* Java 8 [lambda expressions](http://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html) are used throughout the code.
* Docker images are build using Spotify's [Docker Maven plugin](https://github.com/spotify/docker-maven-plugin).


