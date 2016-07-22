# Dockerfile for Apache Spark application.
# FIXME: Need to automate copying the application JAR.

FROM analytics/hadoop-spark

CMD mkdir -p /app

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY provision/config.properties /app/config.properties

COPY sparkapp-wordcount-1.0-SNAPSHOT.jar /app/sparkapp-wordcount-1.0-SNAPSHOT.jar

ENTRYPOINT ["/app/entrypoint.sh"]
