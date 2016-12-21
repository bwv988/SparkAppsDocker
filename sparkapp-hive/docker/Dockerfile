# Dockerfile for Apache Spark application.
# FIXME: Need to automate copying the application JAR.

FROM bwv988/ds-spark-base

CMD mkdir -p /app

COPY provision/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY provision/config.properties /app/config.properties

COPY sparkapp-hive-1.0-SNAPSHOT.jar /app/sparkapp-hive-1.0-SNAPSHOT.jar

ENTRYPOINT ["/app/entrypoint.sh"]
