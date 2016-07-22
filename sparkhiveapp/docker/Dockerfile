# Dockerfile for Apache Spark application.
# FIXME: Need to automate copying the application JAR.

FROM bwv988/hadoop-spark

CMD mkdir -p /app

COPY provision/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY provision/config.properties /app/config.properties

COPY sparkhiveapp-1.0-SNAPSHOT.jar /app/sparkhiveapp-1.0-SNAPSHOT.jar

ENTRYPOINT ["/app/entrypoint.sh"]
