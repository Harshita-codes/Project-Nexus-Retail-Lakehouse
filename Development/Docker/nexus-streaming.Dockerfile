FROM apache/spark:3.5.9 AS spark-distribution

FROM eclipse-temurin:17-jre

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
COPY --from=spark-distribution --chown=185:0 /opt/spark /opt/spark
WORKDIR /opt/project-nexus
COPY --chown=185:0 target/app.jar /opt/project-nexus/app.jar

RUN useradd --uid 185 --gid 0 --create-home --home-dir /home/spark --shell /usr/sbin/nologin spark
ENV HOME=/home/spark

USER 185:0
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
