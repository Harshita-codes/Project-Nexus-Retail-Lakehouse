FROM sbtscala/scala-sbt:eclipse-temurin-17.0.13_11_1.10.7_2.13.16 AS build
WORKDIR /workspace
COPY build.sbt ./
COPY project ./project
COPY 00_batch_helpers.scala ./
COPY src ./src
RUN sbt --batch test assembly

FROM apache/spark:3.5.1
USER root
RUN mkdir -p /opt/spark/app \
    && chown -R 185:0 /opt/spark/app
COPY --from=build --chown=185:0 /workspace/target/scala-2.12/nexus-batch.jar /opt/spark/app/nexus-batch.jar
USER 185:0
ENV BATCH_OUTPUT_FORMAT=delta
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--class", "com.nexus.batch.ProjectNexusBatchApp", "/opt/spark/app/nexus-batch.jar"]
