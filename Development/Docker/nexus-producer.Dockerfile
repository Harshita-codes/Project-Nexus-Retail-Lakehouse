FROM sbtscala/scala-sbt:eclipse-temurin-17.0.13_11_1.10.7_2.13.16 AS build
WORKDIR /workspace
COPY ["Data Ingestion/Producer/build.sbt", "./build.sbt"]
COPY ["Data Ingestion/Producer/project", "./project"]
COPY ["Data Ingestion/Producer/src", "./src"]
RUN sbt --batch assembly

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=build --chown=10001:10001 /workspace/target/out/jvm/scala-2.13.16/nexus-kafka-producer/nexus-kafka-producer.jar ./app.jar
COPY --chown=10001:10001 ["Datasets/streaming/order-events", "./data/events"]
USER 10001:10001
ENV EVENT_FILE_PATH=/app/data/events
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
