FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/out/jvm/scala-2.13.16/nexus-kafka-producer/nexus-kafka-producer.jar ./app.jar
COPY data ./data
USER 10001:10001
ENV EVENT_FILE_PATH=/app/data/events
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
