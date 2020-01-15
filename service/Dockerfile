FROM openjdk:11-jre-slim

WORKDIR /app

COPY service/target/libs /app/lib/
COPY service/target/dapla-spark-service.jar /app/lib/

CMD ["java", "-cp", "/app/lib/*", "no.ssb.dapla.spark.service.Application"]

EXPOSE 1408