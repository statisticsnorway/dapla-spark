FROM openjdk:11-jre-slim

WORKDIR /app

COPY service/target/libs /app/lib/
COPY service/target/dapla-spark-service*.jar /app/lib/

CMD ["java", "-p", "/app/lib", "-m", "dapla.spark.service/no.ssb.dapla.spark.service.Application"]

EXPOSE 10120
EXPOSE 10128
