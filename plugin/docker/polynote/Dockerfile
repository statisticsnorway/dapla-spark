FROM bde2020/spark-base

RUN wget https://github.com/polynote/polynote/releases/download/0.2.13/polynote-dist.tar.gz
RUN tar xzf polynote-dist.tar.gz
RUN rm polynote-dist.tar.gz

RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar
RUN mv gcs-connector-hadoop2-latest.jar /polynote/gcs-connector-hadoop.jar

RUN apk update && apk add gettext && rm -rf /var/cache/apk/*

COPY docker/polynote/config.yml polynote/config-template.yml
COPY docker/polynote/docker-run-spark-env.sh /
RUN chmod +x docker-run-spark-env.sh

ENV SPARK_HOME=/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin

COPY target/lds-gsim-spark-*-shaded.jar /polynote/lds-gsim-spark.jar

EXPOSE 8192

CMD ["/docker-run-spark-env.sh", "polynote/polynote"]

