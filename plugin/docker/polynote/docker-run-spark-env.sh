#!/bin/sh
envsubst < /polynote/config-template.yml > /polynote/config.yml
env | grep SPARK | awk '{print "export \"" $0 "\""}' > ${SPARK_HOME}/conf/spark-env.sh
exec $@