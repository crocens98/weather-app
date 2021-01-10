#!/usr/bin/env bash

./gradlew clean build fatJar

java -Dcom.sun.management.jmxremote.port=5555 \
       -Dcom.sun.management.jmxremote.authenticate=false \
       -Dcom.sun.management.jmxremote.ssl=false \
       -jar build/libs/kafka-streams-scaling-all.jar

