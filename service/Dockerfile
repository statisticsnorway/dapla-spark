#
# Copyright (c) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# 1st stage, build the app
FROM maven:3-jdk-11-slim as build

WORKDIR /helidon

# Create a first layer to cache the "Maven World" in the local repository.
# Incremental docker builds will always resume after that, unless you update
# the pom
ADD pom.xml .
ADD service/pom.xml ./service/pom.xml
ADD protobuf/pom.xml ./protobuf/pom.xml
RUN mvn package -DskipTests --no-transfer-progress --batch-mode -pl service -am

# Do the Maven build!
# Incremental docker builds will resume here when you change sources
ADD service/src     ./service/src
ADD protobuf/src    ./protobuf/src
RUN mvn package -DskipTests --no-transfer-progress --batch-mode -pl service -am

RUN echo "done!"

# 2nd stage, build the runtime image
FROM openjdk:11-jre-slim
WORKDIR /helidon

# Copy the binary built in the 1st stage
COPY --from=build /helidon/service/target/dapla-spark-service.jar ./
COPY --from=build /helidon/service/target/libs ./libs

CMD ["java", "-jar", "dapla-spark-service.jar"]

EXPOSE 1408