# Spark integration with LDS GSIM

[![Build Status](https://drone.prod-bip-ci.ssb.no/api/badges/statisticsnorway/lds-gsim-spark/status.svg?ref=refs/heads/develop)](https://drone.prod-bip-ci.ssb.no/statisticsnorway/lds-gsim-spark)

This project integrates spark with LDS and the GSIM model. It implements a new format so that users can read from and write to a dataset that exists in a LDS instance.

## Usage

The integration is implemented as a custom `gsim` (or `no.ssb.gsim.spark`) format. You can read and write datasets from a GSIM
LDS instance using a special URI with the scheme `lds+gsim`. 

```scala
// Read 
val latestDataset = spark.read.format("gsim")
  .load("lds+gsim:///b9c10b86-5867-4270-b56e-ee7439fe381e")
```

The complete URI format is as follow: 

`[lds+gsim://][host[:port]]/[prefix/][dataset id][#timestamp]`

The `host`, `port` and `prefix` parts of the URI are not supported yet. When reading a 
dataset, the `fragment` part of the URI is interpreted as a date-time in ISO-8601 format.
This date-time (together with the versioned nature of the LDS GSIM metadata) allows
 to read historical versions of a dataset:  

```scala
// Read dataset as of first of january 2000
val snapshotDataset = spark.read.format("gsim")
  .load("lds+gsim:///b9c10b86-5867-4270-b56e-ee7439fe381e#2000-01-01T00:00:00Z")
```



## Test it

The project contains Docker images with the Zeppelin and Polynote notebook UIs. 
They both includes the Google Cloud Storage connector and the gsim integration jar file. 

A docker compose file with a three-nodes spark cluster as well as zeppelin and polynote. 
When using the docker compose the environment variables can be set up in a `.env` file at the
root.   

```bash
# Compile the project
> mvn clean package
# edit env variables
> vim .env
> docker-compose up
```

In order to function, the following environment variables need to be sent to the 
zeppelin and polynote docker images.
*Note that if one of the oAuth variable is missing, the module with try to access 
the lds resources __without__ authentication*

|ENV Variable| Spark variable|Purpose|
|---|---|---|
|LDS_GSIM_SPARK_LOCATION|spark.ssb.gsim.location|Prefix used when writing data|
|LDS_GSIM_SPARK_LDS_URL|spark.ssb.gsim.ldsUrl|LDS url to use|
|LDS_GSIM_SPARK_OAUTH_TOKEN_URL|spark.ssb.gsim.oauth.tokenUrl|OAUTH token url|
|LDS_GSIM_SPARK_OAUTH_GRANT_TYPE|spark.ssb.gsim.oauth.grantType|OAUTH grant type|
|LDS_GSIM_SPARK_OAUTH_CLIENT_ID|spark.ssb.gsim.oauth.clientId|OAUTH client id|
|LDS_GSIM_SPARK_OAUTH_CLIENT_SECRET|spark.ssb.gsim.oauth.clientSecret|OAUTH client secret|
|LDS_GSIM_SPARK_OAUTH_USERNAME|spark.ssb.gsim.oauth.userName|OAUTH username|
|LDS_GSIM_SPARK_OAUTH_PASSWORD|spark.ssb.gsim.oauth.password|OAUTH password|

Using only docker, one can build and run zeppelin or polynote with the following commands:  

```bash
> # Compile the project
> mvn clean package

> # Build & start zeppelin
> docker build -t lds-zeppelin -f docker/zeppelin/Dockerfile .
> docker run -p 8080:8080 -e LDS_GSIM_SPARK_LDS_URL=https://lds-c.staging.ssbmod.net/ns/ \
                        -e LDS_GSIM_SPARK_LOCATION=gs://bucket/prefix/ \
                        -e LDS_GSIM_SPARK_OAUTH_TOKEN_URL=https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token \
                        -e LDS_GSIM_SPARK_OAUTH_GRANT_TYPE=password \
                        -e LDS_GSIM_SPARK_OAUTH_CLIENT_ID=lds-c-postgres-gsim \
                        -e LDS_GSIM_SPARK_OAUTH_USERNAME=api-user-3 \
                        -e LDS_GSIM_SPARK_OAUTH_PASSWORD=PASSWORD \
                        -v /full/path/to/sa.json:/gcloud/key.json lds-zeppelin

> # Build & start polynote
> docker build -t lds-polynote -f docker/polynote/Dockerfile .
> docker run -p 8192:8192 -e LDS_GSIM_SPARK_LDS_URL=https://lds-c.staging.ssbmod.net/ns/ \
                        -e LDS_GSIM_SPARK_LOCATION=gs://bucket/prefix/ \
                        -e LDS_GSIM_SPARK_OAUTH_TOKEN_URL=https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token \
                        -e LDS_GSIM_SPARK_OAUTH_GRANT_TYPE=password \
                        -e LDS_GSIM_SPARK_OAUTH_CLIENT_ID=lds-c-postgres-gsim \
                        -e LDS_GSIM_SPARK_OAUTH_USERNAME=api-user-3 \
                        -e LDS_GSIM_SPARK_OAUTH_PASSWORD=PASSWORD \
                        -v /full/path/to/sa.json:/gcloud/key.json lds-polynote


```

Open the zeppelin [interface](http://localhost:8080/).

Check that all the configuration starting with `spark.ssb.gsim` are setup correctly.

## Usage in spark

Read a dataset:

```scala
val personWithIncome = spark.read.format("no.ssb.gsim.spark")
    .load("lds+gsim://b9c10b86-5867-4270-b56e-ee7439fe381e")

personWithIncome.printSchema()

personWithIncome.show()
```

```sql
%sql
CREATE TEMPORARY VIEW personWithIncome
USING no.ssb.gsim.spark
OPTIONS (
  path "lds+gsim://b9c10b86-5867-4270-b56e-ee7439fe381e"
)
SELECT * FROM personWithIncome
```

```r
%r
pWithIncome <- sql("SELECT * FROM personWithIncome")
head(pWithIncome)
```

Write to a dataset. Note the mode.

```scala
val myProcessedDataset = Seq(
  ("hadrien", 42, "1", null, "0101", "Very good")
).toDF("PERSON_ID", "INCOME", "GENDER", "MARITAL_STATUS", "MUNICIPALITY", "DATA_QUALITY")

myProcessedDataset.write
  .format("no.ssb.gsim.spark")
  .mode("append")
  .save("lds+gsim://b9c10b86-5867-4270-b56e-ee7439fe381e");
```

## Diagrams

![Append to dataset](http://www.plantuml.com/plantuml/png/VL7DJiCm3BxxAQpSzm8x84qhJ4XmYYQatRBKgyAof4fSnkFZqB51ct6Bldn_jjcn9rprFOKMEM9hs6HY06Cv9jncIj2RnCwwtWH6jIFXUXVKN_YbqNC4D_hv5RN0HmKsUa-MNGmPrJU6rW-PAIaegNl9HRM9iPD2Qn-75hLKC1qfWD835m_uauvBVFmaEp315PBlYQ-mD4idV8_xCf3xC4opyAcewhEEfxwarSYJIONTaAUkP9sJ4z4jUhgKcLRy2hJ4LJxYGIxWNUQRWVn18XvIg4ehLIx5CT1vzBeV-LRA_aySxpmsm2VdAPWJGNKhrKjUKmwY_RMN-jalEKqCTE_z1G00)

![Docker deployment diagram](http://www.plantuml.com/plantuml/png/LP0nRmCX38Lt_mgBhOEb6wEkZPGE7IerMzN11OUWS6S4P6Yh_lV2eLEQ0Ga_VtuFKqEDWdkr5yde94NzccMfw0Bxp3E0ZNfrQ0wgle5FQrMgPlPYaCjs1xRjWjSY6HnN_kGYQ5xsRtWeOLx9w0e0d9ghuBUa934ir4Jy0KIhSzAb9s-jEx4apfUcSAxXrABmlGsIRzQqjZvwG2_l66yBMLqMwM-ZCplLD4XRu1TW7KMQ7kYMEZGb6dR_oZRJpi2thJip5FDyFBwQiMJ_1QGS_BdUdF4HEuAxQJVz0G00)

## Setup in DataProc

### Create DataProc cluster

To create the lds spark interface in DataProc use this (or similar) command:

```bash
gcloud beta dataproc clusters create staging-bip-lds-dataproc \
--enable-component-gateway \
--region europe-north1 \
--subnet dataproc-subnet \
--zone europe-north1-a\
--master-machine-type n1-highmem-4 \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-highmem-4 \
--worker-boot-disk-size 500 \
--image-version 1.4-debian9 \
--optional-components ZEPPELIN \
--project staging-bip \
--properties "dataproc:dataproc.conscrypt.provider.enable=false" \
--metadata 'PIP_PACKAGES=pandas' \
--initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh
```

This creates a cluster with the given name (e.g. staging-bip-lds-dataproc) according to the specifications.

**NOTE:** If this is the first DataProc cluster being created in the given GCP project (and the project is set up according to security best practices, disabling "default" networks/subnets), some network configurations have to be done. See <https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/network>.

The setting "enable-component-gateway" will make the Zeppelin web interface available from GCP console, but is not supported yet in Terraform, so the cluster needs to be created manually.
When <https://github.com/terraform-providers/terraform-provider-google/pull/4073> is merged, Zeppelin can be added as a "optional component". The branch <https://github.com/statisticsnorway/platform/tree/dataproc_lds_terraform_example> include a WIP example of the setup.

The property `dataproc:dataproc.conscrypt.provider.enable=false` is used to disable Conscrypt Java security provider to support lds-gsim-spark interpreter. With Conscrypt it will currently result in a bug in DataProc.

### Manually copy the lds-gsim-spark to the DataProc master node

The jar needs to be manually added to the master node.
Get the SSH and SCP command from the console, under VM Instances.

#### Add jar to master node: Alternative 1

Connect to the master VM Instance and fetch the latest jar from Nexus directly to the master node (find the right version & URL by using the Nexus GUI):

```bash
gcloud compute ssh --project "staging-bip" --zone "europe-north1-a" "lds-spark-gsim-m"
curl -O https://nexus.infra.ssbmod.net/repository/maven-snapshots/no/ssb/lds/lds-gsim-spark/1.0-SNAPSHOT/lds-gsim-spark-1.0-20190918.092747-15.jar
```

#### Add jar to master node: Alternative 2

Alternatively, build the jar locally and copy the jar to the master node (remember to replace &lt;version&gt; with the right version) before connecting to the master VM instance:

```bash
mvn clean package
gcloud compute scp --project "staging-bip" --zone "europe-north1-a" \
target/lds-gsim-spark-<version>.jar "lds-spark-gsim-m:"
gcloud compute ssh --project "staging-bip" --zone "europe-north1-a" "lds-spark-gsim-m"
```

#### Set permissions

While staying connected to the master node, copy the file to Zeppelin folder while renaming it (drop the version number) and change permissions (file owner and group):

```bash
sudo cp lds-gsim-spark-1.0-SNAPSHOT.jar /etc/zeppelin/lds-gsim-spark.jar
sudo chown zeppelin:zeppelin /etc/zeppelin/lds-gsim-spark.jar
```

### Add configuration to the Zeppelin interface

1. Go to Web Interfaces under DataProc Clusters in the GCP Console, click "Zeppelin" under "Component gateway". This will open up the Zeppelin interface.

2. Click the user menu at the right and select "Interpeter". Under "spark" interpeter, click "edit".

3. Under Dependencies, add "/etc/zeppelin/lds-gsim-spark.jar" as artifact.

4. Under "Properties", add the settings, with string values:

|Spark variable|Example|
|---|---|
|spark.ssb.gsim.location|gs://ssb-data-staging/data/|
|spark.ssb.gsim.ldsUrl|<https://lds.staging.ssbmod.net/ns/>|
|spark.ssb.gsim.oauth.tokenUrl|<https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token>|
|spark.ssb.gsim.oauth.clientId|lds-postgres-gsim|
spark.ssb.gsim.oauth.grantType|client_credential|
spark.ssb.gsim.oauth.clientSecret|*get this from keycloak or Ansible vault*|

Click Save and  OK for the dialog to restart the interpreter.

### Notebook storage

By default, notebooks are saved in Cloud Storage in the Cloud Dataproc staging bucket, which is specified by the user or auto-created when the cluster is created. The location can be changed at cluster creation time via the `zeppelin.notebook.gcs.dir` property.
