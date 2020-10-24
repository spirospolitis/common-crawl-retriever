# Common Crawl retrieval and Elastic indexing

## Azure infrastructure

### *Spark* cluster setup

1.	Installation process

    1.1. *Spark* cluster setup

    Configure and deploy an *Azure* *HDInsights* *Spark* cluster. A minimal and cost-effective configuration includes the following resources:

    - Head nodes: 2 x D12 v2 (4 Cores, 28 GB RAM)
    
    - Worker nodes: 4 x D13 v2 (8 Cores, 56 GB RAM)

    1.2. SSH-ing into *Spark* head node:

    ```bash
    ssh <spark SSH user>@<Spark cluster name>-ssh.azurehdinsight.net
    ```

2.	Software dependencies

    2.1. Installing *Python* packages on the cluster.

    Scripts marked as **script-action** must be run as *Scripts Actions* on the *Azure* portal. For more info on *Script Actions*, please see https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux.

    2.1.1. Create *Python* virtual environment using *Conda*

    ```bash
    (script-action) sudo /usr/bin/anaconda/bin/conda create --prefix /usr/bin/anaconda/envs/<env name> python=3.5 anaconda --yes
    ```

    2.1.2. Install external *Python* packages in the created virtual environment.
    
    ```bash
    (script-action) sudo /usr/bin/anaconda/env/<env name>/bin/pip install <package name>==<version number>
    ```

    2.1.3. Change *Spark* and *Livy* configs and point to the created virtual environment.

    a. Open *Ambari* UI, go to Spark2 page, Configs tab.

    b. Expand Advanced livy2-env, add below statements at bottom.

    ```bash
    export PYSPARK_PYTHON=/usr/bin/anaconda/envs/<env name>/bin/python
    export PYSPARK_DRIVER_PYTHON=/usr/bin/anaconda/envs/<env name>/bin/python
    ```

    c. Expand Advanced spark2-env, replace the existing export PYSPARK_PYTHON statement at bottom.

    ```bash
    export PYSPARK_PYTHON=${PYSPARK_PYTHON:-/usr/bin/anaconda/envs/<env name>/bin/python}
    ```

    d. Save the changes and restart affected services. These changes need a restart of Spark2 service. 

    2.1.4. Use the new created virtual environment on *Jupyter*. Change *Jupyter* configs and restart *Jupyter*. Run script actions on all header nodes with below statement to point *Jupyter* to the new created virtual environment. After running this script action, restart *Jupyter* service through *Ambari* UI to make this change available.

    ```bash
    (script-action) sudo sed -i '/python3_executable_path/c\ \"python3_executable_path\" : \"/usr/bin/anaconda/envs/<env name>/bin/python3\"' /home/spark/.sparkmagic/config.json
    ```

    More info on the setup process on https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-python-package-installation

### *ElasticSearch* cluster setup

We opted for creating a self-managed *Elastic* cluster on *Azure* with the following characteristics:

- 3 x Standard D2s v3 (2 vcpus, 8 GiB memory) nodes

- 256 GB of RAM per node

- External load balancer for pushing data from *Spark* on *Elastic* with the use of the *ES-Hadoop* library


## Common Crawl sraping with Spark

### Being a good Common Crawl citizen (and avoiding 5xx errors!)

We have applied parallelization of HTTP requests to *Common Crawl* content endpoints. Each *Spark* worker spawns parallel HTTP requests, defined by the *cc.content.http.concurrency* parameter.

We have found that flooding the *Common Crawl* servers results in an increasing number of 5xx HTTP errors. In order to avoid this, the following formula roughly outlines the maximum number of parallel HTTP requests parameters that should be used to mitigate this:

- cc.content.http.concurrency: we have found that configuring 10 Spark workers * 100 parallel HTTP requests is a sweet spot for querying the *Common Crawl* content servers.

### Executing the Common Crawl retriever task on Spark

#### Building the application

**IMPORTANT**: Make sure that the *ES-Hadoop* connector has been downloaded from https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-7.9.0.zip and extracted under /lib/jars/.

SSH into Spark cluster head node and build the code:

```bash
make build
```

#### Migrating (copying) Common Crawl index files from S3 to Azure

```bash
cd dist && \
nohup /usr/bin/anaconda/envs/<your_env_name>/bin/python main.py \
--app.name "common-crawl-index-copier" \
--app.jobs "common-crawl-index-copier" \
--cp.cc.index.id <Common Crawl index identifier>
--cp.az.connection.string <your Azure connection string> \
--cp.az.destination.container <your Azure destination container> \
> common-crawl-index-copier.out &
```

#### Retrieve / Filter Common Crawl Index

```bash
cd dist && \
nohup spark-submit --master yarn --deploy-mode client --py-files common_crawl_retriever.zip main.py \
--app.name "common-crawl-index-retrieve" \
--app.jobs common-crawl-index-retrieve \
--spark.driver.cores "6" \
--spark.driver.memory "10g" \
--spark.executor.cores "10" \
--spark.executor.memory "20g" \
--spark.yarn.executor.memoryOverhead "3g" \
--spark.executor.instances "8" \
--spark.memory.offHeap.enabled True \
--spark.memory.offHeap.size "10g" \
--az.storage.account <your Azure storage account> \
--az.storage.in.container.name <your Azure input container name> \
--az.storage.out.container.name <your Azure output container name> \
--az.in.scope.file.name <your scope file name> \
--az.in.cc.index.file.path <path of the Common Crawl index files> \
--az.out.cc.index.file.name <name of the Common Crawl index files (parquet is implied> \
--az.out.cc.index.file.format <filtered Common Crawl index output file format> \
--cc.index.partitions <number of Spark RDD partitions> \
> common-crawl-index-retrieve.out &
```

#### Retrieve / Filter Common Crawl Content

```bash
cd dist && \
nohup spark-submit --master yarn --deploy-mode client --py-files common_crawl_retriever.zip main.py \
--app.name "common-crawl-content-retrieve" \
--app.jobs common-crawl-content-retrieve \
--spark.driver.cores "6" \
--spark.driver.memory "10g" \
--spark.executor.cores "10" \
--spark.executor.memory "15g" \
--spark.yarn.executor.memoryOverhead "3g" \
--spark.executor.instances "8" \
--spark.memory.offHeap.enabled True \
--spark.memory.offHeap.size "10g" \
--az.storage.account <your Azure storage account> \
--az.storage.in.container.name <your Azure input container name> \
--az.storage.out.container.name <your Azure output container name> \
--az.out.cc.index.file.name "cc_index" \
--az.out.cc.index.file.format "parquet" \
--az.out.cc.content.file.name "cc_content" \
--az.out.cc.content.file.format "parquet" \
--cc.index.partitions 100 \
--cc.content.partitions 1000 \
> common-crawl-content-retrieve.out &
```

#### Push to Elastic

```bash
cd dist && \
nohup spark-submit --master yarn --deploy-mode client --jars ../lib/jars/elasticsearch-hadoop-7.9.0/dist/elasticsearch-hadoop-7.9.0.jar --py-files common_crawl_retriever.zip main.py \
--app.name "common-crawl-elastic-push" \
--app.jobs common-crawl-elastic-push \
--spark.driver.cores "6" \
--spark.driver.memory "25g" \
--spark.executor.cores "2" \
--spark.executor.memory "20g" \
--spark.yarn.executor.memoryOverhead "3g" \
--spark.executor.instances "8" \
--spark.memory.offHeap.enabled True \
--spark.memory.offHeap.size "10g" \
--az.storage.account <your Azure storage account> \
--az.storage.in.container.name <your Azure input container name> \
--az.storage.out.container.name <your Azure output container name> \
--az.out.cc.content.file.name "cc_content" \
--az.out.cc.content.file.format "parquet" \
--cc.elastic.partitions 100 \
--es.nodes <ElasticSearch endpoint> \
--es.port <ElasticSearch port> \
--es.net.http.auth.user <ElasticSearch Basic HTTP auth username> \
--es.net.http.auth.pass <ElasticSearch Basic HTTP auth password> \
--es.resource <ElasticSearch index to write to> \
--es.input.json "no" \
--es.mapping.id "doc_id" \
--es.nodes.wan.only "yes" \
> common-crawl-elastic-push.out &
```

#### Watching the log

```bash
tail -f dist/main.out
```