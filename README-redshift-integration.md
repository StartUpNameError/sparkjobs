# Amazon Redshift integration for Apache Spark

The connection between Amazon Redshift and Apache Spark is done
through the [RedshiftData Source for Apache Spark - Community Edition](https://github.com/spark-redshift-community/spark-redshift?tab=readme-ov-file). 
Such taks can be accomplished using the following instructions.

## Instructions

### 1. Download Jars

* **1.1** Download Amazon ZIP file. Amazon provides a ZIP file containing every needed jar at 
https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-install.html.
Just download and uzip.

```
cd $SPARK_HOME/jars
wget https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.29/redshift-jdbc42-2.1.0.29.zip
unzip redshift-jdbc42-2.1.0.29.zip
rm redshift-jdbc42-2.1.0.29.zip
```


* **1.2** It is posible you still need to install the following jars available at 
the Maven Central repository:

    - aws-java-sdk-s3-1.12.741.jar
    - aws-java-sdk-dynamodb-1.12.741.jar


```
cd $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.741/aws-java-sdk-s3-1.12.741.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.741/aws-java-sdk-dynamodb-1.12.741.jar
```


### 2. Use the RedshiftData Source for Apache Spark - Community Edition
    

* **2.1** Init Spark with the ``packages`` option set to ``com.amazon.redshift:redshift-jdbc42:2.1.0.24,org.apache.spark:spark-avro_2.12:3.5.0,io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.5``.

```bash
spark-submit \
--deploy-mode cluster \
--master yarn \
--packages com.amazon.redshift:redshift-jdbc42:2.1.0.29,org.apache.spark:spark-avro_2.12:3.5.1,io.github.spark-redshift-community:spark-redshift_2.12:6.3.0-spark_3.5 \
main.py
```

> [!NOTE]  
> `--conf spark.jars.packages` can be used instead of `--packages`.


* **2.2** When reading from a redshift url, set the datasource format to  
``io.github.spark_redshift_community.spark.redshift``.
        
```python
spark.read \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
    .option("query", "select x, count(*) from my_table group by x") \
    .option("tempdir", "s3://path/for/temp/data") \
    .load()
```

* **2.3** Since this datasource unloads data to S3 as an intermediate step, 
authentication is something to keep in mind. Different authentication methods between Spark and Redshift are datailed here:
[Authenticating with Amazon Redshift integration for Apache Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-redshift-auth.html)

> [!IMPORTANT] 
> In cluster mode, AWS credentials must be set in all nodes.


### 3. Edit core-site.xml

* **3.1** Add the following properties to ``core-site.xml``.

```bash
<property><name>fs.s3.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
<property><name>fs.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
<property><name>fs.s3a.aws.credentials.provider</name><value>com.amazonaws.auth.DefaultAWSCredentialsProviderChain</value></property>
<property><name>fs.s3.aws.credentials.provider</name><value>com.amazonaws.auth.DefaultAWSCredentialsProviderChain</value></property>
```
