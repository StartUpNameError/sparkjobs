This repo aims to provide a systematic and structured flow of data
processing by providing a single entrypoint (``main.py``) for all the stored 
jobs.


# Running a PySpark Job

Every job module must be located inside ``src/jobs`` and can be run via

```
make build
cd dist 
spark-submit --py-files jobs.zip main.py --job <jobName>
```


# What does ``make build`` do?
...



# Third-party dependencies
PySpark provides multiple ways to manage package Python dependencies making
them available inside jobs. Please visit the Python [Package Management](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html) 
site for more details.

However, I find the **Virtualenv** approach the most straightforward. 
Say you have a virtualenv ``my-env`` created with ``python3 -m venv my-env``,
you can package and save it to hdfs with

```bash
source my-env/bin/activate
pip3 install venv-pack
venv-pack -o my-env.tar.gz
hdfs dfs -put -f my-env.tar.gz <destination>
```

where `<destination>` can be, for example, `/shared/python-envs`.
Then, use the `--archives` option in the `spark-submit` to make your virtual 
environment avaibale within your jobs,

```bash
spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--archives spark.yarn.dist.archives=hdfs:///shared/python-envs/my-env.tar.gz#environment \
--master yarn \
--deploy-mode cluster \
main.py --job <jobName>
```


> [!NOTE]  
> `--conf spark.yarn.dist.archives` can be used instead of `--archives`.


# Writing a PySpark Job
PySpark jobs must be python modules exposing the 
``run(spark: SparkSession, **kwargs)`` function.
The ``main.py`` module will then try to import this function under the 
specified job module using the ``importlib`` library. This logic is depicted 
in the following code,

```python
import importlib

jobArgs = {...} 
jobName = "my-job"
jobModule = importlib.import_module(f"jobs.{jobName}")
jobModule.run(spark=spark, **jobArgs)
```
