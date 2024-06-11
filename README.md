This repo aims to provide a systematic and structured flow of data
processing by providing a single entrypoint (``main.py``) for all the stored 
scripts.


# Running a PySpark Job

Every job module must be located inside ``src/jobs`` and can be run via

```
make build
cd dist 
spark-submit --py-files jobs.zip main.py --job wordcount
```

That is, all files and dependencies are first packaged into a single ZIP file
so Spark can import them within the job. Subsecuent runs do not need to rerun 
the ``make`` command (except when the job has been modified).


# Writing a PySpark Job
...

# Writing Transformations
...

# Unit Testing
...


