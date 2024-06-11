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
the ``make`` command (except when the job has been modified or extra 
dependencies have been added).

The ``wordcount`` job is included in this repo, so the above command should work
perfectly fine. Give it a try!


# Third-party dependencies

This repo already comes with collection of common third-party dependencies 
located inside ``src/libs`` that can be shipped on every ``spark-submit`` call
by using the same ZIP packaging technique.

You can include more by pip installing them into this folder via

```
pip install -r requirements.txt -t ./src/libs
```
The option ``-t`` allows us to specify a target directory for the installation.




# Writing a PySpark Job
...

# Writing Transformations
...

# Unit Testing
...


