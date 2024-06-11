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

This repo already comes with a minimal collection of common third-party dependencies defined in the ``requirements.txt`` and are required to use the shared library. These are installed inside ``src/libs`` and can be shipped on every ``spark-submit`` call by using the same ZIP packaging technique.

You can include more by pip installing them into this folder via

```
pip install -r extra_requirements.txt -t ./src/libs
```
The option ``-t`` allows us to specify a target directory for the installation.


Now we can import all our 3rd party dependencies within our jobs 
(e.g., ``import pandas as pd``) by running Spark using

```
spark-submit --py-files jobs.zip,libs.zip main.py --job <your_job>
```
(don't forget to ``make build`` as neccesary).



# Writing a PySpark Job
...

# Writing Transformations
...

# Unit Testing
...


