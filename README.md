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

This repository includes a minimal set of common third-party dependencies listed in the ``requirements.txt`` file, necessary for using the shared library located in ``src/shared``. These are installed in ``src/libs`` and can be included on each 
Spark job using the same ZIP packaging technique.

To include extra dependencies, you can install them into this folder by running:

```
pip install -r extra_requirements.txt -t ./src/libs
```

The ``-t`` option allows you to specify a target directory for the installation.

Now, you can import these dependencies within your jobs (e.g., ``import pandas as pd``) by specifying the libs.zip file in the spark-submit command:

```
spark-submit --py-files jobs.zip,libs.zip main.py --job <your_job>
```

Don't forget to run ``make build`` as necessary.



# Writing a PySpark Job
...

# Writing Transformations
...

# Unit Testing
...


