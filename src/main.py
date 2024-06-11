import importlib
import os
import sys
import time

import click
from pyspark.sql import SparkSession

if os.path.exists("libs.zip"):
    sys.path.insert(0, "libs.zip")
else:
    sys.path.insert(0, "./libs")

if os.path.exists("jobs.zip"):
    sys.path.insert(0, "jobs.zip")
else:
    sys.path.insert(0, "./jobs")


def args_to_dict(arguments: list[str]) -> dict[str, str]:
    user_dict: dict[str, str] = {}

    for arg in arguments:
        split = arg.split("=", maxsplit=1)

        if len(split) != 2:
            raise ValueError(
                f"Invalid format for argument '{arg}'. Use -A name=value."
            )

        name, value = split
        if name in user_dict:
            raise ValueError(f"Repeated parameter: '{name}'")

        user_dict[name] = value
    return user_dict


@click.command()
@click.option(
    "--job",
    required=True,
    type=str,
    help="The name of the job module you want to run.",
)
@click.option(
    "--job-args",
    "-A",
    multiple=True,
    help="An argument for the job of the form -A name=value.",
)
def main(job, job_args):
    """Runs a PySpark job."""

    args_dict = args_to_dict(job_args)
    args_str = " ".join(job_args) if job_args else ""

    spark = (
        SparkSession.builder.appName(job)
        .config("spark.executorEnv.PYSPARK_JOB_ARGS", args_str)
        .getOrCreate()
    )

    job_module = importlib.import_module(f"jobs.{job}")

    start = time.time()
    job_module.run(spark, **args_dict)
    end = time.time()

    print(f"Execution of job {job} took {end - start} seconds.")


if __name__ == "__main__":
    main()
