import importlib
import time

import click
from pyspark.sql import SparkSession


def args_to_dict(arguments: list[str]) -> dict[str, str]:
    """Converts user args to a dictionary.

    Parameters
    ----------
    arguments : list of str
        User arguments.

    Returns
    -------
    dict : dict, str -> str
        User args parsed into a dictionary of key:value pairs.
    """
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
    "-J",
    required=True,
    type=str,
    help="The name of the job module you want to run.",
)
@click.option(
    "--args-list",
    "-A",
    metavar="NAME=VALUE",
    multiple=True,
    help="An argument for the run, of the form -A name=value.",
)
def main(job, args_list):
    """Runs a PySpark job."""

    args_dict = args_to_dict(args_list)
    spark = SparkSession.builder.appName(job).getOrCreate()
    job_module = importlib.import_module(f"jobs.{job}")

    start = time.time()
    job_module.run(spark=spark, **args_dict)
    end = time.time()

    print(f"Execution of job {job} took {end - start} seconds.")


if __name__ == "__main__":
    main()
