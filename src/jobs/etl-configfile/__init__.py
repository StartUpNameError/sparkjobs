"""
Executes ETL using the configuration specified in the configfile

This job is intended to be a generic, configuration oriented, ETL job.

Config files can by stored anywhere (S3, for example) and be customized
to the user's need.

Usage example:

spark-submit main.py --job etl-config-file --job-args config=<path-to-configfile>
"""

__all__ = ["run"]

from ._run import run
