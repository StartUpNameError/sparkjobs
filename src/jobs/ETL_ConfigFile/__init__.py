"""
Executes ETL using the configuration specified in the configfile

This job is intended to be a generic, configuration oriented, ETL job.

Config files can by stored anywhere (S3, for example) and be customized
to the user's need.

Usage example:

spark-submit \
--master yarn \
--deploy-mode cluster \
main.py --job ETL_ConfigFile -A config=<path-to-configfile>
"""

from ._run import run
