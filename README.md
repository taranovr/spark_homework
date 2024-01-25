# spark_homework
Building a datamart with PySpark

To run it, I used https://github.com/cluster-apps-on-docker/spark-standalone-cluster-on-docker

I've attached a notebook where the results of the operations are visually presented.

The entry point for running spark-submit is the run.sh script, which takes 2 positional arguments - the folder from which we read files and the destination folder for the result. Then, run.sh executes the submit using main.py.
