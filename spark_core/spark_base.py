#!/usr/bin/python

import requests
import json
import sys
import os
import pandas as pd
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)


@magics_class
class Spark(Magics):
    spark_running = False
    myip = None
    def __init__(self, shell, *args, **kwargs):
        super(Spark, self).__init__(shell)
        self.myip = get_ipython()

    def sparkHelp(self):
        print("Help with Spark Functions")
        print("%spark            - This Help")
        print("%spark start      - Start Spark Driver on this notebook")
        print("%spark status     - Show the Connection Status of Drill")
        print("%drill stop        - Stop Spark Driver on this notebook")
        print("")
        print("Sample Spark Queries")

    def sparkStatus(self):
        if self.spark_running == True:
            print("Currently there is an instance of spark running using the SparkSession variable 'spark'")
        else:
            print("There is no instance of spark running")

    def stopSpark(self):
        if self.spark_running == False:
            print("I can't find a current instance of spark running")
        else:
            cmd = "spark.stop()\nspark = None"
            print("Running the following code to stop spark:")
            print(cmd)
            self.myip.run_cell(cmd)
            self.spark_running = False
            print("Spark Driver is stopped and SparkSession variable 'spark' is set to None")

    def startSpark(self):
        try:
            spark_home = os.environ['SPARK_HOME']
            myuser = os.environ['JPY_USER']
            sname = myuser + "nbspark"
        except:
            print("SPARK_HOME not set, please work with your administrator to setup - Nothing started")

        if self.spark_running == False:
            cmd = "import findspark\nimport time\nfindspark.init()\nimport pyspark\nimport pyspark.sql\nspark = pyspark.sql.SparkSession.builder.appName(\"" + sname + "\").getOrCreate()"
            print("Running the following code to start spark:")
            print(cmd)
            self.myip.run_cell(cmd)
            self.spark_running = True
            print("Running Spark - Variables available at:  spark - sparksession - spark.sparkContext - sparkcontext")
        else:
            print("Spark already appears to be running, please try using the spark variable")

    @line_magic
    def spark(self, line):
        if line == "":
            self.sparkHelp()
        elif line.find("status") >= 0:
            self.sparkStatus()
        elif line.find("stop") >= 0:
            self.stopSpark()
        elif line.find("start") >= 0:
            self.startSpark()
        else:
            print("I am no unsure what you mean by that... instead try these items: ")
            self.sparkHelp()



