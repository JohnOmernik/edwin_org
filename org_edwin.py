#!/usr/bin/python

import requests
import json
import sys
import os
import os.path

from IPython.core.magic import (register_line_magic, register_cell_magic, register_line_cell_magic)
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)

edwin_org_base_dir = os.path.dirname(os.environ['EDWIN_ORG_CODE'])

print("Edwin Base: %s" % edwin_org_base_dir)
sys.path.append(edwin_org_base_dir)

from drill_core import Drill
from spark_core import Spark

prev_drill = ""
tpass = ""

ip = get_ipython()

print("Attempting to load Drill Magics")
d = Drill(ip)
ip.register_magics(d)

print("Attempting to load Spark Magics")
s = Spark(ip)
ip.register_magics(s)

try:
    org_name = os.environ['ORG_NAME']
except:
    org_name = "org not specified"


@register_line_magic
def edwin_org(line):

    tuser = ""
    tsshport = ""
    tsshhost = ""
    try:
        tuser = os.environ['JPY_USER']
    except:
        pass
    try:
        tsshport = os.environ['JPY_USER_SSH_PORT']
    except:
        pass
    try:
        tsshhost = os.environ['JPY_USER_SSH_HOST']
    except:
        pass



    org_matrix = {}
    org_matrix['base'] = [
                    "Greetings, I am Edwin, please allow me to assist you. I see you didn't ask about anything, so let me offer you thoughts on what you could ask me about:",
                    "",
                    "I can help with the following topics in relation to your org:"
                    "",
                    "%edwin_org        - This help",
                    "%edwin_org spark  - Apache Spark",
                    "%edwin_org drill  - Apache Drill", 
                    "%edwin_org ssh    - Container SSH and File Copy"
                    ]
    org_matrix['spark'] = [
                    "Apache Spark is available to run - More information found by typing %spark"
                    ]
    org_matrix['drill'] = [
                    "Apache Drill is available to run - More information found by typing %drill"
                    ]
    org_matrix['ssh'] = [
                    "Your notebook container also has an SSH Port for SSH for the user %s" % tuser, 
                    "The host to connect to is: %s" % tsshhost,
                    "The port to connect to is: %s" % tsshport,
                    "",
                    "Connect Example:",
                    "> ssh -p%s %s@%s" % (tsshport, tuser, tsshhost),
                    ""
                    "Copy Files Example:",
                    "> scp -P%s /path/to/your/file.txt %s@%s:/home/%s/" % (tsshport, tuser, tsshhost, tuser)
                    ]
    org_matrix['unknown'] = [
                    "I am not sure what you are looking to do, please try %edwin_org for options"
                    ]
    org_matrix['vis'] = [
                    "matplotlib is available to do basic visualizations on returned data frames",
                    ""
                    "To use, you must first run in a cell:",
                    "",
                    "%matplotlib inline",
                    "",
                    "Then you can use any dataframe variable and .plot to make it happen",
                    "For example, let's say you ran a query with %%drill, those results would be in the prev_drill variable by default",
                    "To visualize with a line chart you could run:",
                    "",
                    "newvar = prev_drill.plot(x='yourXaxisCol', y='yourYaxisCol', figsize=(15,10))",
                    "",
                    "There is much more you can do with this, please stay tuned for updates or check the MatPlotLib Doc Pages!"
                    ]


    outtree = 'base'
    if line == "":
        outtree = 'base'
    else:
        bFound = False
        for x in org_matrix:
            if x != 'base' and x != 'unknown':
                if line.lower().find(x) >= 0:
                    outtree = x
                    bFound = True
                    break
        if bFound == False:
            outtree = 'unknown'

    for l in org_matrix[outtree]:
        print(l)


