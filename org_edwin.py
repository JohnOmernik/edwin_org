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
from edwin_org_core import Edwin_org

prev_drill = ""
tpass = ""

ip = get_ipython()

print("Attempting to load Edwin_org Magics")
e = Edwin_org(ip)
ip.register_magics(e)

print("Attempting to load Drill Magics")
d = Drill(ip)
ip.register_magics(d)

print("Attempting to load Spark Magics")
s = Spark(ip)
ip.register_magics(s)


