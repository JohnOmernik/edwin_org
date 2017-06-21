#!/usr/bin/python

import requests
import json
import sys
import os
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.magic import (register_line_magic, register_cell_magic, register_line_cell_magic)

@magics_class
class Edwin_org(Magics):
    myip = None
    nbuser = ""
    nbsshhost = ""
    nbsshport = ""
    org_matrix = {}
    edwin_org_base_dir = ""
    edwin_org_matrix_file = ""

    def __init__(self, shell, *args, **kwargs):
        super(Edwin_org, self).__init__(shell)
        self.myip = get_ipython()
        try:
            self.nbuser = os.environ['JPY_USER']
        except:
            pass
        try:
            self.nbsshhost = os.environ['JPY_USER_SSH_HOST']
        except:
            pass

        try:
            self.nbsshport = os.environ['JPY_USER_SSH_PORT']
        except:
            pass

        self.edwin_org_base_dir = os.path.dirname(os.environ['EDWIN_ORG_CODE'])
        self.edwin_org_matrix_file = self.edwin_org_base_dir + "/edwin_org_core/edwin_org.json"
        self.loadOrgMatrix()


    def loadOrgMatrix(self):
        try:
            f = open(self.edwin_org_matrix_file, "r")
            j = f.read()
            f.close()
            self.org_matrix = json.loads(j)
        except:
            print("Could not load Edwin Org Matrix at %s" % self.edwin_org_matrix_file)
            raise Exception("Couldn't Load Matrix, I am stupid")

    @line_magic
    def edwin_org(self, line):

        outtree = 'base'
        if line == "":
            outtree = 'base'
        else:
            bFound = False
            for x in self.org_matrix['tree_root']:
                if x != 'base' and x != 'unknown':
                    if line.lower().find(x) >= 0:
                        outtree = x
                        bFound = True
                        break
            if bFound == False:
                outtree = 'unknown'

        cur = self.org_matrix['tree_root'][outtree]
        for l in cur['resp']:
            print(l)







