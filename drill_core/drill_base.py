#!/usr/bin/python

import requests
import json
from getpass import getpass
import sys
import os
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from requests.packages.urllib3.exceptions import SubjectAltNameWarning
requests.packages.urllib3.disable_warnings(SubjectAltNameWarning)

import pandas as pd
pd.set_option('display.max_columns', None)

@magics_class
class Drill(Magics):
    session = None
    drill_connected = False
    drill_user = ""
    drill_pass = ""
    drill_base_url = ""

    def __init__(self, shell, *args, **kwargs):
        super(Drill, self).__init__(shell)

    def retConnStatus(self):
        if self.drill_connected == True:
            print("Drill is currrently connected to %s" % self.drill_base_url)
        else:
            print("Drill is not connected")
    def disconnectDrill(self):
        if self.drill_connected == True:
            print("Disconnected Drill Session from %s" % self.drill_base_url)
            self.session = None
            self.drill_base_url = None
            self.drill_pass = None
            self.drill_connected = False
        else:
            print("Drill Not Currently Connected")

    def connectDrill(self):
        global tpass
        if self.drill_connected == False:
            try:
                tuser = os.environ['JPY_USER']
            except:
                tuser = ""
            print("Currently, the user is set to %s" % tuser)
            print("To use this user, just press enter at the prompt, otherwise type a different user name")
            tmp = input("Please type new user name if desired: ")
            if tmp != "":
                tuser = tmp
            try:
                turl = os.environ['DRILL_BASE_URL']
            except:
                turl = ""
            print("Currently the drill base url is set to %s" % turl)
            print("To use this URL, please press enter at the prompt, otherwise type a different url to connect with")
            tmpu = input("Please type a new URL if desired:")
            if tmpu != "":
                turl = tmpu
            print("Now, please enter the password you wish to connect with:")
            ip = get_ipython()
            tpass = ""
            ip.ex("from getpass import getpass\ntpass = getpass(prompt='Drill Connect Password: ')")
            tpass = ip.user_ns['tpass']
            self.session = requests.Session()
            self.drill_base_url = turl
            self.drill_user = tuser
            self.drill_pass = tpass
            ip.user_ns['tpass'] = ""
            try:
                self.session = self.authDrill()
                self.drill_connected = True
                print("%s - Drill Connected!" % self.drill_base_url)
            except:
                print("Connection Error - Perhaps Bad Usename/Password?")

        else:
            print("Drill is already connected - Please type %drill for help on what you can you do")


    def runQuery(self, query):

        if query.find(";") >= 0:
            print("WARNING - Do not type a trailing semi colon on queries, your query will fail (like it probably did here)")

        if self.drill_connected == True:
            url = self.drill_base_url + "/query.json"
            payload = {"queryType":"SQL", "query":query}
            headers = {"Content-type": "application/json"}
            r = self.session.post(url, data=json.dumps(payload), headers=headers, verify="/etc/ssl/certs/ca-certificates.crt")
            return r


    def authDrill(self):
        url = self.drill_base_url + "/j_security_check"
        login = {'j_username': self.drill_user, 'j_password': self.drill_pass}

        r = self.session.post(url, data=login, verify="/etc/ssl/certs/ca-certificates.crt")
        if r.status_code == 200:
            if r.text.find("Invalid username/password credentials") >= 0:
                raise Exception("Invalid username/password credentials")
            elif r.text.find("Number of Drill Bits") >= 0:
                pass
            else:
                raise Exception("Unknown HTTP 200 Code: %s" % r.text)
        else:
            raise Exception("Status Code: %s - Error" % r.status_code)
        return self.session

    @line_cell_magic
    def drill(self, line, cell=None):
        if cell is None:
            if line == "":
                print("Help with Drill Functions")
                print("%drill            - This Help")
                print("%drill connect    - Connect to your instance of Drill") 
                print("%drill status     - Show the Connection Status of Drill")
                print("%drill disconnect - Disconnect from your instance of Drill")
                print("")
                print("Run Drill Queries")
                print("%%drill")
                print("select * from your table")
                print("")
                print("Ran with two % and a query, it queries a table and returns a df")
                print("The df is displayed but also stored in  variable called prev_drill")
                print("")
            elif line.lower() == "status":
                self.retConnStatus()
            elif line.lower() == "disconnect":
                self.disconnectDrill()
            elif line.lower() == "connect":
                self.connectDrill()
            else:
                print("I am sorry, I don't know what you want to do, try just %drill for help options")

        else:
            if self.drill_connected == True:
                res = self.runQuery(cell)
                if res == "notconnected":
                    pass
                else:
                    if res.status_code == 200:
                        jrecs = json.loads(res.text)
                        myrecs = jrecs['rows']
#                    df = pd.DataFrame.from_records([l for l in myrecs], coerce_float=True)
                        df = pd.read_json(json.dumps(myrecs))
                        ip = get_ipython()
                        ip.user_ns['prev_drill'] = df
                        return df
                    else:
                        print("Error Returned - Code: %s" % res.status_code)
                        emsg = json.loads(res.text)
                        print("Error Text:\n%s" % emsg['errorMessage'])
            else:
                print("Drill is not connected: Please see help at %drill  - To Connect: %drill connect")






