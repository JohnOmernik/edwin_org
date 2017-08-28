#!/usr/bin/python

import requests
import json
from getpass import getpass
import sys
import os
import time
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from requests.packages.urllib3.exceptions import SubjectAltNameWarning, InsecureRequestWarning
from requests_toolbelt.adapters import host_header_ssl
requests.packages.urllib3.disable_warnings(SubjectAltNameWarning)
from collections import OrderedDict
from IPython.core.display import HTML

#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets


@magics_class
class Mapr(Magics):
    myip = None
    mapr_connected = False
    mapr_host = ""
    mapr_user = ""
    mapr_pass = ""
    mapr_base_url = ""
    mapr_cluster = ""

    def __init__(self, shell, *args, **kwargs):
        super(Mapr, self).__init__(shell)
        self.myip = get_ipython()

    def retConnStatus(self):
        if self.mapr_connected == True:
            print("Mapr is currrently connected as %s to %s" % (self.mapr_user, self.mapr_base_url))
        else:
            print("Mapr is not connected")

    def connectMapr(self, use_defaults=True):
        global tpass
        if self.mapr_connected == False:
            # Get User Information
            try:
                tuser = os.environ['JPY_USER']
            except:
                raise Exception("Could not find user at ENV JPY_USER - Please use '%mapr connect alt' to specify")

            if use_defaults != True:
                print("Currently, the user is set to %s" % tuser)
                print("To use this user, just press enter at the prompt, otherwise type a different user name")
                tmp = input("Please type new user name if desired: ")
                if tmp != "":
                    tuser = tmp
            print("Connecting as user %s" % tuser)

            cldb = self.getMapRCLDB()
            print("Now, please enter the password you wish to connect with for %s:" % tuser)
            tpass = ""
            self.myip.ex("from getpass import getpass\ntpass = getpass(prompt='MapR Connect Password: ')")
            tpass = self.myip.user_ns['tpass']
            self.mapr_user = tuser
            self.mapr_pass = tpass

            retcode, rettext = self.sendMaprRequest('/entity/list')

            if retcode == 401:
                print("MapR Connect failed due to HTTP 401 (Likely Bad Credentials)")
                print("MapR - NOT Connected")
                self.mapr_connected = False
            elif retcode == 200:
                print("MapR Connected - %s at %s" % (self.mapr_user, self.mapr_base_url))
                self.mapr_connected = True
            else:
                print("Unknown Return code: %s - Text of Error:" % retcode)
                print(rettext)
                self.mapr_connected = False
        else:
            print("MapR is already connected - Please type %mapr for help on what you can you do")



    def disconnectMapr(self):
        if self.mapr_connected == True:
            print("Disconnected Mapr Session from %s" % self.mapr_base_url)
            self.session = None
            self.mapr_base_url = None
            self.mapr_pass = None
            self.mapr_connected = False
        else:
            print("Mapr Not Currently Connected")

    def sendMaprRequest(self, apireq):
        s = requests.session()
        verify = False
        uri = self.mapr_base_url + "/rest"+ apireq
        print("URL Requested: %s" % uri)
        r = s.get(uri, verify=verify, auth=(self.mapr_user, self.mapr_pass))
        return r.status_code, r.text

    def getMapRCLDB(self):
        r = open("/opt/mapr/conf/mapr-clusters.conf", "r")
        m = r.read()
        r.close()
        confitems = m.strip().split(" ")
        cluster_name = confitems[0]
        cluster_secure = confitems[1].split("=")[1]
        cluster_cldbs = confitems[2].split(",")
        print("Searching for Active CLDB in %s" % cluster_cldbs)
        bfound = False

        for x in cluster_cldbs:
            cldbhost = x.split(":")[0]
            cldburl = "https://" + cldbhost + ":8443"
            print("Trying %s" % cldburl)
            verify = False
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
            if 1 == 1:
                s = requests.session()
                r = s.get(cldburl, verify=verify)
                if r.status_code == 200:
                    print("Using %s as Active CLDB Host at %s" % (cldbhost, cldburl))
                    self.mapr_base_url = cldburl
                    bfound = True
                    break
            #except:
            #    print("CLDB %s did not respond" % cldbhost)
        if bfound != True:
            raise Exception("Could not find active CLDB!")

        return bfound




    def runQuery(self, query):
        if query.find(";") >= 0:
            print("WARNING - Do not type a trailing semi colon on queries, your query will fail (like it probably did here)")
        if self.drill_pin_to_ip == True:
            verify = False
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        else:
            verify = "/etc/ssl/certs/ca-certificates.crt"

        if self.drill_connected == True:
            url = self.drill_base_url + "/query.json"
            payload = {"queryType":"SQL", "query":query}
            cur_headers = self.drill_headers
            cur_headers["Content-type"] = "application/json"
            starttime = int(time.time())
            r = self.session.post(url, data=json.dumps(payload), headers=cur_headers, verify=verify)
            endtime = int(time.time())
            query_time = endtime - starttime
            return r, query_time

    def printStreamHelp(self):
        h = """{"stream": [{"create": ["-path Stream Path", "[ -ttl Time to live in seconds. default:604800 ]", "[ -autocreate Auto create topics. default:true ]", "[ -defaultpartitions Default partitions per topic. default:1 ]", "[ -compression off|lz4|lzf|zlib. default:inherit from parent directory ]", "[ -produceperm Producer access control expression. default u:creator ]", "[ -consumeperm Consumer access control expression. default u:creator ]", "[ -topicperm Topic CRUD access control expression. default u:creator ]", "[ -copyperm Stream copy access control expression. default u:creator ]", "[ -adminperm Stream administration access control expression. default u:creator ]", "[ -copymetafrom Stream to copy attributes from. default:none ]"]}, {"edit": ["-path Stream Path", "[ -ttl Time to live in seconds ]", "[ -autocreate Auto create topics ]", "[ -defaultpartitions Default partitions per topic ]", "[ -compression off|lz4|lzf|zlib ]", "[ -produceperm Producer access control expression. default u:creator ]", "[ -consumeperm Consumer access control expression. default u:creator ]", "[ -topicperm Topic CRUD access control expression. default u:creator ]", "[ -copyperm Stream copy access control expression. default u:creator ]", "[ -adminperm Stream administration access control expression. default u:creator ]"]}, {"info": ["-path Stream Path"]}, {"delete": ["-path Stream Path"]}, {"purge": ["-path Stream Path"]}, {"topic": [{"create": ["-path Stream Path", "-topic Topic Name", "[ -partitions Number of partitions. default: attribute defaultpartitions on the stream ]"]}, {"edit": ["-path Stream Path", "-topic Topic Name", "-partitions Number of partitions"]}, {"delete": ["-path Stream Path", "-topic Topic Name"]}, {"info": ["-path Stream Path", "-topic Topic Name"]}, {"list": ["-path Stream Path"]}]}, {"cursor": [{"delete": ["-path Stream Path", "[ -consumergroup Consumer Group ID ]", "[ -topic Topic Name ]", "[ -partition Partition ID ]"]}, {"list": ["-path Stream Path", "[ -consumergroup Consumer Group ID ]", "[ -topic Topic Name ]", "[ -partition Partition ID ]"]}]}, {"assign": [{"list": ["-path Stream Path", "[ -consumergroup Consumer Group ID ]", "[ -topic Topic Name ]", "[ -partition Partition ID ]", "[ -detail Detail Parameter takes no value  ]"]}]}, {"replica": [{"add": ["-path stream path", "-replica remote stream path", "[ -paused start replication in paused state. default: false ]", "[ -throttle throttle replication operations under load. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -synchronous replicate to remote stream before acknowledging producers. default: false ]", "[ -networkcompression on-wire compression type: off|lz4|lzf|zlib default: compression setting on stream ]"]}, {"edit": ["-path stream path", "-replica remote stream path", "[ -newreplica renamed stream path ]", "[ -throttle throttle replication operations under load ]", "[ -networkencryption enable on-wire encryption ]", "[ -synchronous replicate to remote stream before acknowledging producers ]", "[ -networkcompression on-wire compression type: off|lz4|lzf|zlib ]"]}, {"list": ["-path stream path", "[ -refreshnow refreshnow. default: false ]"]}, {"remove": ["-path stream path", "-replica remote stream path"]}, {"pause": ["-path stream path", "-replica remote stream path"]}, {"resume": ["-path stream path", "-replica remote stream path"]}, {"autosetup": ["-path stream path", "-replica remote stream path", "[ -synchronous replicate to remote stream before acknowledging producers. default: false ]", "[ -multimaster set up bi-directional replication. default: false ]", "[ -throttle throttle replication operations under load. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -networkcompression on-wire compression type: off|lz4|lzf|zlib default: compression setting on stream ]"]}]}, {"upstream": [{"add": ["-path stream path", "-upstream upstream stream path"]}, {"list": ["-path stream path"]}, {"remove": ["-path stream path", "-upstream upstream stream path"]}]}]}"""
        hj = json.loads(h)
        print(json.dumps(hj, sort_keys=False, indent=4, separators=(',', ': ')))

    def convertCli2Rest(self, cli):
        tks = cli.split(" ")
        params = False
        curpramname = ""
        retval = ""
        cur = ""
        valid = 1
        for x in toks:
            if x.find("-") == 0:
                cur = "paramname"
                if params == False:
                    ratval = retval + "?"
                    params = True
            elif params == False:
                cur = "base"
            else:
                cur = "paramval"

            if cur == "base":
                retval = retval +  "/" + x
            elif  cur == "paramname" and curparamname == "":
                curparamname = x
            elif cur == "paramval" and curparamname != "":
                retval = retval + "&" + curparamname + "=" + x
            else:
                print("We have an issue with statement cli")
                valid = 0
        return valid, retval




    @line_cell_magic
    def mapr(self, line, cell=None):
        if cell is None:
            if line == "":
                print("Help with MapR Functions")
                print("%mapr            - This Help")
                print("%mapr connect    - Provide Credentials your instance of MapR") 
                print("%mapr connect alt   - Connect to a different mapr cluster or use a different user (will prompt)")
                print("%mapr status     - Show the Connection Status of MapR")
                print("%mapr disconnect - Disconnect from your instance of MapR")
                print("")
                print("Run MapR Commands")
                print("%%mapr")
                print("stream create -path /path/to/your/stream")
                print("")
                print("")
            elif line.lower() == "status":
                self.retConnStatus()
            elif line.lower() == "disconnect":
                self.disconnectMapr()
            elif line.lower() == "connect alt":
                self.connectMapr(False)
            elif line.lower() == "connect":
                self.connectMapr(True)
            else:
                print("I am sorry, I don't know what you want to do, try just %mapr for help options")
        elif cell is not None and line == "cli":
            if self.mapr_connected == True:
                print("MapR CLI Requested with %s" % cell)
                ret, rest = self.convertCli2Rest(cell)
                if ret == 1:
                    code, text = self.sendMaprRequest(rest)
                    print("Code: %s\n%s" % (code, text))
                else:
                    print("Count not convert %s to rest" % cell)

        elif cell is not None and line == "rest":
            if self.mapr_connected == True:
                code, text = self.sendMaprRequest(cell)
                print("Code: %s\n%s" % (code, text))
            else:
                print("Mapr is not connected: Please see help at %mapr  - To Connect: %mapr connect")






  
