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



    def printHelp(self, helpkey):
        tophelp = """{"acl": "acl [show|set|edit] -type [cluster|volume] -name <volume name>", "alarm": "alarm [list|raise|clear|clearall|config|names]", "audit": "audit [data|cluster|info]", "blacklist": "blacklist [user|listusers]", "cluster": "cluster [mapreduce [get|set] | gateway | feature [enable|list]]", "config": "config [load|save]", "dashboard": "dashboard [info] -[version|multi_cluster_info]", "debugdb": "debugdb cdscan|bmap|checkTablet|rawScan|multiOp", "dialhome": "dialhome [metrics|enable|status|ackdial|lastdialed]", "disk": "disk [list|listall|add|remove]", "dump": "dump [containerinfo|containers|volumeinfo|volumenodes|replicationmanagerinfo|replicationmanagerqueueinfo|rereplicationinfo|rereplicationmetrics|balancerinfo|balancermetrics|rolebalancerinfo|rolebalancermetrics|assignvouchers|fileserverworkinfo|zkinfo|supportdump|cldbnodes]", "entity": "entity [list|info|modify|remove]", "fid": "fid [dump|stat]", "job": "job [kill|changepriority|linklogs|status]", "license": "license [add|remove|list|apps|showid|addcrl|listcrl]", "nagios": "nagios [generate]", "nfsmgmt": "nfsmgmt [refreshexports|refreshgidcache]", "node": "node [list|move|modify|allow-into-cluster|services|topo|remove|heatmap|listcldbs|listzookeepers|cldbmaster|canremovesp|maintenance|metrics|show|listcldbzks]", "rlimit": "rlimit [set|get] -resource <resource>", "schedule": "schedule [create|modify|list|remove]", "security": "security [genkey|genticket|getmapruserticket]", "service": "service [list|info]", "setloglevel": "setloglevel [cldb|fileserver|nfs|jobtracker|tasktracker|hbmaster|hbregionserver]", "stream": "stream [create|edit|info|delete|topic|cursor|replica|upstream]", "table": "table [create|edit|delete|listrecent|info|cf|region|replica|upstream]", "task": "task [killattempt|failattempt]", "trace": "trace [dump|setmode|setlevel|resize|reset|info|print] -host x.x.x.x -port <port>", "urls": "getting service webserver link", "virtualip": "virtualip [add|remove|edit|move|list]", "volume": "volume [create|modify|remove|mount|unmount|list|snapshot|mirror|dump|rename|upgradeformat|move|link|info|showmounts|fixmountpath]"}"""
        topjson = json.loads(tophelp)

        curhelp = {}
        curfound = False
        help = """
[
{"stream": [{"create": ["-path Stream Path", "[ -ttl Time to live in seconds. default:604800 ]", "[ -autocreate Auto create topics. default:true ]", "[ -defaultpartitions Default partitions per topic. default:1 ]", "[ -compression off|lz4|lzf|zlib. default:inherit from parent directory ]", "[ -produceperm Producer access control expression. default u:creator ]", "[ -consumeperm Consumer access control expression. default u:creator ]", "[ -topicperm Topic CRUD access control expression. default u:creator ]", "[ -copyperm Stream copy access control expression. default u:creator ]", "[ -adminperm Stream administration access control expression. default u:creator ]", "[ -copymetafrom Stream to copy attributes from. default:none ]"]}, {"edit": ["-path Stream Path", "[ -ttl Time to live in seconds ]", "[ -autocreate Auto create topics ]", "[ -defaultpartitions Default partitions per topic ]", "[ -compression off|lz4|lzf|zlib ]", "[ -produceperm Producer access control expression. default u:creator ]", "[ -consumeperm Consumer access control expression. default u:creator ]", "[ -topicperm Topic CRUD access control expression. default u:creator ]", "[ -copyperm Stream copy access control expression. default u:creator ]", "[ -adminperm Stream administration access control expression. default u:creator ]"]}, {"info": ["-path Stream Path"]}, {"delete": ["-path Stream Path"]}, {"purge": ["-path Stream Path"]}, {"topic": [{"create": ["-path Stream Path", "-topic Topic Name", "[ -partitions Number of partitions. default: attribute defaultpartitions on the stream ]"]}, {"edit": ["-path Stream Path", "-topic Topic Name", "-partitions Number of partitions"]}, {"delete": ["-path Stream Path", "-topic Topic Name"]}, {"info": ["-path Stream Path", "-topic Topic Name"]}, {"list": ["-path Stream Path"]}]}, {"cursor": [{"delete": ["-path Stream Path", "[ -consumergroup Consumer Group ID ]", "[ -topic Topic Name ]", "[ -partition Partition ID ]"]}, {"list": ["-path Stream Path", "[ -consumergroup Consumer Group ID ]", "[ -topic Topic Name ]", "[ -partition Partition ID ]"]}]}, {"assign": [{"list": ["-path Stream Path", "[ -consumergroup Consumer Group ID ]", "[ -topic Topic Name ]", "[ -partition Partition ID ]", "[ -detail Detail Parameter takes no value  ]"]}]}, {"replica": [{"add": ["-path stream path", "-replica remote stream path", "[ -paused start replication in paused state. default: false ]", "[ -throttle throttle replication operations under load. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -synchronous replicate to remote stream before acknowledging producers. default: false ]", "[ -networkcompression on-wire compression type: off|lz4|lzf|zlib default: compression setting on stream ]"]}, {"edit": ["-path stream path", "-replica remote stream path", "[ -newreplica renamed stream path ]", "[ -throttle throttle replication operations under load ]", "[ -networkencryption enable on-wire encryption ]", "[ -synchronous replicate to remote stream before acknowledging producers ]", "[ -networkcompression on-wire compression type: off|lz4|lzf|zlib ]"]}, {"list": ["-path stream path", "[ -refreshnow refreshnow. default: false ]"]}, {"remove": ["-path stream path", "-replica remote stream path"]}, {"pause": ["-path stream path", "-replica remote stream path"]}, {"resume": ["-path stream path", "-replica remote stream path"]}, {"autosetup": ["-path stream path", "-replica remote stream path", "[ -synchronous replicate to remote stream before acknowledging producers. default: false ]", "[ -multimaster set up bi-directional replication. default: false ]", "[ -throttle throttle replication operations under load. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -networkcompression on-wire compression type: off|lz4|lzf|zlib default: compression setting on stream ]"]}]}, {"upstream": [{"add": ["-path stream path", "-upstream upstream stream path"]}, {"list": ["-path stream path"]}, {"remove": ["-path stream path", "-upstream upstream stream path"]}]}]},
{"table": [{"create": ["-path path", "[ -copymetafrom SrcTablePath ]", "[ -copymetatype all|cfs|aces|splits|attrs ]", "[ -regionsizemb Region Size in MB ]", "[ -autosplit Auto Split table ]", "[ -bulkload Bulk load ]", "[ -audit Enable Audit ]", "[ -tabletype Table Type - json or binary. default: binary ]", "[ -insertionorder Retain Insertion Order within document - only for JSON table type. default : true ]", "[ -packperm Pack Permission settings ]", "[ -bulkloadperm Bulk load Permission settings ]", "[ -splitmergeperm Split and Merge Permission settings ]", "[ -createrenamefamilyperm Add/Rename Family Permission settings ]", "[ -deletefamilyperm Delete Family Permission settings ]", "[ -adminaccessperm Ace Admin Permission settings ]", "[ -replperm Replication Admin Permission settings ]", "[ -defaultversionperm CF Versions Default Permission for binary tabletype ]", "[ -defaultcompressionperm CF Compression Default Permission ]", "[ -defaultmemoryperm CF Memory Default Permission ]", "[ -defaultreadperm CF Read Default Permission ]", "[ -defaultwriteperm CF Write Default Permission ]", "[ -defaulttraverseperm CF Traverse Default Permission for json tabletype ]", "[ -defaultappendperm CF Append Default Permission for binary tabletype ]"]}, {"edit": ["-path path", "[ -autosplit Auto Split table ]", "[ -regionsizemb Region Size in MB ]", "[ -bulkload Bulk load ]", "[ -audit Enable Audit ]", "[ -deletettl delete TTL in secs ]", "[ -packperm Pack Permission settings ]", "[ -bulkloadperm Bulk load Permission settings ]", "[ -splitmergeperm Split and Merge Permission settings ]", "[ -createrenamefamilyperm Add/Rename Family Permission settings ]", "[ -deletefamilyperm Delete Family Permission settings ]", "[ -adminaccessperm Ace Admin Permission settings ]", "[ -replperm Replication Admin Permission settings ]", "[ -defaultversionperm CF Versions Default Permission for binary tabletype ]", "[ -defaultcompressionperm CF Compression Default Permission ]", "[ -defaultmemoryperm CF Memory Default Permission ]", "[ -defaultreadperm CF Read Default Permission ]", "[ -defaultwriteperm CF Write Default Permission ]", "[ -defaulttraverseperm CF Traverse Default Permission for json tabletype ]", "[ -defaultappendperm CF Append Default Permission for binary tabletype ]"]}, {"delete": ["-path path"]}, {"info": ["-path table path"]}, {"cf": [{"create": ["-path Table path", "-cfname Column family name", "[ -minversions Min versions to keep. default: 0 ]", "[ -maxversions Max versions to keep. default: 1 ]", "[ -ttl Time to live. Enter 0 for forever. Otherwise enter time in seconds. default: 0 ]", "[ -inmemory In-memory. default: false ]", "[ -compression off|lzf|lz4|zlib. default: table's compression setting is applied. ]", "[ -versionperm Version Permissions for binary tabletype ]", "[ -compressionperm Compression Permissions ]", "[ -memoryperm Memory Permissions ]", "[ -readperm Read Permissions ]", "[ -writeperm Write Permissions ]", "[ -appendperm Append Permissions for binary tabletype ]", "[ -traverseperm Traverse Permissions for json tabletype ]", "[ -jsonpath Json Family Path - needed for JSON column family, like a.b.c ]", "[ -force Force create non-default column family for json tabletype. default: false ]"]}, {"edit": ["-path Table path", "-cfname Column family name", "[ -newcfname New column family name ]", "[ -minversions Min versions to keep ]", "[ -maxversions Max versions to keep ]", "[ -ttl Time to live (in seconds) ]", "[ -inmemory In-memory ]", "[ -compression off|lzf|lz4|zlib ]", "[ -versionperm Version Permissions for binary tabletype ]", "[ -compressionperm Compression Permissions ]", "[ -memoryperm Memory Permissions ]", "[ -readperm Read Permissions ]", "[ -writeperm Write Permissions ]", "[ -appendperm Append Permissions for binary tabletype ]", "[ -traverseperm Traverse Permissions for json tabletype ]"]}, {"delete": ["-path Table path", "-cfname Column family name"]}, {"list": ["-path Table path", "[ -cfname Column family name ]"]}, {"colperm": [{"get": ["-path Table path", "-cfname Column family name", "[ -name Column name ]"]}, {"set": ["-path Table path", "-cfname Column family name", "-name Column name", "[ -readperm Read column permission settings ]", "[ -writeperm Write column permission settings ]", "[ -appendperm Append column permission settings ]", "[ -traverseperm Traverse column permission settings ]"]}, {"delete": ["-path Table path", "-cfname Column family name", "-name Column name"]}]}]}, {"region": [{"split": ["-path table path", "-fid fid"]}, {"pack": ["-path table path", "-fid fid|all", "[ -nthreads nthreads. default: 16 ]"]}, {"merge": ["-path table path", "-fid fid"]}, {"list": ["-path table path", "[ -start start. default: 0 ]", "[ -limit limit. default: 2147483647 ]"]}]}, {"replica": [{"add": ["-path table path", "-replica replica table path", "[ -columns comma separated list of <family>[:<column>] ]", "[ -paused is replication paused. default: false ]", "[ -throttle throttle replication ops. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -synchronous is synchronous replication. default: false ]", "[ -networkcompression on-wire compression type: off|on|lzf|lz4|zlib. default: on ]"]}, {"edit": ["-path table path", "-replica replica table path", "[ -newreplica renamed table path ]", "[ -columns comma separated list of <family>[:<column>] ]", "[ -throttle throttle replication ops ]", "[ -networkencryption enable on-wire encryption ]", "[ -synchronous is synchronous replication ]", "[ -networkcompression on-wire compression type: off|on|lzf|lz4|zlib ]"]}, {"list": ["-path table path", "[ -refreshnow refreshnow. default: false ]"]}, {"remove": ["-path table path", "-replica replica table path"]}, {"pause": ["-path table path", "-replica replica table path"]}, {"resume": ["-path table path", "-replica replica table path"]}, {"autosetup": ["-path table path", "-replica replica table path", "[ -columns comma separated list of <family>[:<column>] ]", "[ -synchronous is synchronous replication. default: false ]", "[ -multimaster is multi master replication. default: false ]", "[ -throttle throttle replication ops. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -networkcompression on-wire compression type: off|on|lzf|lz4|zlib. default: on ]"]}, {"elasticsearch": [{"autosetup": ["-path source table path", "-target Target cluster name", "-index Elasticsearch index name", "-type Elasticsearch type name", "[ -conversionclass Conversion class name ]", "[ -conversionjar Path to conversion class JAR file ]", "[ -columns comma separated list of <family>[:<column>] ]", "[ -indexedcolumns comma separated list of <family>:<column> pairs which are indexed ]", "[ -throttle throttle replication ops. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -networkcompression on-wire compression type: off|on|lzf|lz4|zlib. default: on ]"]}, {"edit": ["-path source table path", "-target Elasticsearch cluster name", "-index Elasticsearch index name", "-type Elasticsearch type name", "[ -conversionclass Conversion class name ]", "[ -conversionjar Path to conversion class JAR file ]", "[ -columns comma separated list of <family>[:<column>] ]", "[ -indexedcolumns comma separated list of <family>:<column> pairs which are indexed ]", "[ -throttle throttle replication ops. default: false ]", "[ -networkencryption enable on-wire encryption. default: false ]", "[ -networkcompression on-wire compression type: off|on|lzf|lz4|zlib. default: on ]"]}, {"list": ["-path source table path", "[ -refreshnow refresh now. default: false ]"]}, {"pause": ["-path source table path", "-target Elasticsearch cluster name", "-index Elasticsearch index name", "-type Elasticsearch type name"]}, {"resume": ["-path source table path", "-target Elasticsearch cluster name", "-index Elasticsearch index name", "-type Elasticsearch type name"]}, {"remove": ["-path source table path", "-target Elasticsearch cluster name", "-index Elasticsearch index name", "-type Elasticsearch type name"]}]}]}, {"upstream": [{"add": ["-path table path", "-upstream upstream table path"]}, {"list": ["-path table path"]}, {"remove": ["-path table path", "-upstream upstream table path"]}]}]},
{"volume": [{"audit": ["[ -cluster cluster_name ]", "-name volumeName", "[ -enabled <true|false> ]", "[ -coalesce interval in mins ]", "[ -dataauditops data audit operations ]"]}, {"create": ["[ -cluster cluster_name ]", "-name volumeName", "[ -path mountdir ]", "[ -createparent createparent. default: false ]", "[ -mount mount. default: true ]", "[ -rootdirperms rootdirperms ]", "[ -rereplicationtimeoutsec rereplicationtimeoutsec. default: 0 ]", "[ -localvolumehost localvolumehost ]", "[ -localvolumeport localvolumeport. default: 5660 ]", "[ -replication replication ]", "[ -minreplication minreplication ]", "[ -nsreplication nsreplication ]", "[ -nsminreplication nsminreplication ]", "[ -replicationtype replicationtype: low_latency or high_throughput. default: high_throughput ]", "[ -user space separated list of user:permissions,perimssions,.. to be set ]", "[ -group space separated list of user:permissions,perimssions,.. to be set ]", "[ -aetype aetype ]", "[ -ae ae ]", "[ -quota quota ]", "[ -advisoryquota advisoryquota ]", "[ -topology topology ]", "[ -readonly readonly ]", "[ -mirrorthrottle mirrorthrottle ]", "[ -type type of volume: rw or mirror ]", "[ -source source ]", "[ -schedule schedule ID ]", "[ -mirrorschedule mirror schedule ID ]", "[ -maxinodesalarmthreshold maxinodesalarmthreshold ]", "[ -dbrepllagsecalarmthresh dbrepllagsecalarmthresh ]", "[ -auditenabled <true|false> ]", "[ -coalesce interval in mins ]", "[ -dataauditops data audit operations ]", "[ -allowgrant allowgrant ]", "[ -inherit volume to copy properties from: defaults to parent volume ]", "[ -allowinherit allowinherit ]", "[ -readAce <Acess Control Expression> ]", "[ -writeAce <Acess Control Expression> ]"]}, {"unmount": ["[ -cluster cluster_name ]", "-name name", "[ -force force. default: false ]"]}, {"mount": ["[ -cluster cluster_name ]", "-name name", "[ -path path ]", "[ -createparent createparent. default: false ]"]}, {"showmounts": ["[ -cluster cluster_name ]", "-name name"]}, {"remove": ["[ -cluster cluster_name ]", "[ -name volumeName ]", "[ -force force. default: false ]", "[ -filter remove volumes that match the filter. default: none ]"]}, {"move": ["[ -cluster cluster_name ]", "-name volumeName", "-topology topology"]}, {"rename": ["[ -cluster cluster_name ]", "-name volumeName", "-newname newVolumeName"]}, {"upgradeformat": ["[ -cluster cluster_name ]", "-name volumeNames"]}, {"modify": ["[ -cluster cluster_name ]", "-name volumeName", "[ -source source ]", "[ -replication replication ]", "[ -minreplication minreplication ]", "[ -nsreplication nsreplication ]", "[ -nsminreplication nsminreplication ]", "[ -user space separated list of user:permissions,perimssions,.. to be set ]", "[ -group space separated list of user:permissions,perimssions,.. to be set ]", "[ -aetype aetype ]", "[ -ae ae ]", "[ -quota quota ]", "[ -advisoryquota advisoryquota ]", "[ -readonly readonly ]", "[ -mirrorthrottle mirrorthrottle ]", "[ -schedule schedule ID ]", "[ -mirrorschedule mirror schedule ID ]", "[ -maxinodesalarmthreshold maxinodesalarmthreshold ]", "[ -dbrepllagsecalarmthresh dbrepllagsecalarmthresh ]", "[ -type type of volume: rw or mirror ]", "[ -auditenabled <true|false> ]", "[ -coalesce interval in mins ]", "[ -dataauditops data audit operations ]", "[ -allowgrant let child volume inherit volume properties <true|false> ]", "[ -rereplicationtimeoutsec <Integer Value> ]", "[ -readAce <Acess Control Expression> ]", "[ -writeAce <Acess Control Expression> ]"]}, {"info": ["[ -cluster cluster_name ]", "[ -output verbose. default: verbose ]", "[ -path mountdir ]", "[ -name volumeName ]", "[ -columns comma separated list of column names. default: all ]"]}, {"list": ["[ -cluster cluster_name ]", "[ -output verbose. default: verbose ]", "[ -start start. default: 0 ]", "[ -limit limit. default: 2147483647 ]", "[ -filter none. default: none ]", "[ -nodes nodes ]", "[ -columns comma separated list of column names. default: all ]", "[ -alarmedvolumes alarmsonly. default: false ]"]}, {"fixmountpath": ["[ -cluster cluster_name ]", "-name volumeName"]}, {"link": [{"create": ["[ -cluster cluster_name ]", "-volume volume", "-type type <writeable|mirror>", "-path path"]}, {"remove": ["[ -cluster cluster_name ]", "-path vollink"]}]}, {"snapshot": [{"create": ["[ -cluster cluster_name ]", "-snapshotname snapshotName", "-volume volume"]}, {"remove": ["[ -cluster cluster name ]", "[ -snapshotname snapshotName ]", "[ -volume volumeName ]", "[ -snapshots comma separated IDs of snapshots ]"]}, {"preserve": ["[ -cluster cluster name ]", "[ -volume comma separated volume names to preserve snapshots for ]", "[ -path comma separated volume pathes to preserve snapshots for ]", "[ -filter filter to preserve snapshots for ]", "[ -snapshots comma separated IDs of snapshots to preserve ]"]}, {"list": ["[ -cluster cluster name ]", "[ -volume volume ]", "[ -path path ]", "[ -output verbose. default: verbose ]", "[ -start start. default: 0 ]", "[ -limit limit. default: 2147483647 ]", "[ -filter none. default: none ]", "[ -columns none. default: none ]"]}]}, {"mirror": [{"start": ["[ -cluster cluster_name ]", "-name name", "[ -full <true|false>. default: false ]"]}, {"stop": ["[ -cluster cluster_name ]", "-name name"]}, {"push": ["[ -cluster cluster_name ]", "-name name", "[ -verbose verbose. default: true ]"]}]}, {"dump": [{"create": ["[ -cluster cluster_name ]", "[ -s start volumepoint name ]", "[ -e end volumepoint name ]", "[ -o (generate dumpfile on stdout) ]", "-dumpfile dumpfilename (not needed if -o is used)", "-name volumename"]}, {"restore": ["[ -cluster cluster_name ]", "[ -i (read dumpfile from stdin) ]", "[ -n (create new volume if it doesn't exist) ]", "-dumpfile dumpfilename (not needed if -i is used)", "-name volumename"]}]}, {"container": [{"move": ["[ -cluster cluster_name ]", "-cid cid", "-fromfileserverid fromfileserverid", "[ -tofileserverid tofileserverid ]"]}, {"switchmaster": ["[ -cluster cluster_name ]", "-cid cid"]}]}]}
]"""
        jsonhelp = json.loads(help)

        if helpkey == "tophelp":
            print(json.dumps(topjson, sort_keys=True, indent=4, separators=(',', ': ')))
        else:
            mykeys = helpkey.split(" ")
            curhelp = jsonhelp
            print(mykeys)

            for x in mykeys:
                for y in curhelp:
                    if x in y:
                        curhelp = y[x]
                        curfound = True
                if curfound == False:
                    break

            if curfound == False:
               curhelp = {"Can't help you dear": "No help for that"}
            print(json.dumps(curhelp, sort_keys=False, indent=4, separators=(',', ': ')))



    def convertCli2Rest(self, cli):
        toks = cli.split(" ")
        params = False
        curparamname = ""
        retval = ""
        cur = ""
        valid = 1
        hc = []
        for x in toks:
            if x.find('-') == 0:
                break
            else:
                hc.append(x)
        helpcontext = " ".join(hc)
        for x in toks:
            firstparm = 0
            if x.find("-") == 0:
                cur = "paramname"
                if params == False:
                    retval = retval + "?"
                    params = True
                    firstparm = 1
            else:
                if params == False:
                    cur = "base"
                else:
                    cur = "paramval"

            if cur == "base":
                retval = retval +  "/" + x
            elif  cur == "paramname" and curparamname == "":
                curparamname = x[1:]
            elif cur == "paramval" and curparamname != "":
                myval = x.replace('"', '')
                if firstparm == 0:
                    retval = retval + "&" + curparamname + "=" + myval
                else:
                    retval = retval + curparamname + "=" + myval
                curparamname = ""
            else:
                print("We have an issue with statement cli")
                valid = 0
        return valid, retval, helpcontext




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
                print("%mapr cli - Print the root level options for maprcli")
                print("")
                print("%%mapr cli - Run maprcli Commands")
                print("Help is available for some items, just try them for example:")
                print("The %%mapr cli on the first line indicates you want to run a maprcli command, the next line starts with the token after you would write maprcli at the command line")
                print("")
                print("%%mapr cli")
                print("stream")
                print(" Prints the help for maprcli stream ")
                print("")
                print("%%mapr cli")
                print("stream create")
                print(" Prints the help for maprcli stream create ")
                print("")
                print("%%mapr cli")
                print("stream create -path /path/to/your/stream")
                print(" Attempts to create a Mapr Stream at /path/to/your/stream" )
                print("")
            elif line.lower() == "status":
                self.retConnStatus()
            elif line.lower() == "disconnect":
                self.disconnectMapr()
            elif line.lower() == "connect alt":
                self.connectMapr(False)
            elif line.lower() == "connect":
                self.connectMapr(True)
            elif line.lower() == 'cli':
                self.printHelp('tophelp')
            else:
                print("I am sorry, I don't know what you want to do, try just %mapr for help options")
        elif cell is not None and line == "cli":
            if self.mapr_connected == True:
                print("MapR CLI Requested with %s" % cell)
                ret, rest, helpcontext  = self.convertCli2Rest(cell.strip())
                if ret == 1:
                    code, text = self.sendMaprRequest(rest)
                    try:
                        resp = json.loads(text)
                        curstatus = resp['status']
                    except:
                        resp = {}
                        curstatus = ''
                    if code == 200 and curstatus == 'OK':
                        print(json.dumps(resp, sort_keys=False, indent=4, separators=(',', ': ')))
                    else:
                        print("Statement not successful")
                        print(text)
                        self.printHelp(helpcontext)
                else:
                    print("Count not convert %s to rest" % cell)

        elif cell is not None and line == "rest":
            if self.mapr_connected == True:
                code, text = self.sendMaprRequest(cell)
                print("Code: %s\n%s" % (code, text))
            else:
                print("Mapr is not connected: Please see help at %mapr  - To Connect: %mapr connect")






  
