#!/usr/bin/python

import requests
import json
import sys
import random
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
    org_matrix_ver = ""
    edwin_org_base_dir = ""
    edwin_org_matrix_file = ""
    org_name = ""
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
            print("Matrix File Loaded")
        except:
            print("Could not load Edwin Org Matrix at %s" % self.edwin_org_matrix_file)
            raise Exception("Couldn't Load Matrix, I am stupid")

        try:
            self.org_name = self.org_matrix['org_name']
            self.org_matrix_ver = self.org_matrix['matrix_version']
            self.search_matrix = self.recurseTree(self.org_matrix['tree_root'], [], [], True)
        except:
            print("Was able to load Matrix file, but it wasn't what I needed to exist. Stopping Existing now")
            raise Exception("Matrix File for Edwin_Org in wrong format")

#


#
    def retScores(self, intext):
        scores = {}
        for x in self.search_matrix:
            tscore = {}
            keywords = x
            score_key = ".".join(keywords)
            cur_score = 0
            found_words = []
            all_match = 1
            for w in keywords:
                if intext.lower().find(w) >= 0:
                    cur_score += 1
                    found_words.append(w)
                else:
                    all_match = 0
                    break
            if all_match == 1:
                # We only include matches where all keywords match. If there are non-matches we ignore
                tscore = {'keywords': keywords, 'found_words': found_words, 'score': cur_score}
                scores[score_key] = tscore
        return scores

#

    def recurseTree(self, nstart, curar, basear, showall):

        for tn in nstart:
            outar = basear + [tn]
            if nstart[tn]['list'] == 1 or showall == True:
                curar.append(outar)
                if 'children' in nstart[tn]:
                    curar = self.recurseTree(nstart[tn]['children'], curar, outar, showall)
        return curar


    def getNode(self, score):
        kwar = score['keywords']
        cur = self.org_matrix['tree_root']
        first = 0
        for x in kwar:
            if first == 1:
                cur = cur['children']
            else:
                first = 1
            cur = cur[x]
        return cur

    def retNode(self, myscores):
        retnode = ""
        if myscores is None:
            retnode = self.org_matrix['tree_root']["unknown"]
        elif len(myscores) == 0:
            retnode = self.org_matrix['tree_root']["unknown"]
        elif len(myscores) == 1:
            t = ""
            for x in myscores:
               t = x 
            s = myscores[t]
            retnode = self.getNode(s)
        else:
            leaderar = []
            highscore = 0
            for s in myscores:
                curscore = myscores[s]['score']
                if curscore > highscore:
                    highscore = curscore

            for s in myscores:
                if myscores[s]['score'] == highscore:
                    leaderar.append(s)
            if len(leaderar) == 1:
                retnode = self.getNode(myscores[leaderar[0]])
            else:
                retnode = leaderar
        return retnode




    def processMulti(self, uinput, rlist):
        print("Mulitple Responses scored to be the same based on your input of %s" % uinput)
        print("I could not guess between the following nodes:")
        for x in rlist:
            print(x)
        print("")
        print("Perhaps more clarity would help me help you... help me help you")
        print("")


    def processResp(self, uinput, node):
        if node['type'] == 'rand': 
            u = random.choice(node['resp'])
            print(self.replaceText(u))
        elif node['type'] == 'full':
            for u in node['resp']:
                print(self.replaceText(u))
        if 'children' in node:
            if 'cheader' in node:
                child_head = node['cheader']
            else:
                child_head = "This topic also has some related topics you'd be interested in, try referencing:"
            print("")
            print(child_head)
            for x in node['children']:
                desc = "No Desc"
                try:
                    desc = node['children'][x]['short_desc']
                except:
                    pass

                print(" - %s - %s" % (x, desc))
        if 'root' in node:
            print("")
            print("I can offer help on a number of topics, please try mentioning any of these for more details:")
            print("")
            for x in self.org_matrix['tree_root']:
                desc = "No Desc"
                try:
                    desc = self.org_matrix['tree_root'][x]['short_desc']
                except:
                    pass
                if self.org_matrix['tree_root'][x]['list'] == 1:
                    print(" - %s - %s" % (x, desc))


#
    def replaceText(self, intext):
        outtext = intext.replace('%nbuser%', self.nbuser)
        outtext = outtext.replace('%nbsshhost%', self.nbsshhost)
        outtext = outtext.replace('%nbsshport%', self.nbsshport)
        outtext = outtext.replace('%org_name%', self.org_name)
        return outtext


    @line_magic
    def edwin_org(self, line):
        # If nothing is entered, we fix that for the user. We put in Base instead. 
        if line == "":
            line = 'edwin_org_root'
        myscores = self.retScores(line)
        responseNode = self.retNode(myscores)
        if type(responseNode) is dict:
            self.processResp(line, responseNode)
        else:
            self.processMulti(line, responseNode)







##


