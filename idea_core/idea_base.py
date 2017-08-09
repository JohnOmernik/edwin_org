#!/usr/bin/python

import requests
import json
import sys
import os
import pandas as pd
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)


@magics_class
class Demo(Magics):
    demo_calls = 0
    myip = None
    def __init__(self, shell, *args, **kwargs):
        super(Demo, self).__init__(shell)
        self.myip = get_ipython()
        self.demo_calls = 0
    def demoHelp(self):
        print("This is a Demo Help function!")
        print("Isn't it grand?")
        print("")
        print("The available items:")
        print("")
        print("%idea         - This help!")
        print("%idea show user - Show ideas, if you put in user it will only show ideas from that user")
        print("%%idea - Put an idea on the next line!")
        print("")


    def showDemoCount(self):
        print("The demo magic has been called %s times including this one!" % self.demo_calls)
        print("")

    def clearDemoCount(self):
        print("We are clearing your demo count!")
        self.demo_calls = 0

    def incrDemoCount(self):
        print("demo_calls + 1!")
        self.demo_calls += 1

    @line_magic
    def demo(self, line):
        self.incrDemoCount()
        if line == "":
            self.demoHelp()
        elif line.find("show") >= 0:
            self.showDemoCount()
        elif line.find("clear") >= 0:
            self.clearDemoCount()
        else:
            print("I am really unclear in what you want me to with %s" % line)
            print("Try just doing %demo")



