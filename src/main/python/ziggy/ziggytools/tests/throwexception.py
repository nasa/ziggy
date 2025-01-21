'''
Throws a divide-by-zero exception that is comes up from several levels down in the call
stack, thus producing a stack trace that can be inspected. The file is here so that 
changes to the stacktracetests.py file don't result in changes to the line numbers at which the
exceptions are thrown.

Created on Nov 23, 2020

@author: PT
'''
class ExceptionGenerator:
    def __init__(self):  
        self.call1()
        
    def call1(self):        
        self.call2()
        
    def call2(self):
        z = int(1) / int(0)
        return z