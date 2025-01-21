'''
Utility function that allows a Python program to write a Ziggy PID file. 
Created on Oct 1, 2021

@author: PT
'''

import socket
import os

def write_pid_file():
    process_id = os.getpid()
    hostname = socket.gethostname()
    pid_string = hostname + ":" + str(process_id)
    encoded_string = pid_string.encode("ISO-8859-1")
    pid_file = open(".matlab.pids", "wb")
    pid_file.write(encoded_string)
