'''
Created on Nov 10, 2021

@author: PT
'''

import os

def chmod(path, file_mode):
    for root, dirs, files in os.walk(path):
        for dir in [os.path.join(root, d) for d in dirs]:
            os.chmod(dir, file_mode)
        for file in [os.path.join(root, f) for f in files]:
            os.chmod(file, file_mode)