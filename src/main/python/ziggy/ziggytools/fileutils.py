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

# Returns the algorithm step name by reading in the task directory name and
# parsing the "#-#-name" format.
def algorithm_step_name():
    working_dir = os.path.normpath(os.getcwd())
    dir_parts = working_dir.split(os.sep)
    num_parts = len(dir_parts)
    task_dir = dir_parts[num_parts - 2]
    task_dir_tokens = task_dir.split("-")
    return task_dir_tokens[2]