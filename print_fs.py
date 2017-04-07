#!/usr/bin/python3


import os
import os.path


def print_tree(dir,
               padding=' ',
               print_files=True):
    print(padding[:-1] + '└─' +
          os.path.basename(os.path.abspath(dir)) + '/')
    padding = padding + ' '
    files = []
    if print_files:
        files = os.listdir(dir)
    else:
        files = [x for x in os.listdir(dir) if os.path.isdir(dir + os.sep + x)]
    count = 0
    for file in files:
        count += 1
        path = dir + os.sep + file
        if os.path.isdir(path):
            if count == len(files):
                print_tree(path, padding + ' ', print_files)
            else:
                print_tree(path, padding + '│', print_files)
        else:
            print(padding + '├─' + file)
