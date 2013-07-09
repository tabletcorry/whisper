import glob
import os
import subprocess
import time
import sys

__author__ = 'chaines'

def janus_test():
    for glob_file in glob.glob("*.wsp*"):
        os.remove(glob_file)

    subprocess.check_output(["./bin/whisper-create.py", "--janus", "test.wsp", "1:60", "2:120"])
    subprocess.check_output(["./bin/whisper-fetch.py", "--janus", "test.wsp"])

def test():
    for glob_file in glob.glob("*.wsp*"):
        os.remove(glob_file)

    subprocess.check_output(["./bin/whisper-create.py", "test.wsp", "1:60", "2:120"])
    subprocess.check_output(["./bin/whisper-fetch.py", "test.wsp"])

t1 = time.time()
for i in xrange(10):
    test()
t2 = time.time()
print t2-t1
sys.exit(0)
t1 = time.time()
for i in xrange(10):
    janus_test()
t2 = time.time()
print t2-t1

