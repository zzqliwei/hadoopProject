import sys
import os

#input = "test"
input = sys.stdin
env_keys = os.environ.keys()
env = ""
if "env" in env_keys:
	env = os.environ["env"]
out_file = open("/home/hadoop-twq/spark-course/outpy.txt", "w")
for ele in input:
	output = "slave1-" + ele.strip('\n') + "-" + env
    	print (output)
	out_file.write(output)

input.close
out_file.close()
