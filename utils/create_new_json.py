"""
"""

import re

def grep(pattern,fileObj):
  r = []
  linenumber=0
  for line in fileObj:
    linenumber +=1
    if re.search(pattern,line):
      print(line)

jsonFile = open('/Users/davelester/mrjob/examples/yelp_academic_dataset.json', 'r')

for business in output:
	splitBusinessData = business.split(",")
	cleanedBizData = splitBusinessData[0]
	cleanedBizData = cleanedBizData.split(" ")
	cleanedBizData = cleanedBizData[1][1:-1]
	grepData = grep(cleanedBizData, jsonFile)
	print(grepData)
	print(cleanedBizData)
	
	reviews_output.write(grepData)

#reviews_output.write(grepData)