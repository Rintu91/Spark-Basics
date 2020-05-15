from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import sys
def formatInput(x):
	values = x.split(' ')
	return (int(values[0]),int(values[1]))
def calContrib(dstList, rank):
	length = len(dstList)
	for j in dstList:
		yield (j,rank/length)

def partFunc(pageId,numParts):
	partNum = pageId/numParts
	print("Node Id: %i, partNum: %i"%(pageId,partNum))
	return partNum

if __name__ == "__main__":
	#run format <pageRank, inputFile, iterations, outputFile, numMachines>
	if len(sys.argv) != 5:
		print("Please verify input format")
		sys.exit(-1)
	conf = SparkConf().setMaster("local").setAppName("pageRank")
	sc   = SparkContext(conf=conf)
	
	inputFile = 'file:'+sys.argv[1]
	readInput = sc.textFile(inputFile)
	#format the input in (src,list(dst)) format
	links = readInput.map(lambda x:formatInput(x))
	links = links.groupByKey()
	#Partition the graph using range partitioner
	numMachines = int(sys.argv[4])
	links = links.partitionBy(numMachines, lambda x: partFunc(x,numMachines)).persist()
	
	
	#initialize ranks by (src,1)
	ranks = links.mapValues(lambda x: 1.0)
	
	for i in range(0,int(sys.argv[2])):
		#join will create a list of format (src,(list(dst),rank_src)) format
		#use flatMap to get it in (dst,rank_src) format
		contrib = links.join(ranks).flatMap(lambda x: calContrib(x[1][0],x[1][1]))
		#now use reduceByKey to get contribution for the node and then calculate pageRank by 0.15+0.85*contrib(node)
		ranks   = contrib.reduceByKey(lambda x,y: x+y).mapValues(lambda x: 0.15+0.85*x)

	#now save the result to outputFolder
	ranks.saveAsTextFile('file:'+sys.argv[3])
	
