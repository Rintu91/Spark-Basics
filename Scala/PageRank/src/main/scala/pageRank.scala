import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner

class DomainPartitioner(numParts: Int) extends Partitioner{
	override def numPartitions: Int = numParts
	override def getPartition(key: Any): Int={
		val partId = (key.toString.toInt/numParts)
		println("NodeId: "+key+ "partId:" +partId)
		if (partId>=numParts){
			numParts - 1
		}
		else{
			partId
		}
	}
	override def equals(other: Any): Boolean = other match{
		case dnp: DomainPartitioner=>
			dnp.numPartitions==numPartitions
		case _=>
			false
	}
}

object pageRank{
	def main(args: Array[String]){
		val conf = new SparkConf().setMaster("local").setAppName("word count")
                val sc   = new SparkContext(conf)
		
		val inputFile = "file:"+args(0)
		val readInput = sc.textFile(inputFile)		
	 	
		val blinks = readInput.map(x=>x.split(" ")).map(x=>(x(0),x(1)))
		val links = blinks.groupByKey().partitionBy(new DomainPartitioner(4)).persist()
		
		var ranks = links.mapValues(x=>1.0)
		
		for(i<-0 until args(1).toInt){
			val contrib = links.join(ranks).flatMap{
				case(nodeId,(dsts,rank))=>dsts.map(dst=>(dst,rank/dsts.size))
			}
			ranks = contrib.reduceByKey((x,y)=>x+y).mapValues(x=>0.15+0.85*x)

		}
		ranks.saveAsTextFile("file:"+args(2))
	}
}
