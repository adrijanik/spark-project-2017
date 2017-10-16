// spamTopWord.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


/* Spark Context */
object Spark {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}

object spamTopWord {

  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {
        val data = sc.wholeTextFiles(filesDir).collect()

        val files = data.map { case (filename, content) => filename}
       
	var nbFiles = 0
        
        nbFiles = files.size
        println(nbFiles)
	val probaWord: RDD[(String, Double)] = sc.emptyRDD[(String, Double)]

	return (probaWord, nbFiles)
  }


  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {

   
	return probaWC

  }

  def main(args: Array[String]) {
        val context = Spark.sc
	println("Number of files:!")
        
        var files, numFiles  = probaWordDir(context)("hdfs:///tmp/ling-spam/ham/ham/")
         

        println(numFiles)


  }

} // end of spamTopWord





