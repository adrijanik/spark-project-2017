// spamTopWord.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object spamTopWord {

  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {

	val probaWord: RDD[(String, Double)] = sc.emptyRDD[(String, Double)]
	val nbFiles = 0

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

  
	println("it works!")

  }

} // end of spamTopWord





