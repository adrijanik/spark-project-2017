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


	val rdd = sc.wholeTextFiles(filesDir)
        // The number of files is counted and stored in a variable nbFiles
        val nbFiles = rdd.count()
        // Non informative words must be removed from the set of unique words. We have also added ! and ?
    	val stopWords = Set(".", ":", ",", " ", "/", "\\", "-", "'", "(", ")", "@")
    	// Each text file must be splitted into a set of unique words (if a word occurs several times, it is saved only one time in the set).
    	val wordBagRdd: RDD[(String, Set[String])] = rdd.map(textTuple => 
		(textTuple._1, textTuple._2.trim().
		split("\\s+").toSet.diff(stopWords)))
    	// Get the Number of occurrences amongst all files
    	val wordCountRdd: RDD[(String, Int)] = wordBagRdd.flatMap(x => x._2.map(y => (y, 1))).reduceByKey(_ + _)
    	val probaWord: RDD[(String, Double)] = wordCountRdd.map(x => (x._1, x._2.toDouble / nbFiles))
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

  	if(args.size > 0){
		val conf = new SparkConf().setAppName("Spam Filter Application").setMaster("local")
		val sc = new SparkContext(conf)
		println("Got the path:"+args(0))
		// args(0) should be something like "hdfs:///project/
		val ff = args(0)+"ham/*.txt"
		val (probaW, nbFiles) = probaWordDir(sc)(ff)
		println("number of files in "+ ff +":")
		println(nbFiles)
	}
	else
		println("Please write te directory where the ham and span")       



  }

} // end of spamTopWord





