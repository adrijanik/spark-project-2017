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
    probaWC: RDD[(String, Double)],//prob of just a class, some word could not be 
    probaW: RDD[(String, Double)],//all words prob, all word
    probaC: Double, //prb of a class : class mails / all mails
    probaDefault: Double // default value when a probability is missing
  ): RDD[(String, Double)] = {
	    //p(occurs) = 
	    val probWJoin: RDD[(String, (Double, Option[Double]))] = probaW.leftOuterJoin(probaWC)// got all class probs, if not -> default
				//p(accurs)  p(accurs,class) 
	    val valueClassAndOcu: RDD[(String, (Double, Double))] = probWJoin.map(x => (x._1, (x._2._1, x._2._2.getOrElse(probaDefault))))
	    //We have to change ln to log2 (by using ln(x)/ln(2)=log2(x)
	    valueClassAndOcu.map(x => (x._1, x._2._2 * (math.log(x._2._2 / (x._2._1 * probaC)) / math.log(2.0))))
  }

  def main(args: Array[String]) {

  	if(args.size > 0){
		val conf = new SparkConf().setAppName("Spam Filter Application").setMaster("local")
		val sc = new SparkContext(conf)
		println("Got the path:"+args(0))
		// args(0) should be something like "hdfs:///project/, see readme

		//process ham files
		val (probaHW, nbHFiles) = probaWordDir(sc)(args(0)+"ham/*.txt")

		//process span files
		val (probaSW, nbSFiles) = probaWordDir(sc)(args(0)+"span/*.txt")
		print("number of files in "+ args(0)+"ham/*.txt" +":")
		println(nbHFiles)
		print("number of files in "+ args(0)+"span/*.txt" +":")
		println(nbSFiles)

		val nbFiles = nbSFiles + nbHFiles
		val probaW = probaSW.union(probaHW).reduceByKey((x,y) => (x*nbSFiles.toDouble+y*nbSFiles.toDouble)/(nbFiles.toDouble)) //not sure

	        //Compute the probability P(occurs, class) for each word.

		val probaH = nbHFiles.toDouble / nbFiles.toDouble // the probability that an email belongs to the given class.
		val probaS = nbSFiles.toDouble / nbFiles.toDouble
		// Compute mutual information for each class and occurs
		val MITrueHam = computeMutualInformationFactor(probaHW, probaW, probaH, 0.2 / nbFiles) // the last is a default value
		val MITrueSpam = computeMutualInformationFactor(probaSW, probaW, probaS, 0.2 / nbFiles)
		val MIFalseHam = computeMutualInformationFactor(probaHW.map(x => (x._1, 1 - x._2)), probaW, probaH, 0.2 / nbFiles)
		val MIFalseSpam = computeMutualInformationFactor(probaSW.map(x => (x._1, 1 - x._2)), probaW, probaS, 0.2 / nbFiles)

		//compute the mutual information of each word as a RDD with the map structure: word => MI(word)
		//sum the prob for all words
		val MI :RDD[(String, Double)] = MITrueHam.union(MITrueSpam).union(MIFalseHam).union(MIFalseSpam).reduceByKey( (x, y) => x + y)

		// print on screen the 10 top words (maximizing the mutual information value)
		//These words must be also stored on HDFS in the file “/tmp/topWords.txt”.
	        val path: String = "/tmp/topWords.txt"
		val topTenWords: Array[(String, Double)] = MI.top(10)(Ordering[Double].on(x => x._2))
		//coalesce to put the results in a single file
		sc.parallelize(topTenWords).keys.coalesce(1, true).saveAsTextFile(path)
	}
	else
		println("Please write te directory where the ham and span")       



  }

} // end of spamTopWord





