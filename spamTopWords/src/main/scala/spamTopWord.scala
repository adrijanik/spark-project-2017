// spamTopWord.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//not to see logs
import org.apache.log4j.Logger
import org.apache.log4j.Level





/* Spark Context */
object Spark {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}

object spamTopWord {

  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {

	//read the files
	val rdd = sc.wholeTextFiles(filesDir)
	//get the number of files
        val nbFiles = rdd.count()

    	val stopWords = Set(".", ":", ",", " ", "/", "\\", "-", "'", "(", ")", "@")
	// get the words in an email, delete the dublicate in one email, delete the stop words    	
    	val wordBagRdd: RDD[(String, Set[String])] = rdd.map(textTuple => 
		(textTuple._1, textTuple._2.trim().
		split("\\s+").toSet.diff(stopWords)))
    	// count the words in all emails
    	val wordCountRdd: RDD[(String, Int)] = wordBagRdd.flatMap(x => x._2.map(y => (y, 1))).reduceByKey(_ + _)

	//calculate the probability
    	val probaWord: RDD[(String, Double)] = wordCountRdd.map(x => (x._1, x._2.toDouble / nbFiles))
        return (probaWord, nbFiles)


  }


  def computeMutualInformationFactor(
    probaWC: RDD[(String, Double)],//prob of just a class, some word could not be 
    probaW: RDD[(String, Double)],//all words prob, all word
    probaC: Double, //prb of a class : class mails / all mails
    probaDefault: Double // default value when a probability is missing
  ): RDD[(String, Double)] = {
	    //got (word,(prob for both classes, prob for class)), if the prob for class does not exist set the default
	    val probWJoin: RDD[(String, (Double, Option[Double]))] = probaW.leftOuterJoin(probaWC)
				
	    val valueClassAndOcu: RDD[(String, (Double, Double))] = probWJoin.map(x => (x._1, (x._2._1, x._2._2.getOrElse(probaDefault))))
	    //calculate the formula for mutual information
	    valueClassAndOcu.map(x => (x._1, x._2._2 * (math.log(x._2._2 / (x._2._1 * probaC)) / math.log(2.0))))
  }

  def main(args: Array[String]) {
	//comment these if you want to see some logs
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

  	if(args.size > 0){
		//initiate spark context
		val conf = new SparkConf().setAppName("Spam Filter Application").setMaster("local")
		val sc = new SparkContext(conf)
		println("Got the path:"+args(0))
		// args(0) should be something like "hdfs:///project/, see readme

		//process ham files
		val (probaHW, nbHFiles) = probaWordDir(sc)(args(0)+"ham/*.txt")

		//process span files
		val (probaSW, nbSFiles) = probaWordDir(sc)(args(0)+"span/*.txt")

		//some debug info
		print("number of files in "+ args(0)+"ham/*.txt" +":")
		println(nbHFiles)
		probaHW.top(10)(Ordering[Double].on(x => x._2)).foreach{ println }
		print("number of files in "+ args(0)+"span/*.txt" +":")
		println(nbSFiles)
		probaSW.top(10)(Ordering[Double].on(x => x._2)).foreach{ println }

		val nbFiles = nbSFiles + nbHFiles
		//a trick to multiply the correct probability
		val probaWs = probaSW.map(x => (x._1,(x._2,1))).union(probaHW.map(x => (x._1,(x._2,0))))
		val probaW = probaWs.reduceByKey((x,y) => if(y._2<1) ((x._1*nbSFiles.toDouble+y._1*nbHFiles.toDouble)/(nbFiles.toDouble),1) else ((y._1*nbSFiles.toDouble+x._1*nbHFiles.toDouble)/(nbFiles.toDouble) ,0)) .map(x => (x._1,x._2._1))

	        //Compute the probability P(occurs, class) for each word.

		val probaH = nbHFiles.toDouble / nbFiles.toDouble 
		val probaS = nbSFiles.toDouble / nbFiles.toDouble

		//compute the mutual information : 1.000001 is for Subject, it is on all email, could just put in the stop words
		val MITrueHam = computeMutualInformationFactor(probaHW, probaW, probaH, 0.2 / nbFiles) // the last is a default value
		val MITrueSpam = computeMutualInformationFactor(probaSW, probaW, probaS, 0.2 / nbFiles)
		val MIFalseHam = computeMutualInformationFactor(probaHW.map((_._1, 1.00000001 - _._2)), probaW, probaH, 0.2 / nbFiles)
		val MIFalseSpam = computeMutualInformationFactor(probaSW.map((_._1, 1.000000001 - _._2)), probaW, probaS, 0.2 / nbFiles)

		//sum the mutual information 
		val MI :RDD[(String, Double)] = MITrueHam.union(MITrueSpam).union(MIFalseHam).union(MIFalseSpam).reduceByKey(_ + _)

		//save the top 20 words
	        val path: String = "/tmp/topWords.txt"
		val topTenWords: Array[(String, Double)] = MI.top(20)(Ordering[Double].on(_._2))

		//debug
		println("print 20 words:")
		topTenWords.foreach{ println }
		sc.parallelize(topTenWords).keys.coalesce(1, true).saveAsTextFile(path)
	}
	else
		println("Please write te directory where the ham and span")       



  }

} // end of spamTopWord





