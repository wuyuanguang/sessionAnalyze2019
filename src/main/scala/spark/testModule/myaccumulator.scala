package spark.testModule

import org.apache.spark.{SparkConf, SparkContext}

object myaccumulator {
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
//    val sc = new SparkContext(sparkConf)
//    val accum = sc.longAccumulator("longAccum")
//    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{
//      accum.add(1L)
//      n+1
//    })
//    numberRDD.count
//    println("accum1:"+accum.value)
//    numberRDD.reduce(_+_)
//    println("accum2: "+accum.value)
//  }
def main(args: Array[String]): Unit = {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName(getClass.getName))
  val extractSessionids = Array(Array(("1", "2"), ("1", "2"), ("1", "2")))
  //    extractSessionids.add(("1", "2"))
  //    extractSessionids.add(("1", "2"))
  //    extractSessionids.add(("1", "2"))
  //    extractSessionids.add(("1", "2"))
  //    extractSessionids.add(("1", "2"))
  //    val unit: Array[AnyRef] = extractSessionids.toArray()
  val data = sc.parallelize(extractSessionids)
  val tupleToOnceToArray = data.flatMap(x => x)
}


}


