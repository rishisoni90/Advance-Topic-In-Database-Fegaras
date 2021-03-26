/* Implemented BY RISHI KUMAR SONI 1001774020 */

/* Packages , importing the functionalities...*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()

/* Calculating  distance by using the Math square root function and seting into the variable distance   */


/* PASSING TWO ARGUMENT    */
  def distance (p: Point, point: Point): Double ={

    var distance = Math.sqrt ((p._1 - point._1) * (p._1 - point._1) + (p._2 - point._2) * (p._2 - point._2) );
    distance
  }



  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)}).collect
    var points=sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})
/*   PROCESS OF FINDING NEW CENTROID FROM PREVIOUS CENTROID USING K MEANS  */
    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
        .groupByKey().map { case(c,pointva)=>
        var count=0
        var R1=0.0
        var R2=0.0

        for(ps <- pointva) {
           count += 1
           R1+=ps._1
           R2+=ps._2
        }
        var sam1=R1/count
        var sam2=R2/count
        (sam1,sam2)

      }.collect
    }

centroids.foreach(println)
    }}

/* IMPLEMENTED AND TESTED BY RISHI KUMAR SONI , 1001774020.... */


