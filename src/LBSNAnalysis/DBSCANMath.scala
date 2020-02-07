package LBSNAnalysis

import java.io.{File, PrintWriter}

import IndexedDBSCAN.DBSCANNlogN
import ca.pfv.spmf.patterns.cluster.DoubleArray
import org.apache.commons.math3.ml.clustering._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
 * Created by XXX on 4/21/XXX.
 */
class DBSCANMath {
  def getClusters(checkinFile: String, writeFile: String): Unit = {
    val minClusterSize = 0
    val eps = 1000 //in meters
    val writer = new PrintWriter(new File(writeFile + "_" + eps + ".txt"))
    val chks = scala.io.Source.fromFile(checkinFile).getLines().toList //.drop(2).toList
    println("total checkins::" + chks.size)
    val locs = chks.map(t => t.split("\t")).map(t => (t(5), (t(2).toDouble, t(3).toDouble))).distinct
    println("total unique locations-coordinate pairs::" + locs.size)
    val locIds = locs.map(t => t._1).distinct
    println("total unique location ids ::" + locs.size)
    val coords = locs.map(t => t._2).distinct
    println("total unique coordinates::" + coords.size)
    val r = 6371000 //meters

    val sTime = System.currentTimeMillis()
    val xyzCoords = coords.map(t => (r * Math.cos(t._1.toRadians) * Math.cos(t._2.toRadians),
      r * Math.cos(t._1.toRadians) * Math.sin(t._2.toRadians), r * Math.sin(t._1.toRadians)))
    println("converted Unique Coordinates(total,unique)::" + xyzCoords.size)
    println("Time ::" + (System.currentTimeMillis() - sTime))


    // create map
    val GPSToXYZMap = scala.collection.mutable.Map[(Double, Double, Double), (Double, Double)]()
    for (i <- 0 until coords.size) {
      GPSToXYZMap += ((xyzCoords(i) -> coords(i)))
    }

    /*val utmCoords=scala.io.Source.fromFile(utmFile).getLines().drop(1).toList.map(t=> t.replaceAll("\\(","").replaceAll("\\)",""))
      .map(t=> (t.split(","))).map(t=> (t(3).split(" "))).map(t=> (t(1).toDouble,t(0).toDouble))*/
    // convert long,lat to lat,long

    val ds = xyzCoords.map(t => new point(t._1, t._2, t._3))
    println("data set size::" + ds.size)
    //utmCoords.foreach(t=> println(t))
    /*

        val testArray=List((25.667713,-100.386301),(33.900483,-117.890205),(35.900483,-119.890205))
        val testArrayMap=testArray.map(t=> new point(t._1,t._2))
    */

    val dbscanmath = new DBSCANClusterer[point](eps, minClusterSize)
    val indDBSCAN = new DBSCANNlogN()

    val dList: ListBuffer[DoubleArray] = ListBuffer()
    //val d:DoubleArray=
    //dList += new DoubleArray(Array(0.1,0.2))


    val xyzArray = xyzCoords.map(t => dList += new DoubleArray(Array(t._1, t._2, t._3))) //Array(t._1,t._2,t._3)
    println("dlist size ::" + dList.size)
    //dList.foreach(t=> println("class is::"+t.getClass))
    //val clusters = indDBSCAN.runAlgorithmOnArray(minClusterSize, eps, dList.asJava)

    val clusters=dbscanmath.cluster(ds.asJavaCollection)
    println("results size is ::" + clusters.size)
    /*var clusterCount=0
    for(i<-0 until clusters.size()){
      clusterCount += 1

      val c=clusters.get(i)

      val points=c.getPoints
        //println(" a new cluster with more than one point")
      /*if(points.size()>=50){
        println(" new cluster::"+points)
        for (j <- 0 until points.size()) {
          val p=points.get(j)
          val coords=GPSToXYZMap.getOrElse((p.x,p.y,p.z),(0.0,0.0))
          println(coords._1+","+coords._2)
          //println("point, coords::"+p.x,p.y,p.z+"-"+coords)
          //writer.println(clusterCount+"\t" +p.x+"\t"+p.y+"\t"+p.z)
          //println(points.get(j).lat, points.get(j).lat)
        }
      }*/
        for (j <- 0 until points.size()) {
          val p=points.get(j)
          writer.println(clusterCount+"\t" +p.x+"\t"+p.y+"\t"+p.z)
          //println(points.get(j).lat, points.get(j).lat)
        }
      }*/
    writer.close()
  }

}

class point(xInput: Double, yInput: Double, zInput: Double) extends Clusterable {
  val x: Double = xInput
  val y: Double = yInput
  val z: Double = zInput

  def getPoint(): Array[Double] = {
    Array(x, y, z)
  }
}
