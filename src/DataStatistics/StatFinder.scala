package DataStatistics

import java.io.{File, PrintWriter}
import java.util.Date

import FormatData.{DataFormatter, fileReaderLBSN}

import scala.collection.mutable.ListBuffer

/**
 * Created by XXX on 4/25/XXX.
 */
class StatFinder {

  def friendsStats(friendsFile:String): Unit ={
    val fr=new fileReaderLBSN
    val rel=fr.readFriendsFile(friendsFile)
    println("relationships are ::"+rel.size)

  }

  def checkinStats(checkinFile:String): Unit ={
    val fr=new fileReaderLBSN
    val checks=fr.readCheckinFile(checkinFile)
    println("total Checkins are::"+checks.size)
    val users=checks.map(t=> t._1).distinct
    println("users are ::"+users.size)
    val locations=checks.map(t=> t._3).distinct
    println("locations are ::"+locations.size)

  }

  def computeFriendsStatistics(friends: List[(Long, Long)]): (Long, Long, Long) = {
    //val totalUsers=friends.map(t=> t._1).distinct.size
    var totalUsers:ListBuffer[Long]=new ListBuffer[Long]()
    friends.map{t =>
      totalUsers +=  t._1
      totalUsers += t._2
    }
    totalUsers=totalUsers.distinct
    val totalLinks = friends.distinct.size //u1->u2, u2->u1 counted as two links
    val uniqueLines = friends.map { t => // max edges among user nodes is 1
        if (t._1 < t._2) (t._2, t._1)
        else (t._1, t._2)
      }.distinct.size
    (totalUsers.size, totalLinks, uniqueLines)
  }

  def computeCheckinStatistics(checkins: List[(Long, Date, Double, Double, String, Long,String)]): (Long,Long,Long,Long) = {
    val totalCheckins = checkins.distinct.size
    val totalVisitor = checkins.map(t => t._1).distinct.size
    val totalLocations = checkins.map(t => t._6).distinct.size
    val totalGPSCoordinates = checkins.map(t => (t._3, t._4)).distinct.size
    (totalVisitor,totalLocations,totalCheckins,totalGPSCoordinates)
  }

  /**Analysis should be put in another file not in Stat Finder !!!*/
  def analyseClusters(checkinFile:String,clusterFile:String): Unit ={
    val df=new DataFormatter
    val lbsnReader=new fileReaderLBSN

    val checkins=lbsnReader.readCheckinFile(checkinFile)
    val coords=checkins.map(t=> (t._3,t._4)).distinct

    val gpsToXyzMap=df.convertGPSToXYZ(coords)
    val xyzToGpsMap=gpsToXyzMap.map(_.swap)
    val clusterLines=scala.io.Source.fromFile(clusterFile).getLines().toList
      .map(t=> t.split("\t")).map(t=> (t(0).toLong,t(1).toDouble,t(2).toDouble,t(3).toDouble))
    val clusterId=clusterLines.groupBy(t=> t._1)
      .map(t=> (t._1,t._2.map(it=> (it._2,it._3,it._4))))
      .toList.sortBy(t=> -t._2.size)
    println("Total clusters::"+clusterId.size)

    clusterId.take(30).foreach {t=>
      val top = t//clusterId(550)
      //println("id,size is::" + top._1, top._2.size)
      top._2.foreach { t =>
        val value: (Double, Double) = xyzToGpsMap.getOrElse((t._1, t._2, t._3), null)
        println(value._1 + "," + value._2)
      }
    }
  }

  def analyseCheckins(checkins: List[(Long, Date, Double, Double, String, Long,String)],
                      minUserCk:Long,minUserLocs:Long,minLocCk:Long,minLocVisitors:Long): (Long,Long,Long,Long) ={
    val UsersOnCheckins=checkins.groupBy(t=> t._1)
      .filter(t=> t._2.size>minUserCk)
    .filter(t=> t._2.map(t=>t._6).distinct.size>minUserLocs)//tem to combine parameters
    val UsersOnLocations=checkins.groupBy(t=> t._1)
      .map(t=> (t._1,t._2.map(it=> it._6).distinct))
      .filter(t=> t._2.size>minUserLocs)
    val LocsOnCheckins=checkins.groupBy(t=> t._6)
      .filter(t=> t._2.size >=minLocCk)
      .map(t=> (t._1,t._2.map(it=> it._1).distinct))//temp to combine parameters
      .filter(t=> t._2.size > minLocVisitors)//temp to combine
    val LocsOnUsers=checkins.groupBy(t=> t._6)
      .map(t=> (t._1,t._2.map(it=> it._1).distinct))
      .filter(t=> t._2.size > minLocVisitors)
    return (UsersOnCheckins.size,UsersOnLocations.size,LocsOnCheckins.size,LocsOnUsers.size)
  }

  def analyseDataWithData(friendsInput: List[(Long,Long)],checkinsInput: List[(Long, Date, Double, Double, String, Long,String)] ): Unit = {

    val fileReaderLBSN = new fileReaderLBSN
    val numberFormatter = java.text.NumberFormat.getIntegerInstance
    val friends = friendsInput//fileReaderLBSN.readFriendsFile(friendsFile) // friends
    val checkins = checkinsInput//fileReaderLBSN.readCheckinFile(checkinFile) // check-ins

    /**Data Cleaner: Synchronize Users in friends file and Visitors in check-inFile*/
    val df=new DataFormatter
    val filteredData=df.synchronizeUsersWithVisitors(friends,checkins)//returns friends, checkins

    println("*****Friends Statistics*****")
    /**Friend statistics*/
    val friendStats=computeFriendsStatistics(filteredData._1)
    println("TotalUsers:"+numberFormatter.format(friendStats._1))
    println("Total friendship Links:"+numberFormatter.format(friendStats._2))
    println("Unique friendship Links:"+numberFormatter.format(friendStats._3))

    println("******Check-ins Statistics******")
    /**Check-in Statistics*/
    val checkinStats=computeCheckinStatistics(filteredData._2)
    println("Total Visitors::"+numberFormatter.format(checkinStats._1))
    println("Total Locations::"+numberFormatter.format(checkinStats._2))
    println("Total Check-ins::"+numberFormatter.format(checkinStats._3))
    println("Total GPS Coordinates::"+numberFormatter.format(checkinStats._4))


    /**Analyse checkins*/
    val minUserCheckins=20
    val minUserLocations=10
    val minLocCheckins=20
    val minLocVisitors=10
    val checkinAnalysis=analyseCheckins(filteredData._2,minUserCheckins,minUserLocations,minLocCheckins,minLocVisitors)
    println("Number of users with check-ins greater than "+minUserCheckins+" are::"+checkinAnalysis._1)
    println("Number of users with unique locations greater than "+minUserLocations+" are::"+checkinAnalysis._2)
    println("Number of locations with check-ins greater than "+minLocCheckins+" are::"+checkinAnalysis._3)
    println("Number of locations with unique users greater than "+minLocVisitors+" are::"+checkinAnalysis._4)

    //println("Apply Clustering !!")

    //println("Starting Modifying data by using clustering ids")

    /*

        //repeating
        println("******Check-ins Statistics******")
        /**Check-in Statistics*/
        val checkinStats1=computeCheckinStatistics(updatedData._2)
        println("Total Visitors::"+numberFormatter.format(checkinStats1._1))
        println("Total Locations::"+numberFormatter.format(checkinStats1._2))
        println("Total Check-ins::"+numberFormatter.format(checkinStats1._3))
        println("Total GPS Coordinates::"+numberFormatter.format(checkinStats1._4))


        /**Analyse checkins*/
        //val minUserCheckins=20
        //val minUserLocations=10
        //val minLocCheckins=50
        //val minLocVisitors=20
        val checkinAnalysis1=analyseCheckins(clusteredCheckins,minUserCheckins,minUserLocations,minLocCheckins,minLocVisitors)
        println("Number of users with check-ins greater than "+minUserCheckins+" are::"+checkinAnalysis1._1)
        println("Number of users with unique locations greater than "+minUserLocations+" are::"+checkinAnalysis1._2)
        println("Number of locations with check-ins greater than "+minLocCheckins+" are::"+checkinAnalysis1._3)
        println("Number of locations with unique users greater than "+minLocVisitors+" are::"+checkinAnalysis1._4)
    */

    /**Compute probabilities*/



  }
  def analyseData(friendsFile: String, checkinFile: String): Unit = {

    val fileReaderLBSN = new fileReaderLBSN
    val numberFormatter = java.text.NumberFormat.getIntegerInstance
    val friends = fileReaderLBSN.readFriendsFile(friendsFile) // friends
    val checkins = fileReaderLBSN.readCheckinFile(checkinFile) // check-ins

    /**Data Cleaner: Synchronize Users in friends file and Visitors in check-inFile*/
    val df=new DataFormatter
    val filteredData=df.synchronizeUsersWithVisitors(friends,checkins)//returns friends, checkins

    println("*****Friends Statistics*****")
    /**Friend statistics*/
    val friendStats=computeFriendsStatistics(filteredData._1)
    println("TotalUsers:"+numberFormatter.format(friendStats._1))
    println("Total friendship Links:"+numberFormatter.format(friendStats._2))
    println("Unique friendship Links:"+numberFormatter.format(friendStats._3))

    println("******Check-ins Statistics******")
    /**Check-in Statistics*/
    val checkinStats=computeCheckinStatistics(filteredData._2)
    println("Total Visitors::"+numberFormatter.format(checkinStats._1))
    println("Total Locations::"+numberFormatter.format(checkinStats._2))
    println("Total Check-ins::"+numberFormatter.format(checkinStats._3))
    println("Total GPS Coordinates::"+numberFormatter.format(checkinStats._4))


    /**Analyse checkins*/
    val minUserCheckins=20
    val minUserLocations=10
    val minLocCheckins=20
    val minLocVisitors=10
    val checkinAnalysis=analyseCheckins(filteredData._2,minUserCheckins,minUserLocations,minLocCheckins,minLocVisitors)
    println("Number of users with check-ins greater than "+minUserCheckins+" are::"+checkinAnalysis._1)
    println("Number of users with unique locations greater than "+minUserLocations+" are::"+checkinAnalysis._2)
    println("Number of locations with check-ins greater than "+minLocCheckins+" are::"+checkinAnalysis._3)
    println("Number of locations with unique users greater than "+minLocVisitors+" are::"+checkinAnalysis._4)

    //println("Apply Clustering !!")

    //println("Starting Modifying data by using clustering ids")
/*

    //repeating
    println("******Check-ins Statistics******")
    /**Check-in Statistics*/
    val checkinStats1=computeCheckinStatistics(updatedData._2)
    println("Total Visitors::"+numberFormatter.format(checkinStats1._1))
    println("Total Locations::"+numberFormatter.format(checkinStats1._2))
    println("Total Check-ins::"+numberFormatter.format(checkinStats1._3))
    println("Total GPS Coordinates::"+numberFormatter.format(checkinStats1._4))


    /**Analyse checkins*/
    //val minUserCheckins=20
    //val minUserLocations=10
    //val minLocCheckins=50
    //val minLocVisitors=20
    val checkinAnalysis1=analyseCheckins(clusteredCheckins,minUserCheckins,minUserLocations,minLocCheckins,minLocVisitors)
    println("Number of users with check-ins greater than "+minUserCheckins+" are::"+checkinAnalysis1._1)
    println("Number of users with unique locations greater than "+minUserLocations+" are::"+checkinAnalysis1._2)
    println("Number of locations with check-ins greater than "+minLocCheckins+" are::"+checkinAnalysis1._3)
    println("Number of locations with unique users greater than "+minLocVisitors+" are::"+checkinAnalysis1._4)
*/

    /**Compute probabilities*/



  }

  def analyseGPSPoints(checkinFile:String): Unit ={
    val fileReaderLBSN = new fileReaderLBSN
    val checkins=fileReaderLBSN.readCheckinFile(checkinFile)
    val gpsPoints=checkins.map(t=> (t._3,t._4)).distinct
    val sorted=gpsPoints.groupBy(t=> t._1).toList.map(t=> t._2.sortBy(it=> it._2)).filter(t=> t.size>10).take(10)
      .sortBy(t=> -t.size).foreach(t=> t.foreach(it=> println(it._1+","+it._2)))
  }

  def findTopKLocations(checkinFile:String, k:Int): Unit ={
    println("Finding top-k locations")
    val freader=new fileReaderLBSN
    val checkins=freader.readCheckinFile(checkinFile)
    val locations=checkins.groupBy(t=> t._6).toList.sortBy(t=> -t._2.size).take(k).map(t=> t._1)
    locations.foreach(t=> println(t))
  }
  def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
    num.toDouble( ts.sum ) / ts.size
  }
  def averageFriends(friendsFile:String): Unit ={
    val freader=new fileReaderLBSN
    val friends=freader.readFriendsFile(friendsFile)
    val group1=friends.groupBy(t=>t._1).map(t=> (t._1,t._2.map(it=> it._2).distinct))
    val group2=friends.groupBy(t=>t._2).map(t=> (t._1,t._2.map(it=> it._1).distinct))
    val totalGroup= group1 ++ group2
    val eachUser=totalGroup.toList.map(t=> (t._1,t._2.size))//.take(10).foreach(println)
    val avg=average(eachUser.map(t=> t._2))
    println("average friends are::"+avg)

  }
}
