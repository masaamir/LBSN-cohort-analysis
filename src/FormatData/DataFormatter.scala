package FormatData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import CoordinateConversion.{Angle, UTMCoord}

import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 4/25/2016.
 */
class DataFormatter {
  // convert string to date:
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def writeLBSNData(friends: List[(Long, Long)], checkins: List[(Long, Date, Double, Double, String, Long, String)], friendsFileWrite: String, checkinFileWrite: String): Unit = {
    val friendWriter = new PrintWriter(new File(friendsFileWrite))
    friends.foreach { t =>
      friendWriter.println(t._1 + "\t" + t._2)
    }
    friendWriter.close()
    val checkinWriter = new PrintWriter(new File(checkinFileWrite))
    checkins.foreach { t =>
      checkinWriter.println(t._1 + "\t" + t._7 + "\t" + t._3 + "\t" + t._4 + "\t" + t._5 + "\t" + t._6)
    }
    checkinWriter.close()
  }

  def synchronizeUsersWithVisitors(friends: List[(Long, Long)],
                                   checkins: List[(Long, Date, Double, Double, String, Long, String)]): (List[(Long, Long)], List[(Long, Date, Double, Double, String, Long, String)]) = {
    // keep only those users in friendship links that have visits
    var totalUsers: ListBuffer[Long] = new ListBuffer[Long]()
    totalUsers = getUsersFromFriends(friends)
    val commonUsers = totalUsers.toSet.intersect(checkins.map(t => t._1).toSet) //first time, need to check it with friendship maps
    val commonUsersMap = commonUsers.map(t => (t, 1)).toMap
    val filteredFriends = friends.filter { t =>
      commonUsersMap.getOrElse((t._1), null) != null && commonUsersMap.getOrElse((t._2), null) != null
    }.distinct
    var finalUsers: ListBuffer[Long] = new ListBuffer[Long]()
    finalUsers = getUsersFromFriends(filteredFriends) //finalUsers.distinct /// because there might be few edges u1-u2 , in which u1 is common in both friends and visitors
    // but u2 is not in visitors so all the edges with u1 will be removed with those nodes that are not common, so only one intersection is not enough
    // because we need edges with both users in the common
    val finalUsersMap = finalUsers.map(t => (t, 1)).toMap
    val filteredCheckins = checkins.filter(t => finalUsersMap.getOrElse((t._1), null) != null).distinct
    (filteredFriends, filteredCheckins)
  }

  def convertGPSToXYZ(coordinates: List[(Double, Double)]): Map[(Double, Double), (Double, Double, Double)] = {
    val r = 6371000 //meters
    val GPSToXYZMap = coordinates.map { t => (
        (t._1, t._2), //gps coordinates
        (r * Math.cos(t._1.toRadians) * Math.cos(t._2.toRadians), //x
          r * Math.cos(t._1.toRadians) * Math.sin(t._2.toRadians), r * Math.sin(t._1.toRadians)) //y,z
        )
      }.toMap
    //println("converted Unique Coordinates(total,unique)::" + GPSToXYZMap.size)
    return GPSToXYZMap
  }

  def getVenuesWeeFromCheckinFile(inCheckinFile:String): Unit ={

    val checkins = scala.io.Source.fromFile(inCheckinFile).getLines().take(100)
    checkins.foreach(t=> println(t))


  }

  def getUsersFromFriends(friends: List[(Long, Long)]): ListBuffer[Long] = {
    var totalUsers: ListBuffer[Long] = new ListBuffer[Long]()
    friends.map { t =>
      totalUsers += t._1
      totalUsers += t._2
    }
    totalUsers = totalUsers.distinct
    return totalUsers
  }

  def getCheckinsWithXY(checkins: List[(Long, Date, Double, Double, String, Long, String)]): List[(Long, Date, Double, Double, String, Long, String)] ={
    checkins.map{t=>
      val xy=UTMCoord.fromLatLon(Angle.fromRadiansLatitude(t._3.toRadians),Angle.fromRadiansLongitude(t._4.toRadians))
      (t._1,t._2,xy.getNorthing,xy.getEasting,t._5,t._6,t._7)
    }
  }

  def convertLatLonToUTM(LatLonLoc:List[(Long,Double,Double)]): List[(Long,Double,Double,Double,Double,Int,String)] ={

    val UTMLocs=LatLonLoc.map{t=>
      //println("to be converted ::"+t)
      val xy=UTMCoord.fromLatLon(Angle.fromRadiansLatitude(t._2.toRadians),Angle.fromRadiansLongitude(t._3.toRadians))
      (t._1,t._2,t._3,xy.getNorthing,xy.getEasting,xy.getZone,xy.getHemisphere)
    }
    return UTMLocs //{(LocLongActual,lat,long,y,x,zone,hemishphere)}

  }

  def getClusterIds(checkins: List[(Long, Date, Double, Double, String, Long, String)], clusterFile: String): List[(Long, Date, Double, Double, String, Long, String)] = {
    val df = new DataFormatter
    val coords = checkins.map(t => (t._3, t._4)).distinct
    val gpsToXyzMap = df.convertGPSToXYZ(coords)
    val xyzToGpsMap = gpsToXyzMap.map(_.swap)
    val xyzLines = scala.io.Source.fromFile(clusterFile).getLines().toList
      .map(t => t.split("\t")).map(t => ((t(1).toDouble, t(2).toDouble, t(3).toDouble), t(0).toLong))
    val xyzToIdMap = xyzLines.toMap
    var missingPointsId = checkins.size
    var missingPointsCount = 0
    val modifiedCheckins = checkins.map { t =>
      val xyz: (Double, Double, Double) = gpsToXyzMap.getOrElse((t._3, t._4), null)
      if (xyz == null) println("error in conversion from gps to XYZ")
      var id: Long = xyzToIdMap.getOrElse((xyz._1, xyz._2, xyz._3), -1L)
      if (id == -1L) {
        missingPointsCount += 1
        missingPointsId += 1
        //testing git
        /*
        println("GPS ::"+t._3,t._4)
        println("xyz::"+xyz)
        println("search in cluster file::")
        xyzLines.filter(t=> t._1._1==xyz._1 &&t._1._2==xyz._2 &&t._1._3==xyz._3).foreach(t=> println("xyzLine::"+t))

        println("error in mapping xyz to cluster id")// not found in clustering points*/
        id = missingPointsId
      }
      (t._1, t._2, t._3, t._4, t._6.toString, id, t._7)
    }
    println("Total missing points are::" + missingPointsCount)
    return modifiedCheckins
  }

  def modifyLBSNDataWithClusteredLocationsIds(friendsFile: String, checkinFile: String, clusterFile: String
                                              , friendsFileWrite: String, checkinFileWrite: String): Unit = {

    val fileReaderLBSN = new fileReaderLBSN
    //val numberFormatter = java.text.NumberFormat.getIntegerInstance
    val friends = fileReaderLBSN.readFriendsFile(friendsFile) // friends
    val checkins = fileReaderLBSN.readCheckinFile(checkinFile) // check-ins

    /** Data Cleaner: Synchronize Users in friends file and Visitors in check-inFile */
    val df = new DataFormatter
    val filteredData = df.synchronizeUsersWithVisitors(friends, checkins) //returns friends, checkins
    /** Utilize Clusters ids */
    val clusteredCheckins = getClusterIds(filteredData._2, clusterFile)

    val updatedData = df.synchronizeUsersWithVisitors(filteredData._1, clusteredCheckins)
    //write file
    df.writeLBSNData(updatedData._1, updatedData._2, friendsFileWrite, checkinFileWrite)
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  def returnCoordinatesByLocations(keysFile: String, spreadFile: String, checkinsFile: String): Unit = {
    val keys = scala.io.Source.fromFile(keysFile).getLines().toList.map(t => t.toLong)
    val spread = scala.io.Source.fromFile(spreadFile).getLines().toList.map(t => t.toLong)
    val totalLocs = keys ++ spread
    //println("keys size::"+totalLocs.size)
    val fr = new fileReaderLBSN
    val coords = fr.readCheckinFile(checkinsFile).map(t => (t._6, (t._3, t._4))).distinct
    //println("orignial coord::"+coords.size)


    println()
    val filteredCoords = coords.filter(t => totalLocs.contains(t._1))
    //println("coord size::"+filteredCoords.size)
    val avgFiltCoords = filteredCoords.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._2))).map { t =>
      val avgLat = average(t._2.map(it => it._1))
      val avgLon = average(t._2.map(it => it._2))
      (t._1, (avgLat, avgLon))
    }
    avgFiltCoords.toList.map(t => t._2).foreach(t => println(t._1 + "," + t._2))


  }

  def returnCoordinatesByLocationsGPSV(keysFile: String, spreadFile: String
                                       , checkinsFile: String, writeFile: String): Unit = {
    val writer = new PrintWriter(new File(writeFile))
    val keys = scala.io.Source.fromFile(keysFile).getLines().toList
      .map(t => (t.toLong))
    val spread = scala.io.Source.fromFile(spreadFile).getLines().toList
      .map(t => (t.toLong))
    val totalLocs = keys ++ spread
    //println("keys size::"+totalLocs.size)
    val fr = new fileReaderLBSN
    val coords = fr.readCheckinFile(checkinsFile)
      .map(t => (t._6, (t._3, t._4))).distinct
    //println("orignial coord::"+coords.size)
    //key
    val keyCoords = coords.filter(t => keys.contains(t._1)).map(t => (t._1, "key", t._2._1, t._2._2))
    println("keys coords size::" + keyCoords.size)

    val avgkeyFiltCoords = keyCoords.groupBy(t => t._1).map { t =>
      val tag = t._2.head._2
      val avgLat = average(t._2.map(it => it._3))
      val avgLon = average(t._2.map(it => it._4))
      (t._1, tag, avgLat, avgLon, 1)
    }
    avgkeyFiltCoords.toList.foreach(t => writer.println(t._1 + "," + t._2 + "," + t._3 + "," + t._4 + "," + t._5))



    val spreadCoords = coords.filter(t => spread.contains(t._1)).map(t => (t._1, "spread", t._2._1, t._2._2))
    println("sprad coords size::" + spreadCoords.size)
    //val totalCoords=keyCoords++spreadCoords
    val avgSpreadFiltCoords = spreadCoords.groupBy(t => t._1).map { t =>
      val tag = t._2.head._2
      val avgLat = average(t._2.map(it => it._3))
      val avgLon = average(t._2.map(it => it._4))
      (t._1, tag, avgLat, avgLon, 1)
    }
    avgSpreadFiltCoords.toList.foreach(t => writer.println(t._1 + "," + t._2 + "," + t._3 + "," + t._4 + "," + t._5))
    writer.close()
    /*

    println()
    val filteredCoords=coords.filter(t=> totalLocs.contains(t._1))
    //println("coord size::"+filteredCoords.size)
    val avgFiltCoords=filteredCoords.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._2))).map{t=>
      val avgLat=average(t._2.map(it=> it._1))
      val avgLon=average(t._2.map(it=> it._2))
      (t._1,(avgLat,avgLon))
    }
    avgFiltCoords.toList.map(t=> t._2).foreach(t=> println(t._1+","+t._2))*/

  }

  def findTopKLocations(checkinFile: String, k: Int, writeFile: String): Unit = {
    // in terms of most check-ins
    val fr = new fileReaderLBSN
    val checkin = fr.readCheckinFile(checkinFile)
    val topLocs = checkin.groupBy(t => t._6).toList
      .sortBy(t => -t._2.size)
      .map(t => t._1).take(k)
    val writer = new PrintWriter(new File(writeFile))
    topLocs.foreach { t =>
      writer.println(t)
    }
    writer.close()
  }
}
