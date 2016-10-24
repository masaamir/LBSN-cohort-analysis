package FormatData

import java.io.{File, PrintWriter}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import ConvoysLBSN.ConvoysPatternAnalysis
import CoordinateConversion.{Angle, UTMCoord}

import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 4/25/2016.
 */
class DataFormatter {
  // convert string to date:
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }
  def dateToString(inDate:Date): String ={// standard which is read
  val df:DateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    //println("original is ::"+inDate)

    val stringDate=df.format(inDate)
    //println("returned ::"+stringDate)
    //println ("agrain read ::"+stringToDate(stringDate))
    return stringDate
  }
  def associateCheckinId(checkins:List[(Long,Date,Double,Double,String,Long,String)]):
  List[(Long,Date,Double,Double,String,Long,String,Long)] ={
    //input={u,t,lat,lon,locStr,locId,Cats}
    //output={u,t,lat,lon,locStr,locId,Cats,checkinId
    var cId:Long=0
    val newCheckins=checkins.map{t=>
      cId += 1
      (t._1,t._2,t._3,t._4,t._5,t._6,t._7,cId)
    }
    return newCheckins
  }

  def getConvoysWithCats(convoysFile:String, venuesFile:String, writeConvoysWithCat:String): Unit ={
    val cpa=new ConvoysPatternAnalysis
    val fr=new fileReaderLBSN
    val writer=new PrintWriter(new File(writeConvoysWithCat))
    val convoys=cpa.readConvoysFile(convoysFile)
    val venues=fr.readVenuesFileWee(venuesFile)
    val venueMap=venues.map(t=> (t.lId,t.lCategories)).toMap
    var cats:ListBuffer[String]=new ListBuffer()
    val newConvoys=convoys.map{c=>
      cats=new ListBuffer()
      c._1.foreach{l=>
        if(venueMap.contains(l))
        cats ++= venueMap.getOrElse(l,ListBuffer("n\\a"))
      }
      (c._1,c._2,c._3,cats)
    }
    newConvoys.foreach{c=>
      writer.println(c._1.mkString(",")+"\t"+c._2.mkString(",")+"\t"+c._3.mkString(",")+"\t"+c._4.mkString(","))
    }
    writer.close
  }

  def splitCheckinsOnCats(fileCheckinsWithCats:String)
  : ListBuffer[(Long,Date,Double,Double,String,Long,String,Long,String)] ={
    val fr=new fileReaderLBSN
    var checkinsWithCats=fr.readCheckinsWithCats(fileCheckinsWithCats)
    println("total checkins with Cats::"+checkinsWithCats.size)
    checkinsWithCats=checkinsWithCats.filter(t=> t._7!="n\\a")
    //println("total checkins with cat not null::"+checkinsWithCats.size)
    val checksWithId=associateCheckinId(checkinsWithCats)
    println("checkins with ids::"+checksWithId.size)
    val splitCheckins:ListBuffer[(Long,Date,Double,Double,String,Long,String,Long,String)]=new ListBuffer()
    var catCount=0
    checksWithId.foreach{t=>
      val cats=t._7.split(",")
      catCount +=cats.size
      cats.foreach{c=>
        if(c!="n\\a")
        splitCheckins+= ((t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,c))
      }
    }
    println("split checkins size::"+splitCheckins.size)
    println("cat count is::"+catCount)
    //splitCheckins.take(100).foreach(println)
    return splitCheckins
  }

  def getCheckinsWithCategories(fileCheckins:String,fileVenues:String, fileWriteCheckWithCat:String): Unit ={
    val fr=new fileReaderLBSN
    val checkins=fr.readCheckinFileNew(fileCheckins)
    //checkins=checkins.filter(t=> t._1==23).sortBy(t=> t._2).take(10)
      //checkins.foreach(println)
    val venues=fr.readVenuesFileWee(fileVenues)
    val writer=new PrintWriter(new File(fileWriteCheckWithCat))
    val lCatMap=venues.map(t=> (t.lId,t.lCategories)).toMap
    val newCheckins:ListBuffer[(Long,Date,Double,Double,String,Long,String)]=new ListBuffer()
    checkins.foreach{t=>
      if(lCatMap.contains(t._6)) {
        val cats = lCatMap.getOrElse(t._6, null)
        if(cats!=null && cats != "n\\a") {
          writer.println(t._1+"\t"+dateToString(t._2)+"\t"+ t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6+"\t"+cats.mkString(","))
          newCheckins += ((t._1, t._2, t._3, t._4, t._5, t._6, cats.mkString(",")))
        }

      }
      /*else{
        println(" id doesn't exist ::"+t._6)
      }*/
    }
    writer.close()
    println("Total checkins are ::"+checkins.size)
    println("new Checkins are ::"+newCheckins.size)
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
