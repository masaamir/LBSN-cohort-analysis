package FormatData

import java.io.{File, PrintWriter}
import java.util.Date
import Basic._
import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 4/25/2016.
 */
class fileReaderLBSN {
  def readCheckinFile(checkinFile: String): List[(Long, Date, Double, Double, String, Long, String)] = {
    // receive file(tab separated): userId time lat long LocId(String) LocId(ConvertibleToLong)
    val df = new DataFormatter // data formatter object
    val checkins = scala.io.Source.fromFile(checkinFile).getLines()
        .map(t => t.split("\t"))
        .map(t => (t(0).toLong, df.stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1)))
        .toList.distinct
    return checkins //{(user,Date,lat,lon,locStr,LocLong,dateStringActual)}
  }

  def readVenuesFileWee(venuesFile:String): ListBuffer[Location] ={
    println("in read venues file")
    val cat:ListBuffer[String]=new ListBuffer()
    val city=""
    var count=0
    val venues=scala.io.Source.fromFile(venuesFile).getLines().to[ListBuffer]
    println("original venue size"+venues.size)
    //venues.map(t=> t.split("\t")).take(10).foreach(t=> println(t.mkString(",")))
    val newVenues:ListBuffer[Location]=venues.map(t=> t.split("\t")).filter(t=> t.size==7)
      .map{t=>
      count += 1
      val partialCat:ListBuffer[String]=t(6).split(":").to[ListBuffer]
        val cat:ListBuffer[String]=new ListBuffer()
        partialCat.foreach{pc=>
          cat ++= pc.split(",").to[ListBuffer]
        }
      (new Location(t(0).toLong, t(1).toDouble, t(2).toDouble, t(3), t(4), t(5), cat))
    }
    println(" after venues size::"+newVenues.size)

return newVenues
  }

  def readVenuesFile(venuesFile: String): ListBuffer[Location] = {
    // for foursquare data-set with semantics for CA
    var cat:ListBuffer[String]=new ListBuffer()
    val venues = scala.io.Source.fromFile(venuesFile).getLines().toList
      .map(t => t.split("\t"))
      .map { t =>
      if (t.size == 7) cat = t(6).split(",").to[ListBuffer] else cat = ListBuffer()
      //println(t.mkString(","))
      (new Location(t(0).toLong, t(1).toDouble, t(2).toDouble, t(3), t(4), t(5), cat))
    }
    //venues.foreach(t => t.printInfo())
    return venues.to[ListBuffer]

  }

  def readFriendsFile(friendsFile: String): List[(Long, Long)] = {
    // receive file(tab separated): user1 user2"
    val friends = scala.io.Source.fromFile(friendsFile).getLines()
      .map(t => t.split("\t")).map(t => (t(0).toLong, t(1).toLong))
      .toList.distinct
    return friends
  }

  def friendWriter(friends: List[(Long, Long)], writeFile: String): Unit = {
    val writer = new PrintWriter(new File(writeFile))
    friends.distinct.foreach { t =>
      writer.println(t._1 + "\t" + t._2)
    }
    writer.close()
  }

  def checkinWriter(checkins: List[(Long, Date, Double, Double, String, Long, String)], writeFile: String): Unit = {
    val writer = new PrintWriter(new File(writeFile))
    checkins.foreach { t =>
      writer.println(t._1 + "\t" + t._7 + "\t" + t._3 + "\t" + t._4 + "\t" + t._5 + "\t" + t._6)
    }
    writer.close()
  }

}
