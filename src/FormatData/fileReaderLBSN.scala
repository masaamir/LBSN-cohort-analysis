package FormatData

import java.io.{File, PrintWriter}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import Basic._

import scala.collection.mutable.ListBuffer

/**
 * Created by XXX on 4/25/2016.
 */
class fileReaderLBSN {
  def dateToString(inDate:Date): String ={// standard which is read
  val df:DateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val stringDate=df.format(inDate)
    return stringDate
  }

  def writeCheckinsInFile(checkins: List[(Long, Date, Double, Double, String, Long)], writeFile:String): Unit ={
    val writer = new PrintWriter(new File(writeFile))
    checkins.foreach { t =>
      writer.println(t._1 + "\t" + dateToString(t._2)+ "\t" + t._3 + "\t" + t._4 + "\t" + t._5 + "\t" + t._6)
    }
    writer.close()

  }

  def readCheckinFileNew(checkinFile: String): List[(Long, Date, Double, Double, String, Long)] = {
    // receive file(tab separated): userId time lat long LocId(String) LocId(ConvertibleToLong)
    val df = new DataFormatter // data formatter object
    val checkins = scala.io.Source.fromFile(checkinFile).getLines()
        .map(t => t.split("\t"))
        .map(t => (t(0).toLong, df.stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong))
        .filter(t=> t._6!=0) // for avoid GW data set anomalies
        .toList.distinct
    return checkins //{(user,Date,lat,lon,locStr,LocLong,dateStringActual)}
  }

  def readCheckinFile(checkinFile: String): List[(Long, Date, Double, Double, String, Long, String)] = {
    // receive file(tab separated): userId time lat long LocId(String) LocId(ConvertibleToLong)
    val df = new DataFormatter // data formatter object
    val checkins = scala.io.Source.fromFile(checkinFile).getLines()//.take(2000)
        .map(t => t.split("\t")).filter(t=> (t(1).contains("T")&&t(1).contains("Z")))
        .map(t => (t(0).toLong, df.stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1)))
          .filter(t=> t._6!=0) // for avoid GW data set anomalies
        .toList.distinct
    return checkins //{(user,Date,lat,lon,locStr,LocLong,dateStringActual)}
  }

  def readCheckinsWithCats(checkinWithCatsFile:String): List[(Long, Date, Double, Double, String, Long, String)] ={
    // receive file(tab separated): userId time lat long LocId(String) LocId(ConvertibleToLong)
    val df = new DataFormatter // data formatter object
    val checkins = scala.io.Source.fromFile(checkinWithCatsFile).getLines()//.take(2000)
        .map(t => t.split("\t"))
        .map(t => (t(0).toLong, df.stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(6)))
        .filter(t=> t._6!=0) // for avoid GW data set anomalies
        .toList.distinct
    return checkins //{(user,Date,lat,lon,locStr,LocLong, Categories)}
  }

  def readVenuesFileWee(venuesFile:String): ListBuffer[Location] ={
    val cat:ListBuffer[String]=new ListBuffer()
    val city=""
    var count=0
    val venues=scala.io.Source.fromFile(venuesFile).getLines().to[ListBuffer]
    //println("original venue size"+venues.size)
    //venues.map(t=> t.split("\t")).take(10).foreach(t=> println(t.mkString(",")))
    val newVenues:ListBuffer[Location]=venues.map(t=> t.split("\t")).filter(t=> t.size==7)
      .map{t=>
      count += 1
      val partialCat:ListBuffer[String]=t(6).split(":").to[ListBuffer] // split categories on ':'
        val cat:ListBuffer[String]=new ListBuffer()
        partialCat.foreach{pc=>
          cat ++= pc.split(",").to[ListBuffer] // split categories on ','
        }
      (new Location(t(0).toLong, t(1).toDouble, t(2).toDouble, t(3), t(4), t(5), cat.distinct))
    }
    //println(" after venues size::"+newVenues.size)

return newVenues.distinct
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
  def readConvoysFile(convoysFile: String): ListBuffer[(ListBuffer[Long], ListBuffer[Long],ListBuffer[Long])] = {
    val convoys=scala.io.Source.fromFile(convoysFile).getLines()
      .map(t=> t.split("\t")).map(t=> (t(0).split(","),t(1).split(","),t(2).split(",")))
      .map(t=> (t._1.map(it=> it.toLong).sortBy(t=> t).to[ListBuffer],t._2.map(it=> it.toLong).sortBy(t=> t).to[ListBuffer],ListBuffer(t._3.head.toLong,t._3.last.toLong))).to[ListBuffer]
    return convoys
  }

  /**read convoys file with categories: file: Users,Locations, Combined distinct categories, (startTime,endTime)*/
  def readConvoyCatsFile(convoyCatFile:String): ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String],(Long,Long))] ={
    val convoysCat=scala.io.Source.fromFile(convoyCatFile).getLines()
      .map(t=> t.split("\t")).map(t=> (t(0).split(","),t(1).split(","),t(2).split(","),t(3).split(","))) //Users,Locs,Cats, (ts,te)
      .map(t=> (t._1.map(it=> it.toLong).sortBy(t=> t).to[ListBuffer],t._2.map(it=> it.toLong).sortBy(t=> t).to[ListBuffer],t._3.map(it=> it).to[ListBuffer],(t._4.head.toLong,t._4.last.toLong))).to[ListBuffer]
    //convoysCat.take(10).foreach(println)
    return convoysCat
  }

  def readConvoyCatsFileTemp(convoyCatFile:String): ListBuffer[(ListBuffer[Long],ListBuffer[Long],(Long,Long))] ={
    val convoysCat=scala.io.Source.fromFile(convoyCatFile).getLines()
      .map(t=> t.split("\t")).map(t=> (t(0).split(","),t(1).split(","),t(2).split(","))) //Users,Locs,Cats, (ts,te)
      .map(t=> (t._1.map(it=> it.toLong).sortBy(t=> t).to[ListBuffer],t._2.map(it=> it.toLong).sortBy(t=> t).to[ListBuffer],(t._3.head.toLong,t._3.last.toLong))).to[ListBuffer]
    //convoysCat.take(10).foreach(println)
    return convoysCat
  }

  def readConvoyTableFile(convoyTableFile:String): List[(List[Long],List[Long],List[String])] ={
    val convoys = scala.io.Source.fromFile(convoyTableFile).getLines().drop(1).toList
      .map(t => t.split("\t")).map(t => (t(7), t(8), t(9)))
      .map(t => (t._1.split(",")
      .map(it => it.toLong).toList, t._2.split(",").map(it => it.toLong).toList, t._3.split(",").toList))
      .distinct
    return convoys
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
