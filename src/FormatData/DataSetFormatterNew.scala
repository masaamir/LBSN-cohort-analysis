package FormatData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import Basic.Location

import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 5/3/2016.
 */
class DataSetFormatterNew {

  def formatFSDataSet(friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String): Unit = {
    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))
    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().drop(2).map(t => t.split("\\|"))
      .map(t => (t(1).trim, t(5).trim.replace(' ', 'T') + "Z", t(3).trim, t(4).trim, "LocStr", t(2).trim)).toList //.take(10) //
    chkLines.foreach { t =>
      chkWriter.println(t._1 + "\t" + t._2 + "\t" + t._3 + "\t" + t._4 + "\t" + t._5 + "\t" + t._6)
    }
    chkWriter.close()
    val users = chkLines.map(t => t._1).distinct
    //To be converted Format= users \t time \t lat \t lon \t stringLoc \t LongLoc

    println("first printed...")
    val friendsLines = scala.io.Source.fromFile(friendsFile).getLines().drop(2)
      .map(t => t.split("\\|")).filter(t => t.size == 2).filter(t => users.contains(t(0)) && users.contains(t(1))).map(t => (t(0).trim, t(1).trim)).toList //.take(10)
    println("size is ::" + friendsLines.size)
    var count = 0
    friendsLines.foreach { t =>
      count = count + 1
      println(count)
      fWriter.println(t._1 + "\t" + t._2)
    }
    println("second printed..")
    fWriter.close()
  }
  def stringToDateFS(dateString:String): Date = {
    val formatter: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val date: Date = formatter.parse(dateString)
    return date // returned date will be 1 hour plus due to time zone
  }
  def StringToDateToString(dateString:String): String ={
    val formatter: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val date: Date = formatter.parse(dateString)
    val newFormatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val result= newFormatter.format(date).replace(" ","T")+"Z"
    return result
  }

  def formatGWDataSet(venue1File: String, venue2File: String, friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String): Unit = {
    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))

    val venue1 = scala.io.Source.fromFile(venue1File).getLines().drop(1)
      .map(t => t.split(",")).map(t => (t(0).toLong, (t(2).toDouble, t(3).toDouble))).toMap
    val venue2 = scala.io.Source.fromFile(venue2File, "latin1").getLines().drop(1)
      .map(t => t.split(",")).map(t => (t(0).toLong, (t(1).toDouble, t(2).toDouble))).toMap
    val venues = venue1 ++ venue2
    println("venue size::" + venues.size)


    var notFoundCount = 0

    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().drop(1) //.take(100000)//.take(10).foreach(t=> println(t))
    println("checkins Size::" + chkLines.size)
    /*.map(t=> t.split(","))
    .map{t=>
    val coords=venues.getOrElse(t(1).trim.toLong,null)
    if(coords!=null) {
      (t(0), t(2), coords._1, coords._2, "str", t(1))
    }
    else {//println("notFound")
      notFoundCount += 1
      (t(0), t(2), "0.0", "0.0", "str", t(1))
    }
  }.toList//.take(10) //
  println("checkins size::"+chkLines.size)
  println("notFound count::"+notFoundCount)
  chkLines.foreach{t=>
    chkWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
    //println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
  }
  chkWriter.close()
  //val users=chkLines.map(t=> t._1).distinct
  //To be converted Format= users \t time \t lat \t lon \t stringLoc \t LongLoc

  println("first printed...")

  val friendsLines=scala.io.Source.fromFile(friendsFile).getLines().drop(1)
    .map(t=> t.split(",")).filter(t=> t.size==2).map(t=> (t(0).trim,t(1).trim)).toList//.take(10) //.filter(t=> users.contains(t(0)) && users.contains(t(1)))
  println("size is ::"+friendsLines.size)
  var count=0
  friendsLines.foreach{t=>
    count =count +1
    //println(count)
    fWriter.println(t._1+"\t"+t._2)
  }
  println("second printed..")
  fWriter.close()*/

  }
  def formatFSSemanticsCA(friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String, venueWrite:String): Unit ={
    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))
    val locationWriter=new PrintWriter(new File(venueWrite))

    var uniqueUsers: ListBuffer[Long] = new ListBuffer[Long]()
    val friendsLines = scala.io.Source.fromFile(friendsFile).getLines().drop(1).map(t => t.split(","))
      .map(t=> (t(0).toLong,t(1).toLong)).toList
    /*friendsLines.foreach { t =>
      uniqueUsers += t._1
      uniqueUsers += t._2
    }
    println("users: total, unique::" + uniqueUsers.size, uniqueUsers.distinct.size)
    uniqueUsers = uniqueUsers.distinct*/
    var lAtt:ListBuffer[String]=new ListBuffer()
    var lCat:ListBuffer[String]=new ListBuffer()
    var locations:ListBuffer[Location]=new ListBuffer()
    var id,lat,lon,area,state,country=""
    val checkinsLines=scala.io.Source.fromFile(checkinFile).getLines().drop(1).toList
      .map(t => t.split("\t"))
      .map{t=>
      lAtt=t(3).replaceAll("\\{","").replaceAll("\\}","").split(",").to[ListBuffer]
      lCat=t(4).replaceAll("\\{","").replaceAll("\\}","").split(",").to[ListBuffer].filter(t=> t.size >0)
      if(lCat.size<1)lCat=ListBuffer("n/a")
      if(lAtt(0)=="")lAtt(0)="0.0"
      if(lAtt(1)=="")lAtt(1)="0.0"
      if(lAtt.size==4){
        //println(t.mkString(","))
        locations += new Location(t(2).toLong,lAtt(0).toDouble,lAtt(1).toDouble,lAtt(2),lAtt(3),"n/a",lCat)
      }else if (lAtt.size==3){
        //println(t.mkString(","))
        locations += new Location(t(2).toLong,lAtt(0).toDouble,lAtt(1).toDouble,lAtt(2),"n/a","n/a",lCat)
      }
      else{
        //println(t.mkString(","))
        locations += new Location(t(2).toLong,lAtt(0).toDouble,lAtt(1).toDouble,lAtt(2),lAtt(3),lAtt(4),lCat)
      }

        //println(t.mkString(",")+" and lAtt::"+lAtt)

      (t(0).toLong, StringToDateToString(t(1)),lAtt(0).toDouble,lAtt(1).toDouble,"LocStr",t(2).toLong)
    } //userID	Time	lat lon lstr lid
    checkinsLines.foreach{t=>
      //println("size is::"+t)
      chkWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
    }
    friendsLines.foreach{t=>
      fWriter.println(t._1+"\t"+t._2)
    }
    locations=locations.distinct
    locations.foreach{t=>
      locationWriter.println(t.lId+"\t"+t.lLat+"\t"+t.lLon+"\t"+t.lArea+"\t"+t.lState+"\t"+t.lCountry+"\t"+t.lCategories.mkString(","))
    }
    chkWriter.close()
    fWriter.close()
    locationWriter.close()


  }

  def formatWEEDataSet(friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String): Unit = {
    // assign a mapping to users and locations
    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))
    //val chkLines=scala.io.Source.fromFile(checkinFile).getLines().toList.take(10).foreach(println)
    /*.map(t=> t.split("\\|")).map(t=> (t(1).trim,t(5).trim.replace(' ','T')+"Z",t(3).trim,t(4).trim,"LocStr",t(2).trim)).toList//.take(10) //
  chkLines.foreach{t=>
    chkWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
  }*/
    //To be converted Format= users \t time \t lat \t lon \t stringLoc \t LongLoc
    /** Create unique ids for each user and map name to respective id */

    var uniqueUsers: ListBuffer[String] = new ListBuffer[String]()
    var usersIdMap: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
    val friendsLines = scala.io.Source.fromFile(friendsFile).getLines().drop(1).map(t => t.split(",")).toList //.take(10).foreach(println)
    friendsLines.foreach { t =>
      uniqueUsers += t(0)
      uniqueUsers += t(1)
    }
    println("users: total, unique::" + uniqueUsers.size, uniqueUsers.distinct.size)
    uniqueUsers = uniqueUsers.distinct
    var countUser: Long = 0
    uniqueUsers.foreach { uu =>
      countUser += 1
      usersIdMap += (uu -> countUser)
    }
    //usersIdMap.foreach(t=> println(t))
    /** Create unique ids for each location and map location name to respective id */
    var uniqueLocs: ListBuffer[String] = new ListBuffer[String]()
    var uniqueCoords: ListBuffer[(String, String)] = new ListBuffer[(String, String)]()
    var locIdMap: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
    var coordIdMap: scala.collection.mutable.Map[(String, String), Long] = scala.collection.mutable.Map[(String, String), Long]()
    // Actual is userid,placeid,datetime,lat,lon,city,category
    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().drop(1).map(t => t.split(",")).toList //.take(100)//.map(t=> (t(1)))//.take(10).foreach(println)
    chkLines.foreach { t =>
      if (t.size > 2) {
        uniqueLocs += t(1)
        uniqueCoords += ((t(3), t(4)))
      }
      else {
        println("anomaly in data set::" + t.mkString(","))
      }
    }
    println("locs: total, unique::" + uniqueLocs.size, uniqueLocs.distinct.size)
    println("coords: total, unique::" + uniqueCoords.size, uniqueCoords.distinct.size)
    uniqueLocs = uniqueLocs.distinct
    uniqueCoords = uniqueCoords.distinct
    var countLocs: Long = 0
    uniqueLocs.foreach { ul =>
      countLocs += 1
      locIdMap += (ul -> countLocs)
    }
    var countCoords: Long = 0
    uniqueCoords.foreach { uc =>
      countCoords += 1
      coordIdMap += (((uc._1, uc._2) -> countCoords))
    }

    println("writing files")

    /** Write files */
    //friends
    println("friends size::" + friendsLines.size)
    friendsLines.foreach { fp =>
      val firstUser = usersIdMap.getOrElse(fp(0), null)
      val secondUser = usersIdMap.getOrElse(fp(1), null)
      if (firstUser == null || secondUser == null) {
        println("did something wrong for users!!")
      } else {
        fWriter.println(firstUser + "\t" + secondUser)
      }
    }
    fWriter.close()

    //location
    chkLines.foreach { ck =>
      val user = usersIdMap.getOrElse(ck(0), null)
      val loc = locIdMap.getOrElse(ck(1), null)
      val cord = coordIdMap.getOrElse((ck(3), ck(4)), null)
      if (user == null || loc == null || cord == null) {
        println("string,user,loc" + ck.mkString("::"), user, loc, cord)
        println("did something wrong for checkins")
      } else {
        chkWriter.println(user + "\t" + ck(2) + "Z" + "\t" + ck(3) + "\t" + ck(4) + "\t" + cord + "\t" + loc)
      }
    }
    chkWriter.close()

  }

}
