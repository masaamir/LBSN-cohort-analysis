package ConvoysLBSN

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * Created by XXX on 30-XXX-XXX.
 */
class ConvoyAnalysis {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }
  def removeConsecutiveLocs(inconvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])]):
  ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] ={
    println("new one ::")
    val distConsLocs=inconvoys.map{t=>
      val actLocs:ListBuffer[Long]=t._2
      val actTS=t._3
      val newLocs:ListBuffer[Long]=new ListBuffer()
      val newTS:ListBuffer[(Double,Double)]=new ListBuffer()
      var tempTS:(Double,Double)=null
      var tempLoc:Long=0L
      for(i<-1 until actLocs.size){
        val preLoc=actLocs(i-1)
        val currLoc=actLocs(i)
        val preTS=actTS(i-1)
        val currTS=actTS(i)
        if(tempLoc==0L){
          tempLoc=preLoc
          tempTS=preTS
        }
        if(tempLoc==currLoc){
          tempTS=(tempTS._1,currTS._2)
        }else if (tempLoc !=currLoc ){
          newLocs+=tempLoc
          newTS+=tempTS
          tempLoc=0L
          tempTS=null
        }
        if(i==actLocs.size-1){
          newLocs+=tempLoc
          newTS+=tempTS
          tempLoc=0L
          tempTS=null
        }
      }
      (t._1,newLocs,newTS)
    }
    return distConsLocs
  }

  /** Get all the convoys in the LBSN */
  def getConvoys(checkinsFile: String, friendsFile: String, deltaTS: Long, writeFilePath: String, writeFileValues: String): Unit = {
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")

    val timeUnit = (1000 * 60 * 60) // in hours now //*24
    val TSMap = getDataSnapShots(checkinsFile, friendsFile, deltaTS * timeUnit, timeUnit) //Divide the data set into SnapShots on the basis of given time unit


        val writeFile=new PrintWriter(new File(writeFilePath))
    val minConvoySize = 3 // min. no. of users a convoy should have
    val minConvoyLocSize = 2 //min. no. of locs a convoy should have visited
    val minConvoyDistinctLocSize = 2
    writeFile.println("TSMAP size"+TSMap.size) //TSMAP=Array[(t1,t2),Hash[user,location]]
    var convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // all convoys
    var currentConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // convoys that are currently progressing
    //Convoy=List[(Users,VisitedLocations,TimeStamps)]
    var loc: Long = 0L
    val start = 1
    val end = TSMap.size
    for (i <- start until end) {
      // group time stamped check-ins on the basis of visited location
      val TimeStampGroups = TSMap(i - 1)._2.groupBy(t => t._2).map(t => (t._2.map(it => it._1).to[ListBuffer],
        ListBuffer(t._1), ListBuffer(TSMap(i - 1)._1))).to[ListBuffer].filter(t => t._1.size >= minConvoySize)
      TimeStampGroups.foreach { g => // only keep the maximal convoys in current convoy
        var containCheck = false
        currentConvoys.foreach { cc =>
          if (g._1.forall(cc._1.contains)) {
            containCheck = true // will be covered in next iteration
          }
        }
        if (containCheck == false) {
          currentConvoys += g
        }
      }
      //Filter convoys that have at least minimum number of users
      currentConvoys = currentConvoys.filter(t => t._1.size >= minConvoySize) //if users are greater than limit
      val tempCurrentConvoy = currentConvoys.clone()
      tempCurrentConvoy.foreach { c =>
        val usersLoc: ListBuffer[(Long, Long)] = c._1
          .map { u => // find user's locations
          loc = TSMap(i)._2.getOrElse(u, 0L)
          (u, loc)
          }
        val newLocGroup = usersLoc.groupBy(t => t._2)
        val filteredLocGroup = newLocGroup.filter(t => t._2.size >= minConvoySize && t._1 != 0L) // filter groups that have at least min users and the location is not NULL.
        if (c._2.size >= minConvoyLocSize && newLocGroup.size >= 1) {//update to 1 from minConvoyDistinctLocSize
          // check if size of convoy have been to multiple locations and broke at this point
          convoys += c
        } else if (c._2.size >= minConvoyLocSize && newLocGroup.size == 1 && newLocGroup.contains(0L)) {
          // add convoy if its users have no next location
          convoys += c
        }
        filteredLocGroup.foreach { t => // update current convoys that may have some part in next timestamp
          currentConvoys += ((t._2.map(it => it._1), c._2.++(Traversable(t._1)), c._3.++(Traversable(TSMap(i)._1)))) //(t._2.map(it=> it._1),c._2+=t._1,c._3+=TSMap(i)._1)
        }
        currentConvoys = currentConvoys - c // delete the considered convoy, after considering its next parts
      }
      if (i == end - 1) {
        //In last timeStamp current convoys are added to convoys
        convoys ++= currentConvoys.filter(t => t._2.size >= minConvoyLocSize)
      }
    }
    // remove two consecutive same locations from the visit history, which happens due to limited time stamp

    //convoys=removeConsecutiveLocs(convoys)




    //convoys=convoys.map(t=> (t._1,co))
    println("No. of convoys size of users >="+minConvoySize+"| and minLocs >="+minConvoyLocSize+" minDistincLocs >="+minConvoyDistinctLocSize+
      ":: " + convoys.size, convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize).size)
    convoys = convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize) // filter convoys that with minimum distinct locations
    var filteredConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer()
    for (i <- 0 until convoys.size) {
      // maintain only maximal convoys
      println(i)
      val current = convoys(i)
      var contained = false
      for (j <- 0 until convoys.size) {
        if (i != j) {
          val itConvoy = convoys(j)
          if (current._1.forall(itConvoy._1.contains) && current._2.forall(itConvoy._2.contains) && current._3.forall(itConvoy._3.contains)) {
            contained = true
          }
        }
      }
      if (contained == false) {
        filteredConvoys += current
      }
    }




    /** At this point we have convoys (group of users that share consecutively share the locations)
      * based on minimum users, total locations and distinct locations */
    /** These convoys can further be processed on to find cliques or connected Components */
    filteredConvoys.foreach(t=> println(t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2)))))


    println("filtered convoys size::"+filteredConvoys.size)
    writeFile.println("#Users,#UniqueLocations,#Locations,#Times")
    // convoys with of only users, without considering friends
    filteredConvoys.foreach{c=>
      writeFile.println(c._1,c._2.distinct,c._2,c._3)
    }
    writeFile.close()



    /** */

  }

  /** Remove conseuctive duplicates from the list */
  def compress[T](values: List[T]): List[T] =
    compressTail(Nil, values)

  def compressTail[T](seen: List[T], remaining: List[T]): List[T] =
    remaining match {
      case Nil => seen
      case x :: y :: xs if (x == y) => compressTail(seen, y :: xs)
      case x :: xs => compressTail(seen ::: List(x), xs)
    }

  /** Divide the LBSN into timeStamps on the basis of given time unit */
  def getDataSnapShots(checkinsFile: String, friendsFile: String, deltaTS: Long, convertMSecTo: Long)
  : Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])] = {
    //val df:DateFormat=new SimpleDateFormat("yyyy-mm-dd:hh:mm:ss")
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    // read check-ins from file

    val ckLines = scala.io.Source.fromFile(checkinsFile).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1), t(1))).toList.distinct //.toArray
    //time unit to convert the visited time from millisecond to given unit
    val timeUnit = convertMSecTo //(1000 * 60 *60 ) // in hours now //*24
    val sortedByTime = ckLines.sortBy(t => t._2) //sort check-ins on the basis of time
    val minT = sortedByTime.head._2.getTime.toDouble // earliest check-in time
    val maxT = sortedByTime.last._2.getTime.toDouble //last check-in time
    val tsCount = Math.ceil((maxT - minT + 1) / (deltaTS)).toInt //total number of timeStamps

    val timeStampsMap: Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])]
    = new Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])](tsCount)
    var startTime = minT
    var endTime = 0.0
    for (i <- 0 until timeStampsMap.size) {
      //find time ranges for each timeStamp
      endTime = startTime + deltaTS
      timeStampsMap(i) = ((startTime, endTime), scala.collection.mutable.HashMap())
      startTime = endTime
    }

    // get all visited locations for each user
    val usersLocs = ckLines.groupBy(t => t._1)
      .map(t => (t._1, t._2.map(it => (it._6, it._2.getTime)).sortBy(iit => iit._2))) // u-> {(l,t),..}
    val totatlUsers = usersLocs.size //total number of users
    println("total number of users::" + totatlUsers)
    var usersCount = 0
    usersLocs.foreach { t => //u,{(l,t)..}
      usersCount += 1
      println("current, total Users::" + usersCount, totatlUsers)
      //val total=t._2.size-2
      /** the last location of a user with-in a timestamp is considered its location throughout that time stamp
      this location will be repeated for all upcoming time stamps until user make a new check-in */
      var i = -1
      while (i < t._2.size - 2) {
        i = i + 1 //first time zero
        var current: (Long, Long) = null
        var next: (Long, Long) = null
        var ind1, ind2 = 0
        var countLoop = 0
        do {
          countLoop += 1
          if (countLoop != 1) {
            i = i + 1 // for first time, in do-while loop not incremented
          }
          current = t._2(i)
          next = t._2(i + 1)
          ind1 = Math.floor(((current._2 - (minT - 1)) / deltaTS)).toInt // timestamp of current check-in


          ind2 = Math.floor(((next._2 - (minT - 1)) / deltaTS)).toInt // time stamp of next check-in
        } while ((ind1 == ind2) && (i < t._2.size - 2)) //consider only last location of a user in a time stamp
        if (i < t._2.size - 2) {
          //add the same location to all timestamps until user make new check-in
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          for (a <- ind1 until ind2) {
            timeStampsMap(a)._2 += (t._1 -> current._1)
            //location on lastest/final time stamp is not set yet
          }
        }
        else if (i == t._2.size - 2 && ind1 == ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
        }
        else if (i == t._2.size - 2 && ind1 != ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
        }
      }
    }
    return timeStampsMap // =startTime,EndTime, Hash(user, locs)
  }
}
