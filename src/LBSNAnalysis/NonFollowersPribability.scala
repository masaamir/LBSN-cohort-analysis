package LBSNAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer

/** Find out the prbability that at least #Friends check on a place and the user u don't check-in within time window #time
  * */
class NonFollowersPribability {
  //var friendSize=5 // to check probability
  var filterSize = 1
  var Users: List[Long] = null
  var usersFriends: Map[(Long), List[(Long)]] = Map()
  //var usersVisitedLocations:Map[(Long),List[(Long,Date,Double,Double,String,Long,String)]]=Map()
  var usersVisitedLocations: Map[(Long), List[Long]] = Map()
  var windowSizeList = List(965)
  //days
  var filterSizeList = List(1)
  // friend size
  var LocationCheckins: Map[(Long), List[(Long, Date)]] = Map()

  def getFriends(FriendsLines: List[(Long, Long)]): Unit = {
    //val lines=scala.io.Source.fromFile(pathFriends).getLines().map(t=> t.split("\t"))
    //.map(t=> (t(0).toLong,t(1).toLong)).toList
    usersFriends = FriendsLines.groupBy(t => t._1).map { t =>
      var f = ListBuffer[Long]()
      t._2.foreach { ftuple =>
        f += ftuple._2
      }
      //4println("user, Friends",t._1,f.toList)
      (t._1, f.toList)
    }
    //Users=usersFriends.map(t=> t._1).toList
    //friends.foreach(println)

  }

  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def getUsersLocations(users: List[Long]): List[Long] = {

    var usersLocations: List[Long] = List()
    users.foreach { u =>
      usersLocations = usersLocations ::: usersVisitedLocations.getOrElse(u, List())
    }
    //val locationFrequency=usersLocations.groupBy(l=> l).map(l=> (l._1,l._2.size))
    //val filteredLocations=locationFrequency.filter(t=>t._2>filterSize).map(t=> t._1).toList
    return usersLocations.distinct
  }

  def getMaxIntervalList(llist: List[(Long, Date)], element: Long): Array[Long] = {
    var maxIntervalArray: Array[List[Long]] = new Array(windowSizeList.size)
    maxIntervalArray = maxIntervalArray.map(t => List())
    var maxIACount: Array[Long] = new Array(windowSizeList.size)
    //val maxInterval:List[(Long,Date)]=List()
    //val maxIntervalUniqueSize=0
    var temp: Array[List[(Long, Date)]] = new Array(windowSizeList.size)
    temp = temp.map(t => List())
    //list.sortBy(t=> t._2)
    //println("list is::"+list)
    val rlist = llist.reverse
    var elementTime = Double.PositiveInfinity
    rlist.foreach { e =>
      if (e._1 != element) {
        //var a=0
        //println("value a, total ::"+a,windowSizeList.size-1)
        for (a <- 0 until (windowSizeList.size)) {
          //println("temp::"+temp(a))
          //if(elementTime- e._2)
          //if(elementTime-e._2.getTime.toDouble / (1000 * 60 * 60 * 24) >= windowSizeList(a) )
          temp(a) = temp(a) :+ e
          //println(temp(a))
        }
        //temp = temp :+ e
      }
      else if (e._1 == element) {
        //println("max,temp size is::"+maxInterval,temp)
        //var tempSize=temp.map(t=> t._1).distinct.size
        //temp=temp.reverse
        //println("max,temp size is::"+maxInterval,temp)
        //println("element ",e)
        //println("previous size::"+temp)
        /*temp.foreach{t=>
          println("test",e._2.getTime,t._2.getTime,(e._2.getTime - t._2.getTime),windowSize)
        }*/
        //val tempSize:Array[Int]=new Array(windowSizeList.size)
        for (a <- 0 until windowSizeList.size) {

          temp(a) = temp(a).filter { t => (elementTime - t._2.getTime) / (1000 * 60 * 60 * 24) >= windowSizeList(a)}
          /*println("previous list, element and window "+temp(a),e,windowSizeList(a))
          temp(a).foreach{t=>
            println("element, arrElement, difference"+e._2.getTime,t._2.getTime,((e._2.getTime - t._2.getTime)))
          }*/
          //temp(a) = temp(a).filter { t => (e._2.getTime - t._2.getTime) <= windowSizeList(a)}
          //println("after filter list "+temp(a))
          //tempSize(a)=temp(a).map(t=> t._1).distinct.size
          //val tempUsers=temp(a).map(t=> t._1).distinct
          // despite of random users check for friends
          val tempUsers = temp(a).map(t => t._1).distinct.intersect(usersFriends.getOrElse(e._1, List()))

          if (tempUsers.size > maxIntervalArray(a).size) {
            maxIntervalArray(a) = tempUsers
            //maxIntervalUniqueSize=tempSize(a)
            temp(a) = List()
          }
          else temp(a) = List()
        }
        //temp=temp.filter{t=> (e._2.getTime - t._2.getTime).toDouble/(1000*60*60*24) <= windowSize }

        //println("after size::"+temp)
        //println("filtered temp "+temp+" where element is::"+e)
        //val tempSize=temp.map(t=> t._1).distinct.size

        elementTime = e._2.getTime

      }
    }
    //println("final max size::"+maxInterval)
    //println("max interval")
    //maxIntervalArray.foreach( println)
    var updateCheck = false
    for (i <- 0 until maxIntervalArray.size) {
      val tempUsers = temp(i).map(t => t._1).distinct
      if (tempUsers.size > maxIntervalArray(i).size) {
        maxIntervalArray(i) = tempUsers
        updateCheck = true
      }
    }
    for (i <- 0 until maxIntervalArray.size) {
      maxIACount(i) = maxIntervalArray(i).size
    }
    return maxIACount

    //return maxIntervalArray
  }

  //def formatDataSetNonFollower(readPath:String, writePath:String,friendsPath:String): Unit ={
  def formatDataSetNonFollower(friendsPath: String, checkinFilePath: String, writePath: String, //minUserCk= minimum check-ins of users
                               minUserCk: Long, maxUserCk: Long, minUserLocs: Long, minLocCk: Long, maxLocCk: Long, minLocVisitors: Long): Unit = {
    val fd = new FilterData()
    val filterData = fd.filterLBSNData(friendsPath, checkinFilePath,
      minUserCk, maxUserCk, minUserLocs, minLocCk, maxLocCk, minLocVisitors)
    //var userLocFolMap:Map[(Long,Long),List[Long]]=Map()
    //val userLocFollowers=List[(Long,Long,Array[Long])]()
    val writerOrder = new PrintWriter(new File(writePath))
    /*val lines=scala.io.Source.fromFile(readPath).getLines().map(t=> t.split("\t"))
      .map(t=> (t(0).toLong,stringToDate(t(1)),t(2).toDouble,t(3).toDouble,t(4),t(5).toLong,t(1))).toList.distinct//.toArray*/
    //user, time(date), latitude, longitude, Loc(string),loc(long),time(string)
    Users = filterData._2.map(t => t._1).toList.distinct
    usersVisitedLocations = filterData._2.groupBy(t => t._1).map { t =>
      val locs: List[Long] = t._2.map(l => l._6).distinct
      //println("visited locations ::"+t._1,locs)
      (t._1, locs)

    }
    LocationCheckins = filterData._2.groupBy(t => t._6).map { l =>
      val orderedUsers = l._2.sortBy(u => u._2).map(inUser => (inUser._1, inUser._2))
      (l._1, orderedUsers)
    }
    val LocationVisitors = filterData._2.groupBy(lv => lv._6).map(t => (t._1, t._2.map(it => it._1).distinct))
    val totalSize = usersVisitedLocations.size
    getFriends(filterData._1)
    val allowedUsers = filterData._2.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6)))
      .filter(t => t._2.size > 30 && t._2.distinct.size > 20).map(t => t._1).toList.distinct
    Users = allowedUsers

    /** Main Computation */
    writerOrder.println("user" + "\t" + "windowSize" + "\t" + "FriendSize" + "\t" + "Probability")
    var count = 0
    val totalCount = Users.size
    var userFriends: List[Long] = List()
    var userLocations: List[Long] = List()
    //var friendsLocations:List[Long]=List()
    val friendsLocationsArray: Array[List[Long]] = new Array[List[Long]](filterSizeList.size)
    var LocVisitors: List[(Long, Date)] = List()
    var filtLocs: List[(Long, Array[Long])] = List()
    var LocVisitorsFriends: Array[Long] = new Array[Long](filterSizeList.length)
    Users.foreach { u =>
      count = count + 1
      println("count,totalCount" + count, totalCount)
      userFriends = usersFriends.getOrElse(u, List())
      //println("user friends are "+userFriends)
      userLocations = usersVisitedLocations.getOrElse(u, List())
      //println("user locations::"+userLocations.size)
      //friendsLocations=getUsersLocations(userFriends).distinct.
      for (b <- 0 until filterSizeList.size) {
        friendsLocationsArray(b) = getUsersLocations(userFriends).distinct.
          filter(l => LocationVisitors.getOrElse(l, List()).intersect(userFriends).size >= filterSizeList(b))
        //println("friendsLocations::"+friendsLocations.size)
        val locVist = friendsLocationsArray(b).map { loc =>
          LocVisitors = LocationCheckins.getOrElse(loc, List())
          LocVisitorsFriends = getMaxIntervalList(LocVisitors, u)
          (loc, LocVisitorsFriends)
        }



        for (a <- 0 until windowSizeList.size) {
          filtLocs = locVist.filter(t => t._2(a) >= filterSizeList(b))
          val probability = filtLocs.size.toDouble / friendsLocationsArray(b).distinct.size.toDouble
          //println("probability of user " + u + " for window size " + windowSizeList(a) +
          // " with filter size::" + filterSizeList(b) + " is::" + probability)
          writerOrder.println(u + "\t" + windowSizeList(a) + "\t" + filterSizeList(b) + "\t" + probability)
          //NaN is for those users whose #friends have not visited any same locations

        }
      }

      /*
      val filteredLocs=friendsLocations.diff(userLocations) // decide it should be total friends locations or own locations
      //println("locations not visited by user::"+filteredLocs)
      val probability= filteredLocs.size.toDouble/friendsLocations.size.toDouble
      println("probability is ::"+probability)
      writerOrder.println(u+"\t"+probability)
      */
    }
    println("Non probability follower: Finished !!")
    writerOrder.close()
  }

}
