package FormatData

import java.io.{PrintWriter,File}
import java.util.Date

/**
 * Created by XXX on 4/26/XXX.
 */
class DataFilter {
  def filterGWOnCategorySpots(fileCheckin:String,fileSpots:String): Unit ={
    val fr=new fileReaderLBSN
    //val checkins=fr.readCheckinFile(fileCheckin)
    println("in function")
    val checkinstest=scala.io.Source.fromFile(fileCheckin).getLines()//.take(1000)
      .map(t=> t.split("\t")).map(t=> t(5)).toList
    //checkinstest.foreach(println)
    println("Original Checkins are::"+checkinstest.size)


    val catSpots=scala.io.Source.fromFile(fileSpots,"latin1").getLines().drop(1).map(t=> t.split(","))
      .map(t=> (t(0),"1")).toMap
    //println(catSpots.getOrElse("482954",null))
    val filteredCheckins=checkinstest.filter{t=>
      catSpots.getOrElse(t,"")=="1"
    }

    println("Checkins with categories::"+filteredCheckins.size)

  }

  def filterFriendsOnCheckins(cFriendsFile:String,cChekcinsFile:String,fileWrite:String): Unit ={
    val fr=new fileReaderLBSN
    val friends=fr.readFriendsFile(cFriendsFile)
    println("current connections are::"+friends.size)
    val usersInChecks=fr.readCheckinFile(cChekcinsFile).filter(t=>  t._6!=0).map(t=> (t._1,1)).distinct.toMap
    val newFriends=friends.filter(f=> usersInChecks.contains(f._1) && usersInChecks.contains(f._2))
    println("new connectsion area ::"+newFriends.size)
    val writer=new PrintWriter(new File(fileWrite))
    newFriends.foreach{c=>
      writer.println(c._1+"\t"+c._2)
    }
    writer.close()

  }

  def filterUsers(friendsFile: String, checkinFile: String, minUserCk: Long, minUserLocs: Long)
  : (List[(Long, Long)], List[(Long, Date, Double, Double, String, Long, String)]) = {
    val fileReaderLBSN = new fileReaderLBSN
    val numberFormatter = java.text.NumberFormat.getIntegerInstance
    val friends = fileReaderLBSN.readFriendsFile(friendsFile) // friends
    val checkins = fileReaderLBSN.readCheckinFile(checkinFile) // check-ins
    val df = new DataFormatter
    val usersOnCheckins = checkins.groupBy(t => t._1)
      .filter(t => t._2.size > minUserCk).map(t => t._1)
    val usersOnLocations = checkins.groupBy(t => t._1)
      .map(t => (t._1, t._2.map(it => it._6).distinct))
      .filter(t => t._2.size > minUserLocs).map(t => t._1)
    val commonUsers: Set[Long] = usersOnCheckins.toSet.intersect(usersOnLocations.toSet)
    val commonUsersMap: Map[Long, Long] = commonUsers.map(t => (t, 1L)).toMap
    val filteredFriends = friends.filter(t => (commonUsersMap.getOrElse(t._1, null) != null && commonUsersMap.getOrElse(t._2, null) != null))
    val users = df.getUsersFromFriends(filteredFriends)
    val usersMap = users.map(t => (t, 1)).toMap
    val filteredCheckins = checkins.filter(t => usersMap.getOrElse(t._1, null) != null)
    return (filteredFriends, filteredCheckins)
  }

  def filterLocs(friendsFile: String, checkinFile: String, minUserCk: Long, minUserLocs: Long)
  : (List[(Long, Long)], List[(Long, Date, Double, Double, String, Long, String)]) = {
    val fileReaderLBSN = new fileReaderLBSN
    val numberFormatter = java.text.NumberFormat.getIntegerInstance
    val friends = fileReaderLBSN.readFriendsFile(friendsFile) // friends
    val checkins = fileReaderLBSN.readCheckinFile(checkinFile) // check-ins
    val df = new DataFormatter
    val usersOnCheckins = checkins.groupBy(t => t._1)
      .filter(t => t._2.size > minUserCk).map(t => t._1)
    val usersOnLocations = checkins.groupBy(t => t._1)
      .map(t => (t._1, t._2.map(it => it._6).distinct))
      .filter(t => t._2.size > minUserLocs).map(t => t._1)
    val commonUsers: Set[Long] = usersOnCheckins.toSet.intersect(usersOnLocations.toSet)
    val commonUsersMap: Map[Long, Long] = commonUsers.map(t => (t, 1L)).toMap
    val filteredFriends = friends.filter(t => (commonUsersMap.getOrElse(t._1, null) != null && commonUsersMap.getOrElse(t._2, null) != null))
    val users = df.getUsersFromFriends(filteredFriends)
    val usersMap = users.map(t => (t, 1)).toMap
    val filteredCheckins = checkins.filter(t => usersMap.getOrElse(t._1, null) != null)
    return (filteredFriends, filteredCheckins)

  }

}
