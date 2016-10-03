package FormatData

import java.util.Date

/**
 * Created by MAamir on 4/26/2016.
 */
class DataFilter {
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
