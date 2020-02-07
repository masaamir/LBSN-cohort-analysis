package FormatData

import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer

/**
 * Created by MXXX on 5/5/XXX.
 */
class syntheticDataGenerator {
  def createSyntheticData(checkinsFile: String, times: Int, checkinFileWrite: String): Unit = {
    val fr = new fileReaderLBSN
    val writer = new PrintWriter(new File(checkinFileWrite))
    //val friends=fr.readFriendsFile(friendsFile)

    val checkins = scala.io.Source.fromFile(checkinsFile).getLines()
      .map(t => t.split("\t")).map(t => (t(0).toLong, t(1), t(2), t(3), t(4), t(5))).toList
    println("original users size::" + checkins.map(t => t._1).distinct.size)
    var newCheckins: ListBuffer[(Long, String, String, String, String, String)] = ListBuffer[(Long, String, String, String, String, String)]()
    newCheckins = newCheckins ++ checkins
    println("checkin size:;" + checkins.size)
    //println("new Checkin size::"+newCheckins.size)
    val checkinTimes = newCheckins.map(t => t._2).distinct.toArray // actual time values
    val checkinTimesSize = checkinTimes.size // number of times
    //println("checkin time size::"+checkinTimesSize)
    val maxUserId = newCheckins.map(t => t._1).distinct.sortBy(t => -t).head
    for (i <- 0 until times) {
      println("current,total::" + i, times)
      //maxUserId= maxUserId * (i+1)
      //var updatedUserId=maxUserId
      var count = 0
      checkins.foreach { t =>
        count += 1
        if (count % 1000000 == 0)
          println(count)
        val r = scala.util.Random
        val randomDate: String = checkinTimes(r.nextInt(checkinTimesSize))
        //updatedUserId +=1
        val newUser = t._1 + maxUserId
        val newTuple = (newUser, randomDate, t._3, t._4, t._5, t._6)
        //println("old Tuple:"+t)
        //println("new Tuple:"+newTuple)
        newCheckins += newTuple
      }
    }
    println("newCheckin Size::" + newCheckins.size)
    //println("")
    //println("now users size::"+newCheckins.map(t=> t._1).distinct.size)
    newCheckins.foreach { t =>
      writer.println(t._1 + "\t" + t._2 + "\t" + t._3 + "\t" + t._4 + "\t" + t._5 + "\t" + t._6)

    }
    writer.close()


  }

}
