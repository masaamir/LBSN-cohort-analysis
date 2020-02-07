package FormatData

/**
 * Created by MXXX on 5/10/XXX.
 */
class CityDataFilter {

  def filterDataOnCity(friendsFile: String, checkinFile: String,
                       minLat: Double, maxLat: Double, minLong: Double, maxLong: Double,
                       writeFileFriends: String, writeFileCheckins: String): Unit = {
    val fr = new fileReaderLBSN
    val friends = fr.readFriendsFile(friendsFile)
    val checkins = fr.readCheckinFile(checkinFile)
    //val checkins=scala.io.Source.fromFile(checkinFile).getLines().toList
    val cityCheckins = checkins.filter { t =>
      (minLat <= t._3 && maxLat >= t._3) &&
        (minLong <= t._4 && maxLong >= t._4)
    }
    println("count is ::" + cityCheckins.size)
    fr.checkinWriter(cityCheckins, writeFileCheckins)
    /*val writer=new PrintWriter(new File(writeFile))
    cityCheckins.foreach{t=>
      writer.println(t._1+"\t"+t._7+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
    }
    writer.close()*/

    /** Data Cleaner: Synchronize Users in friends file and Visitors in check-inFile */
    val df = new DataFormatter
    val synData = df.synchronizeUsersWithVisitors(friends, cityCheckins) //returns friends, checkins
    fr.friendWriter(synData._1, writeFileFriends) //without synchronization
    fr.checkinWriter(synData._2, writeFileCheckins) //without
    //val sf=new StatFinder
    //sf.analyseData(friendsFile,writeFile)


  }

}
