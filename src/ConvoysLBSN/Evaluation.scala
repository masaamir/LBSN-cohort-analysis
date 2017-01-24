package ConvoysLBSN

import java.io.{File, PrintWriter}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import FormatData.fileReaderLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by aamir on 13/10/16.
  */
class Evaluation {
  var friends: Map[List[Long], List[Long]] = Map()

  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def dateToString(inDate: Date): String = {
    // standard which is read
    val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val stringDate = df.format(inDate)
    return stringDate
  }

  def createDatasetForCrossValidation(inPercentTest: Double, inCCheckinsFile: String, inTrainCheckinFile: String, inTestCheckinFile: String): Unit = {
    val fr = new fileReaderLBSN
    /*val tempCheckins = scala.io.Source.fromFile(inCCheckinsFile).getLines().take(2)
      .map(t => t.split("\t"))
      .map{t =>
        println("\n")
        println("Date before::"+t(1))
        println("Date After::"+stringToDate(t(1)))
        println("Trying to convert::"+stringToDate(t(1)))
        dateToString(stringToDate(t(1)))
        (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))}
      .toList.distinct*/
    val totalCheckins = fr.readCheckinFileNew(inCCheckinsFile).sortBy(t => t._2)
    println("minimum Date::" + totalCheckins.minBy(t => t._2)._2)
    println("maximum Date::" + totalCheckins.maxBy(t => t._2)._2)

    val partIndex = totalCheckins((totalCheckins.size * inPercentTest).floor.toInt)
    //divide total data-set into to two parts first on the basis of number of tuple and then make them separate on the basis of time stamps
    //println("partition Index::"+partIndex.size)
    val partIndexTime = partIndex._2
    println("partition Date::" + partIndexTime)
    println("Total Checkins::" + totalCheckins.size)
    val trainCheckins = totalCheckins.filter(t => t._2.before(partIndexTime) || t._2 == partIndexTime)
    println("Training tuple size::" + trainCheckins.size)
    val testCheckins = totalCheckins.filter(t => t._2.after(partIndexTime))
    println("Testing tuple size::" + testCheckins.size)

    /** writing check-ins for training data-set */
    fr.writeCheckinsInFile(trainCheckins, inTrainCheckinFile)

    /** writing check-ins for testing data-set */
    fr.writeCheckinsInFile(testCheckins, inTestCheckinFile)
  }

  def findMaxPairGreedy(userScorePair: List[(Long, Double)], friends: Map[Long, List[Long]], k: Int)
  : (ListBuffer[Long],Double) = {
    val groupScorePair: List[(ListBuffer[Long], Double)] = userScorePair.map(t => (ListBuffer(t._1), t._2))
    //val groupFriends=friends.map(t=> (List(t._1),t._2)).toMap
    var currentPair: (ListBuffer[Long], Double) = groupScorePair.head
    val headGroup = currentPair._1
    for (i: Int <- 1 until k) {
      // there might not be a group of k users
      if (headGroup.size < i ) {
        println("no more increase in group size")

      } else {
        var loopContinue = true
        for (j: Int <- 0 until userScorePair.size) {
          if (loopContinue) {
            val cUser = userScorePair(j)._1
            if (!headGroup.contains(cUser)) {
              // if group doesn't already contain this element
              // check if the whole group is friend with this member:cUser
              var groupFriend = true
              headGroup.foreach { t =>
                if (!friends.getOrElse(t, null).contains(cUser)) {
                  groupFriend = false
                }
              }
              if (groupFriend) {
                //if whole group is friend
                //head +=
                val newHead: ListBuffer[Long] = currentPair._1
                newHead += cUser
                currentPair = ((newHead, currentPair._2 + userScorePair(j)._2))
                loopContinue = false
              }
            }
          }
        }
      }
    }
    println("current pair is ::"+currentPair)
    return currentPair
  }

  /*def findMaxPair(score: List[(List[Long], Double)], lb: Double): Unit = {
    if (score.size < 2) {
      return lb
    } else {
      var i = 0
      var j = 0
      var continue = true
      for (i <- 0 until score.size) {
        val head = score(i)._1
        val headFriends = friends.getOrElse(head, List())
        if (continue == true) {
          for (j <- i + 1 until score.size) {
            if (continue == true) {
              if (headFriends.contains(score(j))) {

              }

            }
          }

        }

      }

    }
  }*/

  def findGroupTopK(userScoreFile: String, k: Int): List[Long] = {
    val userScores = scala.io.Source.fromFile(userScoreFile).getLines().toList
      .map(t => t.split("\t")).map(t => (t(0).toLong, t(1).toDouble))
    val group = userScores.sortBy(t => -t._2).take(k).map(t => t._1)
    return group
  }

  def findGroupTopKFriends(userScoreFile: String, k: Int, friendsFile: String): List[Long] = {
    val userScores = scala.io.Source.fromFile(userScoreFile).getLines().toList
      .map(t => t.split("\t")).map(t => (t(0).toLong, t(1).toDouble)).sortBy(t => -t._2)
    val fr = new fileReaderLBSN
    val friendsMap = fr.readFriendsFile(friendsFile).groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._2)))
    val maxPair=findMaxPairGreedy(userScores,friendsMap,k)
    return maxPair._1.toList

  }

  def findMeasurement2(userActsCatsTSFile:String,groupActsCatsTSFile:String,friendsFile:String,inCat:List[String]): Unit ={
    val userActsTS=scala.io.Source.fromFile(userActsCatsTSFile).getLines()
      .map(t=> t.split("\t"))
      .map(t=> (ListBuffer(t(0).toLong),t(1).toLong,t(2).split(",").to[ListBuffer],(t(3),t(4)))).to[ListBuffer]
    //.take(10).foreach(t=> println(t))
    println("user ats size ::"+userActsTS.size)
    println("now group")
    val groupActsTS=scala.io.Source.fromFile(groupActsCatsTSFile).getLines()
      .map(t=> t.split("\t"))
      .map(t=> (t(0).split(",").map(it=> it.toLong).to[ListBuffer],t(1).toLong, t(2).split(",").to[ListBuffer],(t(3).split(",").head,t(3).split(",").last))).to[ListBuffer]
      //.take(10).foreach(t=> println(t))
    println("group size::"+groupActsTS.size)
    //val totalActsTS=userActsTS ++ groupActsTS
    val usersActsMap=userActsTS.groupBy(t=> t._1)
    println("user map size ::"+usersActsMap.size)
    val groupActsMap=groupActsTS.groupBy(t=> t._1)
    println(" Group size is ::"+ groupActsMap.size)


    val allCats:ListBuffer[String]=new ListBuffer()
    //get map on categories
    val catSplitTotalActsTS:ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],(String,String),String)]=new ListBuffer()
    userActsTS.foreach{t=>
      val cats=t._3
      cats.foreach{c=>
        allCats += c
        if(c!="n\\a")
        catSplitTotalActsTS += ((t._1,t._2,t._3,t._4,c))
      }
    }
    val catActsMap=catSplitTotalActsTS.groupBy(t=> t._5)
    println("cat map size ::"+catActsMap.size)
    println("all cats size ::"+allCats.distinct.size)
    val tripCatActsMap=catActsMap.filter(t=> inCat.contains(t._1))
    println("new map size::"+tripCatActsMap.size)

    // no new addition in the map: individual and group activities can be send separately as they have redundant activities
    // for every new group have to match for all the superset of this new potential group: that will be the activities of this

  }

  def findCombinedAffinityOfGroup(headGroup:(ListBuffer[Long],Double),currentPair:(Long,Double)): (ListBuffer[Long],Double) ={
    //val newGroup= (headGroup._1 :+ currentPair._1)
    val result= (headGroup._1 :+ currentPair._1, headGroup._2+currentPair._2)
    return result

  }

  def findGroupCohTripCat(headGroup:ListBuffer[Long],cUser:Long, inCats:ListBuffer[String],
                          allGroupActs:ListBuffer[(ListBuffer[Long],Long,ListBuffer[String])],
                          userActsMap:Map[Long,ListBuffer[(Long,ListBuffer[String])]]): Unit = {

    val pGroup = headGroup :+ cUser
    inCats.foreach { c =>
    // should be for each category
    val pGroupActs = allGroupActs.filter { t =>
      t._3.contains(c) &&
        pGroup.forall(t._1.contains)
    }
    val totalIndActs: ListBuffer[(Long, ListBuffer[String])] = new ListBuffer()
    pGroup.foreach { t =>
      if (userActsMap.contains(t)) {
        val acts = userActsMap.getOrElse(t, ListBuffer()) .filter(t=> inCats.forall(t._2.contains))
        totalIndActs ++= acts
      }
    }
    val score = pGroupActs.size.toDouble / totalIndActs.size.toDouble
    println("group is ::" + pGroup + " score all categories is ::" + score)
  }



  }

  def findGroupCohAllCat(headGroup:ListBuffer[Long],cUser:Long, inCats:ListBuffer[String],
                         allGroupActs:ListBuffer[(ListBuffer[Long],Long,ListBuffer[String])],
                         userActsMap:Map[Long,ListBuffer[(Long,ListBuffer[String])]]): Unit ={
    val pGroup= headGroup :+ cUser
    // should be for each category
    val pGroupActs=allGroupActs.filter{t=>
      //inCats.forall(t._3.contains) &&
      pGroup.forall(t._1.contains)
    }
    val totalIndActs:ListBuffer[(Long,ListBuffer[String])]=new ListBuffer()
    pGroup.foreach{t=>
      if(userActsMap.contains(t)){
        val acts=userActsMap.getOrElse(t,ListBuffer())//.filter(t=> inCats.forall(t._2.contains))
        totalIndActs ++= acts
      }
    }
    val score=pGroupActs.size.toDouble/totalIndActs.size.toDouble

    println("group is ::"+pGroup +" score all categories is ::"+score)


  }

  def findMeasurement(userScoreFile: String, k: Int, friendsFile: String): Unit ={
    val userScorePair = scala.io.Source.fromFile(userScoreFile).getLines().toList
      .map(t => t.split("\t")).map(t => (t(0).toLong, t(1).toDouble))//.sortBy(t => -t._2)
    val userScoreMap=userScorePair.toMap
    val fr = new fileReaderLBSN
    val friendsMap = fr.readFriendsFile(friendsFile).groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._2)))
    var GroupScoreList:ListBuffer[(ListBuffer[Long],Double)]=new ListBuffer()
    //val candidateUsersScore=userScorePair
    val firstMax=userScorePair.maxBy(t=> t._2) // first max
    var currentGroupPair=(ListBuffer(firstMax._1),firstMax._2) // first as
    /***/
    //sort first list on the basis of score
    //userScorePair.take(10).foreach(println)
    for (i: Int <- 1 to k) {
      val currentGroupUsers=currentGroupPair._1
      println("value of iteration is::"+i)
      // there might not be a group of k users
      println("at k ="+ i +" current pair is::"+currentGroupPair)
      println("values of each of member is ::")
      currentGroupPair._1.foreach{t=>
        println("for user::"+t+" value is::"+userScoreMap.getOrElse(t,null))
      }
      if (currentGroupUsers.size < i ) {
        println("no more increase in group size")
      } else {
        GroupScoreList=new ListBuffer()
        for (j: Int <- 0 until userScorePair.size) {
            val currentPair=userScorePair(j)
            val cUser = currentPair._1
            if (!currentGroupUsers.contains(cUser)) {
              // if group doesn't already contain this element
              // check if the whole group is friend with this member:cUser
              var groupFriend = true
              currentGroupUsers.foreach { t =>
                if (!friendsMap.getOrElse(t, null).contains(cUser)) {
                  groupFriend = false
                }
              }
              if (groupFriend) {
                //store values of each pair for new group
                // filter friends - done
                // find cohesiveness on all the categories
                val newPairScore=findCombinedAffinityOfGroup(currentGroupPair,currentPair)
                //val newPairScore= findCombinedAffinityAllCat()
                //val pairOnTripCats=
                GroupScoreList += newPairScore
                // find cohesiveness on trip categories
              }
              /*var groupFriend = true
              headGroup.foreach { t =>
                if (!friends.getOrElse(t, null).contains(cUser)) {
                  groupFriend = false
                }
              }
              if (groupFriend) {
                //if whole group is friend
                //head +=
                val newHead: ListBuffer[Long] = currentPair._1
                newHead += cUser
                currentPair = ((newHead, currentPair._2 + userScorePair(j)._2))
                loopContinue = false
              }*/
            }
        }
        //make new group head
        if(GroupScoreList.size>0)
        currentGroupPair=GroupScoreList.maxBy(t=> t._2)
      }
    }
    println("final group is ::"+currentGroupPair)
  }

  def findTogetherTravelScore(inPair:(List[Long],Long),inCheckins:List[(Long,Date,Double,Double,String,Long,String)]
                              ,inConvoys:List[(List[Long],List[Long],List[String])]): Unit ={
    val inGroup= inPair._2 :: inPair._1
    val newConvoys=inConvoys.filter{c=>
      inGroup.forall(c._1.contains)
    }
    //val

  }

  def callFunGroupScore(checkinsWithCatFile:String, convoysTableFile:String, users:List[Long]): Unit ={
    val fr=new fileReaderLBSN
    val checkinsWithCat=fr.readCheckinsWithCats(checkinsWithCatFile)
    val convoys=fr.readConvoyTableFile(convoysTableFile)
    val group=users.map(t=> List(t))
    group.foreach{g=>
      users.foreach{u=>
        if(!g.contains(u)){
          findTogetherTravelScore((g,u),checkinsWithCat,convoys)
        }
      }
    }

  }

  def evaluateGroup(inGroup: List[Long], inCat: List[String], convoysTableFile: String): Unit = {
    val cpa = new ConvoysPatternAnalysis
    val convoys = scala.io.Source.fromFile(convoysTableFile).getLines().drop(1).toList
      .map(t => t.split("\t")).map(t => (t(7), t(8), t(9)))
      .map(t => (t._1.split(",").map(it => it.toLong).toList, t._2.split(",").map(it => it.toLong).toList, t._3.split(",").toList)).distinct
    println("Convoys are ::" + convoys.size)
    println("input user group::" + inGroup)
    println("input loc categories::" + inCat)
    val contConvoys = convoys.filter { t =>
      inGroup.forall(t._1.contains) &&
       inCat.forall(t._3.contains)
    }
    println("filtered Convoys size ::" + contConvoys.size)
    //println("these convoys are ::")
    //contConvoys.foreach(t=> println(t))
    /*
    println("contained convoys are below ::")
    convoys.filter(t=> t._1.contains(9345))// && t._1.contains(9345))
      .foreach(println)
    println("contained convoys for cats are below::")
    //convoys.filter(t=> t._3.contains("Nightlife") && t._3.contains("Bar")).foreach(println)
    //convoys.foreach(t=> println(t._3))
    */

  }

  def getCheckinsOfUser(checkins: ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)], user: Long)
  : ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)] = {
    val newCheckins = checkins.filter { t =>
      t._1 == user
    }
    return newCheckins
  }

  def getCheckinsAtCat(checkins: ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)], cat: String)
  : ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)] = {
    val newCheckins = checkins.filter { t =>
      t._9 == cat
    }
    return newCheckins
  }

  def getCheckinsByUserNCat(checkins: ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)], user: Long, cat: String)
  : ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)] = {
    val newCheckins = checkins.filter { t =>
      t._1 == user && t._9 == cat
    }
    return newCheckins
  }

  def getCatScore(checkins: ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)]
                  , inputCat: List[String], catScorefile: String): Unit = {
    val lambada = 0.5
    val users = checkins.map(t => t._1).distinct
    val catScoreWriter = new PrintWriter(new File(catScorefile))
    /*val filteredCheckins=checkins.filter{t=>
      inputCat.contains(t._9)
    }*/
    val CKGroupByCat = checkins.groupBy(t => t._9)
    println("checkins with cat::" + checkins.size)
    val CKGroupByUser = checkins.map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, "")).distinct
      .groupBy(t => t._1)
    var personalizedScore: Double = 0
    var globalScore: Double = 0
    //val userScore:Double=0
    var categoryScore: Double = 0
    var catCheckins: ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)] = new ListBuffer()
    var userCatCheckins: ListBuffer[(Long, Date, Double, Double, String, Long, String, Long, String)] = new ListBuffer()
    println("total users ::" + users.size)
    var uCount = 0
    val userScore = users.map { u =>
      uCount += 1
      if (uCount % 100 == 0) println(uCount, users.size)
      //println("user ::"+u)
      //personalizedScore=0
      //globalScore=0
      categoryScore = 0
      inputCat.foreach { cat =>

        /** Global contribution of user u towards category cat */
        catCheckins = CKGroupByCat.getOrElse(cat, null) // all check-ins at the given category
        userCatCheckins = catCheckins.filter(t => t._1 == u) // check-ins of the user at the given category
        personalizedScore = lambada * (userCatCheckins.size.toDouble / catCheckins.size.toDouble)

        /** personalized preference of user u towards category cat */
        globalScore = (1 - lambada) * userCatCheckins.size.toDouble / CKGroupByUser.getOrElse(u, ListBuffer()).size.toDouble

        /** score of user u for category */
        categoryScore += 1.toDouble / inputCat.size.toDouble * (personalizedScore + globalScore)
      }
      catScoreWriter.println(u + "\t" + categoryScore)
      (u, categoryScore)
    }

    catScoreWriter.close()
    userScore.sortBy(t => -t._2).take(10).foreach(println)
  }

  /*
    def getCatScore(checks: ListBuffer[(Long,Date,Double,Double,String,Long,String,Long,String)], inputCats:List[String]): Unit ={
      val lambda=0.5
      val totalCheckGBUsers=checks.groupBy(t=> t._1)
      val totalChecksGBCats=checks.groupBy(t=> t._9)

      //check-in filtered on given categories of the trip
      val iCChecks=checks.filter{t=>
        inputCats.contains(t._9)
      }
      val iCCheckGBUsers=iCChecks.groupBy(t=> t._1)
      val iCCheckGBCats=iCChecks.groupBy(t=> t._9  )

      val users=checks.map(t=> t._1).distinct

      users.map{u=>
        inputCats.foreach{ic=>
          val score= 1.toDouble/inputCats.size.toDouble
          iCCheckGBUsers.getOrElse(u,ListBuffer())
        }

      }

    }
  */

}
