package ConvoysLBSN

import FormatData.fileReaderLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by XXX on 01/11XXX.
  */
class evaluation2 {

  def findUserActs(userActsCatsTSFile:String)
  : ListBuffer[(Long, Long, ListBuffer[String], (String, String))] ={
    val userActsTS=scala.io.Source.fromFile(userActsCatsTSFile).getLines()
      .map(t=> t.split("\t"))
      .map(t=> (t(0).toLong,t(1).toLong,t(2).split(",").to[ListBuffer],(t(3),t(4)))).to[ListBuffer]
    println("user ats size ::"+userActsTS.size)
    //val userActsMap=userActsTS.groupBy(t=> t._1)
    //println("user map size ::"+userActsMap.size)
    return userActsTS
  }

  def findGroupActsMap(groupActsCatsTSFile:String)
  : Map[ListBuffer[Long], ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]] ={
    val groupActsTS=scala.io.Source.fromFile(groupActsCatsTSFile).getLines()
      .map(t=> t.split("\t"))
      .map(t=> (t(0).split(",").map(it=> it.toLong).to[ListBuffer],t(1).toLong, t(2).split(",").to[ListBuffer],(t(3).split(",").head,t(3).split(",").last))).to[ListBuffer]
    //.take(10).foreach(t=> println(t))
    println("group size::"+groupActsTS.size)
    val groupActsMap=groupActsTS.groupBy(t=> t._1)
    println(" Group size is ::"+ groupActsMap.size)
    return groupActsMap
  }

  def findCatActsMap(userActsTS:ListBuffer[(Long, Long, ListBuffer[String], (String, String))], inCat:List[String])
  : Map[String, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]] ={
    val allCats:ListBuffer[String]=new ListBuffer()
    //get map on categories
    val catSplitTotalActsTS:ListBuffer[(Long,Long,ListBuffer[String],(String,String),String)]=new ListBuffer()
    userActsTS.foreach{t=>
      val cats=t._3
      cats.foreach{c=>
        allCats += c
        if(c!="n\\a")
          catSplitTotalActsTS += ((t._1,t._2,t._3,t._4,c))
      }
    }
    val catActsMap=catSplitTotalActsTS.groupBy(t=> t._5)
      .map(t=> (t._1,t._2.map(it=> (it._1,it._2,it._3,it._4))))
    println("cat map size ::"+catActsMap.size)
    println("all cats size ::"+allCats.distinct.size)
    val tripCatActsMap=catActsMap.filter(t=> inCat.contains(t._1))
    println("new map size::"+tripCatActsMap.size)
    return tripCatActsMap
  }

  def  anyActsMapToGroupCatActsMap(groupActMap:Map[ListBuffer[Long], ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]],
                                  inCats:List[String])
  : Map[(ListBuffer[Long],String), ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]] ={
    var newActs:ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String),String)]=new ListBuffer()
    var allCatActMap:Map[(ListBuffer[Long],String), ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]]= Map()
    var localCatActMap:Map[(ListBuffer[Long], String), ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]]= Map()
    var sizeCountCheck=0
    var sizeEqualCheck=0
    groupActMap.foreach{g=>
      //println("user ::"+u)
      newActs=new ListBuffer()
      g._2.foreach{act=>
        act._3.foreach{cat=>
          newActs += ((act._1,act._2,act._3,act._4,cat))
        }
      }
      localCatActMap=newActs.groupBy(t=> t._5)// group by category
        .map(t=> (t._1,t._2.map(it=> (it._1,it._2,it._3,it._4)))) // map to bring normal activity form
        .filter(t=> inCats.contains(t._1))//filter on only trip categories
        .map(t=> ((g._1,t._1),t._2) )//map: user-category= activities
      allCatActMap ++= localCatActMap
      sizeCountCheck+=localCatActMap.size
    }
    println("all user map size ::"+allCatActMap.size)
    return allCatActMap
  }

  /*def groupActsMapToGroupCatActsMap(groupActs:ListBuffer[(Any, Long, ListBuffer[String], (String, String))],
                                  inCats:List[String])
  : Unit ={
    var newActs:ListBuffer[(Any, Long, ListBuffer[String], (String, String),String)]=new ListBuffer()
    //var allCatActMap:Map[(Any,String), ListBuffer[(Any, Long, ListBuffer[String], (String, String))]]= Map()
    var localCatActMap:Map[(Any, String), ListBuffer[(Any, Long, ListBuffer[String], (String, String))]]= Map()
    var sizeCountCheck=0
    var sizeEqualCheck=0
    //groupActs.foreach{g=>
      //println("user ::"+u)
      newActs=new ListBuffer()
    groupActs.foreach{act=>
        act._3.foreach{cat=>
          newActs += ((act._1,act._2,act._3,act._4,cat))
        }
      }
      localCatActMap=newActs.groupBy(t=> t._5)// group by category
        .filter(t=> inCats.contains(t._1))//filter on only trip categories
        .map(t=> (t._1,t._2.map(it=> (it._1,it._2,it._3,it._4)))) // map to bring normal activity form
        .map(t=> ((t,t._1),t._2) )//map: user-category= activities
      allCatActMap ++= localCatActMap
      sizeCountCheck+=localCatActMap.size
    //}
    println("all user map size ::"+allCatActMap.size)
    //return allCatActMap
  }
  */

  def userActsMapToUserCatActsMap(userActMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                                  inCats:List[String])
  : Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]] ={
    var newActs:ListBuffer[(Long, Long, ListBuffer[String], (String, String),String)]=new ListBuffer()
    var allUserCatActMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]]= Map()
    var userCatActMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]]= Map()
    var sizeCountCheck=0
    var sizeEqualCheck=0
    userActMap.foreach{u=>
      //println("user ::"+u)
      newActs=new ListBuffer()
      u._2.foreach{act=>
        act._3.foreach{cat=>
          newActs += ((act._1,act._2,act._3,act._4,cat))
        }
      }
      userCatActMap=newActs.groupBy(t=> t._5)// group by category
        .map(t=> (t._1,t._2.map(it=> (it._1,it._2,it._3,it._4)))) // map to bring normal activity form
        .filter(t=> inCats.contains(t._1))//filter on only trip categories
        .map(t=> ((u._1,t._1),t._2) )//map: user-category= activities
      allUserCatActMap ++= userCatActMap
      sizeCountCheck+=userCatActMap.size
    }
    println("all user map size ::"+allUserCatActMap.size)
    return allUserCatActMap
  }

  def getUsersAffinity(users:ListBuffer[Long],
                       userActMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                       inUserCatActMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                       catActsMap:Map[String, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                       inCats:List[String],
                       lambda:Double)
  : ListBuffer[(Long, Double)] ={

    var uCount=0
    var categoryScore: Double = 0
    val userCatActMap=inUserCatActMap//userActsMapToUserCatActsMap(userActMap,inCats)
    var userActSize=0
    var catActSize=0
    var userCatActSize=0
    var personalizedScore:Double=0
    var globalScore:Double=0
    val userScore = users.map { u =>
      uCount += 1
      //if (uCount % 100 == 0) println(uCount, users.size)
      //println("user ::"+u)
      personalizedScore=0
      globalScore=0
      if(userActMap.contains(u))
        userActSize=userActMap.getOrElse(u,null).size
      else userActSize=0
      //println("user activities::"+userActMap.getOrElse(u,null))
      categoryScore = 0
      inCats.foreach { cat =>

        if(catActsMap.contains(cat))
          catActSize=catActsMap.getOrElse(cat,null).size
        else catActSize=0
        //println("activities::"+catActsMap.getOrElse(cat,null))
        if(userCatActMap.contains((u,cat)))
          userCatActSize=userCatActMap.getOrElse((u,cat),null).size
        else userCatActSize=0

        /**personalized Score*/
        if(userActSize!=0)
        personalizedScore = lambda * userCatActSize.toDouble/userActSize.toDouble
        else personalizedScore=0
        /**global Score*/
        if(catActSize!=0)
        globalScore = (1- lambda) * userCatActSize.toDouble/catActSize.toDouble
        else globalScore=0
        categoryScore += personalizedScore +globalScore
/*
        /** personalized preference of user u towards category cat */
        //val userActs=userActMap

        val userActCat=userCatActMap.getOrElse((u,cat),0).

        val userActCat=userCatActMap.getOrElse((u,cat),null)

        globalScore = (1 - lambada) * userCatCheckins.size.toDouble / CKGroupByUser.getOrElse(u, ListBuffer()).size.toDouble

        /** Global contribution of user u towards category cat */
        val userActs = CKGroupByCat.getOrElse(cat, null) // all check-ins at the given category
        val userCatCheckins = catCheckins.filter(t => t._1 == u) // check-ins of the user at the given category
        personalizedScore = lambada * (userCatCheckins.size.toDouble / catCheckins.size.toDouble)

        /** score of user u for category */
        categoryScore += 1.toDouble / inputCat.size.toDouble * (personalizedScore + globalScore)
        */
      }
      if(inCats.size!=0)
      categoryScore = categoryScore/inCats.size.toDouble
      else categoryScore=0
      //catScoreWriter.println(u + "\t" + categoryScore)
      (u, categoryScore)
    }
    userScore.sortBy(t=> -t._2).take(10).foreach(println)
    return userScore
  }

  def findAffinityOfGroup(headGroup:(ListBuffer[Long],Double),currentPair:(Long,Double)): (ListBuffer[Long],Double) ={
    //val newGroup= (headGroup._1 :+ currentPair._1)
    val result= (headGroup._1 :+ currentPair._1, headGroup._2+currentPair._2)
    return result

  }

  def splitActOnCat(groupActs: ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))],
                    inCats:List[String])
  : Map[String, ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]] ={
    val newActs:ListBuffer[(ListBuffer[Long],Long,ListBuffer[String], (String, String),String)]=new ListBuffer()
      groupActs.foreach{t=>
        t._3.foreach{cat=>
          newActs += ((t._1,t._2,t._3,t._4,cat))
        }
      }
    val newCatMap=newActs.groupBy(t=> t._5)
        .filter(t=> inCats.contains(t._1))
      .map(t=> (t._1,t._2.map(it=> (it._1,it._2,it._3,it._4))))
    return newCatMap
  }

  def findGroupActs(inGroup:ListBuffer[Long],
                     groupActsMap:Map[ListBuffer[Long], ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]])
  : ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))] ={
    val newGroupActs:ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]=new ListBuffer()
      groupActsMap.keys.foreach{t=>
        if(inGroup.forall(t.contains)){
          newGroupActs ++= groupActsMap.getOrElse(t,null)
        }
      }
    return newGroupActs
  }

  def findCohGroup(inPrevGroup:ListBuffer[Long],
                   inCUser:Long,
                   userActMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                   userCatActMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                   groupActsMap:Map[ListBuffer[Long], ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]],
                   //inPreGroupActs:ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],(String,String))],
                   inPreGroupActMap:Map[ListBuffer[Long],ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],(String,String))]],
                   inPreGroupCatActMap:Map[(ListBuffer[Long],String),ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],(String,String))]],
                   inCats:List[String]): Double ={

    val newGroup=inPrevGroup :+ inCUser
    val newGroupActs=findGroupActs(newGroup,groupActsMap)
    val newGroupActMap=ListBuffer((newGroup,newGroupActs)).toMap
    var preGroupActSize=0
    if(inPreGroupActMap.contains(inPrevGroup))
      preGroupActSize=inPreGroupActMap.getOrElse(inPrevGroup,null).size
    else preGroupActSize=0

    var userActSize=0
    if(userActMap.contains(inCUser)){
      userActSize=userActMap.getOrElse(inCUser,null).size
    }else userActSize=0

    /**Cohesiveness on all categories*/
    var genCoh:Double=0
      if(userActSize!=0 || preGroupActSize!=0 )
    genCoh=newGroupActs.size.toDouble/(userActSize.toDouble + preGroupActSize.toDouble)

    /**Cohesiveness on Trip categories*/
    //val preGroupCatActs=anyActsMapToGroupCatActsMap(inPreGroupActMap,inCats)//splitActOnCat(preGroupActs,inCats)
    val newGroupCatActs=anyActsMapToGroupCatActsMap(newGroupActMap,inCats)
    var tripCoh:Double=0
    inCats.foreach{c=>
      var newGroupCatActSize=0
      var userCatActSize=0
      var preGroupCatActSize=0
      if(userCatActMap.contains(inCUser,c)){
        userCatActSize=userCatActMap.getOrElse((inCUser,c),null).size
      }

      //var userCatActSize=userCatActMap.getOrElse((inCUser,c),null).size
      if(inPreGroupCatActMap.contains((inPrevGroup,c)))
      preGroupCatActSize=inPreGroupCatActMap.getOrElse((inPrevGroup,c),null).size

      if(newGroupCatActs.contains((newGroup,c))){
        newGroupCatActSize=newGroupCatActs.getOrElse((newGroup,c),null).size
      }
      if(userCatActSize!=0 || preGroupActSize!=0)
      tripCoh += newGroupCatActSize.toDouble/ (userCatActSize + preGroupActSize).toDouble
    }
    val alpha=0.5
    val finalScore= alpha*(genCoh)+ (1-alpha)*tripCoh
    return finalScore
  }

  def findBestGreedyGroup(k:Int,
                          userActMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                          inUserCatActMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                          groupActsMap:Map[ListBuffer[Long], ListBuffer[(ListBuffer[Long], Long, ListBuffer[String], (String, String))]],
                          catActsMap:Map[String, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                          friendsMap:Map[Long,List[Long]],
                          inCats:List[String])
  : ListBuffer[Long] ={
    val users= friendsMap.keys.to[ListBuffer]
    println("all users are ::"+users.size)
    val userScorePair=getUsersAffinity(users,userActMap,inUserCatActMap,catActsMap,inCats,0.5)
      .sortBy(t=> -t._2)
    val userScoreMap=userScorePair.toMap
    var GroupScoreList:ListBuffer[(ListBuffer[Long],Double)]=new ListBuffer()
    //val candidateUsersScore=userScorePair
    val firstMax=userScorePair.maxBy(t=> t._2) // first max
    var currentGroupPair=(ListBuffer(firstMax._1),firstMax._2) // first as
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
          val newPair=userScorePair(j)
          val cUser = newPair._1
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
              val newPairScore=findAffinityOfGroup(currentGroupPair,newPair) // affinity
              //GroupScoreList += newPairScore
              // find cohesiveness on trip categories
              val preGroupActs=findGroupActs(currentGroupPair._1,groupActsMap)
              val preGroupActMap=ListBuffer((currentGroupPair._1,preGroupActs)).toMap

              val preGroupCatActsMap=anyActsMapToGroupCatActsMap(preGroupActMap,inCats)//splitActOnCat(preGroupActs,inCats)
              val cohScore=findCohGroup(currentGroupPair._1,cUser,userActMap,inUserCatActMap,groupActsMap,preGroupActMap,preGroupCatActsMap,inCats)
              GroupScoreList += ((newPairScore._1,newPairScore._2+cohScore))
              //ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],(String,String)]
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
    println("final pair is ::"+currentGroupPair)
    return currentGroupPair._1

  }

  def findMeasurement2(userActsCatsTSFile:String,
                       groupActsCatsTSFile:String,friendsFile:String,inCat:List[String])
  : ListBuffer[Long] ={

    val fr = new fileReaderLBSN
    val friendsMap = fr.readFriendsFile(friendsFile).groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._2)))
    val userActsTS=findUserActs(userActsCatsTSFile)
    val userActsMap=userActsTS.groupBy(t=> t._1)
    println("user acts map size ::"+userActsMap.size)
    val userCatActMap=userActsMapToUserCatActsMap(userActsMap,inCat)
    println("now group")
    val groupActsMap=findGroupActsMap(groupActsCatsTSFile)
    println("group size::"+groupActsMap.size)
    val catsActsMap=findCatActsMap(userActsTS,inCat)
    val group=findBestGreedyGroup(2,userActsMap,userCatActMap,groupActsMap,catsActsMap,friendsMap,inCat)
    /*
    //val totalActsTS=userActsTS ++ groupActsTS
    /*val usersActsMap=userActsTS.groupBy(t=> t._1)
    println("user map size ::"+usersActsMap.size)
    val groupActsMap=groupActsTS.groupBy(t=> t._1)
    println(" Group size is ::"+ groupActsMap.size)*/
    val allCats:ListBuffer[String]=new ListBuffer()
    //get map on categories
    val catSplitTotalActsTS:ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],(String,String),String)]=new ListBuffer()
    userActsTS.values.foreach{t=>
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
    // for every new group have to match for all the superset of this new potential group: that will be the activities of this*/
    return group
  }


}
