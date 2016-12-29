package TravelGroupRecommendation

import FormatData.fileReaderLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by aamir on 23/12/16.
  */
class GroupFinderGreedy {


  //read file into List[(user, location, List[cat], (startTime,endTime))]
  def findUserActs(userActsCatsTSFile:String)
  : ListBuffer[(Long, Long, ListBuffer[String], (String, String))] ={

    val userActsTS=scala.io.Source.fromFile(userActsCatsTSFile).getLines()
      .map(t=> t.split("\t"))
      .map(t=> (t(0).toLong,t(1).toLong,t(2).split(",").to[ListBuffer],(t(3),t(4)))).to[ListBuffer]
    //println("user ats size ::"+userActsTS.size)
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
    //println("group size::"+groupActsTS.size)
    val groupActsMap=groupActsTS.groupBy(t=> t._1)
    //println(" Group size is ::"+ groupActsMap.size)
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
    //println("cat map size ::"+catActsMap.size)
    //println("all cats size ::"+allCats.distinct.size)
    val tripCatActsMap=catActsMap.filter(t=> inCat.contains(t._1))
    //println("new map size::"+tripCatActsMap.size)
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
    //println("all user map size ::"+allCatActMap.size)
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
    userActMap.foreach{u=>
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
    //println("all user map size ::"+allUserCatActMap.size)
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
        /**In current state global value will be very small as negligible as it will be divided by all the activities*/
        if(userActSize!=0)
          personalizedScore = lambda * userCatActSize.toDouble/userActSize.toDouble // if any changes required in formula then here
        else personalizedScore=0
        /**global Score*/
        if(catActSize!=0)
          globalScore = (1- lambda) * userCatActSize.toDouble/catActSize.toDouble // if any changes required in formula then here
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
    //userScore.sortBy(t=> -t._2).take(10).foreach(println)
    return userScore
  }




  def findPairCohesiveness(inUserActsMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                           inUserCatsActsMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                           inPairActs:Map[(Long,Long),ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))]],
                           inPairCatsActs:Map[((Long,Long),String),ListBuffer[(((Long,Long),String),Long,ListBuffer[String],(String,String))]],
                           inCats:List[String],inAlpha:Double,
                          inUserPair:(Long,Long))
  : Double ={
    val pairUser=if(inUserPair._1<inUserPair._2) inUserPair else (inUserPair._2,inUserPair._1)

    /**cohesiveness for all activities*/
    var cohAll:Double=0.0
      if(inPairActs.contains(pairUser)) {
        cohAll = inPairActs.getOrElse(pairUser, null).size.toDouble / (inUserActsMap.getOrElse(pairUser._1, null).size + inUserActsMap.getOrElse(pairUser._2, null).size).toDouble
      }
    //println("all cat score::"+cohAll)
    /**cohesiveness for input Categories*/
    var catScore:Double=0.0
    inCats.foreach{cat=>
      if(inPairCatsActs.contains((pairUser,cat))) {
        println("num:"+inPairCatsActs.getOrElse((pairUser, cat), null).size)
        println("denom 1::"+(inUserCatsActsMap.getOrElse((pairUser._1, cat), null).size))
        println("denom 2::"+pairUser._2, cat)
        println("denom 2::"+inUserCatsActsMap.getOrElse((pairUser._2, cat), null).size)
        catScore += inPairCatsActs.getOrElse((pairUser, cat), null).size.toDouble / (inUserCatsActsMap.getOrElse((pairUser._1, cat), null).size + inUserCatsActsMap.getOrElse((pairUser._2, cat), null).size).toDouble
        //println("for category ::" + cat + " total score now is::" + catScore)
      }//else println("zero this time")

    }
    //println("input categories score is ::"+catScore/inCats.size)
    val pairScore= inAlpha * cohAll + (1-inAlpha) * (catScore/inCats.size)
    //println("pair score for "+pairUser+" is ::"+pairScore)
    return  pairScore

  }

  def findPairActsCats(pairActs:ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))])
  : ListBuffer[(((Long,Long),String),Long,ListBuffer[String],(String,String))] ={
    val pairActsSplitCat=new ListBuffer[(((Long,Long),String),Long,ListBuffer[String],(String,String))]()
    pairActs.foreach{t=>
      t._3.foreach{cat=>
        if(cat!="n\\a")
        pairActsSplitCat += (((t._1,cat),t._2,t._3,t._4))
      }
    }
    //println("new Acts Size::"+pairActsSplitCat.size)
    //pairActsSplitCat.take(5).foreach(println)
    return pairActsSplitCat

  }

  def findPairActs(groupActsFile:String)
  : ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))] ={
    //val pairActsWriter=new PrintWriter(new File(pairActsFile))
    val groupActs=scala.io.Source.fromFile(groupActsFile).getLines()
      .map(t=> t.split("\t")).map(t=> (t(0).split(","),t(1),t(2).split(","),t(3).split(",")))
      .map(t=> (t._1.map(it=> it.toLong).to[ListBuffer],t._2.toLong,t._3.to[ListBuffer],(t._4.head,t._4.last)))
    //.filter(t=> t._4.size <1)
    var pairActs= new ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))]()
    groupActs.toList.map{t=>
      if(t._1.size==2) {
        // only two users
        if(t._1(0)<t._1(1)){
          pairActs += (((t._1(0),t._1(1)),t._2,t._3,t._4)) // sort users u1< u2 and there is only single instance, i.e., u1,u2 only but not u2,u1
        } // if u1 < u2
        else{
          pairActs += (((t._1(1),t._1(0)),t._2,t._3,t._4))
        }
      }
      else if (t._1.size > 2){ // if group is bigger than 2 break group into pairs one sided
      var i,j=0
        for (i<-0 until t._1.size){
          for(j<- i+1 until t._1.size){
            //println("inserting ::+",(ListBuffer(t._1(i),t._1(j)),t._2,t._3,t._4))
            pairActs += (((t._1(i),t._1(j)),t._2,t._3,t._4))
          }
        }
      }
    }
    //println("pair Act size::"+pairActs.size)
    /*var pairActsDouble=new ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],ListBuffer[Long])]()
    pairActsDouble ++= pairActs
    pairActs.foreach{t=>
      pairActsDouble += ((ListBuffer(t._1(1),t._1(0)),t._2,t._3,t._4))
    }*/
    //pairActs.take(5).foreach(println)
    return pairActs
  }


  def findPairEdgesInGroup(inUsers:ListBuffer[Long]): ListBuffer[(Long,Long)] ={
    val users=inUsers.sortBy(t=> -t)
    val i,j=0
    val edges:ListBuffer[(Long,Long)]=new ListBuffer[(Long, Long)]()
    for(i<-0 until users.length){
      for(j<-i+1 until users.length){
        edges+= ((users(i),users(j)))
      }
    }
    //println("total edges are::")
    //edges.foreach(println)
    return edges
  }

  def findGroupTravelScore(affinity:Map[Long,Double],
                           inUserActsMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                           inUserCatsActsMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                           inPairActs:Map[(Long,Long),ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))]],
                           inPairCatsActs:Map[((Long,Long),String),ListBuffer[(((Long,Long),String),Long,ListBuffer[String],(String,String))]],
                           inCats:List[String],
                           inAlpha:Double,
                           inMu:Double,
                           users:ListBuffer[Long]
                          ): Double ={

    var groupAffinityScore:Double=0.0
    users.foreach{u=>
      groupAffinityScore += affinity.getOrElse(u,0.0)
      //println("user is:;"+u)
      //println("affinity now is::"+groupAffinityScore)
    }
    //println("total affinity score is ::"+groupAffinityScore)
    var cohScore:Double=0.0
    val pairs=findPairEdgesInGroup(users)
    pairs.foreach{p=>
      cohScore += findPairCohesiveness(inUserActsMap,inUserCatsActsMap,inPairActs,inPairCatsActs,inCats,inAlpha,p)
    }
    val totalScore=inMu*groupAffinityScore + (1-inMu)*cohScore
    //println("total group score is::"+totalScore)
    return totalScore

  }

  def choose(n:Long, k:Long): Long ={
    if(k==0) return 1
    return ((n * choose(n-1,k-1))/k)
  }

  def findBestGroupGreedy(
                          affinity:Map[Long,Double],
                          inUserActsMap:Map[Long, ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                          inUserCatsActsMap:Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]],
                          inPairActs:Map[(Long,Long),ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))]],
                          inPairCatsActs:Map[((Long,Long),String),ListBuffer[(((Long,Long),String),Long,ListBuffer[String],(String,String))]],
                          inCats:List[String],
                          inAlpha:Double,
                          inMu:Double,
                          inSurplusAlpha:Double,
                          inUserGroup:ListBuffer[Long]
                         ): (ListBuffer[Long],Double) ={ //Using maximal cliques plus surplus
    // receive group of users and return a subset that is the best in terms of surplus
    var maxSurplus= findGroupTravelScore(affinity,inUserActsMap,inUserCatsActsMap,inPairActs
      ,inPairCatsActs,inCats,inAlpha,inMu,inUserGroup) - inSurplusAlpha* choose(inUserGroup.size,2).toDouble
    var runGroup=new ListBuffer[Long]()
    runGroup=inUserGroup.clone()
    var i= 0
    while(i<runGroup.size ){
      val u=runGroup(i)
      //println("runGroup is ::"+runGroup)
      //println("user is ::"+u)
      val newInnerGroup=runGroup - u
      // do check the surplus here
      val currentSurplus=findGroupTravelScore(affinity,inUserActsMap,inUserCatsActsMap,inPairActs
        ,inPairCatsActs,inCats,inAlpha,inMu,newInnerGroup) - choose(newInnerGroup.size,2).toDouble
      //println("------current vs. max::"+currentSurplus,maxSurplus)
      if(currentSurplus>maxSurplus){
        //println("group changed !!")
        maxSurplus=currentSurplus
        runGroup=newInnerGroup.clone()
        //println("group changed to !!"+runGroup )
        i = 0
      }
      i += 1
    }
    /*runGroup.foreach{u=>
      println("runGroup is ::"+runGroup)
      println("user is ::"+u)
      val newInnerGroup=runGroup - u
      if(newInnerGroup.contains(3)){
        println("group changed !!")
        runGroup=newInnerGroup.clone()
        println("group changed to !!"+runGroup )
      }

    }*/

    println("group and surplus is ::",runGroup,maxSurplus)
    return (runGroup,maxSurplus)

  }

  /**Global Variables populated by runner function*/
  var userScorePair:Map[Long, Double]=Map()
  var userActsMap:Map[Long,ListBuffer[(Long, Long, ListBuffer[String], (String, String))]]=Map()
  var userCatActMap: Map[(Long,String), ListBuffer[(Long, Long, ListBuffer[String], (String, String))]]=Map()
  var pairActsMap:Map[(Long,Long),ListBuffer[((Long,Long),Long,ListBuffer[String],(String,String))]]=Map()
  var pairCatsActsMap:Map[((Long,Long),String),ListBuffer[(((Long,Long),String),Long,ListBuffer[String],(String,String))]]=Map()
  var lambda:Double=0.0
  var alpha:Double=0.0
  var mu:Double=0.0
  var surplusAlpha:Double=0.0
  var cats:List[String]=List()

  def runner(userActsCatsTSFile:String,
                       groupActsCatsTSFile:String,friendsFile:String,inCat:List[String], inLambda:Double,inAlpha:Double,inMu:Double,inSurplusAlpha:Double)
  : ListBuffer[Long] ={

    val fr = new fileReaderLBSN
    val friendsMap = fr.readFriendsFile(friendsFile).groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._2)))
    val userActsTS=findUserActs(userActsCatsTSFile) // ListBuffer[(user, Loc, List[categories], (ts, te))]
    userActsMap=userActsTS.groupBy(t=> t._1) // group activities by user: List[(user, List[(user, location, List[cat], (startTime,endTime))])]
    //println("user acts map size ::"+userActsMap.size)
    userCatActMap=userActsMapToUserCatActsMap(userActsMap,inCat) //Map[(user,cat), ListBuffer[(user, loc, List[cat], (ts, te))]]
    val catsActsMap=findCatActsMap(userActsTS,inCat) //Map[cat, List[(user, loc, List[cats], (ts, te))]]

    /**user affinities*/
    val users= friendsMap.keys.to[ListBuffer]
    //println("all users are ::"+users.size)
    lambda=inLambda
    cats=inCat
    userScorePair=getUsersAffinity(users,userActsMap,userCatActMap,catsActsMap,cats,lambda)
      .sortBy(t=> -t._2).toMap

    // pair activities
    val pairActs=findPairActs(groupActsCatsTSFile)
    pairActsMap=pairActs.groupBy(t=> t._1)
    val pairCatsActs=findPairActsCats(pairActs)
    pairCatsActsMap=pairCatsActs.groupBy(t=> t._1)
    alpha=inAlpha
    mu=inMu
    surplusAlpha=inSurplusAlpha

/*
    val alpha:Double=inAlpha
    val mu:Double=inMu
    val usersGroup:ListBuffer[Long]=ListBuffer(9683,6181,1718)
    val pairStartTime=System.nanoTime()
    findBestGroupGreedy(userScorePair,userActsMap,userCatActMap,pairActsMap,pairCatsActsMap,inCat,alpha,mu,usersGroup)
    //findGroupTravelScore(userScorePair,userActsMap,userCatActMap,pairActsMap,pairCatsActsMap,inCat,alpha,mu,usersGroup)
    //findPairCohesiveness(userActsMap,userCatActMap,pairActsMap,pairCatsActsMap,inCat,alpha,(9683,6181))
    println("pair time is::"+(System.nanoTime()- pairStartTime))
    //findPairEdgesInGroup(usersGroup)

*/


   /* println("now group")
    val groupActsMap=findGroupActsMap(groupActsCatsTSFile) //Map[List[Users], List[(List[Users], Loc, List[cats], (ts, te))]]
    println("group size::"+groupActsMap.size)
    val group=findBestGreedyGroup(2,userActsMap,userCatActMap,groupActsMap,catsActsMap,friendsMap,inCat)*/




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
    return ListBuffer(1L)//group
  }


}
