package MaximalCliquesBase

import FormatData.fileReaderLBSN
import TravelGroupRecommendation.{GroupFinderGreedy, GroupFinderGreedyOld}

import scala.collection.mutable.ListBuffer

/**
  * Created by maaamir on 12/19/16.
  */
class TravelGroupFinder {

  // find a vertex u with maximum number of neighbours with candidate
  def findMaxU(subg:ListBuffer[Long],cand:ListBuffer[Long], friends:Map[Long,ListBuffer[Long]])
  : (Long, ListBuffer[Long]) ={
    //println("candidates::"+cand)
    val commonUsers=subg.map{t=>
      val uFriends=friends.getOrElse(t,null)
      //val uCommon=uFriends.intersect(cand)
      (t,uFriends.intersect(cand))
    }
    return commonUsers.sortBy(t=> -t._2.size).head
        //.foreach(t=> println(t._1,t._2))
  }

  def choose(n:Long, k:Long): Long ={
    if(k==0) return 1
    return ((n * choose(n-1,k-1))/k)
  }

  def maxPossibleSurplus(inGroup:ListBuffer[Long]): Double ={
    //return (choose(inGroup.size,2) - gfg.surplusAlpha) // changed from this to following
    //return (choose(inGroup.size,2) - gfg.surplusAlpha*choose(inGroup.size,2))
    return (choose(inGroup.size,2)*(1 - gfg.surplusAlpha))

  }

  var currentGroup:ListBuffer[Long]=new ListBuffer[Long]()
  var currentSurplus=Double.NegativeInfinity
  var selectedGroup:(ListBuffer[Long],Double)=null
  var selectedGroupList:ListBuffer[(ListBuffer[Long],Double)]=new ListBuffer[(ListBuffer[Long], Double)]()
  var topK:ListBuffer[(ListBuffer[Long],Double)]=new ListBuffer[(ListBuffer[Long], Double)]()
  //topK ++= ListBuffer(Double.NegativeInfinity,)
  var k:Int=0

  def updateTopKGroups(cList:ListBuffer[(ListBuffer[Long],Double)],newList:ListBuffer[(ListBuffer[Long],Double)],k:Int)
  : ListBuffer[(ListBuffer[Long],Double)] ={
    if(cList.size>0) {
      if (cList.last._2 < newList.head._2) {
        cList ++= newList
        return cList.sortBy(t => -t._2).distinct.take(k)
      }
      else return cList
    }else return newList
  }

  var savingCount=0
  var notSaved=0
  def Expand(SUBG:ListBuffer[Long],CAND:ListBuffer[Long],friends:Map[Long,ListBuffer[Long]],Q:ListBuffer[Long]): Unit ={
    //println("------------------------EXPAND----------------------")
    //println("----------------------------------------------")
    val inSubg=SUBG.clone()
    var inCand=CAND.clone()
    var inQ=Q.clone()
    //println("input",inSubg,inCand)
    if(inSubg.size==0) {
      //println("clique")
      //println("Printing cliques **********")
      //println("Clique is **::"+inQ) //
      // compute travel score
      /**compute Travel Score*/
      val groupNSurplus:ListBuffer[(ListBuffer[Long],Double)]=gfg.findBestGroupGreedy(gfg.userScorePair,gfg.userActsMap,gfg.userCatActMap,
        gfg.pairActsMap,gfg.pairCatsActsMap, gfg.cats,gfg.alpha,gfg.mu,gfg.surplusAlpha,inQ,k)
      //println("existing topk::"+topK)
      //println("new Coming ::"+groupNSurplus)

      topK=updateTopKGroups(topK,groupNSurplus,k)
      //println("updated topk::"+topK)

      /*
      var groupNSurplus:(ListBuffer[Long],Double)=gfg.findBestGroupGreedy(gfg.userScorePair,gfg.userActsMap,gfg.userCatActMap,
        gfg.pairActsMap,gfg.pairCatsActsMap, gfg.cats,gfg.alpha,gfg.mu,gfg.surplusAlpha,inQ)
      /*if(inQ.contains(6802) && inQ.contains(13553)){
        println("required group n Surplus::"+groupNSurplus)
      }*/
      if(groupNSurplus._2==0.0){
        groupNSurplus=(groupNSurplus._1,Double.NegativeInfinity)
      }
      //val lastPair=
      if(groupNSurplus._2> currentSurplus) {
        currentGroup = groupNSurplus._1
        currentSurplus = groupNSurplus._2
        println(" Changed:: group,surplus::",groupNSurplus)
        selectedGroup=groupNSurplus
      }*/
    }
    else {
      // find a vertex u that maximize difference CAND intersection friends of u
      val maxU = findMaxU(inSubg, inCand, friends)
      //println("max user, commonFriends is::" + maxU._1, maxU._2)
      val testList= maxU._2 ++ inQ :+ maxU._1
      /*if (testList.sum <= 24 && false) { //maxU._2
        println("restricted is ::"+inQ)
      }
      else if (true){*/
      while (inCand.filterNot(maxU._2.toSet).size > 0) {
        //println("--------------While-------------------------")
        val commonVertices = inCand.filterNot(maxU._2.toSet)
        //println("inCand,neighbour, subtracted" + inCand, maxU._2, commonVertices)
        //println("while for ::" + commonVertices)
        commonVertices.foreach { q =>
          //println("---------For Each---------------------------")
          //println("vertex::" + q)
          val friendsQ = friends.getOrElse(q, null)
          inQ += q
          //println("friends of q::" + friendsQ)
          //println("subgq,candq::" + inSubg, inCand)
          val subgq = inSubg.intersect(friendsQ)
          val candq = inCand.intersect(friendsQ)
          val potentialGroup=(candq.clone() ++ inQ.clone() :+ q).distinct
          //println("newTest is ::"+newTest)
          /** need to remove true for our algo, it is just for time comparison*/
          var currentMinSurplus=Double.NegativeInfinity
          if(topK.size>0){
            currentMinSurplus=topK.last._2
          }
          if(maxPossibleSurplus(potentialGroup) < currentMinSurplus ){ // replace sum with maximum possible surplus of the group bound here !!!
            //subgq.clear()// new ListBuffer[Long]() // even no need
            savingCount += 1
            if((inQ:+q).distinct.size>1) {
              //println("restricted !!!!! "+(inQ :+q).distinct) /** record restricted pairs here*/
              //inQ.clear()
              //println("NEW subgroup and queue should be empty it is::"+subgq,inQ)
            }
          }
          //println("Q before expanding is::"+inQ)
          else {
            // expand for only those elements which have potential to be clique
            Expand(subgq, candq, friends, inQ)
            notSaved +=1
          }
          //println("removing " + q)
          inCand -= q
          //println("back")
          inQ -= q
        }
      }
    //}
    }

  }

  /*def findTravelGroup(fileFriends:String): Unit ={
    val fr=new FileReader
    val friends=fr.readFriendsFile(fileFriends).groupBy(t=> t._1)
      .map(t=> (t._1.toLong, t._2.map(it=> it._2.toLong).to[ListBuffer]))
    //println("friends ::")
    //friends.foreach(t=> println(t))
    val users= friends.map(t=> t._1).to[ListBuffer]

    //println("all users are::"+users)
    //println("friends size::"+friends)
    val Q:ListBuffer[Long]=new ListBuffer[Long]()
    Expand(users,users,friends,Q) //subg,cand, Q
  }*/
var gfg=new GroupFinderGreedy
  def runnerTravelGroup(userActsCatsTSFile:String,groupActsCatsTSFile:String, ConvoysPairUserPairSeqLoc:String,
                        friendsFile:String,inCat:List[String],inLambda:Double,inAlpha:Double,inMu:Double,inEta:Double,inSurplusAlpha:Double,
                        globalAff:Boolean,localAff:Boolean,globalCoh:Boolean,catCoh:Boolean,globalSeqCoh:Boolean,catSeqCoh:Boolean,inK:Int)
  : ListBuffer[(ListBuffer[Long],Double)] ={
    //val usersGroup:ListBuffer[Long]=ListBuffer(9683,6181,1718)
    //val gfg=new GroupFinderGreedy
    /**initialize values*/
    currentGroup=new ListBuffer[Long]()
    currentSurplus=Double.NegativeInfinity
    selectedGroup=null
    topK= ListBuffer((ListBuffer(0L),Double.NegativeInfinity))//,new ListBuffer[(ListBuffer[Long], Double)]()
    k=inK
    savingCount=0
    notSaved=0


    gfg.runner(userActsCatsTSFile,groupActsCatsTSFile,ConvoysPairUserPairSeqLoc,friendsFile,inCat,inLambda,inAlpha,inMu,inEta,
      inSurplusAlpha,globalAff,localAff,globalCoh,catCoh,globalSeqCoh,catSeqCoh,k)
    /*val group=gfg.findBestGroupGreedy(gfg.userScorePair,gfg.userActsMap,gfg.userCatActMap,gfg.pairActsMap,gfg.pairCatsActsMap,
      gfg.cats,gfg.alpha,gfg.mu,usersGroup)*/

    val fr=new fileReaderLBSN
    val friends=fr.readFriendsFile(friendsFile).groupBy(t=> t._1)
      .map(t=> (t._1.toLong, t._2.map(it=> it._2.toLong).to[ListBuffer]))
    val users= friends.map(t=> t._1).to[ListBuffer]
    val Q:ListBuffer[Long]=new ListBuffer[Long]()

    Expand(users,users,friends,Q)
    println("Saving, notSaved, %saved count is ::"+savingCount,notSaved, (savingCount.toDouble/(savingCount+notSaved)))
    println("Top k selections are ::"+topK)
    return topK//selectedGroup

  }

}
