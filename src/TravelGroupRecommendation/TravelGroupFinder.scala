package MaximalCliquesBase

import FormatData.fileReaderLBSN
import TravelGroupRecommendation.GroupFinderGreedy

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
    return (choose(inGroup.size,2) - gfg.surplusAlpha)

  }

  var currentGroup:ListBuffer[Long]=new ListBuffer[Long]()
  var currentSurplus=Double.NegativeInfinity
  var selectedGroup:(ListBuffer[Long],Double)=null

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

      var groupNSurplus:(ListBuffer[Long],Double)=gfg.findBestGroupGreedy(gfg.userScorePair,gfg.userActsMap,gfg.userCatActMap,
        gfg.pairActsMap,gfg.pairCatsActsMap,
        gfg.cats,gfg.alpha,gfg.mu,gfg.surplusAlpha,inQ)
      /*if(inQ.contains(6802) && inQ.contains(13553)){
        println("required group n Surplus::"+groupNSurplus)
      }*/
      if(groupNSurplus._2==0.0){
        groupNSurplus=(groupNSurplus._1,Double.NegativeInfinity)
      }
      if(groupNSurplus._2> currentSurplus) {
        currentGroup = groupNSurplus._1
        currentSurplus = groupNSurplus._2
        //println(" Changed:: group,surplus::",groupNSurplus)
        selectedGroup=groupNSurplus
      }
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
          if(maxPossibleSurplus(potentialGroup) < currentSurplus){ // replace sum with maximum possible surplus of the group bound here !!!
            //subgq.clear()// new ListBuffer[Long]() // even no need
            if((inQ:+q).distinct.size>1) {
              //println("restricted !!!!! "+(inQ :+q).distinct) /** record restricted pairs here*/
              //inQ.clear()
              //println("NEW subgroup and queue should be empty it is::"+subgq,inQ)
            }
          }
          //println("Q before expanding is::"+inQ)
          else // expand for only those elements which have potential to be clique
          Expand(subgq, candq, friends, inQ)
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
                        globalAff:Boolean,localAff:Boolean,globalCoh:Boolean,catCoh:Boolean,globalSeqCoh:Boolean,catSeqCoh:Boolean)
  : (ListBuffer[Long],Double) ={
    //val usersGroup:ListBuffer[Long]=ListBuffer(9683,6181,1718)
    //val gfg=new GroupFinderGreedy
    /**initialize values*/
    currentGroup=new ListBuffer[Long]()
    currentSurplus=Double.NegativeInfinity
    selectedGroup=null


    gfg.runner(userActsCatsTSFile,groupActsCatsTSFile,ConvoysPairUserPairSeqLoc,friendsFile,inCat,inLambda,inAlpha,inMu,inEta,
      inSurplusAlpha,globalAff,localAff,globalCoh,catCoh,globalSeqCoh,catSeqCoh)
    /*val group=gfg.findBestGroupGreedy(gfg.userScorePair,gfg.userActsMap,gfg.userCatActMap,gfg.pairActsMap,gfg.pairCatsActsMap,
      gfg.cats,gfg.alpha,gfg.mu,usersGroup)*/

    val fr=new fileReaderLBSN
    val friends=fr.readFriendsFile(friendsFile).groupBy(t=> t._1)
      .map(t=> (t._1.toLong, t._2.map(it=> it._2.toLong).to[ListBuffer]))
    val users= friends.map(t=> t._1).to[ListBuffer]
    val Q:ListBuffer[Long]=new ListBuffer[Long]()

    Expand(users,users,friends,Q)
    return selectedGroup

  }

}
