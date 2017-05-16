import ConvoysLBSN.ConvoyStatsFinder
import FormatData.fileReaderLBSN
import MaximalCliquesBase.TravelGroupFinder
import TravelGroupRecommendation.GroupFinderGreedy

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, PriorityQueue}

/**
  * Created by aamir on 20/01/17.
  */
class mainFunctionRunner {
/*
  /**Wee places*/
  val weeFriends="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/Wee.txt"

  /***/
  val inputCategoriesFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/categoriesTestData.txt"
  /**Training Data Files*/
  val convoysTrainingFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys/Wee.txt"
  val catCheckins="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/CheckinsWithCats/Wee.txt"
  var convoysCatTrainingFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys/convoysWithCats/Wee.txt"

  val userActivitiesTSWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/UserActivitiesTS_1Hour/Wee_cat.txt"
  val groupActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/GroupActivitiesWithCats/Wee.txt"
  val pairUserPairSeqActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/GroupActivities/PairGroupParSequentialActsWithCats/Wee.txt"
  /**Test data*/
  val convoysCatTestFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/ConvoysWithCat/Wee.txt"
*/

  /*
  /**FourSquare*/
  val FSFriends="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/FS.txt"

  /***/
  val inputCategoriesFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/InputCats/FS.txt"
  /**Training Data Files*/
  val convoysTrainingFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Convoys/FS.txt_first_0.5"
  val catCheckins="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/CheckinsWithCats/FS.txt_first_0.5"
  var convoysCatTrainingFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/FS.txt_first_0.5"

  val userActivitiesTSWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/UserActsTSWithCat/FS.txt_first_0.5"
  val groupActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/GroupActsTSWithCat/FS.txt_first_0.5"
  val pairUserPairSeqActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/PairSeqActsWithCat/FS.txt_first_0.5"
  /**Test data*/
  val convoysCatTestFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/FS.txt_second_0.5"
*/

  /**Gowalla*/
  val GWFriends="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/GW_New.txt_first_0.5"

  /***/
  val inputCategoriesFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/InputCats/GW_New.txt"
  /**Training Data Files*/
  val convoysTrainingFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Convoys/GW_New.txt_first_0.5"
  val catCheckins="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/CheckinsWithCats/GW_New.txt_first_0.5"
  var convoysCatTrainingFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/GW_New.txt_first_0.5"

  val userActivitiesTSWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/UserActsTSWithCat/GW_New.txt_first_0.5"
  val groupActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/GroupActsTSWithCat/GW_New.txt_first_0.5"
  val pairUserPairSeqActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/PairSeqActsWithCat/GW_New.txt_first_0.5"
  /**Test data*/
  val convoysCatTestFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/GW_New.txt_second_0.5"



  def findMatchingCats(cats:ListBuffer[String],filePath:String)
  :ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String],(Long,Long))] ={
    val fr=new fileReaderLBSN
    val convoys=fr.readConvoyCatsFile(filePath)
    val filtConvoys=convoys.filter(t=> cats.forall(t._3.contains))
    return filtConvoys
  }

  def findMatchingUsersGroup(users:ListBuffer[Long],convoyFilePath:String)
  :ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String],(Long,Long))] ={
    val fr=new fileReaderLBSN
    val convoys=fr.readConvoyCatsFile(convoyFilePath)
    val filtConvoys=convoys.filter(t=> users.forall(t._1.contains))
    return filtConvoys

  }

  def readInputCategories(inputCats:String): ListBuffer[ListBuffer[String]] ={
    val inputCatsList= scala.io.Source.fromFile(inputCats).getLines().map(t=> t.split(",").to[ListBuffer]).to[ListBuffer]
    return inputCatsList
  }

  def convoysContainGroup(convoys:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String],(Long,Long))], group:ListBuffer[Long])
  : Int ={
    val convoysWithGroup=convoys.filter(t=> group.forall(t._1.contains))
    return convoysWithGroup.size
  }

  def findAvgGroupSize(predGroup:ListBuffer[(ListBuffer[Long], Double)]): Double ={
    var sumSize=0.0
    predGroup.foreach{g=>
      sumSize += g._1.size
    }
    return sumSize/predGroup.size.toDouble
  }

  def findSuitableInputCats(): Unit ={

    val trainConvoysFile=convoysCatTrainingFile
    val testConvoysFile=convoysCatTestFile
    val fr=new fileReaderLBSN
    val trainConvoys=fr.readConvoyCatsFile(trainConvoysFile)
    val testConvoys=fr.readConvoyCatsFile(testConvoysFile)

    trainConvoys.foreach{tc=>
      testConvoys.foreach{testC=>
        if((tc._1.intersect(testC._1)).size>1){
          if((tc._3.intersect(testC._3)).size>1){
            println("categories are ::"+tc._3.intersect(testC._3))
          }
        }
      }
    }

  }

  def evaluatePredictedGroups: Unit = {

    /** get categories from the sample file for input */
    var catsList = readInputCategories(inputCategoriesFile)
    val tgf = new TravelGroupFinder

    /** choose which factors to incorporate */
    val globalAff = false
    val localAff = false
    val globalCoh = false
    val catCoh = true
    val globalSeqCoh = false
    val catSeqCoh = false

    /***Time and memory consumption***/
      var computationTime=0L
    var starTime:Long=0L
    var endTime:Long=0L

    //var k = 5 //k is the parameter for top-k groups.
    val kList=List(1,3,5)
    kList.foreach{k=>

      println("------------K-------::"+k)

    /** choose values of the parameters */
    val lambda = 0 // affinity global vs local
    var alpha = 0 // cohesiveness global vs local
    val mu = 0 // affinity vs cohesiveness -- should be zero as we are not considering affinity , previously it was 0.5
    val eta = 0 // sequential activities on all locations vs. locations of input categories
    var surplusAlpha: Double = 0.1 //surplus value is inversely proportional to the size of group



    val maxCorrectPredictionParamters: ListBuffer[(Double, Double, Double)] = new ListBuffer[(Double, Double, Double)]()

    //while(alpha<=1){
    //alpha += 0.1
    surplusAlpha = 0l//-0.1
    while (surplusAlpha <= 0.2) {
      surplusAlpha += 0.1
      println("surplusAlpha is ::" + surplusAlpha)
      /** initialize variables */
      //var predictedGroup:(ListBuffer[Long], Double)=null
      var predictedGroupList: ListBuffer[(ListBuffer[Long], Double)] = new ListBuffer[(ListBuffer[Long], Double)]()
      var correctPrediction: Long = 0L // at least one group out to top k triggers
      var correctPredicitonRatio: Double = 0.0
      var catConvoyTestCount = 0
      var avgGroupSize=0.0
      var totalCorrectPred=0
      var totalAvailableGroups=0
      catsList=catsList.map(t=> t.map(it=> it.trim))

      catsList = catsList.take(2)
      catsList.foreach { cats =>
        println("categories are::" + cats)
        starTime=System.currentTimeMillis()
        /** Predict group on inputs */
        predictedGroupList = tgf.runnerTravelGroup(userActivitiesTSWithCat, groupActsWithCatFile, pairUserPairSeqActsWithCatFile, GWFriends,
          cats.toList, lambda, alpha, mu, eta, surplusAlpha, globalAff, localAff, globalCoh, catCoh, globalSeqCoh, catSeqCoh, k)
        endTime=System.currentTimeMillis()
        println("Time for this query in mSec::"+(endTime-starTime))
        computationTime += (endTime-starTime)
        println("predicted Group n Surplus List is ::" + predictedGroupList)
        avgGroupSize=findAvgGroupSize(predictedGroupList)
        println("Average group size is ::"+avgGroupSize)

        //val matchingCatsConvoys=findMatchingCats(cats,convoysCatTestFile)
        var correctPredictionPerList = 0
        var countCorrectMatch: Int = 0
        val convoysWithCatsInTD = findMatchingCats(cats, convoysCatTestFile)
        val totalConvoysWithCatsInTD=convoysWithCatsInTD.map(t=> t._1).distinct
        if(totalConvoysWithCatsInTD.size>k){
          totalAvailableGroups += k
        }else{
          totalAvailableGroups += totalConvoysWithCatsInTD.size
        }

        println("Total Convoys of this category in test dataset are ::"+totalConvoysWithCatsInTD.size,totalConvoysWithCatsInTD)
        if (convoysWithCatsInTD.size > 0) {
          catConvoyTestCount += 1 // any group with these cats exist in test data
        }
        predictedGroupList.foreach { predictedGroup =>
          countCorrectMatch = 0
          if (predictedGroup != null) {
            //countCorrectMatch = convoysContainGroup(matchingCatsConvoys, predictedGroup._1) // for categories
            val matchedGroup = findMatchingUsersGroup(predictedGroup._1, convoysCatTestFile) // users exist
            val matchedGroupsNCats = matchedGroup.filter(t => cats.forall(t._3.contains))
            println("matched Groups are ::" + matchedGroupsNCats)
            //countCorrectMatch = matchedGroup.size // for all cats only concerned about weather group exist or not, without categories of locations they have been to
            countCorrectMatch = matchedGroupsNCats.size // for given input cats
          }
          if (countCorrectMatch > 0) {
            correctPredictionPerList += 1
            println("prediction is true")
          }
        }
        println(" Correct in this top k list are::" + correctPredictionPerList, "", predictedGroupList.size)

        if (correctPredictionPerList > 0) {
          correctPrediction += 1
          println("prediction is true")
        }
        totalCorrectPred+=correctPredictionPerList
        correctPredicitonRatio += (correctPredictionPerList.toDouble / predictedGroupList.size.toDouble)
      }
      println()
      println()
      println()
      println("*****Alpha,SurplusAlpha*****" + alpha, surplusAlpha)
      println("correct Predictions (at least one correct per list) ratio::" + correctPrediction, "/", catsList.size)
      println("correct Predictions ratio in Total ::" + correctPredicitonRatio / catsList.size)
      println("Number of times any convoys exist with the input categories in test Data::" + catConvoyTestCount)
      println("Average time in milliSec is ::"+computationTime.toDouble/catsList.size.toDouble)
      println(" Correct prediction based on availability in Test Data::"+totalCorrectPred.toDouble/totalAvailableGroups.toDouble)
      println()
      println()
      println()
      maxCorrectPredictionParamters += ((alpha, surplusAlpha, correctPrediction.toDouble / catsList.size.toDouble))


    }
    //}

    println("top 10 parameter values are ")
    maxCorrectPredictionParamters.sortBy(t => -t._3).take(10).foreach(println)

  }
  }

  def getNEvaluatePredictedGroupsOnConvoys(): Unit ={
    val fr=new fileReaderLBSN
    val convoysTraining=fr.readConvoyCatsFile(convoysCatTrainingFile)
    val convoysTest=fr.readConvoyCatsFile(convoysCatTestFile)

    //for all categories
    println(" For all categories based on Convoys")
    val predictedGroup=convoysTraining.groupBy(t=>t._1).toList.sortBy(t=> -t._2.size).head._1
    println("prediction group is::"+predictedGroup)
    val timesExistInTest=convoysTest.filter(t=> predictedGroup.forall(t._1.contains)).distinct.size
    if(timesExistInTest>0){
      println("prediction is true")
    }

  }

  def getNEvaluatePredictedGroupsOnConvoysInputCats(): Unit ={
    val fr=new fileReaderLBSN
    val convoysTraining=fr.readConvoyCatsFile(convoysCatTrainingFile)
    val convoysTest=fr.readConvoyCatsFile(convoysCatTestFile)

    val catsList=readInputCategories(inputCategoriesFile)
    //for input categories
    println(" For input categories based on Convoys")
    var correctPredictions=0

    catsList.foreach{cl=>
      println("categories are ::"+cl)
      val filtConvoys=convoysTraining.filter(t=> cl.forall(t._3.contains))
        .groupBy(t=> t._1).toList.sortBy(t=> -t._2.size)
      if(filtConvoys.size>0) {
        val predictGroupCat=filtConvoys.head._1
        println("predicted Group is::"+predictGroupCat)
        val timesAppearInTest = convoysTest
          .filter(t => predictGroupCat.forall(t._1.contains) && cl.forall(t._3.contains)).distinct.size
        if (timesAppearInTest > 0) {
          println("prediction is true")
          correctPredictions += 1
        }
      }

    }
    println("correct predictions are::"+correctPredictions,"/",catsList.size)

  }

  def improveBestCohesiveGroup(): Unit ={
    /**choose which factors to incorporate*/
    val globalAff=false
    val localAff=false
    val globalCoh=false
    val catCoh=false
    val globalSeqCoh=false
    val catSeqCoh=true

    /**choose values of the parameters*/
    val lambda=0.5 // affinity global vs local
    val alpha=1 // cohesiveness global vs local
    val mu=0 // affinity vs cohesiveness -- should be zero as we are not considering affinity , previously it was 0.5
    val eta=0 // sequential activities on all locations vs. locations of input categories
    val surplusAlpha=0.1 //surplus value is inversely proportional to the size of group
    val k=3

    val cats=List("Religious","Nightlife")

    val gfg=new GroupFinderGreedy
    gfg.runner(userActivitiesTSWithCat, groupActsWithCatFile, pairUserPairSeqActsWithCatFile, GWFriends,
      cats, lambda, alpha, mu,eta, surplusAlpha, globalAff, localAff, globalCoh, catCoh,globalSeqCoh,catSeqCoh,k)

  }

  def updateTopKGroups( cList:ListBuffer[(ListBuffer[Long],Double)],element:(ListBuffer[Long],Double),k:Int)
  : ListBuffer[(ListBuffer[Long],Double)] ={
    //var updatedQueue=eQueue.tak

    if(element._2>=cList.last._2){
      cList += element
      return cList.sortBy(t=> -t._2).take(k)
    }else  return cList

  }
  def updateTopKGroups(cList:ListBuffer[(ListBuffer[Long],Double)],newList:ListBuffer[(ListBuffer[Long],Double)])
  : ListBuffer[(ListBuffer[Long],Double)] ={
    if(cList.last._2>newList.head._2){
      cList ++=newList
      return cList.sortBy(t=> -t._2).take(10)
    }
    else return cList
  }

  val pq=mutable.PriorityQueue.empty[(ListBuffer[Long],Double)](
    Ordering.by((_:(ListBuffer[Long],Double))._2)
  )
  def testPriorityQueue(): Unit ={

    /*type T2=Tuple2[ListBuffer[Long],Double]
    val normalOrder=implicitly[Ordering[(ListBuffer[Long],Double)]]
    val pq= new scala.collection.mutable.PriorityQueue[T2](
    )*/

    var list:ListBuffer[(ListBuffer[Long],Double)]=new ListBuffer[(ListBuffer[Long], Double)]()

    list += ((ListBuffer(2L,3L),0.2))

    list += ((ListBuffer(3L,4L),0.3))

    println("list is ::"+list)

    val newElement=(ListBuffer(3L,5L),0.4)

    val returned=updateTopKGroups(list,newElement,2)
    println("returned list:"+returned)

    list=updateTopKGroups(returned,(ListBuffer(3L,5L),0.1),2)

    println("priority list is ::"+list)
    //println("minimum element::"+pq.minBy(t=> t._2))

  }

}
