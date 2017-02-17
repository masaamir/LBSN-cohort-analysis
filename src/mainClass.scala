//import scala.reflect.io.File

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import DataPreparation.DataPreparator
import MaximalCliquesBase.TravelGroupFinder
import sun.font.TrueTypeFont

import scala.io.Source;

import Basic.Location
import ConvoysLBSN._
import CoordinateConversion.{Angle, UTMCoord}
import DataStatistics.StatFinder
import FormatData.{DataFilter, DataFormatter, DataSetFormatterNew, fileReaderLBSN}
import GridClustering.GridCluster
import LBSNAnalysis.DataSetFormatter
import TravelGroupRecommendation.GroupFinderGreedyOld

import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 13-03-2016.
 */
object mainClass {


  def main(args: Array[String]) = {
    //println("testing")
    val startTime = System.currentTimeMillis()
    /** Convoys */
    val mb = 1024*1024
    val runtime = Runtime.getRuntime


    val fr=new fileReaderLBSN
    //val convoy = new ConvoyAnalysis_fsl_upd
    //val fileConvoys="E:\\DataSet\\old\\LBSNAnalysis\\Convoy\\newConvoys_FS_Semantic.txt"
    //convoy.getConvoys(filePathCk_FS_Se_CA, filePathFF_FS_Se_CA,30, ficileConvoys, "") // in minutes now
    //val venueWee="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"

    //val df=new DataFormatter
    //df.getVenuesWeeFromCheckinFile(venueWee)
    /*val chk_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"
    val ff_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_friends.csv"
    val venues_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Venues\\Wee.txt"
    val idToName_user_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Mapping\\IdToName\\User\\Wee.txt"
    val idToName_loc_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Mapping\\IdToName\\Location\\Wee.txt"
    */
    val jiepangFile="/q/storage/aamir/Dataset/LBSN/New/Jiepang/sample/sample.txt"
    val dsfn=new DataSetFormatterNew
    //dsfn.formatWEEDataSetNew(ff_We,chk_We,venues_Wee,idToName_user_Wee,idToName_loc_Wee)

    //dsfn.formatJiepangData(jiepangFile)

    val filesConsidered:ListBuffer[String]=ListBuffer("Wee.txt")
    /**Original data to Clustered Data*/
    val dirCheckinsNC="/q/storage/aamir/Dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins" // checkin directory non-clustered
    val dirCheckinsC="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Checkins"
    val dirMapGridIdToLocIds="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Mappings/GridToLocId"
    val dirNCVenues="/q/storage/aamir/Dataset/LBSN/withSemantics/NonClustered/DataSet/Venues"
    val dirClusteredVenues="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Venues"
    val fileName=""
    val gridX=10
    val gridY=10
    val filesCheckins=new java.io.File(dirCheckinsNC).listFiles
    /*
    filesCheckins.foreach{t=>
      if(filesConsidered.contains(t.getName)) {
        val gc = new GridCluster
        val fileName = t.getName
        println("File Name--------------::" + t.getName)
        //gc.getClusters(dirCheckinsNC + "/" + fileName, gridX, gridY, dirCheckinsC + "/" + fileName, dirMapGridIdToLocIds + "/" + fileName) //(dirCheckinsNC+"\\"+fileName,gridX,gridY,dirCheckinsC+"\\"+fileName)
        gc.getClusterdVenues(dirMapGridIdToLocIds + "/" + fileName,dirNCVenues+"/"+fileName,dirClusteredVenues+"/"+fileName)
        //convoyPattern.getConvoyTable(weeConvoys,weeFriendsFile,weeVenues,weeConvoyTableFile)
      }
    }
    //test
*/
    /** Clustered Data to Convoys*/
    println("**Finding Convoys**")
    val dirConvoysC="E:\\DataSet\\withSemantics\\Clustered\\Convoys"
    val dirFriendsNC="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Friends"
    /*filesCheckins.take(1).foreach{t=>
      val gc=new GridCluster
      //val fileName=t.getName
      val fileName="SmallFile.txt"
      val convoy = new ConvoyAnalysis_fsl_upd
      //val fileConvoys="E:\\DataSet\\old\\LBSNAnalysis\\Convoy\\newConvoys_FS_Semantic.txt"
      //convoy.getConvoys(filePathCk_FS_Se_CA, filePathFF_FS_Se_CA,30, fileConvoys, "")
      println("File Name--------------::"+t.getName)
      convoy.getConvoys(dirCheckinsC+"\\"+fileName,dirFriendsC+"\\"+fileName,60,dirConvoysC+"\\"+fileName)// time in minutes write file

    }

*/
    //val gc=new GridCluster
    //gc.evaluateClustering(dirMapGridIdToLocIds+"\\"+fileName,dirCheckinsNC+"\\"+fileName)
    /**Filtering check-ins having locations with categories*/
    /*
    val GWC="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Checkins\\GW_New.txt"
    val GWSpots1="E:\\DataSet\\New\\others\\gowalla\\gowalla_spots_subset1.csv"
    val GWSpots2="E:\\DataSet\\New\\others\\gowalla\\gowalla_spots_subset2.csv"
    val dFi=new DataFilter
    dFi.filterGWOnCategorySpots(GWC,GWSpots2 )
    */
    /**Convoys patterns Analysis*/
    //val convoyFileFS="C:\\Users\\MAamir\\Desktop\\Convoys\\FS.txt"
    //val convoyFile="E:\\DataSet\\old\\convoys\\FourSquarewithSemantics\\fileConvoy_FS_Seman_CA_1 hour.txt"
    //val convoyFile="E:\\DataSet\\old\\convoys\\wee\\fileConvoy_Wee_1hour.txt"
    /*
    val weeConvoyTableFile="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/ConvoysTable/Wee.txt"
    val weeFriendsFile="/q/storage/aamir/Dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/Wee.txt"
    val weeConvoys="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/Convoys/Wee.txt"
    val weeVenues="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Venues/Wee.txt"
    val convoyPattern=new ConvoysPatternAnalysis
    //val weeConvoys=convoyPattern.readFile(convoyFile)
    //val fr=new fileReaderLBSN
    //val Convoys=convoyPattern.readConvoysFile(weeConvoys)
    //val weeFriends=fr.readFriendsFile(weeFriendsFile)
    //convoyPattern.findConvoyStats(Convoys,weeFriendsFile)
    //convoyPattern.getConvoyTable(weeConvoys,weeFriendsFile,weeVenues,weeConvoyTableFile)//filePathFF_FS_Se_CA
    convoyPattern.evaluateCategoryAffect4(weeConvoyTableFile,weeFriendsFile)
    */


    val withSemantics="/home/aamir/Study/dataset/LBSN/withSemantics/"

    /**Evaluation*/
    /**creation of training and test data-sets*/
    val dirCheckinsTraining="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Checkins/"
    //val dirCheckinsTesting="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Checkins"
    val eval=new Evaluation
    //val fileN="Wee.txt"
    //eval.createDatasetForCrossValidation(0.5,dirCheckinsC+"/"+fileN,dirCheckinsTraining+"/"+fileN,dirCheckinsTesting+"/"+fileN)

    val checkinsWee="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Checkins/Wee.txt"
    //val originalCheckinsWee="/q/storage/aamir/Dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins/Wee.txt"
    val venuesCheckins="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Venues/Wee.txt"
    val checinsWithCatsWee="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/CheckinsWithCats/Wee.txt"






    val df=new DataFormatter
    var cats=List("Nightlife","Bar","Food")
    println("Function starts")
    //df.getCheckinsWithCategories(checkinsWee,venuesCheckins,checinsWithCatsWee)
    //val newChecks=df.splitCheckinsOnCats(checinsWithCatsWee)
    val catScoreWee=withSemantics+"Clustered/GroupFinder/CrossValidation/temp/catScoreWee.txt"
    //eval.getCatScore(newChecks,cats,catScoreWee)

    /**Evaluate group*/
    val weeConvoyTable=withSemantics+"Clustered/ConvoysTable/Wee.txt"
    val weeFriends=withSemantics+"NonClustered/DataSet/Friends/Wee.txt"
    //val group=eval.findGroupTopK(catScoreWee,2)
    //val group=eval.findGroupTopKFriends(catScoreWee,2,weeFriends)
    //eval.evaluateGroup(group,cats,weeConvoyTable)
    val con=new ConvoyAnalysis_Test//ConvoyAnalysis_fsl_upd
    val dirConvoysCTest="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Convoys/-"
    val checkinFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Checkins/Wee.txt"
    val checkinWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/CheckinsWithCats/Wee.txt"
    val file="Wee.txt"
    val groupActivityFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/GroupActivities/Wee.txt"
    val userActivitiesTS="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/UserActivitiesTS_1Hour/Wee.txt"
    val userActivitiesTSWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/UserActivitiesTS_1Hour/Wee_cat.txt"
    val groupActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/GroupActivitiesWithCats/Wee.txt"
    //con.findGroupActivities(checkinFile,60,groupActivityFile)
    //con.findUserActivitiesTS(checkinFile,60,userActivitiesTS)

    //df.associateCatWithGroupActs(groupActivityFile,checkinWithCatFile,groupActsWithCatFile)
    //df.associateCatWithUserActsTS(userActivitiesTS,checkinWithCatFile,userActivitiesTSWithCat)
    //val gact=
    //val group=eval.findMeasurement(catScoreWee,6,weeFriends)
    //eval.findMeasurement2(userActivitiesTSWithCat,groupActsWithCatFile,weeFriends,cats)

    //val checkins=fr.readCheckinsWithCats(checkinWithCatFile)
      //.map(t=> (t._1,t._6,t._7.split(",").to[ListBuffer],("",""))).to[ListBuffer]
    //val newEval=new evaluation2
    //val group=newEval.findMeasurement2(userActivitiesTSWithCat,groupActsWithCatFile,weeFriends,cats)
    //eval.evaluateGroup(group.toList,cats,weeConvoyTable)
    /**sequential activities from convoys */
    val ConvoysWeeTrain="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys/Wee.txt"
    val pairUserPairSeqActsFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/GroupActivities/PairGroupParSequentialActs/Wee.txt"
    val pairUserPairSeqActsWithCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/GroupActivities/PairGroupParSequentialActsWithCats/Wee.txt"
    //df.getPairUserWSeqActsConvoys(ConvoysWeeTrain,pairUserPairSeqActsFile)
    //df.getConvoysWithCatsPairs(checkinWithCatFile,pairUserPairSeqActsFile,pairUserPairSeqActsWithCatFile)
    /** Activity Graph Formation */
    val agm=new ActivityGraphMaker
    val activityGraphEdges="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/activityGraph/edges.txt"
    //agm.createActivityGraph(groupActsWithCatFile,activityGraphEdges)

    //val chk=scala.io.Source.fromFile("/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Checkins/WeeFull.txt0.25").getLines().size
    //println("size is ::"+chk)

    /**call Function for Wee **/
    val mfr=new mainFunctionRunner
    //mfr.evaluatePredictedGroups // based on formula
    //mfr.findSuitableInputCats
    //mfr.getNEvaluatePredictedGroupsOnConvoys()
    //mfr.getNEvaluatePredictedGroupsOnConvoysInputCats
    //mfr.improveBestCohesiveGroup

    val FSVenues="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Venues/FS.txt"
    val FSVenuesNew="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Venues/FS.txt_New"
    val dfn=new DataSetFormatterNew
    //dfn.formatFSVenues(FSVenues,FSVenuesNew)


    /**format gowalla venues*/
    val venue1="/home/aamir/Study/dataset/LBSN/New/GowallaNew/original/gowalla_spots_subset1.csv"
    val venue2="/home/aamir/Study/dataset/LBSN/New/GowallaNew/original/gowalla_spots_subset2.csv"
    val GWChks="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins/GW_New.txt"
    val venuesFile="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Venues/GW_New.txt"
    //dsfn.formatGWVenues(venue1,venue2,GWChks,venuesFile)



    /**new Data prepration*/
      //val weeTrainFile=""
    val dp=new DataPreparator
    dp.getPairSeqDataFromConvoys()

    /*
    /**new group formation*/
    val gfg=new GroupFinderGreedy

    //gfg.runner(userActivitiesTSWithCat,groupActsWithCatFile,weeFriends,cats,0.5,0.5,0.5)
    val catsFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/categoriesTestData.txt"
    val categories=scala.io.Source.fromFile(catsFile).getLines()

    val tgf=new TravelGroupFinder
    cats=List("Travel", "Train Station", "Train", "Travel")
    cats=List("Food", "Burgers", "Home / Work / Other", "Corporate / Office")

    /**Lets play with convoys*/
    val convoysTraining="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys/Wee.txt"
    val catCheckins="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/CheckinsWithCats/Wee.txt"
    var convoysCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys/convoysWithCats/Wee.txt"

    /** take categories from test data */
    //convoysCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/ConvoysWithCat/Wee.txt"
    val csf=new ConvoyStatsFinder
    println("Game start with Convoys")
    //csf.readConvoysFile(convoysTraining)
    //csf.createConvoysWithCats(catCheckins,convoysTraining,convoysCatFile)
    val k=10
    var listOfCatsList=csf.findPopularGroupLocPair(convoysCatFile,k).map(t=> t._3)
    //val predictedGroup=(ListBuffer(2803L, 10336L),0.1)
    listOfCatsList=ListBuffer(ListBuffer("Nightlife","Travel"))
    /**to run*///
    val globalAff=false
    val localAff=false
    val globalCoh=true
    val catCoh=true
    val globalSeqCoh=false
    val catSeqCoh=false

    val lambda=0.5 // affinity global and local
    val alpha=0.1 // cohesiveness global and local
    val mu=0.5 // affinity and cohesiveness
    val eta=0.0001
    val surplusAlpha=0.09 //surplus value is inversely proportional to the size of group

    var predictedMatched:Int=0
    //val listOfCatsList=ListBuffer(List("Food", "Burgers", "Home / Work / Other", "Corporate / Office"))

    var predictedGroup=(ListBuffer[Long](),0.0)
    listOfCatsList.foreach { l =>
      println("categories are::"+l)
      cats=l.toList
      predictedGroup = tgf.runnerTravelGroup(userActivitiesTSWithCat, groupActsWithCatFile, pairUserPairSeqActsWithCatFile, weeFriends,
        cats, lambda, alpha, mu,eta, surplusAlpha, globalAff, localAff, globalCoh, catCoh,globalSeqCoh,catSeqCoh)
      println("predicted Group n Surplus is ::" + predictedGroup)
      //gfg.findBestGroupGreedy(ListBuffer(2,4,8))
      //println(gfg.choose(5,2))
      /**test data*/
      convoysCatFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/ConvoysWithCat/Wee.txt"

      var matched:Boolean=false
      if(predictedGroup!=null) //if any group is found
      matched=csf.evaluatePredictedGroup(predictedGroup._1, cats, convoysCatFile)
      else println("No predicted group for these categories")

      println("prediction is ::"+matched)
      if(matched){
        predictedMatched += 1
      }
    }
    println(" # times predicted group matched / Total predictions:: "+predictedMatched+ " /"+listOfCatsList.size)
    println("Function finished")


    */
    /**Test data-set */
    /**test data preparation*/
      /*
    val checkinsWeeTest="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Checkins/Wee.txt"
    //val originalCheckinsWee="/q/storage/aamir/Dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins/Wee.txt"
    val venuesCheckinsTest="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Venues/Wee.txt"
    val checinsWithCatsWeeTest="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/CheckinsWithCats/Wee.txt"
    val convoysTest="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Convoys/Wee.txt"
    val convoysWithCatTest="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/ConvoysWithCat/Wee.txt"

    val csf=new ConvoyStatsFinder
    df.getCheckinsWithCategories(checkinsWeeTest,venuesCheckinsTest,checinsWithCatsWeeTest)
    df.getConvoysWithCatsPairs(checinsWithCatsWeeTest,convoysTest,convoysWithCatTest)
    */

    //val checkCats=scala.io.Source.fromFile("/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/CheckinsWithCats/GW_New.txt_first_0.5")
      //.getLines().take(100).toList.foreach(println)





    /**create convoys*/
      /*val file="Wee.txt"
    val con=new ConvoyAnalysis_fsl_upd
    val dirConvoysCTest="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Convoys/-"
    con.getConvoys(dirCheckinsTesting+"/"+file,dirFriendsNC+"/"+file,60,dirConvoysCTest+"/"+file)

    */
      /*

    val file="Wee.txt"
    val con=new ConvoyAnalysis_fsl_upd
    val dirConvoysCTrain="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys"
    con.getConvoys(dirCheckinsTraining+"/"+file,dirFriendsNC+"/"+file,60,dirConvoysCTrain+"/"+file)
    */
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)

    val endTime = System.currentTimeMillis()
    println("Time take is ::" + (endTime - startTime) + "milliSeconds")
    println("finished!!!")
  }


}
