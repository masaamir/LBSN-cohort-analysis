;
//import scala.reflect.io.File

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import Basic.Location
import ConvoysLBSN._
import CoordinateConversion.{Angle, UTMCoord}
import DataStatistics.StatFinder
import FormatData.{DataFilter, DataFormatter, DataSetFormatterNew, fileReaderLBSN}
import GridClustering.GridCluster
import LBSNAnalysis.DataSetFormatter
import LocationBehaviorAnalysis.DeltaTimeFinder
import TaskRunner.TaskExecuter
import VisotorsPrediction.VPDataPreprator

import scala.collection.mutable.ListBuffer

/**
 * Created by XXX on 13-03-2016.
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
    //convoy.getConvoys(filePathCk_FS_Se_CA, filePathFF_FS_Se_CA,30, fileConvoys, "") // in minutes now
    //val venueWee="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"

    //val df=new DataFormatter
    //df.getVenuesWeeFromCheckinFile(venueWee)
    /*val chk_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"
    val ff_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_friends.csv"
    val venues_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Venues\\Wee.txt"
    val idToName_user_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Mapping\\IdToName\\User\\Wee.txt"
    val idToName_loc_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Mapping\\IdToName\\Location\\Wee.txt"
    */
    val jiepangFile="/q/storage/XXX/Dataset/LBSN/New/Jiepang/sample/sample.txt"
    val dsfn=new DataSetFormatterNew
    //dsfn.formatWEEDataSetNew(ff_We,chk_We,venues_Wee,idToName_user_Wee,idToName_loc_Wee)

    //dsfn.formatJiepangData(jiepangFile)




    /**
      * cluster dataset*/
    var te=new TaskExecuter
    //te.clusterDatasets()

    /**
      * find cdf tau
      * */
    te.findCDFTau()


    //test

    /**checkins to checkinswithCats*/
    val checkinsWithCatWeeFull="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/CheckinsWithCats/WeeFull.txt"

    val df2=new DataFormatter
    //df2.getCheckinsWithCategories(dirCheckinsC+"/WeeFull.txt",dirClusteredVenues+"/WeeFull.txt",checkinsWithCatWeeFull)


    /** Clustered Data to Convoys*/
    //println("**Finding Convoys**")
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
    //val convoyFileFS="C:\\Users\\XXX\\Desktop\\Convoys\\FS.txt"
    //val convoyFile="E:\\DataSet\\old\\convoys\\FourSquarewithSemantics\\fileConvoy_FS_Seman_CA_1 hour.txt"
    //val convoyFile="E:\\DataSet\\old\\convoys\\wee\\fileConvoy_Wee_1hour.txt"
    /*
    val weeConvoyTableFile="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/ConvoysTable/Wee.txt"
    val weeFriendsFile="/q/storage/XXX/Dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/Wee.txt"
    val weeConvoys="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/Convoys/Wee.txt"
    val weeVenues="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/Venues/Wee.txt"
    val convoyPattern=new ConvoysPatternAnalysis
    //val weeConvoys=convoyPattern.readFile(convoyFile)
    //val fr=new fileReaderLBSN
    //val Convoys=convoyPattern.readConvoysFile(weeConvoys)
    //val weeFriends=fr.readFriendsFile(weeFriendsFile)
    //convoyPattern.findConvoyStats(Convoys,weeFriendsFile)
    //convoyPattern.getConvoyTable(weeConvoys,weeFriendsFile,weeVenues,weeConvoyTableFile)//filePathFF_FS_Se_CA
    convoyPattern.evaluateCategoryAffect4(weeConvoyTableFile,weeFriendsFile)
    */
    /**Evaluation*/
    /**creation of training and test data-sets*/
    val dirCheckinsTraining="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Checkins/"
    //val dirCheckinsTesting="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Checkins"
    val eval=new Evaluation
    //val fileN="Wee.txt"
    //eval.createDatasetForCrossValidation(0.5,dirCheckinsC+"/"+fileN,dirCheckinsTraining+"/"+fileN,dirCheckinsTesting+"/"+fileN)

    val checkinsWee="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Checkins/Wee.txt"
    //val originalCheckinsWee="/q/storage/XXX/Dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins/Wee.txt"
    val venuesCheckins="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/Venues/WeeFull.txt"
    val checinsWithCatsWee="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/CheckinsWithCats/Wee.txt"

    val df=new DataFormatter
    val cats=List("Nightlife","Bar")
    //println("Function starts")
    //df.getCheckinsWithCategories(checkinsWee,venuesCheckins,checinsWithCatsWee)
    //val newChecks=df.splitCheckinsOnCats(checinsWithCatsWee)
    val catScoreWee="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/temp/catScoreWee.txt"
    //eval.getCatScore(newChecks,cats,catScoreWee)



    /**Evaluate group*/
      val weeConvoyTable="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/ConvoysTable/Wee.txt"
    val weeFriends="/q/storage/XXX/Dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/Wee.txt"

    //val group=eval.findGroupTopK(catScoreWee,2)
    //val group=eval.findGroupTopKFriends(catScoreWee,2,weeFriends)
    //eval.evaluateGroup(group,cats,weeConvoyTable)

    //println("Function finished")


    /**Test data-set */
    /**create convoys*/
      /*val file="Wee.txt"
    val con=new ConvoyAnalysis_fsl_upd
    val dirConvoysCTest="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/Convoys/-"
    con.getConvoys(dirCheckinsTesting+"/"+file,dirFriendsNC+"/"+file,60,dirConvoysCTest+"/"+file)

    */
      /*

    val file="Wee.txt"
    val con=new ConvoyAnalysis_fsl_upd
    val dirConvoysCTrain="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/trainingData/Convoys"
    con.getConvoys(dirCheckinsTraining+"/"+file,dirFriendsNC+"/"+file,60,dirConvoysCTrain+"/"+file)
    */

    /**visitor prediction */
    /**prepare data-set*/
    val vpdp=new VPDataPreprator

    //vpdp.getU2LMatrixData()
    //vpdp.getU2CatMatrixData()
    //vpdp.getU2TimeMatrixData()

    /**parallel convoy finder*/
    val cfp=new ConvoyFinderParallel
    val ds="GW_New.txt_first_0.25"//"GW_New.txt_first_0.5" //GW_New.txt_second_0.5
    val tempCheckins="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/Checkins/"+ds
    val partConvoys="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/PartConvoys/"+ds
    val convoys="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/"+ds
    //cfp.divideActivities(tempCheckins,partConvoys)
    //cfp.readPartConvoys(partConvoys)
    //cfp.mergePartConvoys(partConvoys,convoys)

    //foursquare : FS.txt_first_0.5, FS.txt_second_0.5
    //Gowalla: "GW_New.txt_first_0.5" //GW_New.txt_second_0.5

    val train="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/"+"FS.txt_first_0.5"
    val test="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/"+"FS.txt_second_0.5"
    val inputCat="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/InputCats/"+"FS.txt"




    //cfp.evaluateConvoyBasedTravelerGroup(train,test,inputCat)
    //cfp.evaluateConvoyBasedTravelerGroupAllCats(train,test)

    val Friends="/q/storage/XXX/Dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/FS.txt"
    val CheckinsNC="/q/storage/XXX/Dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins/FS.txt"
    val CheckinsC="/q/storage/XXX/Dataset/LBSN/withSemantics/Clustered/DataSet/Checkins/FS.txt"
    val sf=new StatFinder
    //sf.friendsStats(Friends)
    //sf.checkinStats(CheckinsNC)
    //println(" now clustered ----------------")
    //sf.checkinStats(CheckinsC)



    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)

    val endTime = System.currentTimeMillis()
    println("Time take is ::" + (endTime - startTime) + " milliSeconds")
    println("finished!!!")
  }


}
