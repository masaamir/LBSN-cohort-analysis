package DataPreparation

import ConvoysLBSN.{ConvoyAnalysis_fsl_upd, Evaluation, ConvoyStatsFinder, ConvoyAnalysis_Test}
import FormatData.DataFormatter
import MaximalCliquesBase.TravelGroupFinder

import scala.collection.mutable.ListBuffer

/**
  * Created by aamir on 01/02/17.
  */
class DataPreparator {

  /**original formatted data*/
  val dirCheckins="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Checkins/" //original checkins but formatted in required format
  val dirFriends="/home/aamir/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/"
  val dirVenues="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Venues/" //location ids coordinates and categories

  /** Time stamped */
  val dirUserActsTS="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/UserActsTS/"// activities based on time stamp (these are different than original dataset based on snapshot of dataset taken after the given time)
  val dirGroupActsTS="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/GroupActsTS/"
  val dirPairSeqActs="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/PairSeqActs/"
  val dirConvoys="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Convoys/" //convoys created from clustered data with only loc ids

  /**with categories (semantics)*/
  val dirCheckinsWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/CheckinsWithCats/"
  val dirUserActsTSWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/UserActsTSWithCat/"
  val dirGroupActsTSWithCat="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/GroupActsTSWithCat/"
  val dirPairSeqActsWithCats="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/PairSeqActsWithCat/"
  val dirConvoysWithCats="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/"

  /**input Cats*/
  val inputCategoriesFile="/home/aamir/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/categoriesTestData.txt"




  /**split data and create Convoys*/
  def getSplitDataAndConvoys(): Unit ={
    val eval=new Evaluation
    val part=0.25
    //eval.createDatasetForCrossValidation(part,dirCheckins+ds,dirCheckinsTraining+"/"+fileN,dirCheckinsTesting+"/"+fileN)
  }




  /**take convoys (without categories) as input and out pair sequential activities with categories*/


  def getPairSeqDataFromConvoys(): Unit ={
    val dataSetsList=List("GW_New.txt_first_0.5")
    dataSetsList.foreach {ds=>
      println();println();println()
      println("Data set is ::"+ds)
      println();println();println()

      /**split dataset for required portion*/
      val eval=new Evaluation

      val part=0.5
      //eval.createDatasetForCrossValidation(part,dirCheckins+ds,dirCheckins+ds+"_first_"+part,dirCheckins+ds+"_second_"+(1-part))

      /**find convoys*/
      val convoys=new ConvoyAnalysis_fsl_upd
      convoys.getConvoys(dirCheckins+ds,dirFriends+"GW_New.txt",60,dirConvoys+ds)



      /** add categories with check-ins */
      val df = new DataFormatter
      //df.getCheckinsWithCategories(dirCheckins+ds,dirVenues+"WeeFull.txt",dirCheckinsWithCat+ds)

      /** create data-set based on time stamped */
      val con=new ConvoyAnalysis_Test
      //con.findGroupActivities(dirCheckins+ds,60,dirGroupActsTS+ds)// 1 hour
      //con.findUserActivitiesTS(dirCheckins+ds,60,dirUserActsTS+ds)

      /** associate categories with time stamped based data-sets*/
      //df.associateCatWithGroupActs(dirGroupActsTS+ds,dirCheckinsWithCat+ds,dirGroupActsTSWithCat+ds)
      //df.associateCatWithUserActsTS(dirUserActsTS+ds,dirCheckinsWithCat+ds,dirUserActsTSWithCat+ds)

      /**associate conovoys with categories*/
      val csf=new ConvoyStatsFinder
      //df.getConvoysWithCats(dirConvoys+ds,dirVenues+"WeeFull.txt",dirConvoysWithCats+ds)

      /** sequential pair data*/
      //df.getPairUserWSeqActsConvoys(dirConvoys+ds,dirPairSeqActs+ds)
      //df.getConvoysWithCatsPairs(dirCheckinsWithCat+ds,dirPairSeqActs+ds,dirPairSeqActsWithCats+ds)


      /**predict groups*/
        /*

      val tgf = new TravelGroupFinder
      var runTime:Long=0

      val catsList = readInputCategories(inputCategoriesFile)
      //catsList.foreach {cats=>
      for(i<-0 until 10){
        val cats=List("Food","Travel")
        val surplus=i.toDouble/10.toDouble
        println("surplus alpha is ::"+surplus)
        val startTime=System.currentTimeMillis()
        val predictedGroupList = tgf.runnerTravelGroup(dirUserActsTSWithCat + ds, dirGroupActsTSWithCat + ds, dirPairSeqActsWithCats + ds, dirFriends + "WeeFull.txt",
          cats.toList, 0, 0, 0, 0, surplus, false, false, false, false, false, true, 1)
        println("predicted group is ::"+predictedGroupList)
        val endTime=System.currentTimeMillis()
        println("for this time is ::"+(endTime-startTime))
        runTime += (endTime-startTime)
      }
      println("average time is ::"+(runTime.toDouble/10.toDouble))
      */


    }
  }
  def readInputCategories(inputCats:String): ListBuffer[ListBuffer[String]] ={
    val inputCatsList= scala.io.Source.fromFile(inputCats).getLines().map(t=> t.split(",").to[ListBuffer]).to[ListBuffer]
    return inputCatsList
  }

}
