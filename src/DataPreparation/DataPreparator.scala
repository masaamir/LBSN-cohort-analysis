package DataPreparation

import ConvoysLBSN.{ConvoyAnalysis_fsl_upd, Evaluation, ConvoyStatsFinder, ConvoyAnalysis_Test}
import FormatData.DataFormatter
import GridClustering.GridCluster
import MaximalCliquesBase.TravelGroupFinder

import scala.collection.mutable.ListBuffer

/**
  * Created by XXX on 01/02/17.
  */
class DataPreparator {

  /**non Clustered*/
  val dirCheckinsNC="/home/XXX/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Checkins/"
  val dirVenuesNC="/home/XXX/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Venues/"

  /**original formatted data*/
  val dirCheckins="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Checkins/" //original checkins but formatted in required format
  val dirFriends="/home/XXX/Study/dataset/LBSN/withSemantics/NonClustered/DataSet/Friends/"
  val dirVenues="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Venues/" //location ids coordinates and categories
val dirMapGridIdToLocIds="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Mappings/GridToLocId/"

  /** Time stamped */
  val dirUserActsTS="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/UserActsTS/"// activities based on time stamp (these are different than original dataset based on snapshot of dataset taken after the given time)
  val dirGroupActsTS="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/GroupActsTS/"
  val dirPairSeqActs="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/PairSeqActs/"
  val dirConvoys="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/Convoys/" //convoys created from clustered data with only loc ids

  /**with categories (semantics)*/
  val dirCheckinsWithCat="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/CheckinsWithCats/"
  val dirUserActsTSWithCat="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/UserActsTSWithCat/"
  val dirGroupActsTSWithCat="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/GroupActsTSWithCat/"
  val dirPairSeqActsWithCats="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/PairSeqActsWithCat/"
  val dirConvoysWithCats="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/DataSet/ConvoysWithCat/"

  /**input Cats*/
  val inputCategoriesFile="/home/XXX/Study/dataset/LBSN/withSemantics/Clustered/GroupFinder/CrossValidation/testData/categoriesTestData.txt"




  /**split data and create Convoys*/
  def getSplitDataAndConvoys(): Unit ={
    val eval=new Evaluation
    val part=0.25
    //eval.createDatasetForCrossValidation(part,dirCheckins+ds,dirCheckinsTraining+"/"+fileN,dirCheckinsTesting+"/"+fileN)
  }




  /**take convoys (without categories) as input and out pair sequential activities with categories*/


  def getPairSeqDataFromConvoys(): Unit ={
    val dataSetsList=List("GW_New.txt_second_0.5")//"GW_New.txt_first_0.5","GW_New.txt_second_0.5", "GW_New.txt"
    dataSetsList.foreach {ds=>
      println();println();println()
      println("Data set is ::"+ds)
      println();println();println()

      /**cluster venues*/
      /**get clustered venues*/
      val gc= new GridCluster
      val gridX=10
      val gridY=10
      //gc.getClusters(dirCheckinsNC + ds, gridX, gridY, dirCheckins + ds, dirMapGridIdToLocIds + ds) //(dirCheckinsNC+"\\"+fileName,gridX,gridY,dirCheckinsC+"\\"+fileName)
      //gc.getClusterdVenues(dirMapGridIdToLocIds + ds,dirVenuesNC+ds,dirVenues+ds)



      /**split dataset for required portion*/
      val eval=new Evaluation

      val part=0.25
      //eval.createDatasetForCrossValidation(part,dirCheckins+ds,dirCheckins+ds+"_first_"+part,dirCheckins+ds+"_second_"+(1-part))



      /**find convoys*/
      val convoys=new ConvoyAnalysis_fsl_upd
      //convoys.getConvoys(dirCheckins+ds,dirFriends+"FS.txt",60,dirConvoys+ds)



      /** add categories with check-ins */
      val df = new DataFormatter
        //df.getCheckinsWithCategories(dirCheckins+ds,dirVenues+"GW_New.txt",dirCheckinsWithCat+ds)

      /** create data-set based on time stamped */
      val con=new ConvoyAnalysis_Test
      //con.findGroupActivities(dirCheckins+ds,60,dirGroupActsTS+ds)// 1 hour
      //con.findUserActivitiesTS(dirCheckins+ds,60,dirUserActsTS+ds)

      /** associate categories with time stamped based data-sets*/
      //df.associateCatWithGroupActs(dirGroupActsTS+ds,dirCheckinsWithCat+ds,dirGroupActsTSWithCat+ds)
      //df.associateCatWithUserActsTS(dirUserActsTS+ds,dirCheckinsWithCat+ds,dirUserActsTSWithCat+ds)

      /**associate conovoys with categories*/
      val csf=new ConvoyStatsFinder
      //df.getConvoysWithCats(dirConvoys+ds,dirVenues+"GW_New.txt",dirConvoysWithCats+ds)

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
        val cats=List("Food","Shop & Service")
        val surplus=i.toDouble/10.toDouble
        println("surplus alpha is ::"+surplus)
        val startTime=System.currentTimeMillis()
        val predictedGroupList = tgf.runnerTravelGroup(dirUserActsTSWithCat + ds, dirGroupActsTSWithCat + ds, dirPairSeqActsWithCats + ds, dirFriends + "FS.txt",
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
