package TaskRunner

import scala.collection.mutable.ListBuffer
import GridClustering.GridCluster
import LocationBehaviorAnalysis.DeltaTimeFinder

/**
  * Created by aamir on 02/05/17.
  */
class TaskExecuter {

  def clusterDatasets(): Unit ={
    val filesConsidered:ListBuffer[String]=ListBuffer("BrightKite.txt","FourSquare.txt","Gowalla.txt")//WeeFull.txt
    /**Original data to Clustered Data*/
    val dirCheckinsNC="/q/storage/aamir/Dataset/LBSN/Old/collective/NClusteredCheckins/" // checkin directory non-clustered
    val dirCheckinsC="/q/storage/aamir/Dataset/LBSN/Old/collective/ClusteredCheckins/"
    val dirMapGridIdToLocIds="/q/storage/aamir/Dataset/LBSN/Old/collective/Mappings/"//"/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Mappings/GridToLocId"
    val dirNCVenues="/q/storage/aamir/Dataset/LBSN/withSemantics/NonClustered/DataSet/Venues"
    val dirClusteredVenues="/q/storage/aamir/Dataset/LBSN/withSemantics/Clustered/DataSet/Venues"
    val fileName=""
    val gridX=10
    val gridY=10
    val filesCheckins=new java.io.File(dirCheckinsNC).listFiles
        filesCheckins.foreach{t=>
          if(filesConsidered.contains(t.getName)) {
            val gc = new GridCluster
            val fileName = t.getName
            println("File Name--------------::" + t.getName)
            gc.getClusters(dirCheckinsNC + "/" + fileName, gridX, gridY, dirCheckinsC + "/" + fileName, dirMapGridIdToLocIds + "/" + fileName) //(dirCheckinsNC+"\\"+fileName,gridX,gridY,dirCheckinsC+"\\"+fileName)
            //gc.getClusterdVenues(dirMapGridIdToLocIds + "/" + fileName,dirNCVenues+"/"+fileName,dirClusteredVenues+"/"+fileName)

            //convoyPattern.getConvoyTable(weeConvoys,weeFriendsFile,weeVenues,weeConvoyTableFile)
          }
        }
  }

  def findCDFTau(): Unit ={
    /** tau values */
    val dirCheckinsNC="/q/storage/aamir/Dataset/LBSN/Old/collective/NClusteredCheckins/" // checkin directory non-clustered
    val dirCheckinsC="/q/storage/aamir/Dataset/LBSN/Old/collective/ClusteredCheckins/"
    val friendsDir="/q/storage/aamir/Dataset/LBSN/Old/collective/Friends/"
    val cdfTau="/q/storage/aamir/Dataset/LBSN/Old/collective/cdfTau/"

    val filesCheckins=new java.io.File(dirCheckinsC).listFiles
    val filesConsidered=ListBuffer("BrightKite.txt","FourSquare.txt","Gowalla.txt")//WeeFull.txt//"Gowalla.txt","BrightKite.txt",


    filesCheckins.foreach{f=>
      val dtf=new DeltaTimeFinder
      if(filesConsidered.contains(f.getName)){
        val fileName = f.getName
        println("File Name--------------::" + f.getName)
        // for absolute, with all friends and potential visitors
        dtf.computeDeltaUsersUpdated(friendsDir+fileName,dirCheckinsC+fileName,8.0,cdfTau)
      }
    }
  }

}
