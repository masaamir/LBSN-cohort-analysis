;
//import scala.reflect.io.File

import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}

import Basic.Location
import ConvoysLBSN._
import CoordinateConversion.{Angle, UTMCoord}
import DataStatistics.StatFinder
import FormatData.{DataFilter, DataFormatter, fileReaderLBSN, DataSetFormatterNew}
import GridClustering.GridCluster
import LBSNAnalysis.DataSetFormatter

/**
 * Created by MAamir on 13-03-2016.
 */
object mainClass {




  def main(args: Array[String]) = {
    //println("testing")
    /** Original Files */
    val filePathSmall = "E:\\DataSet\\old\\Brightkite\\ModifiedSmall\\ModifiedSmallPartBrightkite_totalCheckinsModified.txt"
    val friendsfilePathSmall = "E:\\DataSet\\old\\Brightkite\\ModifiedSmall\\ModifiedSmallPartBrightkite_edges.txt"
    val filePath = "E:\\DataSet\\old\\Brightkite\\modified\\Brightkite_totalCheckins.txt"
    val filePathff = "E:\\DataSet\\old\\FourSquare\\Modified\\FourSquare_totalCheckins.txt"
    val friendsPath = "E:\\DataSet\\old\\Brightkite\\modified\\Brightkite_edges.txt"
    val friendsPathff = "E:\\DataSet\\old\\FourSquare\\Modified\\FoursquareFriendship.txt"
    val filePathGW = "E:\\DataSet\\old\\Gowalla\\Modified\\Gowalla_totalCheckins.txt"
    val friendsPathGW = "E:\\DataSet\\old\\Gowalla\\Modified\\loc-gowalla_edges\\Gowalla_edges.txt"
    //new foursquare dataset with semantics of CA
    val filePathCk_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\Modified\\checkIns.txt"
    val filePathFF_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\Modified\\friends.txt"
    val filePathVenues_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\Modified\\venues.txt"

    val startTime = System.currentTimeMillis()

    /*
        /** Order the data set in descending order of check-in time for each user */
        val fileOrderedPath="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\Ordered\\Brightkite_totalCheckins.txt"
        orderDataSet(filePath,fileOrderedPath)

        val writePath="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\BrightKite.txt"
        val writeSmall="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\BrightKiteSmall.txt"
        readFile(fileOrderedPath,writePath)
    */
    /*
        /** LBSN Analysis - Number of followers for each user*/

        val filePathFollowers="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\Followers\\Brightkite.txt"

        val fileNonFollowersProbability="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\NonFollowers\\Brightkite.txt"

        val fip=new FriendsInfluenceProbabilities()

        //fip.getFriends(friendsPath)
        val fipPath="E:\\DataSet\\old\\LBSNAnalysis\\BrightKite\\Probabilities\\Fip\\Brightkite_fip.txt"
        val moreSmallFile="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\Brightkite\\ModifiedSmall\\MoreSmall\\moreSmall.txt"
        //fip.formatDataSet(filePath,fipPath,friendsPath)
        val nfp=new NonFollowersPribability()
        val nfpPath="E:\\DataSet\\old\\LBSNAnalysis\\BrightKite\\Probabilities\\PNF\\Brightkite_pnf.txt"
        //nfp.formatDataSetNonFollower(filePathff,nfpPath,friendsPathff)
        val pff=new ProbabilityToFollowFriends()
        val pffPath="E:\\DataSet\\old\\LBSNAnalysis\\BrightKite\\Probabilities\\PFF\\Brightkite_pff.txt"
        //pff.formatDataSetNonFollower(filePathff,pffPath,friendsPathff)
        //val list:List[(Long,Date)]=List((3L,new Date(1L)),(2L,new Date(2L)),(1L,new Date(4L)),(6L,new Date(5L)),(7L,new Date(6L)),(8L,new Date(8L)),(1L,new Date(10L))) //(u,t)
        //pff.getMaxIntervalList(list,1)

        //Brightkite
        val usersFile="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\CheckInAnalysis\\LocationsPerUser.txt"
        val locationsFile="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\BrightKite\\CheckInAnalysis\\UsersPerLocation.txt"
        //FourSquare
        //val mainFile="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\Others\\FourSquare\\Modified\\FourSquare_totalCheckins.txt"
        //val usersFileff="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\FourSquare\\LBSNAnalysis\\CheckinAnalysis\\LocationsPerUser_fq.txt"
        //val locationsFileff="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\FourSquare\\LBSNAnalysis\\CheckinAnalysis\\UsersPerLocation_fq.txt"

        val ck=new CheckinsAnalysis()
        //ck.getCheckInAnalysis(filePath,usersFile,locationsFile)


        /** Visitors co-relation**/
        val vc=new visitorsCorrelation()
        //vc.computeCorrelation(friendsfilePathSmall,filePathSmall)


        /** Filter Data*/
        val spatialGraphEdges="D:\\Data_BigMachine\\aamirTest\\DataSet\\Others\\FourSquare\\Modified\\fourSquare_spatial_2FCV"
        val fd=new FilterData()
        //fd.countLocations(filePathff)
        //fd.countSpatialGraphEdges(spatialGraphEdges)
        //fd.filterLocations(filePath)
        //fd.filterLocations(filePath)
        //fd.filterUsers(filePath)
        val writeFile="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\VisitCorrelation\\visitCorrelation.txt"
        val writeFileFourSquare="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\VisitCorrelation\\visitCorrelation_Fq.txt"
        /*val minUserCk:Long=1//100
        val maxUserCk:Long=10000000//50000 //500
        val minUserLocs:Long=1//30
        val minLocCk:Long=1//50
        val maxLocCk:Long=10000000//50000 //500
        val minLocVisitors:Long=1*/
        //val minVLocSize:Long=100 // for user
        //val minVisitorsSize:Long=100 // for location
        //fd.filterLBSNData()
        //fd.findCorrelation(friendsPath,filePath,minUserCk,maxUserCk,minUserLocs,minLocCk,maxLocCk,minLocVisitors,writeFileFourSquare)


        /**FIP*/
        //friendsPathff,filePathff
        //fip.formatDataSet(filePath,fipPath,friendsPath)
        //fip.formatDataSet(friendsPathff,filePathff,fipPath,minUserCk,maxUserCk,minUserLocs,minLocCk,maxLocCk,minLocVisitors)
        //pff.computeProbabilityToFollowFriends(friendsfilePathSmall,filePathSmall,pffPath,minUserCk,maxUserCk,minUserLocs,minLocCk,maxLocCk,minLocVisitors)
        //nfp.formatDataSetNonFollower(friendsPathff,filePathff,nfpPath,minUserCk,maxUserCk,minUserLocs,minLocCk,maxLocCk,minLocVisitors)

        /**Convey Analysis*/
        val ca=new ConveyAnalysis_org()
        var l:ListBuffer[Long]=new ListBuffer()
        val writeFileConvoyFF="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\Convoy\\Convoy_FS.txt"
        val writeFileConvoyBK="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\Convoy\\Convoy_BK.txt"
        val writeFileConvoyFFValues="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\Convoy\\Convoy_FS_Values.txt"
        //l=l+=1L
        val deltaTime=3L
        /**Find analysis values*/
        //small file
        //val convoyFileFF="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\Convoy\\Convoy_FS.txt"
        //val convoyValuesFile="C:\\Users\\MAamir\\Desktop\\Scalable Processing\\DataSet\\LBSNAnalysis\\Convoy\\Convoy_FS_Values.txt"

        //fourSquare
        val convoyFileFF="C:\\Users\\MAamir\\Desktop\\Convoy\\Convoy_GW_CC.txt"
        val convoyValuesFile="C:\\Users\\MAamir\\Desktop\\Convoy\\Convoy_GW_CC_Values.txt"

        val avc=new AnalysisValuesComputer()
        //avc.getAnalysisValues(convoyFileFF,convoyValuesFile)
        val dsa=new DataSetAnalaysis()

        /**connected Components*/
        //val cc=new ConnectedComponentFinder()
        //cc.getConnectedComponents()

        /**New Data sets*/
        //Four Square
        val venues_FS="E:\\DataSet\\New\\FourSquare\\umn_foursquare_datasets\\venues.dat"
        val dwnldCheckinFileFSNew="E:\\DataSet\\New\\FourSquare\\umn_foursquare_datasets\\checkins.dat"
        val dwnldFriendsFileFSNew="E:\\DataSet\\New\\FourSquare\\umn_foursquare_datasets\\socialgraph.dat"
        val origCheckinFileFSNew="E:\\DataSet\\New\\FourSquare\\Modified\\checkIns.txt"
        val origFriendFileFSNew="E:\\DataSet\\New\\FourSquare\\Modified\\friends.txt"
        //val df=new DataSetFormatter
        //df.formatFSDataSet(venues_FS,dwnldFriendsFileFSNew,dwnldCheckinFileFSNew,origFriendFileFSNew,origCheckinFileFSNew)

        //WEE
        val chk_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"
        val ff_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_friends.csv"
        val chk_We_Write="E:\\DataSet\\New\\others\\weeplaces\\Modified\\checkIns.txt"
        val ff_We_Write="E:\\DataSet\\New\\others\\weeplaces\\Modified\\friends.txt"
        //val df=new DataSetFormatter
        //df.formatWEEDataSet(ff_We,chk_We,ff_We_Write,chk_We_Write)
        //df.getUniqueLocIdWithCrds(filePathGW)


        //GW
        val dwnldCheckinFileGWNew="E:\\DataSet\\New\\others\\gowalla\\gowalla_checkins.csv"
        val dwnldFriendsFileGWNew="E:\\DataSet\\New\\others\\gowalla\\gowalla_friendship.csv"
        val origCheckinFileGWNew="E:\\DataSet\\New\\others\\gowalla\\Modified\\checkIns.txt"
        val origFriendFileGWNew="E:\\DataSet\\New\\others\\gowalla\\Modified\\friends.txt"
        val df=new DataSetFormatter
        val venues1="E:\\DataSet\\New\\others\\gowalla\\gowalla_spots_subset1.csv"
        val venues2="E:\\DataSet\\New\\others\\gowalla\\gowalla_spots_subset2.csv"
        //df.formatGWDataSet(venues1,venues2,dwnldFriendsFileGWNew,dwnldCheckinFileGWNew,origFriendFileGWNew,origCheckinFileGWNew)


//FourSquare with Semantics for CA
    val ff_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\CA Dataset\\fs_friendship_CA.txt"
    val chk_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\CA Dataset\\checkin_CA_venues.txt"
    //val filePathCk_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\Modified\\checkIns.txt"
    //val filePathFF_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\Modified\\friends.txt"
    //val filePathVenues_FS_Se_CA="E:\\DataSet\\New\\FourSquare\\withSemantics\\Modified\\venues.txt"
    val df=new DataSetFormatterNew
    df.formatFSSemanticsCA(ff_FS_Se_CA,chk_FS_Se_CA,filePathFF_FS_Se_CA,filePathCk_FS_Se_CA,filePathVenues_FS_Se_CA)



        //df.analyzeData(part1,part2)
        val filePath1="E:\\DataSet\\New\\FourSquare\\umn_foursquare_datasets\\venues.dat"
        //findLocationIds
        //df.findLocationIds(filePath1)
        //df.DBSCAN()
        val smallFile="E:\\DataSet\\old\\UTM\\files\\BKSmall.csv"
        val fourSquareFile="E:\\DataSet\\old\\UTM\\files\\FS1.csv"
        //df.clusterLocations(filePathff,fourSquareFile)
        val clusterIdsFile="E:\\DataSet\\old\\Brightkite\\ClusterIds\\clusters"
        //df.clusterLocations(filePathSmall,clusterIdsFile)
        //val radians=(90).toRadians
        //println("degree to radians::"+radians)
        //println("tst::"+Math.cos((radians)))
        val dbscanm=new DBSCANMath
        //println("New one !!")
        //dbscanm.getClusters(filePathff,smallFile,clusterIdsFile)
        //dbscanm.getClusters(filePathSmall,clusterIdsFile)

        /**Convert GPS to XYZ coordinates system (simple considering earth as sphere)*/
        val xyzFS="E:\\DataSet\\old\\UTM\\XYZCoordinateSystem\\Coords_XYZ_FS.txt"
        val xyzSmall="E:\\DataSet\\old\\UTM\\XYZCoordinateSystem\\Coords_XYZ_BKSmall.txt"
        //df.convertGPSToXY(filePathff,xyzFS)



        /**IGNORE PREVIOUS CONTENTS**/

        /**Cluster XYZ using Indexed DBSCAN*/
        //val idbscan=new DBSCANNlogN()


        /**Format Data set*/
        val clusterFileGW="E:\\DataSet\\old\\Clusters\\Radius10Meters\\GW_clusters_10.txt"
        val clusterFileFS="E:\\DataSet\\old\\Clusters\\Radius10Meters\\FS_clusters_10.txt"
        val clusterFileBK="E:\\DataSet\\old\\Clusters\\Radius10Meters\\BK_clusters_10.txt"

        /***/
        //val updatedFriendsFileFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\LBSNData\\friends.txt"
        //val updatedCheckinsFileFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\LBSNData\\checkins.txt"
        //val updatedFriendsFileBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\LBSNData\\friends.txt"
        //val updatedCheckinsFileBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\LBSNData\\checkins.txt"
        val updatedFriendsFileGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\LBSNData\\friends.txt"
        val updatedCheckinsFileGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\LBSNData\\checkins.txt"
    /*

          /**Filter data on city*/
          //manhattan//washington//brooklyn//pittsurgh//New jersey//manhattan// new york data
          val minLat= 40.680396//38.803150//40.551042//40.361369//40.496044//40.666882//40.680396 //40.496044 // south latitude www.mapdevelopers.com/geocode_bounding_box.php
          val maxLAt= 40.882214//38.995548//40.739446//40.501368//40.915256//40.768867//40.882214 //40.915256 //north latitude
          val maxLong= -73.907//-76.909393//-73.833365//-79.865723//-73.700272//-74.026614//-73.907000 //-73.700272 //east longitude
          val minLong=  -74.047285//-77.119740//-74.056630//-80.095278//-74.255735//-74.109499 //-74.047285//-74.255735 //west longitude
          val updatedFriendsFileCityFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\cityData\\new\\friends.txt"
          val updatedCheckinsFileCityFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\cityData\\new\\checkins.txt"
          //val updatedFriendsFileCityBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\cityData\\new\\friends.txt"
          //val updatedCheckinsFileCityBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\cityData\\new\\checkins.txt"
          //val updatedFriendsFileCityGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\cityData\\new\\friends.txt"
          //val updatedCheckinsFileCityGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\cityData\\new\\checkins.txt"

          //val dataset="GW"

          val cdf=new CityDataFilter
          cdf.filterDataOnCity(updatedFriendsFileFS,updatedCheckinsFileFS,
              minLat,maxLAt,minLong,maxLong,updatedFriendsFileCityFS,updatedCheckinsFileCityFS)

        */

        /**stat finder*/
          val sf=new StatFinder
        //sf.analyseData(updatedFriendsFileCityGW,updatedCheckinsFileCityGW)
        //sf.analyseGPSPoints(filePathff)
        val df1=new DataFormatter
        //fourSquare
        //df1.modifyLBSNDataWithClusteredLocationsIds(friendsPathff,filePathff,clusterFileFS,updatedFriendsFileFS,updatedCheckinsFileFS)
        //BrightKite
        //df1.modifyLBSNDataWithClusteredLocationsIds(friendsPath,filePath,clusterFileBK,updatedFriendsFileBK,updatedCheckinsFileBK)
        //Gowalla
        //df1.modifyLBSNDataWithClusteredLocationsIds(friendsPathGW,filePathGW,clusterFileGW,updatedFriendsFileGW,updatedCheckinsFileGW)



        //sf.analyseData(updatedFriendsFileFS,updatedCheckinsFileFS)
        //sf.analyseClusters(filePath,clusterFileBK)
        /**FilterData*/
        val minUserCk:Long=20//20//100
        //val maxUserCk:Long=10000000//50000 //500
        val minUserLocs:Long=0//10//30
        //val minLocCk:Long=1//50
        //val maxLocCk:Long=10000000//50000 //500
        //val minLocVisitors:Long=1
        val dff=new DataFilter
          val fr= new fileReaderLBSN

        //
        val filteredData=dff.filterUsers(updatedFriendsFileGW,updatedCheckinsFileGW,minUserCk,minUserLocs)
        /*
          //println("New")
          //sf.analyseDataWithData(filteredData._1,filteredData._2)
        //println("Filtering done")
        /**Probabilities to followFriends*/
        val pffFileFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\pff\\pff"
        val ptbfFileFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\ptbf\\ptbf"
        val pffFileBK="E:\\DataSet\\old\\BrightKite\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\pff\\pff"
        val ptbfFileBK="E:\\DataSet\\old\\BrightKite\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\ptbf\\ptbf"
        val pffFileGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\pff\\pff"
        val ptbfFileGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\ptbf\\ptbf"
        val pfff=new ProbabilityToFollowFriendsFinder
        //pfff.computeProbabilityToFollowFriends(filteredData._1,filteredData._2,pffFileFS+"_"+minUserCk+"_"+minUserLocs)
        val ptbf=new ProbabilityToBeFollowedByFriendsFinder
        //ptbf.computeProbabilityToBeFollowed(filteredData._1,filteredData._2,ptbfFileGW+"_"+minUserCk+"_"+minUserLocs)

          */
          /**Correlation*/
          // for minUserCk=20 and minUserLocs=0
        //val cf=new correlationFinder
        //val corrFileFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\correlation\\city\\new\\correlation.txt"
        //val corrFileBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\correlation\\city\\new\\correlation.txt"
        //val corrFileGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\FriendsBehaviorAnalysis\\correlation\\city\\new\\correlation.txt"
          //val filteredData=dff.filterUsers(updatedFriendsFileCityGW,updatedCheckinsFileCityGW,minUserCk,minUserLocs)
          //cf.findCorrelationFNF(filteredData._1,filteredData._2,corrFileFS)//(fr.readFriendsFile(updatedFriendsFileCityFS),fr.readCheckinFile(updatedCheckinsFileCityFS),corrFileFS)//
        //sf.findTopKLocations(updatedCheckinsFileGW,100)






        /** delta Time and delta Distance*/
        /*
        val dtf=new DeltaTimeFinder
        //val deltaTDFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\LocationBehaviorAnalysis\\deltaTRaw\\delta" //previous deltaTD
        //val deltaTDBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\LocationBehaviorAnalysis\\deltaTRaw\\delta"
        val deltaTDGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\LocationBehaviorAnalysis\\deltaTRaw\\delta"
        //dtf.computeDeltaTnDeltaD(filteredData._2,deltaTDBK)
        val t:Double=965*24 // days * 24 = hours
        //dtf.computeDeltaTRaw(filteredData._2,t,deltaTDGW+"_T")
        dtf.computeDeltaUsersUpdated(filteredData._1,filteredData._2,t,deltaTDGW+"_Users")
        */

    /*

        /*val timeThresholdList:List[Double]=List(1,7,30,180,365)//in days
        timeThresholdList.foreach {t=>
          dtf.computeDeltaTnDeltaD(filteredData._2, t, deltaTDGW)
        }*/
          val deltaUsersFS="E:\\DataSet\\old\\FourSquare\\ModifiedWithClustersIds\\LocationBehaviorAnalysis\\deltaUsers"
          val deltaUsersBK="E:\\DataSet\\old\\BrightKite\\ModifiedWithClustersIds\\LocationBehaviorAnalysis\\deltaUsers"
          val timeThreshold:List[Double]=List(23160)//in hours
          /*timeThreshold.foreach { t =>

              dtf.computeDeltaUsers(filteredData._2, t, deltaUsersBK)
          }*/


        /**Generate Synthetic Data*/
        val sdg=new syntheticDataGenerator
        val syntheticDataBK="E:\\DataSet\\old\\Brightkite\\ModifiedWithClustersIds\\Synthetic\\checkins"
        val syntheticDataGW="E:\\DataSet\\old\\Gowalla\\ModifiedWithClustersIds\\synthetic\\checkins"
        val times=6
        //sdg.createSyntheticData(updatedCheckinsFileGW,times,syntheticDataGW+"_"+times+".txt")

        val keysBK="E:\\DataSet\\old\\Results\\Plotting\\BrightKite\\BrightKite_s5.keys"
        val spreadBK="E:\\DataSet\\old\\Results\\Plotting\\BrightKite\\BrightKite_s5.spread"
        val naiveKeysBK="E:\\DataSet\\old\\Results\\Plotting\\BrightKite\\BrightKite_naive5.keys"
        val naiveSpreadBK="E:\\DataSet\\old\\Results\\Plotting\\BrightKite\\BrightKite_naive5.spread"

        val keysGW="E:\\DataSet\\old\\Results\\Plotting\\Gowalla\\Gowalla_s5.keys"
        val spreadGW="E:\\DataSet\\old\\Results\\Plotting\\Gowalla\\Gowalla_s5.spread"
        val naiveKeysGW="E:\\DataSet\\old\\Results\\Plotting\\Gowalla\\Gowalla_naive5.keys"
        val naiveSpreadGW="E:\\DataSet\\old\\Results\\Plotting\\Gowalla\\Gowalla_naive5.spread"

        val keysFS="E:\\DataSet\\old\\Results\\Plotting\\FourSquare\\foursquare_s5.keys"
        val spreadFS="E:\\DataSet\\old\\Results\\Plotting\\FourSquare\\foursquare_s5.spread"
        val naiveKeysFS="E:\\DataSet\\old\\Results\\Plotting\\FourSquare\\foursquare_naive5.keys"
        val naiveSpreadFS="E:\\DataSet\\old\\Results\\Plotting\\FourSquare\\foursquare_naive5.spread"

        val naiveGSVPlotsBK="E:\\DataSet\\old\\Results\\Plotting\\GSV\\BrightKite\\naive.txt"
        val ourGSVPlotsBK="E:\\DataSet\\old\\Results\\Plotting\\GSV\\BrightKite\\our.txt"
        val naiveGSVPlotsFS="E:\\DataSet\\old\\Results\\Plotting\\GSV\\FourSquare\\naive.txt"
        val ourGSVPlotsFS="E:\\DataSet\\old\\Results\\Plotting\\GSV\\FourSquare\\our.txt"


    */
        /**Plotting*/
        //sf.averageFriends(updatedFriendsFileGW)
    /*
        val dataform=new DataFormatter
        dataform.returnCoordinatesByLocationsGPSV(naiveKeysFS,naiveSpreadFS
          ,updatedCheckinsFileCityFS,naiveGSVPlotsFS)

    */
        /**Find top-k locations wtih most check-ins*/
        /*val list=List(5,10,50,100)
        list.foreach { t =>
          val keysCount = t
          val topK = "E:\\DataSet\\old\\Results\\Qualitative Analysis\\NYC\\topk_mck\\Gowalla\\mCK-keys"
          dataform.findTopKLocations(updatedCheckinsFileCityGW, keysCount, topK + "_" + keysCount + ".txt")

        }*/
    */
    /** Convoys */
    val mb = 1024*1024
    val runtime = Runtime.getRuntime
    val fr=new fileReaderLBSN
    //val convoy = new ConvoyAnalysis_fsl_upd
    //val fileConvoys="E:\\DataSet\\old\\LBSNAnalysis\\Convoy\\newConvoys_FS_Semantic.txt"
    //convoy.getConvoys(filePathCk_FS_Se_CA, filePathFF_FS_Se_CA,30, fileConvoys, "") // in minutes now

    /**Convoys patterns Analysis*/

    val convoyFileFS="C:\\Users\\MAamir\\Desktop\\Convoys\\FS.txt"
    //val convoyFile="E:\\DataSet\\old\\convoys\\FourSquarewithSemantics\\fileConvoy_FS_Seman_CA_1 hour.txt"
    //val convoyFile="E:\\DataSet\\old\\convoys\\wee\\fileConvoy_Wee_1hour.txt"
    val weeConvoyTableFile="E:\\DataSet\\old\\convoys\\ConvoyTableWee.txt"
    val weeFriendsFile="E:\\DataSet\\New\\others\\weeplaces\\Modified\\friends.txt"
    val weeConvoys="E:\\DataSet\\withSemantics\\Clustered\\Convoys\\Wee.txt"
    val weeVenues="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Venues\\Wee.txt"
    val convoyPattern=new ConvoysPatternAnalysis
    //val weeConvoys=convoyPattern.readFile(convoyFile)
    //val fr=new fileReaderLBSN
    //val Convoys=convoyPattern.readConvoysFile(weeConvoys)
    //val weeFriends=fr.readFriendsFile(weeFriendsFile)
    //convoyPattern.findConvoyStats(Convoys,weeFriendsFile)
    convoyPattern.getConvoyTable(weeConvoys,weeFriendsFile,weeVenues,weeConvoyTableFile)//filePathFF_FS_Se_CA
    //fr.readVenuesFileWee(weeVenues)


    //val venueWee="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"
    val df=new DataFormatter
    //df.getVenuesWeeFromCheckinFile(venueWee)
    val chk_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_checkins.csv"
    val ff_We="E:\\DataSet\\New\\others\\weeplaces\\weeplace_friends.csv"
    val venues_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Venues\\Wee.txt"
    val idToName_user_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Mapping\\IdToName\\User\\Wee.txt"
    val idToName_loc_Wee="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Mapping\\IdToName\\Location\\Wee.txt"
    val dsfn=new DataSetFormatterNew
    //dsfn.formatWEEDataSetNew(ff_We,chk_We,venues_Wee,idToName_user_Wee,idToName_loc_Wee)



    /*
    /**coordinates conversion*/
/*
    val lat:Double=33.73762686110111
    val long:Double= -118.02981856080525

    val xy=UTMCoord.fromLatLon(Angle.fromRadiansLatitude(lat.toRadians),Angle.fromRadiansLongitude(long.toRadians))
    println("conversion is::"+xy.getNorthing,xy.getEasting,xy.getZone,xy.getHemisphere)
*/

    val nFile="E:\\DataSet\\New\\others\\weeplaces\\Modified\\checkIns.txt"
    //val gc=new GridCluster
    //gc.getClusters(nFile)// in filePathCk_FS_Se_CA one coordinate belongs to multiple locations

    //val result=df.getCheckinsWithXY(fr.readCheckinFile(filePathCk_FS_Se_CA))
    //println("result size is::"+result.size)


    /**Stat Finder*/
    val fr=new fileReaderLBSN
    val checkins=fr.readCheckinFile(filePathCk_FS_Se_CA)
    val friends=fr.readFriendsFile(filePathFF_FS_Se_CA)
    val sf=new StatFinder
    //sf.analyseDataWithData(friends,checkins)
    val clusteredFile="E:\\DataSet\\New\\FourSquare\\withSemantics\\ModifiedWithClustersIds\\checkins.txt"

    /**Cluster locations*/
    //val dsf=new DataSetFormatter
    //dsf.clusterLocations(filePathCk_FS_Se_CA,clusteredFile)

*/




    //fr.readVenuesFile(filePathVenues_FS_Se_CA)

    //println("Converting original check-in files to clustered location checkin file")
    /**Original data to Clustered Data*/

    val dirCheckinsNC="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Checkins" // checkin directory non-clustered
    val dirCheckinsC="E:\\DataSet\\withSemantics\\Clustered\\Dataset\\Checkins"
    val dirMapGridIdToLocIds="E:\\DataSet\\withSemantics\\Clustered\\Dataset\\Mappings\\GridIdToLocId"
    val fileName="SmallFile.txt"
    val gridX=10
    val gridY=10
    val filesCheckins=new java.io.File(dirCheckinsNC).listFiles
    /*filesCheckins.take(1).foreach{t=>
      val gc=new GridCluster
      //val fileName=t.getName
      val fileName="SmallFile.txt"
      val fileMapGridToLoc=fileName
      println("File Name--------------::"+t.getName)
      gc.getClusters(dirCheckinsNC+"\\"+fileName,gridX,gridY,dirCheckinsC+"\\"+fileName,dirMapGridIdToLocIds+"\\"+fileName)//(dirCheckinsNC+"\\"+fileName,gridX,gridY,dirCheckinsC+"\\"+fileName)

    }*/
    /*
    /** Clustered Data to Convoys*/
    println("**Finding Convoys**")

    val dirConvoysC="E:\\DataSet\\withSemantics\\Clustered\\Convoys"
    val dirFriendsC="E:\\DataSet\\withSemantics\\NonClustered\\Dataset\\Friends"
    filesCheckins.take(1).foreach{t=>
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



    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)

    val endTime = System.currentTimeMillis()
    println("Time take is ::" + (endTime - startTime) + " milliSeconds")
    println("finished!!!")
  }


}
