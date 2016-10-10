package GridClustering



import FormatData.{DataFormatter, fileReaderLBSN}
import java.io.{File, PrintWriter}

import Basic.Location

import scala.collection.mutable.ListBuffer
import collection.mutable.HashMap

/**
 * Created by MAamir on 9/29/2016.
 */
class GridCluster {

  def totalGrids(distX:Double,distY:Double,lengthX:Double,lengthY:Double): Long ={
    val noRows:Long=if(lengthY % distY > 0.0) (lengthY/distY).floor.toLong +1 else (lengthY/distY).floor.toLong
    val noCols:Long=if(lengthX % distX > 0.0) (lengthX/distX).floor.toLong +1 else (lengthX/distX).floor.toLong
    val totalGridCount:Long= noRows*noCols
    return totalGridCount
  }

  def findGrid(findPoint:Point, initialPoint:Point, distX:Double,distY:Double,lengthX:Double,lengthY:Double, increment:Long)
  : Long ={

    val noRows:Long=if(lengthY % distY > 0.0) (lengthY/distY).floor.toLong +1 else (lengthY/distY).floor.toLong
    val noCols:Long=if(lengthX % distX > 0.0) (lengthX/distX).floor.toLong +1 else (lengthX/distX).floor.toLong
    val totalGrids:Long= noRows*noCols
    var colNo=Math.abs((( findPoint.x - initialPoint.x) / distX).floor.toLong) //+ 1 (+1 removed)
    if(colNo!=0) colNo += 1
    val rowNo=Math.abs(((findPoint.y - initialPoint.y) / distY).floor.toLong)
    val gridNo:Long=((rowNo*noCols) + colNo)

    if(gridNo<0){
      println("Negative Grid--------------------------------------------rowNo, no of cols, col no::"+rowNo,noCols,colNo)
    }
    else if(gridNo>totalGrids){
      println("total rows, total columns, total grids::"+noRows,noCols,totalGrids)
      println("More than max---------------rowNo, no of cols, clno::"+rowNo,noCols,colNo)
    }

    return gridNo + increment

  }


  def evaluateClustering(fileGridToLoc:String,fileNCCheckins:String): Unit ={
    val gridToLoc=scala.io.Source.fromFile(fileGridToLoc).getLines().toList.map(t=> t.split("\t"))
      .map(t=> (t(0),t(1).split(","))).map(t=> (t._1.toLong,t._2.map(it=> it.toLong).toList))
    val fr=new fileReaderLBSN

    val locToCoords=fr.readCheckinFile(fileNCCheckins).map(t=>(t._6,(t._3,t._4))).distinct.toMap

    val gridWithLocCoord=gridToLoc.map{t=>
      (t._1,t._2.map{it=>
        (it,locToCoords.getOrElse(it,(-1.0,-1.0)))
      })
    }
    gridWithLocCoord.sortBy(t=> -t._2.size).take(10).foreach{t=>
      println("For grid Id::"+t._1)
      println("Common locations wit size::"+t._2.size+" are::")
      t._2.foreach(it=> println(it._2._1+","+it._2._2))
    }

  }

  def getClusterdVenues(fileMapGridToLoc:String, fileNCVenues:String, fileClusteredVenues:String): Unit ={
    val MapGridToLoc=scala.io.Source.fromFile(fileMapGridToLoc).getLines().toList
      .map(t=> t.split("\t")).map(t=> (t(0).toLong,t(1).split(",").to[ListBuffer].map(it=> it.toLong))).toMap
    val fr=new fileReaderLBSN
    val NCVenues=fr.readVenuesFileWee(fileNCVenues).map(t=> (t.lId,t)).toMap
    val CVenuesWriter=new PrintWriter(new File(fileClusteredVenues))

    println("total NC Venues::"+NCVenues.size)
    val CVenues:ListBuffer[Location]=new ListBuffer()
    MapGridToLoc.toList.foreach{t=>
      val mappedLocs=MapGridToLoc.getOrElse(t._1,ListBuffer(0L))
      var lId:Long=0L
      var lLat:Double=0.0
      var lLon:Double=0.0
      var lArea:String="n\\a"
      var lState:String="n\\a"
      var lCountry:String="n\\a"
      var lCategories:ListBuffer[String]=new ListBuffer()

      mappedLocs.foreach{it=>
        val venue=NCVenues.getOrElse(it,null )
        if(venue!=null){
          if(venue.lLat!=0) lLat+=venue.lLat
          if(venue.lLon!=0)  lLon+=venue.lLon
          if(lArea!="")lArea=venue.lArea
          if(lState!="")lState=venue.lState
          if(lCountry!="")lCountry=venue.lCountry
          lCategories ++= venue.lCategories
        }
      }

      CVenuesWriter.println(t._1+"\t"+lLat/mappedLocs.size.toDouble+"\t"+lLon/mappedLocs.size.toDouble
        +"\t"+lArea+"\t"+lState+"\t"+lCountry+"\t"+lCategories.distinct.mkString(","))
      //CVenues += new Location((t._1.toLong,lLat.toDouble,lLon.toDouble,lArea.toString,lCountry.toString,lCategories))
    }
    CVenuesWriter.close()
      //.take(10).foreach(println)
    //val NCVenues=
  }

  def getClusters(fileCheckin:String,inGridX:Double,inGridY:Double,fileNewCheckins:String, fileMapGridToLoc:String): Unit ={
    val df=new DataFormatter
    val fr=new fileReaderLBSN
    val fw=new PrintWriter((new File(fileNewCheckins)))
    val mapGridToLocWriter=new PrintWriter(new File(fileMapGridToLoc))

    val checkins=fr.readCheckinFile(fileCheckin)
    println("actual check-ins::"+checkins.size)
    val latLonLocs=checkins.map(t=> (t._6,t._3,t._4)).distinct
    println("distinct locations originally size::"+latLonLocs.size)
    val UTMLocs=df.convertLatLonToUTM(latLonLocs) // convert lat, long to UTM coordinates
    /**Start Clustering*/
    val distX:Double=inGridX // grid size x
    val distY:Double=inGridY // grid size y
    var incrementValue:Long=0
    val newLocs:ListBuffer[(Long,Double,Double,Double,Double,Int,String,Long)]=new ListBuffer()
    val clustLocsMaps=new HashMap[Long,Long] // oldLocId, newLocId/gridId
    val zones=UTMLocs.groupBy(t=> t._6).foreach{z=> // per zone out of 60
      val heim=z._2.groupBy(t=> t._7)
      heim.foreach{h=> // per hemisphere, out of two: South and North
        val minY=h._2.minBy(t=> t._4)._4
        val maxY=h._2.maxBy(t=> t._4)._4
        val minX=h._2.minBy(t=> t._5)._5
        val maxX=h._2.maxBy(t=> t._5)._5
        val lengthX=maxX-minX
        val lengthY=maxY-minY
        val initialPoint=new Point(minX,minY)
        val GCount:Long=totalGrids(distX,distY,lengthX,lengthY)
        //incrementValue= incrementValue+ GCount +10 // 10= extra to avoid conflicts on safe side
        h._2.foreach{it=>
          val gridId:Long=findGrid(new Point(it._5,it._4),initialPoint,distX,distY,lengthX,lengthY,incrementValue)
          clustLocsMaps += (it._1->gridId)
        }
        incrementValue=incrementValue +GCount + 10 // increment location ids for every grid as individual it start with 0: thus new grid id += total size of non-empty grids + safety margin, i.e., 10    // 10= extra margin in location id between grids ids of two zones or hemisphere
      }
        if(incrementValue >Long.MaxValue){
          println("Error~ grid id exceeded Long Max value")
        }
    }
    println("converted locs, ::"+clustLocsMaps.values.toList.distinct.size)
    /**Write file to map Grid ids to corresponding location ids*/
    clustLocsMaps.toList.groupBy(t=> t._2).foreach{t=>
      mapGridToLocWriter.println(t._1+"\t"+t._2.map(it=> it._1).mkString(","))
    }
    mapGridToLocWriter.close()

    /** Write new check-ins with new locationId */
    var newCheckinsCount= 0
    checkins.foreach{t=>
      newCheckinsCount += 1
      var newId=clustLocsMaps.getOrElse(t._6,null)
      if(newId==null){
        println("Error !! new id doesn't exist")
        newId=0
      }
      fw.println(t._1+"\t"+t._7+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+newId) // user, time, lat, lon, locString, newLoc
    }
    fw.close()
    println("New check-ins size are::"+newCheckinsCount)
  }

}
