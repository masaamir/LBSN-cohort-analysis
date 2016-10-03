package GridClustering



import FormatData.{fileReaderLBSN, DataFormatter}
import java.io.{File, PrintWriter}
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

  def findGrid(findPoint:Point, initialPoint:Point, distX:Double,distY:Double,lengthX:Double,lengthY:Double, increment:Long): Long ={

    val noRows:Long=if(lengthY % distY > 0.0) (lengthY/distY).floor.toLong +1 else (lengthY/distY).floor.toLong
    val noCols:Long=if(lengthX % distX > 0.0) (lengthX/distX).floor.toLong +1 else (lengthX/distX).floor.toLong
    val totalGrids:Long= noRows*noCols

    //val noCols= if(lengthX%distX == 0) lengthX/distX else (lengthX/distX).floor.toInt +1
    //println("number of cols are lenghtX, distX::"+lengthX,distX)
    var colNo=Math.abs((( findPoint.x - initialPoint.x) / distX).floor.toLong) //+ 1 (+1 removed)
    if(colNo!=0) colNo += 1
    //println("col number is ::"+colNo)
    val rowNo=Math.abs(((findPoint.y - initialPoint.y) / distY).floor.toLong)
    //println("col number is ::"+colNo)
    val gridNo:Long=((rowNo*noCols) + colNo)//.floor.toLong

    if(gridNo<0){
      //println("total rows, total columns, total grids"+noRows,noCols,totalGrids)
      println("Negative Grid--------------------------------------------rowNo, no of cols, clno::"+rowNo,noCols,colNo)
    }
    else if(gridNo>totalGrids){
      println("total rows, total columns, total grids::"+noRows,noCols,totalGrids)
      println("More than max---------------rowNo, no of cols, clno::"+rowNo,noCols,colNo)
    }
    //val gridNo:Int= (colNo* distY).floor.toInt + rowNo
    //println("point number::"+findPoint.x,findPoint.y+" is in grid number ::"+gridNo)

    return gridNo + increment

  }


  def getClusters(fileCheckin:String,inGridX:Double,inGridY:Double,fileNewCheckins:String): Unit ={
    val df=new DataFormatter
    val fr=new fileReaderLBSN
    val fw=new PrintWriter((new File(fileNewCheckins)))

    val checkins=fr.readCheckinFile(fileCheckin)
    println("actual checkins::"+checkins.size)
    val latLonLocs=checkins.map(t=> (t._6,t._3,t._4)).distinct
    println("distinct locations originally size::"+latLonLocs.size)
    //latLonLocs.groupBy(t=> (t._2,t._3)).toList.sortBy(t=> -t._2.size).take(1).foreach(println)
    //println("actual locations::"+latLonLocs.size)
    val UTMLocs=df.convertLatLonToUTM(latLonLocs) // convert lat, long to UTM coordinates
    //println("UTM Size::"+UTMLocs.size)

    //val UTMLocs=UTMLocsLatLon.map(t=> (t.))
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
        //println("latitude -Y :: min, max "+minY,maxY)
        //println("longitude- X :: min, max "+minX,maxX)
        val lengthX=maxX-minX
        val lengthY=maxY-minY
        val initialPoint=new Point(minX,minY)
        val GCount:Long=totalGrids(distX,distY,lengthX,lengthY)
        //incrementValue= incrementValue+ GCount +10 // 10= extra to avoid conflicts on safe side
        h._2.foreach{it=>
          val gridId:Long=findGrid(new Point(it._5,it._4),initialPoint,distX,distY,lengthX,lengthY,incrementValue)
          //println("old id, new id"+it._1, gridId)
          //newLocs += ((it._1,it._2,it._3,it._4,it._5, it._6,it._7, gridId))
          clustLocsMaps += (it._1->gridId)
          //println(it._1,it._2,it._3,it._4,it._5, it._6,it._7, gridId)
        }
        incrementValue=incrementValue +GCount + 10 // increment location ids for every grid as individual it start with 0: thus new grid id += total size of non-empty grids + safety margin, i.e., 10    // 10= extra margin in location id between grids ids of two zones or hemisphere
      }
        if(incrementValue >Long.MaxValue){
          println("Error~ grid id exceeded Long Max value")
        }
    }
    println("converted locs, ::"+clustLocsMaps.values.toList.distinct.size)
    //println("new ids size::"+newLocs.groupBy(ng=> ng._8).size)
   //println("List::")
    //newLocs.filter(t=> t._1==10).foreach(println)
    //println("clustered Loc Map::"+clustLocsMaps.getOrElse(10,null))

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
      //(t._1,t._2,t._3,t._4,t._5,newId,t._7)
    }
    fw.close()
    println("New checkins size are::"+newCheckinsCount)
    /*

    val test=newLocs.groupBy(t=> t._8).toList.sortBy(t=> -t._2.size).take(10)//.map(t=> (t._2.map(it=>it._2,t.)))

    test.foreach{t=>
      println("id is ....................................... ::"+t._1)
      t._2.map(it=> (it._2,it._3)).foreach(iiit=> println(iiit._1+","+iiit._2))

    }
*/
    //println("after locs::"+UTMLocs.size)

    //println("zones::"+UTMLocs.groupBy(t=> t._6).size)

    //println("Hemispheres::"+UTMLocs.groupBy(t=> t._7).size)


  }

}
