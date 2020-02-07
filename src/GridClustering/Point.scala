package GridClustering

/**
 * Created by XXX on 24-12-XXX.
 */
class Point (xInput:Double,yInput:Double) {

  val x:Double=xInput
  val y:Double=yInput

}

class Grid(idInput:Int,blInput:Point,lengthX:Double,widthY:Double,neighborsIn:Neighbors){
  val id:Long=idInput
  val bl=blInput // bottom left point
  val br:Point=new Point(bl.x+lengthX,bl.y) //bottom right point
  val tl=new Point(bl.x,bl.y+widthY) // top left point
  val tr=new Point (bl.x+lengthX,bl.y+widthY) //top right point
  //val locationList=new util.ArrayList[S]()()
  //Neighbors
  val neighbors:Neighbors=neighborsIn
  /*
  var URL= -1
  var URM= -1
  var URR= -1
  var MRL= -1
  var MRR= -1
  var BRL= -1
  var BRM= -1
  var BRR= -1
  */
}

class Neighbors(URLIn:Long,URMIn:Long,URRIn:Long,MRLIn:Long,MRRIn:Long,BRLIn:Long,BRMIn:Long,BRRIn:Long){
  var URL= URLIn
  var URM= URMIn
  var URR= URRIn
  var MRL= MRLIn
  var MRR= MRRIn
  var BRL= BRLIn
  var BRM= BRMIn
  var BRR= BRRIn
}
