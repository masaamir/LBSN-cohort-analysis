package Basic

/**
 * Created by MAamir on 9/26/2016.
 */
import scala.collection.mutable.ListBuffer
class Location (id:Long,lat:Double,lon:Double,area:String,state:String,country:String,categories:ListBuffer[String]) {
  val lId:Long=id
  val lLat:Double=lat
  val lLon:Double=lon
  val lArea:String=area
  val lState:String=state
  val lCountry:String=country
  val lCategories:ListBuffer[String]=categories

  def this(id:Long,lat:Double,lon:Double){
    this(id,lat,lon,"","","",null)
    //println("only id lat and long")
  }
  /*def this(id:Long,lat:Double,lon:Double,area:String,state:String,country:String,categories:ListBuffer[String]){
    this(id,lat,lon,area,state,country,categories)
  }*/



  def printInfo(): Unit ={
    println("id:"+lId+" lat:"+lLat+" lon:"+lLon+" Area:"+lArea+" state:"+lState+" country:"+lCountry+" Categories:"+lCategories.mkString(","))
  }

}
class category{
  val name:String=""
  val subCategory=null

}
