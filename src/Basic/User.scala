package Basic

/**
 * Created by MAamir on 9/26/2016.
 */
class User(id:Long,name:String,address:String) {
  val uid:Long=id
  val uName:String=""
  val uAddress:String=""

  def this(id:Long){
    this(id,"","")
    //println("only id is given")
  }

  def getUId(): Long ={
    return  uid
  }
}
