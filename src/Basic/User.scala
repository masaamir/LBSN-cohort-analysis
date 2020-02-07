package Basic

/**
 * Created by XXX on XXX.
 */
class User(id:Long,name:String,address:String) {
  val uid:Long=id
  val uName:String=""
  val uAddress:String=""

  def this(id:Long){
    this(id,"","")
  }

  def getUId(): Long ={
    return  uid
  }
}
