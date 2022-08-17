import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

class ManageKafkaOffsets(val pathToFile: String,val fileSystem: FileSystem, val applicationId: String) {

  val filePath: String = pathToFile
  val fs: FileSystem = fileSystem
  val sparkapplicationId: String = applicationId
  val lockFile: String = pathToFile+"_lock"
  val fileLockedMessage: String = "OffsetsFile locked by another instance of application, lockFile: "+ lockFile


  def saveOffsets(offsetsJson: String): Unit = {
      val path = new Path(filePath)
      if (fs.exists(path))
        fs.delete(path, true)
      val dataOutputStream = fs.create(path)
      dataOutputStream.write(offsetsJson.getBytes("UTF-8"))
      dataOutputStream.close()
  }

  def readOffsets(): String = {

    tryLock()
    val path = new Path(filePath)
    val inputStream: FSDataInputStream = fs.open(path)
    val out = IOUtils.toString(inputStream, "UTF-8");
    inputStream.close()
    out

  }

  def tryLock(): Unit = {

    try{
      val path = new Path(lockFile)
      if (fs.exists(path))
        throw new Exception(fileLockedMessage)
      val dataOutputStream = fs.create(path)
      dataOutputStream.write(sparkapplicationId.getBytes("UTF-8"))
      dataOutputStream.close()
      println("locked offsetsFile with lock:"+lockFile)
    } catch {
      case e: Exception => throw new Exception(fileLockedMessage, e)
    }

  }

  def releaseLock(): Unit = {
    try{
      val path = new Path(lockFile)
      if (fs.exists(path)) {

        val inputStream: FSDataInputStream = fs.open(path)
        val out = IOUtils.toString(inputStream, "UTF-8");
        inputStream.close()
        if (applicationId == out) {
          fs.delete(path, true)
          println("released lock :" + lockFile)
        }else{
          throw new Exception("Cannot release a lock detained by another instance of the application, CurrentapplicationId:"+ applicationId+ " , lockOwnerApplicationId:"+ out)
        }
      }
    } catch {
      case e: Exception => throw new Exception("Cannot release offsetsFile lock: "+ lockFile, e)
    }

  }



}
