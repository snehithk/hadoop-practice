package ca.mcit.bigdata.hadoop




import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object Main extends App  {
   val conf = new Configuration()
  val uri ="/user/fall2019/snehith/"

  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
 conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem= FileSystem.get(conf)
  println(fs.getUri)
  try {

   fs
     .listStatus(new Path(uri))
       .foreach(println)
    println("found my file")
  }
 catch{
   case f : FileNotFoundException =>
     println("file not found")
 }
}
