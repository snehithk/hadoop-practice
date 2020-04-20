package ca.mcit.bigdata.hadoop




import java.io.FileNotFoundException

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}



object Main extends App  {
  val conf = new Configuration()
  val uri ="/user/fall2019/snehith/hadoop1"

  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem= FileSystem.get(conf)
  println(fs.getUri)

  try {

    fs
      .listStatus(new Path(uri))
      .foreach(println)
    println("->found my file")

    //deleting folder
    fs.delete(new Path(uri),true)
    println("->deleted folder")

    //create directory
    fs.mkdirs(new Path(uri))
    if(fs.exists(new Path(uri))){
      println("->created sucessful")
    }
    else println("->failed creating folder")

    //creating subfolder Stm

    fs.mkdirs(new Path("/user/fall2019/snehith/hadoop1/stm"))
    if(fs.exists(new Path("/user/fall2019/snehith/hadoop1/stm"))){
      println("->created stm sucessful")


      //copying stops.txt to hdfs
      fs.copyFromLocalFile(new Path("/home/bd-user/Documents/stops.txt"),
        new Path("/user/fall2019/snehith/hadoop1/stm/"))

      //checking if file is created
      if (fs.exists(new Path("/user/fall2019/snehith/hadoop1/stm/stops.txt"))) {
        println("->file copied")
      }
      //changing file name to stops2.txt
      fs.copyFromLocalFile(new Path("/home/bd-user/Documents/stops.txt"),
        new Path("/user/fall2019/snehith/hadoop1/stops2.txt"))

      //rename file
      fs.rename(new Path("/user/fall2019/snehith/hadoop1/stops2.txt"),new Path("/user/fall2019/snehith/hadoop1/stops.csv"))
      println("changed name successfully")

      //writing data to csv
      val in: FSDataInputStream = fs.open(new Path("/user/fall2019/snehith/hadoop1/stops.csv"))
      val write:String= IOUtils.toString(in)
      write.split("\n").slice(0,5).foreach(println)
    }
    else print("->failed creating  sub folder")

  }
  catch{
    case _: FileNotFoundException =>
      println("file not found")
  }

}

