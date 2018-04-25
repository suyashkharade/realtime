package com.sdk.realtime

import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}


object LoadCSV {
   def main(args : Array[String]): Unit = {
      val ss = SparkSession.builder.master("local").getOrCreate()
      val df=ss.read.format("csv").option("delimiter","\t").load("/home/suyash/scala-spark/realtime/import-csv/src/main/scala/resources/actions_hotel.tsv")
      df.createOrReplaceTempView("input_table")
      val tmpdf = ss.sql("select reflect(\"java.util.UUID\", \"randomUUID\"),_c0,\"user\",_c2,\"hotel\",\"{}\",_c1,\"UTC\",NULL,NULL from input_table").distinct()
     print(tmpdf.schema)
     tmpdf.write.format("jdbc").option(
        "url","jdbc:mysql://suyash-Vostro-3546"
      ).option("user","suyash").option("password","").
        option("driver","com.mysql.jdbc.Driver").
        option("dbtable","sample.pio_event_1").mode("append").save()
   }
}
