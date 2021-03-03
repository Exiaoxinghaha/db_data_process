package com.exx.engine.udf

import org.apache.spark.sql.api.java.UDF1

import scala.util.Random

class MoreFieldUDF extends UDF1[String, String]{
    override def call(t1: String): String = {
        val i = Random.nextInt(10)
        "{\"name\": \""+t1+i+"\", \"city\":\"bj"+i+"\"}"
    }
}
