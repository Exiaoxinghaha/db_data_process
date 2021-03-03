package com.exx.engine.udf

import org.apache.spark.sql.api.java.UDF1

class DUDF1 extends UDF1[String, String]{
    override def call(t1: String): String = {
        t1 + "_new"
    }
}
