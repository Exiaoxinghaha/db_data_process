package com.exx.engine

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.exx.engine.udf.DUDF1
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object DataProcessEngineBySparkSQL {

    def getKafkaConsumerParams(kafkaSourceServers: String, consumerGroupId: String): Map[String, Object] = {
        Map[String, Object] (
            "bootstrap.servers" -> kafkaSourceServers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> consumerGroupId,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )
    }

    /**
      * 组装StructType
      * @param kafkaSourceSchema
      * @return
      */
    def getStructTypeFromKafkaSourceSchema(kafkaSourceSchema: String): StructType= {
        val jsonInfo = JSON.parseObject(kafkaSourceSchema)
        val it = jsonInfo.entrySet().iterator()
        val buff = ArrayBuffer[StructField]()
        while(it.hasNext) {
            val entry = it.next()
            val fieldName = entry.getKey
            val fieldType = entry.getValue.toString
            fieldType.toLowerCase match {
                case "string" => buff.append(StructField(fieldName, StringType, nullable = false))
                case "int" => buff.append(StructField(fieldName, IntegerType, nullable = false))
                case "double" => buff.append(StructField(fieldName, DoubleType, nullable = false))
                case "float" => buff.append(StructField(fieldName, FloatType, nullable = false))
                case "boolean" => buff.append(StructField(fieldName, BooleanType, nullable = false))
                case "long" => buff.append(StructField(fieldName, LongType, nullable = false))
                case "null" => buff.append(StructField(fieldName, NullType, nullable = true))
                case "object" => buff.append(StructField(fieldName, ObjectType(new Object().getClass), nullable = false))
                case "date" => buff.append(StructField(fieldName, DateType, nullable = false))
                case "array<int>" => buff.append(StructField(fieldName, DataTypes.createArrayType(IntegerType), nullable = false))
                case "array<string>" => buff.append(StructField(fieldName, DataTypes.createArrayType(StringType), nullable = false))
                case "map<string, string>" => buff.append(StructField(fieldName, DataTypes.createMapType(StringType, StringType)))
                case "map<string, int>" => buff.append(StructField(fieldName, DataTypes.createMapType(StringType, IntegerType), nullable = false))
                case "map<string, object>" => buff.append(StructField(fieldName, DataTypes.createMapType(StringType, ObjectType(new Object().getClass)), nullable = false))
                case "array<map<string, string>>" => buff.append(StructField(fieldName, DataTypes.createArrayType(DataTypes.createMapType(StringType, StringType)), nullable = false))
                case _ => buff.append(StructField(fieldName, StringType, nullable = false))
            }
        }
        StructType(buff.toArray)
    }

    /**
      * 组装Row
      * @param line
      * @param kafkaSourceSchema
      * @return
      */
    def getRowRDDFromKafkaSourceSchema(line: String, kafkaSourceSchema: String): Row = {
        val lineJson = JSON.parseObject(line)
        val jsonInfo = JSON.parseObject(kafkaSourceSchema)
        val buff = ArrayBuffer[Any]()
        val it = jsonInfo.entrySet().iterator()
        while (it.hasNext) {
            val entry = it.next()
            val fieldName = entry.getKey
            val fieldType = entry.getValue.toString
            fieldType.toLowerCase match {
                case "string" => buff.append(lineJson.getString(fieldName))
                case "int" => buff.append(lineJson.getInteger(fieldName))
                case "double" => buff.append(lineJson.getDouble(fieldName))
                case "float" => buff.append(lineJson.getFloat(fieldName))
                case "boolean" => buff.append(lineJson.getBoolean(fieldName))
                case "long" => buff.append(lineJson.getLong(fieldName))
                case "null" => buff.append(null)
                case "object" => buff.append(lineJson.getObject(fieldName, new Object().getClass))
                case "date" => buff.append(lineJson.getDate(fieldName))
                case "array<int>" => buff.append(lineJson.getObject(fieldName, new Array[Int](0).getClass))
                case "array<string>" => buff.append(lineJson.getObject(fieldName, new Array[String](0).getClass))
                case "map<string, string>" => buff.append(lineJson.getJSONObject(fieldName))
                case "map<string, int>" => buff.append(lineJson.getJSONObject(fieldName))
                case "map<string, object>" => buff.append(lineJson.getJSONObject(fieldName))
                case "array<map<string, string>>" => buff.append(lineJson.getObject(fieldName, new Array[util.LinkedHashMap[String, String]](0).getClass))
            }
        }
        Row.fromSeq(buff)
    }

    def getKafkaProducerParams(kafkaSinkServers: String): Properties = {
        val properties = new Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkServers)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//        properties.put(ProducerConfig.ACKS_CONFIG, "all")
        properties

    }

    def getJsonStringFromRow(row: Row, kafkaSinkSchema: String): String = {
        val kafkaSinkSchemaJson = JSON.parseObject(kafkaSinkSchema)
        val it = kafkaSinkSchemaJson.entrySet().iterator()
        val resultJson = new JSONObject()
        while(it.hasNext) {
            val entry = it.next()
            val fieldName = entry.getKey
            val fieldType = entry.getValue.toString
            fieldType.toLowerCase match {
                case "string" => resultJson.put(fieldName, row.getAs[String](fieldName))
                case "int" => resultJson.put(fieldName, row.getAs[Int](fieldName))
                case "double" => resultJson.put(fieldName, row.getAs[Double](fieldName))
                case "float" => resultJson.put(fieldName, row.getAs[Float](fieldName))
                case "boolean" => resultJson.put(fieldName, row.getAs[Boolean](fieldName))
                case "long" => resultJson.put(fieldName, row.getAs[Long](fieldName))
                case "null" => resultJson.put(fieldName, null)
//                case "object" => resultJson.put(fieldName, row.getAs[Object](fieldName))
                case "date" => resultJson.put(fieldName, row.getAs[java.sql.Date](fieldName))
                case "array<string>" => resultJson.put(fieldName, row.getAs[Seq[String]](fieldName).toArray)
                case "array<int>" => resultJson.put(fieldName, row.getAs[Seq[Int]](fieldName).toArray)
                case "map<string, string>" => resultJson.put(fieldName, row.getAs[Map[String, String]](fieldName))
                case "map<string, int>" => resultJson.put(fieldName, row.getAs[Map[String, Int]](fieldName))
                case "map<string, object>" => resultJson.put(fieldName, row.getAs[Map[String, Object]](fieldName))
                case "array<map<string, string>>" => resultJson.put(fieldName, row.getAs[Seq[Map[String, String]]](fieldName))
            }
        }
        resultJson.toJSONString
    }

    def registerUDF(functionInfo: String, session: SparkSession) = {
        val functionInfoJson = JSON.parseArray(functionInfo)
        for(i <- 0 until functionInfoJson.size()) {
            val theFunc = functionInfoJson.getJSONObject(i)
            val funcName = theFunc.getString("funcName")
            val inParams = theFunc.getString("inParams").split(",")
            val mainClass = theFunc.getString("mainClass")
            val returnType = theFunc.getString("returnType")
            val rType = returnType.toLowerCase match {
                case "string" => StringType
                case "int" => IntegerType
                case "float" => FloatType
                case "double" => DoubleType
                case "long" => LongType
                case "boolean" => BooleanType
                case "null" => NullType
                case "date" => DateType
                case _ => StringType
            }

            inParams.size match {
                case 1 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF1[String, String]], rType)
                case 2 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF2[String, String, String]], rType)
                case 3 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF3[String, String, String, String]], rType)
                case 4 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF4[String, String, String, String, String]], rType)
                case 5 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF5[String, String, String, String, String, String]], rType)
                case 6 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF6[String, String, String, String, String, String, String]], rType)
                case 7 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF7[String, String, String, String, String, String, String, String]], rType)
                case 8 => session.udf.register(funcName, Class.forName(mainClass).newInstance().asInstanceOf[UDF8[String, String, String, String, String, String, String, String, String]], rType)
                case _ =>
            }
        }
    }

    def main(args: Array[String]): Unit = {
        var masterUrl = "local[2]"
        var appSecond = 1
        var appName = "DataProcessEngineBySparkSQL"
        var kafkaSourceServers = "naga:9092,naga-1:9092,naga-2:9092"
        var kafkaSinkServers = "naga:9092,naga-1:9092,naga-2:9092"
        var kafkaSourceTopics = "input"
        var kafkaSinkTopics = "output"
        var kafkaSourceSchema = "{\"name\": \"string\", \"age\": \"int\", \"score\": \"array<int>\"}"
        var kafkaSinkSchema = "{\"newname\": \"string\", \"city\": \"string\", \"age\": \"int\", \"score\": \"array<int>\"}"
        var consumerGroupId = "test"
        var functionInfo = "[{\"funcName\":\"morefield\", \"mainClass\":\"com.exx.engine.udf.MoreFieldUDF\", \"inParams\": \"string\", \"returnType\": \"string\"}]"

        var sql = "select b.newname, b.city, a.age, a.score from source a lateral view json_tuple(morefield(name), 'name', 'city') b as newname, city"

        // kafka消费者配置参数
        val kafkaConsumerParams = getKafkaConsumerParams(kafkaSourceServers, consumerGroupId)

        val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
        val ssc = new StreamingContext(conf, Seconds(appSecond))
        val session = SparkSession.builder().config(conf).getOrCreate()

        // 创建kafka数据流
        val stream = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(kafkaSourceTopics), kafkaConsumerParams))

        // 处理数据流
        stream.map(_.value()).foreachRDD(rdd => {
            // 组装StructType
            val st: StructType = getStructTypeFromKafkaSourceSchema(kafkaSourceSchema)
            // 组装Row
            val rowRdd = rdd.map(line => {
                getRowRDDFromKafkaSourceSchema(line, kafkaSourceSchema): Row
            })
            // 建表
            val df = session.createDataFrame(rowRdd, st)
            df.createOrReplaceTempView("source")
            // 注册自定义函数
            session.udf.register("myudf", new DUDF1, StringType)
            if(!"".equals(functionInfo)){
                registerUDF(functionInfo, session)
            }
            //
            val res = session.sql(sql)
//            res.show()
            res.rdd.foreachPartition(pr => {
                val kafkaProducerParams = getKafkaProducerParams(kafkaSinkServers): Properties
                val producer = new KafkaProducer[String, String](kafkaProducerParams)
                pr.foreach(row => {
                    val resultJson = getJsonStringFromRow(row, kafkaSinkSchema): String
                    println(resultJson)
                    producer.send(new ProducerRecord(kafkaSinkTopics, resultJson.toString))
                })
                producer.close()
            })
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
