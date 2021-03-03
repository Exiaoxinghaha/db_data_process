package com.exx.engine.utils

import java.io.IOException
import java.net.URISyntaxException
import java.util

import org.apache.http.NameValuePair
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

/**
  * httpclient连接池
  */
object HttpClientUtil {

    private var cm: PoolingHttpClientConnectionManager = null
    val EMPTY_STR = ""
    val UTF_8 = "UTF-8"

    private def init(): Unit ={
        if (cm == null) {
            cm = new PoolingHttpClientConnectionManager
            // 整个连接池最大连接数
            cm.setMaxTotal(5)
            // 每路由最大连接数，默认值是2
            cm.setDefaultMaxPerRoute(5)
        }
    }



    /**
      * 通过连接池获取HttpClient
      *
      * @return
      */
    private def getHttpClient = {
        init()
        HttpClients.custom.setConnectionManager(cm).build
    }



    /**
      * @param url
      * @return
      */
    def httpGetRequest(url: String): String = {
        val httpGet = new HttpGet(url)
        getResult(httpGet)
    }



    @throws[URISyntaxException]
    def httpGetRequest(url: String, params: Map[String,Any]): String = {
        val ub = new URIBuilder(url)
        val pairs = covertParams2NVPS(params)
        ub.setParameters(pairs)
        val httpGet = new HttpGet(ub.build)
        getResult(httpGet)
    }



    @throws[URISyntaxException]
    def httpGetRequest(url: String, headers: Map[String, Any], params: Map[String, Any]): String = {
        val ub = new URIBuilder(url)
        val pairs = covertParams2NVPS(params)
        ub.setParameters(pairs)
        val httpGet = new HttpGet(ub.build)
        val it = headers.keysIterator
        while(it.hasNext){
            val key = it.next()
            val value = headers(key)
            httpGet.addHeader(key,String.valueOf(value))
        }
        getResult(httpGet)
    }



    def httpPostRequest(url: String): String = {
        val httpPost = new HttpPost(url)
        getResult(httpPost)
    }



    def httpPostRequest(url: String, json: String): String = {
        val httpPost = new HttpPost(url)
        val entity = new StringEntity(json, "utf-8") //解决中文乱码问题
        entity.setContentEncoding("UTF-8")
        entity.setContentType("application/json")
        httpPost.setEntity(entity)
        getResult(httpPost)
    }


    import java.io.UnsupportedEncodingException

    import org.apache.http.client.entity.UrlEncodedFormEntity
    import org.apache.http.client.methods.HttpPost

    @throws[UnsupportedEncodingException]
    def httpPostRequest(url: String, params: Map[String,Any]): String = {
        val httpPost = new HttpPost(url)
        val pairs = covertParams2NVPS(params)
        httpPost.setEntity(new UrlEncodedFormEntity(pairs, UTF_8))
        getResult(httpPost)
    }



    @throws[UnsupportedEncodingException]
    def httpPostRequest(url: String, headers: Map[String, Any], params: Map[String, Any]): String = {
        val httpPost = new HttpPost(url)
        val it = headers.keysIterator
        while(it.hasNext){
            val key = it.next()
            val value = headers(key)
            httpPost.addHeader(key,String.valueOf(value))
        }
        val pairs = covertParams2NVPS(params)
        httpPost.setEntity(new UrlEncodedFormEntity(pairs, UTF_8))
        getResult(httpPost)
    }



    private def covertParams2NVPS(params: Map[String, Any]) = {
        val pairs = new util.ArrayList[NameValuePair]()
        val it = params.keysIterator
        while(it.hasNext){
            val key = it.next()
            val value = params(key)
            pairs.add(new BasicNameValuePair(key, String.valueOf(value)))
        }
        pairs
    }




    /**
      * 处理Http请求
      */
    private def getResult(request: HttpRequestBase): String = {
        // CloseableHttpClient httpClient = HttpClients.createDefault();
        val httpClient = getHttpClient
        try {
            val response = httpClient.execute(request)
            // response.getStatusLine().getStatusCode();
            val entity = response.getEntity
            if (entity != null) {
                // long len = entity.getContentLength();// -1 表示长度未知
                val result = EntityUtils.toString(entity,"UTF-8")
                response.close()
                // httpClient.close();
                return result
            }
        } catch {
            case e: IOException =>
                e.printStackTrace()
        }
        EMPTY_STR
    }

    def main(args: Array[String]): Unit = {
        val content = httpGetRequest("http://wiki.xuwei.tech")
        print(content)
    }


}
