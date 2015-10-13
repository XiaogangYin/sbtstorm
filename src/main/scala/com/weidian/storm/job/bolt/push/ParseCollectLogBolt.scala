package com.weidian.storm.job.bolt.push

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }
import com.redis._
import java.net.{ URLEncoder, URLDecoder }
import java.text.{ SimpleDateFormat, ParsePosition }
import scalaj.http.Http

class ParseCollectLogBolt extends BaseRichBolt{
  var collector: OutputCollector = _
  var redis: RedisClient = _;
  val validActions: Set[String] = Set("uploadStoreProducts.do",
    "delProduct.do"
    )

      
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")

  override def prepare(config: JMap[_, _], context: TopologyContext, collector: OutputCollector) {
    this.collector = collector
    reconnect()
  }

  //Source.fromFile("/data/resys/yinxiaogang/kafka_2.11-0.8.2.1/udc.log").getLines.toList filter {y => {val parts = y.split(" "); parts.size > 8 && parts(7)=="url[/uploadProductCollect3.do]" && y.indexOf("\"appid\":null") == -1 && y.indexOf("\"productID\":\"vdian") > 0 && y.indexOf("h5.geili.glmi") == -1 }}
  override def execute(tuple: Tuple) {
    val line = tuple.getString(0)

    try {
      val attrMap = (line.split("\\|") foldLeft Map.empty[String, String])((mm, part) => {
        if (part.length > 2) {
          val tr = part.substring(1, part.length - 1)
          val colonPos = tr.indexOf(":")
          mm + (tr.take(colonPos) -> tr.drop(colonPos + 1))
        } else
          mm
      })

      attrMap("src_interface") match {
        case "http://aproxy/uploadStoreProducts.do" => { //添加收藏
          val userID = attrMap.getOrElse("userID", "")
          val productID = attrMap.getOrElse("productID", "").split(",") filter (_.indexOf("vdian") == 0) map (_.substring(5))
          val timestamp: Long = formatter.parse(attrMap("requestTime_ms"), new ParsePosition(0)).getTime()
          if (userID.length() > 0)
              collect()

          def collect(): Unit = {
            productID foreach { id =>
                val price = getProdcutPrice(id)
                if (price > 0) {
                  redis.zadd(id, price, userID)
                  redis.zadd("expireZset", timestamp, id + "_" + userID)
                  redis.set(id + "_" + userID, "koudai_" + timestamp)
                }
            }
          }

        }
        case "http://aproxy/delProduct.do" => { //取消收藏
          val userID = attrMap.getOrElse("userID", "")
          val productID = attrMap.getOrElse("productID", "").split(",") filter (_.indexOf("vdian") == 0) map (_.substring(5))
          if (userID.length() > 0)
              delCollect()
           def delCollect(): Unit = {
            productID foreach { id => 
              redis.zrem(id, userID)
              redis.zrem("expireZset", id + "_" + userID)
              redis.del(id + "_" + userID)
            }
          }
        }
        case _ => 
      }
    } catch {
      case _: java.net.ConnectException => reconnect
      case ex: java.lang.Exception => ex.printStackTrace()
    }finally {
      this.collector.emit(tuple, new Values(tuple.getString(0)))
      this.collector.ack(tuple)
    }
  }
  
  /**
   * 价格 * 100
   */
  def getProdcutPrice(id: String): Int = {
    try {
      val response = Http("http://idc01-wdin-web-vip00.dns.koudai.com/wd/item/getPubInfo").
        param("param",s"""{"itemID":"$id", "dealEmptyArr":1}""").
        timeout(connTimeoutMs = 3000, readTimeoutMs = 15000).asString
      val pricePattern = """"price":"([0-9|\.]+)"""".r
      val pricePattern(price) = pricePattern.findFirstMatchIn(response.body).get
      (java.lang.Double.parseDouble(price) * 100).intValue
    } catch {
      case ex: java.lang.Exception => {ex.printStackTrace(); println(id); -1}
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("word"))
  }
   
  def reconnect() {
    //this.redis = new RedisClient("idc01-tjrtlog-redis-vip00", 6004)
    //this.redis = new RedisClient("localhost", 6379)
    this.redis = new RedisClient("10.1.5.106", 6379)
  } 
}