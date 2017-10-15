package me.hanyu.spark

import org.apache.spark.sql.SparkSession

object UserActiveDegreeAnalyze {
  case class UserActionLog(logId: Long, userId: Long, actionTime: String, actionType: Long, purchaseMoney: Double)
  case class UserActionLogVO(logId: Long, userId: Long, actionValue: Long)
  case class UserActionLogWithPurchaseMoneyVO(logId: Long, userId: Long, purchaseMoney: Double)

  def main(args: Array[String]) {
    val startDate = "2016-09-01";
    val endDate = "2016-11-01";

    val spark = SparkSession
      .builder()
      .appName("UserActiveDegreeAnalyze")
      .master("local")
      .config("spark.sql.warehouse.dir", "~/Desktop/spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val userActionLog = spark.read.json("files/user_action_log.json")
    val userBaseInfo = spark.read.json("files/user_base_info.json")

    //get top 10 users have most views in date range

    userActionLog
      //get data in range
      .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0")
      //join and to get user information
      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      //group
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      //aggregate
      .agg(count(userActionLog("logId")).alias("actionCount"))
      //sort
      .sort($"actionCount".desc)
      //smaple
      .limit(10) 
      .show() 

    //top purchased user
    userActionLog
      .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 1")
      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLog("purchaseMoney")), 2).alias("totalPurchaseMoney"))
      .sort($"totalPurchaseMoney".desc).limit(10)
      .show()

    //top increase views users
    val userActionLogInFirstPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
      .map { userActionLogEntry => UserActionLogVO(userActionLogEntry.logId, userActionLogEntry.userId, 1) }

    val userActionLogInSecondPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-01-01' and actionTime <= '2016-09-30' and actionType = 0")
      .map { userActionLogEntry => UserActionLogVO(userActionLogEntry.logId, userActionLogEntry.userId, -1) }

    val userActionLogDS = userActionLogInFirstPeriod.union(userActionLogInSecondPeriod)

    userActionLogDS
      .join(userBaseInfo, userActionLogDS("userId") === userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(sum(userActionLogDS("actionValue")).alias("actionIncr"))
      .sort($"actionIncr".desc)
      .limit(10)
      .show()

    //top increase purchased money
    val userActionLogWithPurchaseMoneyInFirstPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 1")
      .map { userActionLogEntry => UserActionLogWithPurchaseMoneyVO(userActionLogEntry.logId, userActionLogEntry.userId, userActionLogEntry.purchaseMoney) }

    val userActionLogWithPurchaseMoneyInSecondPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-09-01' and actionTime <= '2016-09-30' and actionType = 1").map { userActionLogEntry => UserActionLogWithPurchaseMoneyVO(userActionLogEntry.logId, userActionLogEntry.userId, -userActionLogEntry.purchaseMoney) }

    val userActionLogWithPurchaseMoneyDS = userActionLogWithPurchaseMoneyInFirstPeriod.union(userActionLogWithPurchaseMoneyInSecondPeriod)

    userActionLogWithPurchaseMoneyDS
      .join(userBaseInfo, userActionLogWithPurchaseMoneyDS("userId") === userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLogWithPurchaseMoneyDS("purchaseMoney")), 2).alias("purchaseMoneyIncr"))
      .sort($"purchaseMoneyIncr".desc)
      .limit(10)
      .show()

  }
}