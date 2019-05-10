package spark.session

import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wyg.sessionanalyze.constant.Constants
import com.wyg.sessionanalyze.dao.factory.DAOFactory
import com.wyg.sessionanalyze.domain._
import com.wyg.sessionanalyze.test.MockData
import com.wyg.sessionanalyze.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object usrActionAnalyze {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()

    //生成模拟数据
    MockData.mock(sc, sparkSession);
    //获取任务
    //创建访问数据库的实例
    val taskDAO = DAOFactory.getTaskDAO
    //访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    println(s"执行的任务id是taskId=$taskId")
    val task = taskDAO.findById(taskId)
    if (task == null) {
      println("没有获取到对应taskID的task信息")
      return
    }

    val taskParam = JSON.parseObject(task.getTaskParam)
    //获取参数指定范围的数据
    val actionRDD = getActionRDDByDateRange(sparkSession, taskParam)
    //(action)
    //(sesionid,action)
    //生成k,v格式的数据，其中k是sessionid,v是一条行为日志信息
    println("产生的原始数据actionRDD session个数是"+actionRDD.count())

    val sessionId2ActionRDD = actionRDD.rdd.map(row => (row(2).toString, row))
    //sessionId2ActionRDD.take(3).foreach(println)
//    println("count" + sessionId2ActionRDD.count())

    //1，缓存后面要反复使用的RDD
    val sessionId2ActionRDDCache = sessionId2ActionRDD.cache()
    println("产生的原始数据session（sessionid,info）个数是"+sessionId2ActionRDDCache.count())


    //2，分区数20，提高并行度？,重分区，repartition
    //3, sc.textFile,sc.parrallize,分区或者分片，过程中会发生shuffle？，不会


    //把用户数据和一个会话内用户访问的行为数据进行一个整合 根据session_id 进行整合,并计算步长,时长指标等等
    val sessionId2AggregateInfoRDD = aggregateByUserid(sc, sparkSession, sessionId2ActionRDD)

    println("整合步长和时长之后的session个数"+sessionId2AggregateInfoRDD.count())

    //自定义累加器
    val sessionAccumulator = new SessionAccumulator()
    sc.register(sessionAccumulator)

    //数据过滤
    val filteridSessionRDD = filterSessionByParamRDD(sessionId2AggregateInfoRDD, taskParam, sessionAccumulator)

//    filteridSessionRDD.take(3).foreach(println)
    /**
      * 累加器懒加载，需要执行一个action算子，并进行cache切断依赖，防止之后的transform算子影响累加器
      */
    filteridSessionRDD.cache().count()
    /**
      * 计算访问时长和步长占总Session的占比
      */
    println("累加器中的数据"+sessionAccumulator.value)
    calculatePercent(sessionAccumulator.value, taskId)

    //过滤后的数据session的明细 将过滤后的数据和没过滤的数据进行join,拿到一个明细数据.(因为其他参数还有用,要做个join操作)
    val sessionDetailRDD: RDD[(String, (String, Row))] = getSessionId2DetailRDD(filteridSessionRDD, sessionId2ActionRDD)
    println("--------------根据任务参数进行过滤+agg sessionid后的RDD--------------")
    sessionDetailRDD.take(5).foreach(println)


    /**
      * 按比例随机抽取session
      */
    extractSessionByRatio(filteridSessionRDD, sessionDetailRDD, taskId, sc)

    /**
      * 计算top10热门品类.并计算点击,下单,支付进行排名.
      */
    val catagoryList = getTop10Category(sessionDetailRDD, taskId)

    /**
      * 计算热门品类下各品类活跃top10 session
      */
    val top10CategorySession = getTop10Session(catagoryList, sessionDetailRDD, taskId, sc)

  }


  //计算热门top10品类中，每个品类点击次数最多的session，取top10 session
  def getTop10Session(categoryList: Array[(CategorySortKey, String)], sessionDetailRDD: RDD[(String, (String, Row))],
                      taskid: Long, sc: SparkContext) = {
    //转化List = >RDD
    //(categoryid,categoryid)
    var list = List[(Long, Long)]()
    for (item <- categoryList) {
      val categoryid = StringUtils.getFieldFromConcatString(item._2, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      list = list.:+(categoryid, categoryid)
    }
    val top10CategoryRDD = sc.parallelize(list)

    //计算每个品类被所有session点击的次数
    val top10CategorySessionClick = top10CategorySessionClickCount(sessionDetailRDD, top10CategoryRDD)


    val top10SessionRDD = calculateTop10Session(top10CategorySessionClick, taskid)
    //将每一个session的明细加进来，最后写到数据库
    insertTop10SessionDetail(top10SessionRDD, sessionDetailRDD, taskid)

  }


  def insertTop10SessionDetail(top10CategorySession: RDD[(String, String)], sessionDetailRDD: RDD[(String, (String, Row))], taskid: Long): Unit = {
    //(string,(string,(string,row)))
    val sesstionDetailsRDD = top10CategorySession.join(sessionDetailRDD)
    sesstionDetailsRDD.foreach(tup => {
      // 获取session的明细数据
      val row = tup._2._2._2
      val sessionDetail = new SessionDetail
      sessionDetail.setTaskid(taskid)
      sessionDetail.setUserid(row.getLong(1))
      sessionDetail.setSessionid(row.getString(2))
      sessionDetail.setPageid(row.getLong(3))
      sessionDetail.setActionTime(row.getString(4))
      sessionDetail.setSearchKeyword(row.getString(5))
      if (!row.isNullAt(6)) {
        sessionDetail.setClickCategoryId(row.getLong(6))
      }
      if (!row.isNullAt(7)) {
        sessionDetail.setClickProductId(row.getLong(7))
      }
      sessionDetail.setOrderCategoryIds(row.getString(8))
      sessionDetail.setOrderProductIds(row.getString(9))
      sessionDetail.setPayCategoryIds(row.getString(10))
      sessionDetail.setPayProductIds(row.getString(11))
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insert(sessionDetail)
    })
  }

  //分组topn,每个品类的top10活跃用户
  def calculateTop10Session(top10CategorySessionClickCount: RDD[(Long, String)], taskid: Long) = {
    //分组按品类
     val top10CategorySessionCountsRDD: RDD[(Long, Iterable[String])] = top10CategorySessionClickCount.groupByKey()
    /**
      *
      */
    val count = top10CategorySessionCountsRDD.count()
//    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=")
//    println("top10CategorySessionCountsRDD 的个数是"+count)
//    top10CategorySessionCountsRDD.foreach(println)
    //取每组top10session，每组排序，取top10
    val top10Session = top10CategorySessionCountsRDD.flatMap(tup => {
      val categoryId = tup._1
      val sessioncountlist: Iterable[String] = tup._2

      val res1: Iterable[(String, Long)] = sessioncountlist.map(x => {
        (x.split(",")(0).toString, x.split(",")(1).toLong)
      })
      val res2: List[(String, Long)] = res1.toList.sortBy(_._2).reverse.take(10)
//      println(res2.toString())
//      val it = tup._2.iterator
//      //定义一个容器，存top10的结果
//      val topsessions = new Array[String](10)
//      while (it.hasNext) {
//        //"sessionid,count"
//        val sessionCount = it.next()
//        val count = sessionCount.split(",")(1).toLong
//        //遍历取topN
//        var i = 0
//        import scala.util.control.Breaks._
//        breakable {
//          while (i < topsessions.length) {
//            if (topsessions(i) == null) {
//              topsessions(i) = sessionCount
//              //相当于Java,break功能
//              break
//            } else {
//              val _count = sessionCount.split(",")(1).toLong
//              //判断，如果count>i位的count
//              var j = 0
//              while (j > i) {
//                topsessions(j) = topsessions(j - 1)
//                j = j - 1
//              }
//              //count插入到有序数组
//              topsessions(i) = sessionCount
//              break
//            }
//          }
//          i = i + 1
//        }
//
//      }
//

//      for(i<-topsessions) {
//        println(i)
//      }

      //把结果存入数据库
      var list = List[(String, String)]()
      for (item <- res2) {
        if (item != null) {
          val sessionid = item._1.toString
          val count = item._2.toLong
          val top10Session = new Top10Session
          top10Session.setTaskid(taskid)
          top10Session.setSessionid(sessionid)
          top10Session.setClickCount(count)
          top10Session.setCategoryid(categoryId)
          val top10SessionDAO = DAOFactory.getTop10SessionDAO
          top10SessionDAO.insert(top10Session)
          list = list :+ (sessionid, sessionid)
        }
      }
      list
    }
    )
    top10Session
  }

  //计算每个session点击某个品类的次数
  def top10CategorySessionClickCount(sessionDetailRDD: RDD[(String, (String, Row))], top10CategoryRDD: RDD[(Long, Long)]) = {
    //sessionDetailRDD根据sessionID分组
    //数据（sessionid,itetaor(sessionid,row)）
    val sessionDetailsRDD = sessionDetailRDD.groupByKey()
    //每个session 的各个品类点击次数(categoryid,"sessionid,count")
    val catagorySessionClickCount = sessionDetailsRDD.flatMap(tup => {
      val sessionid = tup._1
      val it = tup._2.iterator
      //声明一个容器，存每个品类的点击次数
      var categoryCount = scala.collection.mutable.Map[Long, Long]()
      //遍历当前session的明细，计算每个品类的点击次数
      while (it.hasNext) {
        val row = it.next()._2
        if (row.get(6) != null) {
          val categoryId = row.getLong(6)
          var count = 0L
          if (categoryCount.get(categoryId).getOrElse(null) != null) {
            count = categoryCount.get(categoryId).getOrElse(0L)
            count = count + 1
          }
          categoryCount.put(categoryId, count)
        }
      }
      //定义一个结果容器，格式：categoryid,"sessionid,count"
      var list = List[(Long, String)]()
      for (item <- categoryCount.keys) {
        var categoryId = item
        val count = categoryCount.get(categoryId).getOrElse(0)
        val strres = sessionid + "," + count
        list = list.:+((categoryId, strres))
      }
      list
    })
    //JOIN热门品类top10
    //(categoryid,(category,"sessionid,count"))=>(category,"sessionid,count")
    val top10CategorySessionCount = top10CategoryRDD.join(catagorySessionClickCount).map(
      tuple => {
        (tuple._1, tuple._2._2)
      }
    )

    top10CategorySessionCount

  }

  //计算热门品top10品类
  def getTop10Category(sessionDetailRDD: RDD[(String, (String, Row))], taskid: Long) = {
    //1,获取用户访问的所有品类信息
    var categoryidRDD: RDD[(Long, Long)] = getAllSessionVisitid(sessionDetailRDD)
    //RDD去除重复品类
    categoryidRDD = categoryidRDD.distinct()
    //计算每个品类的点击次数，下单次数，支付次数
    val clickCategoryCountRDD = getClickCategoryCountRDD(sessionDetailRDD)
    val orderCategoryCountRDD = getOrderCategoryCountRDD(sessionDetailRDD)
    val payCategoryCountRDD = getPayCategoryCountRDD(sessionDetailRDD)
    //join 访问的所有品类、下单品类、点击品类、支付品类
    //(categoryid,"categoryid=1|....")
    val categoryCountRDD = joinCategoryCount(categoryidRDD, clickCategoryCountRDD, orderCategoryCountRDD, payCategoryCountRDD)
    //自定义一个排序函数
    //把结果转化成（categorySortKey，countinfo）
    val sortKeyCountRDD = categoryCountRDD.map(tup => {
      val countinfo = tup._2
      val clickCount = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
      val sortobj = new CategorySortKey(clickCount, orderCount, payCount)
      (sortobj, countinfo)
    })
    //按降序排列数据
    val sortedCategoryCountRDD = sortKeyCountRDD.sortByKey(false)
    //取top10数据，写入数据库
    val topCategoryList = sortedCategoryCountRDD.take(10)
    val top10CategoryDAO = DAOFactory.getTop10CategoryDAO
    //遍历结果数据，写入数据库
    for (item <- topCategoryList) {
      val countinfo = item._2
      val categoryid = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val payCount = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
      val clickCount = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val top10CategoryObj = new Top10Category
      top10CategoryObj.setTaskid(taskid)
      top10CategoryObj.setClickCount(clickCount)
      top10CategoryObj.setOrderCount(orderCount)
      top10CategoryObj.setPayCount(payCount)
      top10CategoryObj.setCategoryid(categoryid)
      top10CategoryDAO.insert(top10CategoryObj)
    }
    topCategoryList
  }

  //把品类、品类的点击次数、下单次数、支付次数整合到一起
  def joinCategoryCount(categoryIdRdd: RDD[(Long, Long)], clickCategoryIdRdd: RDD[(Long, Long)],
                        orderCategoryIdRdd: RDD[(Long, Long)], payCategoryIdRdd: RDD[(Long, Long)]) = {
    //(categoryid,(categoryid,Option(count)))
    val joinResult1 = categoryIdRdd.leftOuterJoin(clickCategoryIdRdd)
    //最后 数据个数（category，“categoryid=1|clickCount=20|paycount=30|ordercount=50”）
    var tmpMapRDD = joinResult1.map(tup => {
      val categoryId = tup._1
      var clickCount = tup._2._2.getOrElse(null)
      if (clickCount == null)
        clickCount = 0L

      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT +
        "=" + clickCount
      (categoryId, value)
    })
    //把ordercount和上次的处理结果拼接到一起
    tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryIdRdd).map(tup => {
      val categoryId = tup._1
      var value = tup._2._1
      var orderCount = tup._2._2.getOrElse(null)
      if (orderCount == null)
        orderCount = 0L
      value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
      (categoryId, value)
    })
    //把paycount和上次进行拼接
    tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryIdRdd).map(tup => {
      val categoryId = tup._1
      var value = tup._2._1
      var payCount = tup._2._2.getOrElse(null)
      if (payCount == null)
        payCount = 0L
      value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
      (categoryId, value)
    })
    tmpMapRDD
  }

  //计算每个品类的点击次数(CATEGORYID,COUNT)
  def getClickCategoryCountRDD(sessionDetailRDD: RDD[(String, (String, Row))]) = {
    //拿到所有有点击行为的session
    val clickActionRDD = sessionDetailRDD.filter(tup => {
      val row = tup._2._2
      if (row.get(6) != null) true
      else
        false
    })
    val clickCategoryCountRDD = clickActionRDD.map(tup => {
      (tup._2._2.getLong(6), 1L)
    }).reduceByKey(_ + _)
    //聚合，计算每个品类的总点击次数
    clickCategoryCountRDD
  }

  //计算每个品类的下单次数
  def getOrderCategoryCountRDD(sessionDetailRDD: RDD[(String, (String, Row))]) = {
    val orderActionRDD = sessionDetailRDD.filter(tup => {
      val row = tup._2._2
      if (row.getString(8) != null) true
      else false
    })
    val orderCategoryRDD = orderActionRDD.flatMap(tup => {
      val orderCategoryIds = tup._2._2.getString(8)
      var list = List[(Long, Long)]()
      if (orderCategoryIds != null) {
        val orderCategoryIdsSplited = orderCategoryIds.split(",")
        for (item <- orderCategoryIdsSplited) {
          list = list.:+(item.toLong, 1L)
        }
      }
      list
    }).reduceByKey(_ + _)
    orderCategoryRDD
  }

  //计算每个品类的支付次数
  def getPayCategoryCountRDD(sessionDetailRDD: RDD[(String, (String, Row))]) = {
    val payActionRDD = sessionDetailRDD.filter(tup => {
      val row = tup._2._2
      if (row.getString(10) != null) true
      else false
    })
    val payCategoryRDD = payActionRDD.flatMap(tup => {
      val payCategoryIds = tup._2._2.getString(10)
      var list = List[(Long, Long)]()
      if (payCategoryIds != null) {
        val payCategoryIdsSplited = payCategoryIds.split(",")
        for (item <- payCategoryIdsSplited) {
          list = list.:+(item.toLong, 1L)
        }
      }
      list
    }).reduceByKey(_ + _)
    payCategoryRDD
  }

  //获取用户访问的所有品类信息(点击，下单，支付)
  def getAllSessionVisitid(sessionDetailRDD: RDD[(String, (String, Row))]) = {

    val categoryIdRDD = sessionDetailRDD.flatMap(tuple => {
      //先定义一个容器，存放点击、下单、支付的品类id
      var list = List[(Long, Long)]()
      var row = tuple._2._2
      //添加点击的品类信息
      if (!row.isNullAt(6)) {
        val clickCategoryId = row.getLong(6)
        list = list.:+(clickCategoryId, clickCategoryId)
      }
      //添加下单(加入购物车)的品类信息id,id
      val orderCategoryIds = row.getString(8)
      if (orderCategoryIds != null) {
        val splitedCategoryIds = orderCategoryIds.split(",")
        for (item <- splitedCategoryIds) {
          list = list.:+(item.toLong, item.toLong)
        }
      }
      //添加下单(支付)的品类信息id,id
      val payCategoryIds = row.getString(10)
      if (payCategoryIds != null) {
        val splitedCategoryIds = payCategoryIds.split(",")
        for (item <- splitedCategoryIds) {
          list = list.:+(item.toLong, item.toLong)
        }
      }
      list
    })
    categoryIdRDD
  }


  /**
    * 获取通过筛选条件的Session的访问明细
    *
    * @param filteredSessionId2AggrInfoRDD
    * @param sessionId2ActionRDD
    * @return
    */
  private def getSessionId2DetailRDD(filteredSessionId2AggrInfoRDD: RDD[(String, String)], sessionId2ActionRDD: RDD[(String, Row)]) = {
    // 过滤后的数据和访问明细数据进行join
    val sessionId2DetailRDDTmp = filteredSessionId2AggrInfoRDD.join(sessionId2ActionRDD)
    // sessionId2DetailRDDTmp.take(3).foreach(println)
    //sessionId2DetailRDDTmp.map(tup => (tup._1, tup._2._2))
    sessionId2DetailRDDTmp
  }

  //按比例随机抽取session
  def extractSessionByRatio(filterSessionRDD: RDD[(String, String)], sessionDetailRDD: RDD[(String, (String, Row))], taskid: Long, sc: SparkContext)
  : Unit = {
    //1,计算每个小时session个数
    //最后数据格式结果（date + "_" + hour,count）

    //这个返回的是(date_Hour, sessionInfo)
    val time2SessionIdRDD = filterSessionRDD.map(tuple => {
      //当前session对应的数据信息取出来
      val sessionInfo = tuple._2
      //取出当前会话的开始时间yyyy-mm--dd hh:mm:ss
      val startTime = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_START_TIME)
      //解析出来session的日期和小时
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, sessionInfo)

    })
    //返回的每天每小时的session个数(date_Hour,count)
    val countMap = time2SessionIdRDD.countByKey()

    //2,按比例随机抽取
    //每天应该抽取的session个数
    //数据格式（date,((hour->count),(hour->count),(hour->count)....）
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    //遍历countMap(dateHour,count)=》（date,(hour,count)）
    import scala.collection.JavaConversions._
    for (countEntry <- countMap.entrySet()) {
      //取出dateHour日期和小时
      val dateHour = countEntry.getKey
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      //取出session个数
      val count = countEntry.getValue

      var hourCountMap = dateHourCountMap.get(date).getOrElse(null)
      if (hourCountMap == null) {
        hourCountMap = new mutable.HashMap[String, Long]()
        dateHourCountMap.put(date, hourCountMap)
      }
      hourCountMap.put(hour, count)
    }

    //计算数据集总天数，计算每天抽取的session个数 (所有时间,抽取总的session样例是100)
    val extractNum = 100 / dateHourCountMap.size
    //每天每小时抽取的个数
    val dateHourExtrarctMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()
    for (dateHourCountEntry <- dateHourCountMap.entrySet) {
      //获取日期
      val date = dateHourCountEntry.getKey
      //当前日期下每小时的session个数 map(hour->count)
      val hourCountMap: mutable.HashMap[String, Long] = dateHourCountEntry.getValue
      //计算当天的session总数
      var sessionCount = 0L
      for (count <- hourCountMap.values) {
        sessionCount += count
      }

      var hourExtractMap: mutable.HashMap[String, ListBuffer[Int]] = dateHourExtrarctMap.get(date).getOrElse(null)
      if (hourExtractMap == null) {
        hourExtractMap = new mutable.HashMap[String, ListBuffer[Int]]()
        dateHourExtrarctMap.put(date, hourExtractMap)
      }
      //计算每个小时抽取的session个数
      for (hourCountEntry <- hourCountMap.entrySet()) {
        val hour = hourCountEntry.getKey
        //取当前小时的session个数
        val count = hourCountEntry.getValue
        //计算当前小时要抽取的session个数 抽取总数,均到每一天,然后每个小时取几个
        /**
          * count:当前小时的session总数
          * sessionCount:当天session的总数
          * extractNum:每天抽取session的总数
          */
        var extractCount = (count.toDouble / sessionCount.toDouble * extractNum).toInt
        if (extractCount > count) extractCount = count.toInt
        //计算抽取的session索引信息
        var extractIndexList = new ListBuffer[Int]()
        extractIndexList = hourExtractMap.get(hour).getOrElse(null)
        if (extractIndexList == null) {
          extractIndexList = new ListBuffer[Int]()
          hourExtractMap.put(hour, extractIndexList)
        }

        var random = new Random()

        //随机生成要抽取的数据的索引
        var i = 0
        while (i < extractCount) {
          var extractIndex = random.nextInt(count.toInt)
          //判断随机的索引是否重复
          while (extractIndexList.contains(extractIndex)) {
            extractIndex = random.nextInt(count.toInt)
          }
          extractIndexList.add(extractIndex)
          i += 1
        }
      }
    }

    //把抽取的信息（date,(hour,(1,34,56,90))）广播到每一个节点上
    val dataHourExtractMapBroadcast = sc.broadcast(dateHourExtrarctMap)

    //3,根据生成的dateHourExtrarctMap字典，在task上抽取相应的session
    //(datehour,sessioninfo)=>(datahour,iterator(sessioninfo))
    val time2SessionRDD = time2SessionIdRDD.groupByKey()
    //根据计算出来的索引信息抽取具体的session
    val extractSessionRDD = time2SessionRDD.map(
      tuple => {
        //存储抽取的结果（sessionid,sessionid）
        val extractSessionids = new util.ArrayList[(String, String)]()
        val dateHour = tuple._1
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        //获取广播变量
        val dateHourExtractMap = dataHourExtractMapBroadcast.value
        //获取抽取的索引列表
        val extractIndexList = dateHourExtractMap.get(date).get(hour)
        val sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO
        val it = tuple._2.iterator
        var index = 0
        var sessionid = ""
        while (it.hasNext) {
          //当前的索引是否在抽取的索引列表里
          val sessioninfo = it.next()
          if (extractIndexList.contains(index)) {
            sessionid = StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_SESSION_ID)
            val sessionRandomExtract = new SessionRandomExtract
            sessionRandomExtract.setTaskid(taskid)
            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_START_TIME))
            sessionRandomExtract.setSessionid(sessionid)
            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
            sessionRandomExtract.setClickCategoryIds(
              StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            )
            sessionRandomExtractDAO.insert(sessionRandomExtract)
            //将抽取的sessionid存入list
            extractSessionids.add((sessionid, sessionid))
          }
          index += 1
        }
        (sessionid, sessionid)
      }
    )

    //4,获取抽取的session的明细数据(sessionid,(sessionid,(sessionid,row)))
    val extractSessionDetailRDD = extractSessionRDD.join(sessionDetailRDD)
    val sessionDetails = new util.ArrayList[SessionDetail]()
    extractSessionDetailRDD.foreachPartition(partition => {
      partition.foreach(tuple => {
        val row = tuple._2._2._2
        val sessionDetail = new SessionDetail
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getAs[Long](6))
        sessionDetail.setClickProductId(row.getAs[Long](7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))
        sessionDetails.add(sessionDetail)
      })
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insertBatch(sessionDetails)
    })


  }


  def filterSessionByParamRDD(sessionId2AggregateInfoRDD: RDD[(String, String)], taskParam: JSONObject,
                              sessionAccumulator: SessionAccumulator) = {
    //解析task的参数信息
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionInfo = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val city = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    //参数拼接成一个字符串
    var param = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|"
    else "") + (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionInfo != null) Constants.PARAM_PROFESSIONALS + "=" + professionInfo + "|" else "") +
      (if (city != null) Constants.PARAM_CITIES + "=" + city + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categorys != null) Constants.PARAM_CATEGORY_IDS + "=" + categorys + "|" else "")

    //如果param结尾是“|”，截掉
    if (param.endsWith("|")) param = param.substring(0, param.length - 1)

    //把拼接之后的参数，传给过滤函数，根据参数值对数据集过滤
    //把每一条数据过滤，如果满足参数条件的，数据中访问时长，访问步长取出来，通过累加器把相应字段值进行累加。
    val sessionFilterRes = sessionId2AggregateInfoRDD.filter(fiterFun(_, param, sessionAccumulator))
//    println("被TaskID过滤后的session个数"+sessionFilterRes.count())
//    sessionFilterRes.count()
    sessionFilterRes
  }

  def fiterFun(tuple: (String, String), param: String, accumulator: SessionAccumulator): Boolean = {
    //session信息取出来
    val info = tuple._2
    //比较info里面信息是否满足param
    if (!ValidUtils.between(info, Constants.FIELD_AGE, param, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
      return false
    if (!ValidUtils.in(info, Constants.FIELD_PROFESSIONAL, param, Constants.PARAM_PROFESSIONALS))
      return false
    if (!ValidUtils.in(info, Constants.FIELD_CITY, param, Constants.PARAM_CITIES))
      return false
    if (!ValidUtils.in(info, Constants.FIELD_SEX, param, Constants.PARAM_SEX))
      return false
    if (!ValidUtils.in(info, Constants.FIELD_SEARCH_KEYWORDS, param, Constants.PARAM_KEYWORDS))
      return false
    if (!ValidUtils.in(info, Constants.FIELD_CLICK_CATEGORY_IDS, param, Constants.PARAM_CATEGORY_IDS))
      return false

    //把满足条件的session的访问时长，访问步长取出来，使用累加器进行累加
    //累加总的session_count
    accumulator.add(Constants.SESSION_COUNT)
    //先把该条session的访问时长和访问步长取出来  时间是毫秒值，起初生成数据的时候已经随机，取出来判断即可。
    val visitLength = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_VISIT_LENGTH).toLong / 1000
    val stepLength = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_STEP_LENGTH).toLong
    //判断当前访问时长和访问步长的分布区间，然后累加相应字段
    if (visitLength >= 1 && visitLength <= 3)
      accumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6)
      accumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9)
      accumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength < 30)
      accumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength >= 30 && visitLength < 60)
      accumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength >= 60 && visitLength < 180)
      accumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength >= 180 && visitLength < 600)
      accumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength >= 600 && visitLength < 1800)
      accumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength >= 1800)
      accumulator.add(Constants.TIME_PERIOD_30m)

    if (stepLength >= 1 && stepLength <= 3)
      accumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6)
      accumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9)
      accumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength < 30)
      accumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength >= 30 && stepLength <= 60)
      accumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength > 60)
      accumulator.add(Constants.STEP_PERIOD_60)
    true
  }

  //计算各个范围访问时长、访问步长在总的session中占比
  def calculatePercent(accumulatorValue: String, taskid: Long) = {
    //计算各个访问时长和访问步长占比
    val session_count = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.SESSION_COUNT)).toLong
    val visit_length_1s_3s = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_1s_3s)).toLong
    val visit_length_4s_6s = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_4s_6s)).toLong
    val visit_length_7s_9s = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_7s_9s)).toLong
    val visit_length_10s_30s = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_10s_30s)).toLong
    val visit_length_30s_60s = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_30s_60s)).toLong
    val visit_length_1m_3m = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_1m_3m)).toLong
    val visit_length_3m_10m = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_3m_10m)).toLong
    val visit_length_10m_30m = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_10m_30m)).toLong
    val visit_length_30m = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.TIME_PERIOD_30m)).toLong
    val step_length_1_3 = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_1_3)).toLong
    val step_length_4_6 = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_4_6)).toLong
    val step_length_7_9 = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_7_9)).toLong
    val step_length_10_30 = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_10_30)).toLong
    val step_length_30_60 = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_30_60)).toLong
    val step_length_60 = (StringUtils.getFieldFromConcatString(accumulatorValue, "\\|", Constants.STEP_PERIOD_60)).toLong
    // 计算各个访问时长和访问步长的范围占比
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)
    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)
    // 将统计结果存入数据库
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)
    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)

  }


  def aggregateByUserid(context: SparkContext, sparkSession: SparkSession, sessionId2ActionRDD: RDD[(String, Row)]) = {
    //先把数据根据sessionid进行聚合，得到当前会话的所有行为（搜索、点击、购买等）信息
    val sessionIdGroupBy = sessionId2ActionRDD.groupByKey()
    println("根据sessionid进行聚合，产生的数据")
   println("聚合后的会话个数"+sessionIdGroupBy.count())
    //把当前sessionID下的所有行为进行聚合
    val sessionuserinfo = sessionIdGroupBy.map(tuple => {
      val sessionid = tuple._1
      val searchKeyWordBuffer = new StringBuffer()
      val clickCategoryIdsBuffer = new StringBuffer()
      //用户id信息
      var usrid = 0L
      var startTime = new Date()
      var endIime = new Date(0L)
      //当前会话的访问步长
      var stepLength = 0
      val it = tuple._2.iterator
      while (it.hasNext) {
        val row = it.next()
        //1值的是位置信息
        usrid = row.getLong(1)
        val searchekeyword = row.getString(5)
        val clickCategoryid = String.valueOf(row.getAs[Long](6))
        //搜索词和点击的品类信息追加到汇总的stringbuffer
        if (!StringUtils.isEmpty(searchekeyword))
          if (!searchKeyWordBuffer.toString.contains(searchekeyword))
            searchKeyWordBuffer.append(searchekeyword + ",")
        if (clickCategoryid != null)
          if (!clickCategoryIdsBuffer.toString.contains(clickCategoryid))
            clickCategoryIdsBuffer.append(clickCategoryid + ",")
        //计算session的开始时间和结束时间 访问时长
        val actionTime = DateUtils.parseTime(row.getString(4))
        if (actionTime.before(startTime)) startTime = actionTime
        if (actionTime.after(endIime)) endIime = actionTime
        //计算访问的步长
        stepLength += 1
      }

      //截取搜索关键字和点击品类的字符串的“，”
      val searchWords = StringUtils.trimComma(searchKeyWordBuffer.toString)
      val clickCategorys = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      //计算当前会话的访问时长
      val visitLength = endIime.getTime - startTime.getTime
      //println("visitLength" + visitLength)

      //聚合数据，key=value|key=value
      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" + Constants.FIELD_SEARCH_KEYWORDS +
        "=" + searchWords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategorys + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"
      (usrid, aggrInfo)

    })

    //查询用户的信息
    val sql = "select * from user_info";
    val userinfoRDD = sparkSession.sql(sql).rdd
    val userinfoRDD2 = userinfoRDD.map(row => (row.getLong(0), row))
    val userFullInfo = sessionuserinfo.join(userinfoRDD2)
    //处理成（sessionID，session行为信息+用户信息）
    val userSessionFullInfo = userFullInfo.map(tuple => {
      //整个会话的信息
      val sessioninfo = tuple._2._1
      //用户信息
      val userinfo = tuple._2._2
      //取用户sessionid
      val sessionid = StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_SESSION_ID)
      // 获取用户信息的age
      val age = userinfo.getInt(3)
      // 获取用户信息的职业
      val professional = userinfo.getString(4)
      // 获取用户信息的所在城市
      val city = userinfo.getString(5)
      // 获取用户信息的性别
      val sex = userinfo.getString(6)
      //整个会话的信息+用户的信息拼接
      val fullAggrInfo = sessioninfo + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex + "|"
      (sessionid, fullAggrInfo)
    })

    /**
      *
      */
    println("结合用户信息的信息后的session个数"+userSessionFullInfo.count())
    //返回整合后的数据
    userSessionFullInfo
  }

  def getActionRDDByDateRange(sparkSession: SparkSession, taskParam: JSONObject): DataFrame = {
    //解析参数，拿到开始日期，结束日期
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endData = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    //写一个sql，过滤满足条件的数据
    val sqlstr = "select * from user_visit_action where date >= '" + startDate +
      "' and date <= '" + endData + "' "
    println(sqlstr)
    val actionDF = sparkSession.sql(sqlstr)
    //展示前5行数据
    print("count:" + actionDF.count())
    actionDF.show(5)
    actionDF
  }

}
