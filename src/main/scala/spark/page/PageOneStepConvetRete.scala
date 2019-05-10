package spark.page

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wyg.sessionanalyze.constant.Constants
import com.wyg.sessionanalyze.dao.factory.DAOFactory
import com.wyg.sessionanalyze.domain.PageSplitConvertRate
import com.wyg.sessionanalyze.test.MockData
import com.wyg.sessionanalyze.util.{DateUtils, NumberUtils, ParamUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import spark.session.usrActionAnalyze.getActionRDDByDateRange

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PageOneStepConvetRete {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).getOrCreate()

    //生成模拟数据
    MockData.mock(sc,sparkSession);
    //获取任务
    //创建访问数据库的实例
    val taskDAO = DAOFactory.getTaskDAO
    //访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE)
    println(taskId)
    val task = taskDAO.findById(taskId)
    if (task == null) {
      println("没有获取到对应taskID的task信息")
      return
    }

    val taskParam = JSON.parseObject(task.getTaskParam)
    //获取参数指定范围的数据
    //(row)
    val actionRDD = getActionRDDByDateRange(sparkSession,taskParam).rdd
    //(sessionid,row)
    val sessionId2ActionRDD = actionRDD.map(row=>{
      (row.getString(2),row)
    })
    //按sessionID分组，得到每一个会话的所有行为
    val groupedSessionId2ActionRDD = sessionId2ActionRDD.groupByKey()
    //计算每个session用户的访问轨迹1,2,3，=》页面切片
    //("1_2",1)
    val pageSplitRDD = generatePageSplit(sc,groupedSessionId2ActionRDD,taskParam)
    //("1_2",count)
    val pageSplitMap =pageSplitRDD.countByKey()

    //统计开始页面的访问次数
    val startPageVisitCount = getStartPageVisit(groupedSessionId2ActionRDD,taskParam)
    //计算页面的单跳转化率
    val convertRateMap = computePageSplitConvertRate(taskParam,pageSplitMap,startPageVisitCount)

    //把上一步结果保存到数据库（taskID，“1_2=0.88|2_3=0.55....”）
    insertConvertRateToDB(taskId,convertRateMap)
   sc.stop()

  }

  def insertConvertRateToDB(taskid: Long, convertRateMap: mutable.HashMap[String, Double])={
    val buffer = new StringBuffer
    import scala.collection.JavaConversions._
    for (convertRateEntry <- convertRateMap.entrySet) { // 获取切片
      val pageSplit = convertRateEntry.getKey
      // 获取转化率
      val convertRate = convertRateEntry.getValue
      // 拼接
      buffer.append(pageSplit + "=" + convertRate + "|")
    }
    // 获取拼接后的切片和转化率
    var convertRate = buffer.toString
    // 截取掉最后的 "|"
    convertRate = convertRate.substring(0, convertRate.length - 1)
    val pageSplitConvertRate = new PageSplitConvertRate
    pageSplitConvertRate.setTaskid(taskid)
    pageSplitConvertRate.setConvertRate(convertRate)
    val pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO
    pageSplitConvertRateDAO.insert(pageSplitConvertRate)

  }

  def computePageSplitConvertRate(taskParam: JSONObject, pageSplitMap: scala.collection.Map[String, Long], startPageVisitCount: Long)
  ={
    //解析参数，获取目标页面流
    var targetPages = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    //上个切片的pv
    var lastPageSplitPV = 0.0
    //根据要计算的切片的访问率，计算每一个切片的转化率（即页面单跳转化率）
    var convertRateMap = new mutable.HashMap[String,Double]()
    var  i=1
    while (i<targetPages.length) {
      val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
      val targetPageSplitPV = pageSplitMap.get(targetPageSplit).getOrElse(0L).toDouble
      var convertRate = 0.0
      if (startPageVisitCount != 0) {
        if (i == 1) convertRate = NumberUtils.formatDouble(targetPageSplitPV / startPageVisitCount, 2)
        else convertRate = NumberUtils.formatDouble(targetPageSplitPV / lastPageSplitPV, 2)
      }
      convertRateMap.put(targetPageSplit,convertRate)
      lastPageSplitPV = targetPageSplitPV
      i +=1
    }
    convertRateMap
  }


  //获取开始页面的访问量
  def getStartPageVisit(groupedSessionId2ActionRDD: RDD[(String, Iterable[Row])], taskParam: JSONObject)={
    //解析首页是哪个页面
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val startPageId = targetPageFlow.split(",")(0).toLong

    //定义一个容器，存放到达第一个页面的访问id
    val list = new mutable.ListBuffer[Long]
    val startPageRDD = groupedSessionId2ActionRDD.map(tup=>{
      val it = tup._2.iterator
      while (it.hasNext){
        val row = it.next()
        val pageId = row.getLong(3)
        if(pageId == startPageId) list+=pageId
      }
    })
    startPageRDD.count()
  }

  def generatePageSplit(sc: SparkContext, groupedSessionId2ActionRDD: RDD[(String, Iterable[Row])], taskParam: JSONObject)={
    //解析参数，拿到页面流
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
     val tagertPageFlowBroadCast=sc.broadcast(targetPageFlow)
    //("1_2",1)
    val list = new ListBuffer[(String,Integer)]
    //计算每一个session中符合条件的页面切片
    //(sessionid,iteraor(action))
    groupedSessionId2ActionRDD.flatMap(tup=>{
      val it = tup._2.iterator
      //取目标页面流
      val targetPages = tagertPageFlowBroadCast.value.split(",")
      //遍历当前会话的页面流
      val rows = new mutable.ListBuffer[Row]
      while(it.hasNext)
        rows += it.next()
      //按照时间，把当前会话的所有行为进行排序
      implicit val keyOrder = new Ordering[Row]{
        override def compare(x: Row, y: Row): Int = {
          //"yyyy--mm--dd hh:mm:ss"
          val actionTime1 = x.getString(4)
          val actionTime2 = y.getString(4)
          val dateTime1 = DateUtils.parseTime(actionTime1)
          val dateTime2 = DateUtils.parseTime(actionTime2)
          (dateTime1.getTime-dateTime2.getTime).toInt
        }
      }
      rows.sorted
      //生成页面切片
      import scala.util.control.Breaks._
      var lastPageId = -1L
      for(row <- rows){
        val pageId = row.getLong(3)
       breakable
       {
         if (lastPageId == -1) {
           lastPageId = pageId
           //类似于Javacontinue
           break
         }
         val pageSplit = lastPageId + "_" + pageId
         //判断当前切片在不在目标页面流
         var i = 1
         breakable {
           while (i < targetPages.length) {
             val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
             if (pageSplit == targetPageSplit) {
                list += ((pageSplit, 1))
               break
             }
             i = i+1
           }
         }
         lastPageId = pageId
       }
      }
      list
    })

  }

}
