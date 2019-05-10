package spark.session

import com.wyg.sessionanalyze.constant.Constants
import com.wyg.sessionanalyze.util.StringUtils
import org.apache.spark.util.AccumulatorV2

class SessionAccumulator extends AccumulatorV2[String,String]{

  var result = 
  Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s +
    "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s +
    "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m +
    "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 +
    "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 +
    "=0|" + Constants.STEP_PERIOD_60 + "=0|"


  override def isZero: Boolean = {
    true
  }

  //创建当前累加器的一个拷贝
  override def copy(): AccumulatorV2[String, String] = {
    val copyAccumulator = new SessionAccumulator()
    copyAccumulator.result = this.result
    copyAccumulator
  }

  //累加器重置
  override def reset(): Unit = {
    this.result = Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s +
      "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s +
      "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m +
      "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 +
      "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 +
      "=0|" + Constants.STEP_PERIOD_60 + "=0|"
  }

  //task调用，实现task在exexutor上对累加器进行累加
  //传入参数为要累加的字段
  override def add(v: String): Unit = {
    val v1= result
    val v2 = v

    if(StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)){
      //提取v1中给v2对应的字段，对这个字段进行累加
      val getFieldValue = StringUtils.getFieldFromConcatString(v1,"\\|",v2)
      if (getFieldValue != null){
        val newValue = getFieldValue.toInt + 1
        result = StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue))
      }
    }
    result
  }
//把另一个同类型累加器的值和当前值进行累加，更新当前累加器的值
  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case map:SessionAccumulator =>
      {
        val fieldList = List(Constants.SESSION_COUNT ,Constants.TIME_PERIOD_1s_3s,
          Constants.TIME_PERIOD_4s_6s , Constants.TIME_PERIOD_7s_9s,Constants.TIME_PERIOD_10s_30s,
          Constants.TIME_PERIOD_30s_60s,Constants.TIME_PERIOD_1m_3m , Constants.TIME_PERIOD_3m_10m ,
          Constants.TIME_PERIOD_10m_30m,Constants.TIME_PERIOD_30m ,Constants.STEP_PERIOD_1_3 ,
          Constants.STEP_PERIOD_4_6,Constants.STEP_PERIOD_7_9 ,Constants.STEP_PERIOD_10_30 ,
          Constants.STEP_PERIOD_30_60,Constants.STEP_PERIOD_60)
        val res = other.value
        var value1 = 0
        var value2 = 0
        for(item <- fieldList){
          value1 = StringUtils.getFieldFromConcatString(res,"\\|",item).toInt
          value2 = StringUtils.getFieldFromConcatString(result,"\\|",item).toInt
          result = StringUtils.setFieldInConcatString(result,"\\|",item,(value1+value2).toString)
        }
      }
    case _ =>
      throw  new UnsupportedOperationException(s"SessionAccumulator error")
  }

  //返回当前累加器的值
  override def value: String = result
}
