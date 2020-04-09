package com.loginfail


import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//输入登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType:String, eventTime: Long)
//输出异常报警信息样例类
case class Warning(userId: Long,firstFailTime: Long, lastFailTime: Long, warningMsg: String)
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000
      })
    val warningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LongWarning(2))
    warningStream.print()
    env.execute("login fail detect job")

  }


}
case class LongWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  lazy val LoginFailStates: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))
  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    if(value.eventType == "fail"){
      //如果是失败，判断之前是否有登录失败事件
      val iter = LoginFailStates.get().iterator()
      if(iter.hasNext){
        //如果有登录失败事件，就比较时间
        val firstFail = iter.next()
        if(value.eventTime < firstFail.eventTime + 2){
          //如果两次登录失败的时间间隔小于2秒这报警
          out.collect(Warning(value.userId,firstFail.eventTime,value.eventTime,"login fail in 2 seconds"))
        }
        //更新最近的一次失败事件，保存在状态里
        LoginFailStates.clear()
        LoginFailStates.add(value)
      }else{
        //如果第一失败保存状态
        LoginFailStates.add(value)
      }
    }else{
      //如果成功清空状态
      LoginFailStates.clear()
    }
  }
}
