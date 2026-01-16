import scala.collection.mutable.{
  ArrayBuffer,
  Map => MutableMap,
  Set => MutableSet
}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui._

// Helper for JSON string escaping
def escapeJson(s: String): String = {
  s.replace("\\", "\\\\")
    .replace("\"", "\\\"")
    .replace("\n", "\\n")
    .replace("\r", "\\r")
    .replace("\t", "\\t")
}

// -----------------------------------------------------------------------------
// Helper classes to write to JSON
// -----------------------------------------------------------------------------
case class Task(id: Long, duration: Long, executor: String) {
  def toJson: String = {
    s"""{"id":$id,"duration":$duration,"executor":$executor}"""
  }
}

case class TaskList(tasks: Seq[Task]) {
  def toJson: String = {
    tasks.map(_.toJson).mkString("[", ",", "]")
  }
}

case class IntList(ints: Seq[Int]) {
  def toJson: String = {
    ints.sorted.map(i => s"$i").mkString("[", ",", "]")
  }
}

case class StageNode(
    id: Int,
    isQueryResult: Boolean,
    tasks: TaskList,
    rdds: IntList,
    parents: IntList,
    accIds: IntList
) {

  def toJson: String = {
    val tasksJson = tasks.toJson
    val rddsJson = rdds.toJson
    val parentsJson = parents.toJson
    val accIdsJson = accIds.toJson
    s"""{"id":$id,"isQueryResult":$isQueryResult,"tasks":$tasksJson,"rdds":$rddsJson,"parents":$parentsJson,"accIds":$accIdsJson}"""
  }
}

case class StagePlan(stages: Seq[StageNode]) {
  def toJson: String = {
    val stagesJson = stages.map(_.toJson).mkString("[", ",", "]")
    s"""{"stages":$stagesJson}"""
  }
}

case class MetricValue(metricName: String, value: Long, accumulatorId: Long) {
  def toJson: String =
    s"""{"metricName":"${escapeJson(
        metricName
      )}","value":$value,"accId":$accumulatorId}"""
}

class MetricInfo(val metricInfo: SQLMetricInfo, val value: Long)

case class ExecutedPlanNode(
    nodeName: String,
    description: String,
    metadata: Map[String, String],
    metrics: Seq[MetricValue],
    children: Seq[ExecutedPlanNode]
) {

  def toJson: String = {
    val metadataJson = metadata
      .map { case (k, v) =>
        s""""${escapeJson(k)}":"${escapeJson(v)}""""
      }
      .mkString("{", ",", "}")

    val metricsJson = metrics.map(_.toJson).mkString("[", ",", "]")
    val childrenJson = children.map(_.toJson).mkString("[", ",", "]")

    s"""{
      "nodeName":"${escapeJson(nodeName)}",
      "description":"${escapeJson(description)}",
      "metadata":$metadataJson,
      "metrics":$metricsJson,
      "children":$childrenJson
    }""".replaceAll("\\s+", " ")
  }
}

object ExecutedPlanNode {
  def build(
      rawPlan: SparkPlanInfo,
      metrics: Map[Long, MetricInfo]
  ): ExecutedPlanNode = {
    new ExecutedPlanNode(
      rawPlan.nodeName,
      rawPlan.simpleString,
      rawPlan.metadata,
      rawPlan.metrics.map { m =>
        MetricValue(
          m.name,
          metrics.get(m.accumulatorId).map(_.value).getOrElse(0L),
          m.accumulatorId
        )
      },
      rawPlan.children.map { c => ExecutedPlanNode.build(c, metrics) }
    )
  }
}

class ExecutionReport(
    val totalTime: Long,
    val metrics: Map[Long, MetricInfo],
    val plan: SparkPlanInfo,
    val stagePlan: StagePlan
) {

  def printablePlan: ExecutedPlanNode = {
    ExecutedPlanNode.build(plan, metrics)
  }

  def toJson: String = {
    val stageJson = stagePlan.toJson
    val planJson = printablePlan.toJson // {"plan":...}
    s"""{"totalTime":$totalTime,"stagePlan":$stageJson,"sqlPlan":$planJson}"""
  }
}

class AggregateExecutionReport(val execs: Seq[ExecutionReport]) {
  def toJson: String = execs.map(_.toJson).mkString("[", ",", "]")
}

// -----------------------------------------------------------------------------
// Collects events for a single SQL execution.
// Gets the events not from spark but from the processor that forwards from
// -----------------------------------------------------------------------------
class SQLExecutionAnalysis(val startEvent: SparkListenerJobStart) {
  // the sql start event if present
  private var sqlStartEvent: Option[SparkListenerSQLExecutionStart] = None
  // the sql end event if present
  private var endEvent: Option[SparkListenerSQLExecutionEnd] = None

  // maps from stages to the respective StageInfo element
  private val stagesById: MutableMap[Int, StageInfo] = MutableMap()
  // maps from stages to all tasks that are part of this stage
  private val tasksByStageId: MutableMap[Int, MutableSet[TaskInfo]] =
    MutableMap()

  // Adaptive updates
  private val adaptiveExecutionUpdates
      : ArrayBuffer[SparkListenerSQLAdaptiveExecutionUpdate] =
    ArrayBuffer()
  private val adaptiveMetricUpdates
      : ArrayBuffer[SparkListenerSQLAdaptiveSQLMetricUpdates] =
    ArrayBuffer()

  // Accumulator updates
  private val accumulatorUpdates: ArrayBuffer[(Long, Long)] = ArrayBuffer()

  // TaskMetrics
  // Maps from stage id and task id to metrics
  private val taskMetricsByStageId
      : MutableMap[Long, MutableMap[Long, TaskMetrics]] =
    MutableMap()

  def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
    // Add stage infos to map
    val si = e.stageInfo
    stagesById(si.stageId) = si
  }

  def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    val si = e.stageInfo
    stagesById(si.stageId) = si
  }

  def onStart(e: SparkListenerSQLExecutionStart): Unit = {
    sqlStartEvent = Some(e)
  }

  def onEnd(e: SparkListenerSQLExecutionEnd): Unit = {
    endEvent = Some(e)
  }

  def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
    // map tasks to stages
    val sid = e.stageId
    val set = tasksByStageId.getOrElseUpdate(sid, MutableSet[TaskInfo]())
    set += e.taskInfo

    // add task metrics
    taskMetricsByStageId
      .getOrElseUpdate(sid, MutableMap[Long, TaskMetrics]())
      .update(e.taskInfo.taskId, e.taskMetrics)

    // accumulator updates
    accumulatorUpdates ++= e.taskInfo.accumulables.map {
      accumulableToAccumUpdate
    }
  }

  def onExecutorMetricsUpdate(event: Seq[AccumulableInfo]): Unit = {
    accumulatorUpdates ++= event.map { accumulableToAccumUpdate }
  }

  def onDriverAccumUpdate(e: SparkListenerDriverAccumUpdates): Unit = {
    accumulatorUpdates.appendAll(e.accumUpdates)
  }

  def onAdaptiveExecutionUpdate(
      e: SparkListenerSQLAdaptiveExecutionUpdate
  ): Unit = {
    adaptiveExecutionUpdates.append(e)
  }

  def onAdaptiveMetricUpdate(
      e: SparkListenerSQLAdaptiveSQLMetricUpdates
  ): Unit = {
    adaptiveMetricUpdates.append(e)
  }

  def report: ExecutionReport = {
    assert(endEvent.isDefined)
    // build a stage plan
    val stagePlan = buildStagePlan

    // aggregate accumulables
    val aggregatedAccums =
      accumulatorUpdates.groupBy(_._1).map { case (k, v) =>
        (k, v.map(_._2).sum)
      }

    // update metrics
    val metrics = allMetrics.map { case (id, m) =>
      (id, new MetricInfo(m, aggregatedAccums.getOrElse(id, 0L)))
    }

    new ExecutionReport(
      endEvent.get.time - sqlStartEvent.get.time,
      metrics,
      plan,
      stagePlan
    )
  }

  // Use the stage events to build a plan containing all stages and their respective tasks
  private def buildStagePlan: StagePlan = {
    val allIds = stagesById.keys
    val queryResultStageId: Option[Int] =
      if (allIds.isEmpty) None else Some(allIds.max)

    val stageNodes = allIds
      .flatMap { sid =>
        stagesById.get(sid).map { si =>
          val isQueryResult = queryResultStageId.contains(sid)
          // Get the tasks
          val taskSet =
            tasksByStageId.getOrElse(sid, MutableSet.empty[TaskInfo])
          val tasks = taskSet.toSeq.map { ti =>
            Task(ti.index, ti.duration, ti.executorId)
          }
          val taskList = TaskList(tasks)
          // get parent stages
          val parentIds = si.parentIds.toSeq
          val parents = IntList(parentIds)
          // get involved rdds
          val rddIds = si.rddInfos.toSeq.map { rdd => rdd.id }
          val rdds = IntList(rddIds)
          // get the accumulable ids
          val accIds =
            IntList(si.accumulables.values.map { acc => acc.id.toInt }.toSeq)
          StageNode(sid, isQueryResult, taskList, rdds, parents, accIds)
        }
      }
      .toSeq
      .sortBy(_.id)

    StagePlan(stageNodes)
  }

  private def accumulableToAccumUpdate(acc: AccumulableInfo): (Long, Long) = {
    val value = acc.update.get match {
      case s: String => s.toLong
      case l: Long   => l
      case o         =>
        throw SparkException.internalError(s"Unexpected accumulator value: $o")
    }

    (acc.id, value)
  }

  private def plan: SparkPlanInfo = {
    adaptiveExecutionUpdates
      .map(_.sparkPlanInfo)
      .lastOption
      .getOrElse(sqlStartEvent.get.sparkPlanInfo)
  }

  private def allMetrics: Map[Long, SQLMetricInfo] = {
    (gatherMetricsInPlan(
      sqlStartEvent.get.sparkPlanInfo
    ) ++ adaptiveExecutionUpdates.flatMap { e =>
      gatherMetricsInPlan(e.sparkPlanInfo)
    } ++ adaptiveMetricUpdates.flatMap { e => e.sqlPlanMetrics }.map { m =>
      new SQLMetricInfo(m.name, m.accumulatorId, m.metricType)
    }).map { m =>
      (m.accumulatorId, m)
    }.toMap
  }

  private def gatherMetricsInPlan(
      plan: SparkPlanInfo
  ): Iterable[SQLMetricInfo] = {
    plan.metrics ++ plan.children.flatMap { c => gatherMetricsInPlan(c) }
  }
}

// -----------------------------------------------------------------------------
// ExecutionEventsProcessor - processes all collected events into reports
// This relays events to the SQLExecutionAnalysis objects
// -----------------------------------------------------------------------------
class ExecutionEventsProcessor {
  // maps from each sql query id to an Analysis object
  private var execs: Map[Long, SQLExecutionAnalysis] = Map()
  // maps from execution stages to the sql query id
  private var stageToExec: Map[Long, Long] = Map()

  def processJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString =
      event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val analysis = execs.getOrElse(executionId, new SQLExecutionAnalysis(event))
    execs = execs + (executionId -> analysis)
    stageToExec ++= event.stageInfos.map { s =>
      s.stageId.toLong -> executionId
    }
  }

  def processStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stageId = event.stageInfo.stageId.toLong
    stageToExec.get(stageId).foreach { execId =>
      execs(execId).onStageSubmitted(event)
    }
  }

  def processStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageId = event.stageInfo.stageId.toLong
    stageToExec.get(stageId).foreach { execId =>
      execs(execId).onStageCompleted(event)
    }
  }

  def processSqlExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    execs(event.executionId).onStart(event)
  }

  def processSqlExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    execs.get(event.executionId).foreach(_.onEnd(event))
  }

  def processTaskEnd(event: SparkListenerTaskEnd): Unit = {
    stageToExec.get(event.stageId).foreach { execId =>
      execs(execId).onTaskEnd(event)
    }
  }

  def processSqlAdaptiveExecutionUpdate(
      event: SparkListenerSQLAdaptiveExecutionUpdate
  ): Unit = {
    execs.get(event.executionId).foreach(_.onAdaptiveExecutionUpdate(event))
  }

  def processSqlAdaptiveMetricUpdate(
      event: SparkListenerSQLAdaptiveSQLMetricUpdates
  ): Unit = {
    execs.get(event.executionId).foreach(_.onAdaptiveMetricUpdate(event))
  }

  def processSqlDriverAccumUpdate(
      event: SparkListenerDriverAccumUpdates
  ): Unit = {
    execs.get(event.executionId).foreach(_.onDriverAccumUpdate(event))
  }

  def processExecutorMetricsUpdate(
      event: SparkListenerExecutorMetricsUpdate
  ): Unit = {
    event.accumUpdates.foreach { case (_, stageId, _, accumUpdates) =>
      stageToExec.get(stageId).foreach { execId =>
        execs(execId).onExecutorMetricsUpdate(accumUpdates)
      }
    }
  }

  def report: AggregateExecutionReport = {
    new AggregateExecutionReport(execs.values.map(_.report).toSeq)
  }
}

// -----------------------------------------------------------------------------
// Event Listeners
// -----------------------------------------------------------------------------
class ExecutionEventListener extends org.apache.spark.scheduler.SparkListener {
  private val jobStarts: ArrayBuffer[SparkListenerJobStart] = ArrayBuffer()
  private val stageSubmitted: ArrayBuffer[SparkListenerStageSubmitted] =
    ArrayBuffer()
  private val stageCompleted: ArrayBuffer[SparkListenerStageCompleted] =
    ArrayBuffer()
  private val executorMetricsUpdates
      : ArrayBuffer[SparkListenerExecutorMetricsUpdate] =
    ArrayBuffer()
  private val taskEnds: ArrayBuffer[SparkListenerTaskEnd] = ArrayBuffer()
  private val sqlStarts: ArrayBuffer[SparkListenerSQLExecutionStart] =
    ArrayBuffer()
  private val sqlEnds: ArrayBuffer[SparkListenerSQLExecutionEnd] = ArrayBuffer()
  private val sqlAdaptiveExecutionUpdates
      : ArrayBuffer[SparkListenerSQLAdaptiveExecutionUpdate] =
    ArrayBuffer()
  private val sqlAdaptiveMetricsUpdates
      : ArrayBuffer[SparkListenerSQLAdaptiveSQLMetricUpdates] =
    ArrayBuffer()
  private val sqlDriverAccumUpdates
      : ArrayBuffer[SparkListenerDriverAccumUpdates] = ArrayBuffer()

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    jobStarts.append(event)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    stageSubmitted.append(event)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    stageCompleted.append(event)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart          => sqlStarts.append(e)
      case e: SparkListenerSQLExecutionEnd            => sqlEnds.append(e)
      case e: SparkListenerSQLAdaptiveExecutionUpdate =>
        sqlAdaptiveExecutionUpdates.append(e)
      case e: SparkListenerSQLAdaptiveSQLMetricUpdates =>
        sqlAdaptiveMetricsUpdates.append(e)
      case e: SparkListenerDriverAccumUpdates =>
        sqlDriverAccumUpdates.append(e)
      case _ => // ignore other events
    }
  }

  override def onExecutorMetricsUpdate(
      event: SparkListenerExecutorMetricsUpdate
  ): Unit = {
    executorMetricsUpdates.append(event)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskEnds.append(event)
  }

  def report: AggregateExecutionReport = {
    Thread.`yield`()
    val processor = new ExecutionEventsProcessor

    jobStarts.foreach(processor.processJobStart)
    stageSubmitted.foreach(processor.processStageSubmitted)
    stageCompleted.foreach(processor.processStageCompleted)
    sqlStarts.foreach(processor.processSqlExecutionStart)
    sqlAdaptiveExecutionUpdates.foreach(
      processor.processSqlAdaptiveExecutionUpdate
    )
    sqlAdaptiveMetricsUpdates.foreach(processor.processSqlAdaptiveMetricUpdate)
    sqlDriverAccumUpdates.foreach(processor.processSqlDriverAccumUpdate)
    executorMetricsUpdates.foreach(processor.processExecutorMetricsUpdate)
    taskEnds.foreach(processor.processTaskEnd)
    sqlEnds.foreach(processor.processSqlExecutionEnd)

    processor.report
  }

  def clear(): Unit = {
    Thread.`yield`()
    jobStarts.clear()
    stageSubmitted.clear()
    stageCompleted.clear()
    sqlStarts.clear()
    sqlAdaptiveExecutionUpdates.clear()
    sqlAdaptiveMetricsUpdates.clear()
    sqlDriverAccumUpdates.clear()
    executorMetricsUpdates.clear()
    taskEnds.clear()
    sqlEnds.clear()
  }
}

// Creates a listener object instance
object ExecutionEventListener {
  def create(sc: SparkContext): ExecutionEventListener = {
    val listener = new ExecutionEventListener
    sc.addSparkListener(listener)
    listener
  }
}

println(
  "Metrics listener loaded. Use ExecutionEventListener.create(sc) to start collecting metrics."
)
