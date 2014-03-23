package com.theladders.storm

import backtype.storm.scheduler.{ExecutorDetails, Cluster, TopologyDetails}
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

class ExecutorManager(cluster: Cluster) extends Interleaver {
  private val logger = LoggerFactory.getLogger(classOf[ExecutorManager])

  def getExecutorsToAssignPerSlot(topology: TopologyDetails): List[List[ExecutorDetails]] = {
    groupExecutorsPerSlot(topology)
  }

  /* group executors that should be assigned per slot */
  private def groupExecutorsPerSlot(topology: TopologyDetails): List[List[ExecutorDetails]] = {
    val executors = getExecutorsByWorkerByComponent(topology)
    logger.debug("executor groups: " + executors.map(_.size).mkString(","))

    val componentsWithExecutorsForOnlyOneWorker = executors.filter(_.size == 1).map(_.head)
    logger.debug("componentsWithExecutorsForOnlyOneWorker: " + componentsWithExecutorsForOnlyOneWorker.map(_.size).mkString(","))
    val componentsWithExecutorsForMoreThanOneWorker = executors.filter(_.size > 1)
    logger.debug("componentsWithExecutorsForMoreThanOneWorker groups: " + componentsWithExecutorsForMoreThanOneWorker.map(_.size).mkString(","))

    val interleavedExecutors = interleave(componentsWithExecutorsForMoreThanOneWorker).map(_.flatten)
    logger.debug("interleaved executor groups: " + interleavedExecutors.map(_.size).mkString(","))
    logger.debug("interleaved executor groups: \r\n\t" + interleavedExecutors.map(_.toString).mkString("\r\n\t"))

    val results = interleavedExecutors
      .zip(componentsWithExecutorsForOnlyOneWorker)
      .map(t => {t._1 ++ t._2})

    results
  }


  private def getExecutorsByWorkerByComponent(topology: TopologyDetails): List[List[List[ExecutorDetails]]] = {
    val executorCalculator = executorsPerWorker(_: Int, topology)

    cluster.getNeedsSchedulingComponentToExecutors(topology).asScala
      .values.toList
      .map(_.asScala.toList)
      .map(e => (e, executorCalculator(e.size))) // calc # of executors to be assigned per worker
      .map(pair => pair._1.grouped(pair._2).toList) // partition executors into groups of size assignable to a worker
  }

  private def executorsPerWorker(totalExecutors: Int,
                                 topology: TopologyDetails): Int = {
    val workersToAssign = topology.getNumWorkers - cluster.getAssignedNumWorkers(topology)
    val executorsPerWorker = math.ceil(totalExecutors.toDouble / workersToAssign.toDouble).toInt
    math.max(1, executorsPerWorker)
  }

}
