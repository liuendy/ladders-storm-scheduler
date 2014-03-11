package com.theladders.storm

import backtype.storm.scheduler.{SupervisorDetails, WorkerSlot, TopologyDetails, Cluster}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class HostBasedScheduler(cluster: Cluster) {
  private val logger = LoggerFactory.getLogger(classOf[HostBasedScheduler])
  private val Executors = new ExecutorManager(cluster)
  private val Slots = new SlotManager(cluster)

  def scheduleOnHosts(topology: TopologyDetails,
                      hosts: Set[String]): Unit = {
    val suitableSupervisors = lookupSupervisorsByHosts(hosts)
    if (suitableSupervisors.isEmpty) {
      logger.info(s"Topology ${topology.getName} could not be scheduled as no suitable supervisors found (${hosts.toString()}})")
      return
    } else {
      logger.info(s"Found suitable supervisors to schedule on ${suitableSupervisors.map(_.getHost).toString()}")
    }

    val numberOfSlotsNeeded = topology.getNumWorkers - cluster.getAssignedNumWorkers(topology)
    if (numberOfSlotsNeeded == 0) {
      logger.info(s"Topology ${topology.getName} has all it workers already assigned")
      return
    }

    val availableSlots = Slots.getAvailableSlots(suitableSupervisors)
    if (availableSlots.size >= numberOfSlotsNeeded) {
      logger.info("Enough slots available.")
      assign(topology, availableSlots, numberOfSlotsNeeded)
    } else {
      logger.warn(s"Not enough slots. Attempting to free upto ${numberOfSlotsNeeded} slots")
      Slots.freeSlots(suitableSupervisors, numberOfSlotsNeeded)
      val newlyAvailableSlots = Slots.getAvailableSlots(suitableSupervisors)
      if (newlyAvailableSlots.size >= numberOfSlotsNeeded) {
        logger.info("Was able to free up slots")
        assign(topology, newlyAvailableSlots, numberOfSlotsNeeded)
      } else {
        logger.warn(s"Could not free up enough slots. ${topology.getName} not scheduled")
      }
    }
  }

  private def assign(topology: TopologyDetails,
                     availableSlots: Set[WorkerSlot],
                     numberOfSlotsNeeded: Int) {
    val executorsBySlot = Executors.getExecutorsToAssignPerSlot(topology)
    logger.debug("executors per slot: " + executorsBySlot.map(_.size.toString).mkString(","))
    val slotsToAssign = Slots.interleaveSlotsByNode(availableSlots).take(numberOfSlotsNeeded)
    logger.debug("slots: " + slotsToAssign.size)
    val pairExecutorsWithSlots = executorsBySlot.zip(slotsToAssign)
    logger.debug("paired: \r\n\6" + pairExecutorsWithSlots.map(x => x._1.size + "->" + x._2.toString).mkString("\r\n\t"))
    pairExecutorsWithSlots.foreach(e => {
      val executors = e._1
      val slot = e._2
      logger.info(s"assigning ${executors.size} to slot ${slot.toString}")
      cluster.assign(slot, topology.getId, executors.asJavaCollection)
    })
    logger.info(s"${topology.getName} assignment complete (${slotsToAssign.size}).")
  }

  private def lookupSupervisorsByHosts(hosts: Set[String]): Set[SupervisorDetails] = {
    try {
      hosts.flatMap(host => cluster.getSupervisorsByHost(host).asScala.toList)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Set.empty
      }
    }
  }
}
