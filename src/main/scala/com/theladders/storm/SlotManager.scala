package com.theladders.storm

import backtype.storm.scheduler.{Cluster, SupervisorDetails, WorkerSlot}
import scala.collection.JavaConverters._

class SlotManager(cluster: Cluster) extends Interleaver {

  /* interleave slots so that slots of different nodes are given priority during assignment of topology as opposed to slots of the same node */
  def interleaveSlotsByNode(slots: Set[WorkerSlot]): List[WorkerSlot] = {
    val slotsGroupedByNode = slots.groupBy(_.getNodeId)
      .values
      .toList.map(_.toList) // turn to list of lists instead of an iterable of sets (to give an order)

    val interleavedSlots = interleaveFlattened(slotsGroupedByNode)
    interleavedSlots
  }

  def freeSlots(suitableSupervisors: Set[SupervisorDetails],
                slotsNeeded: Int): Unit = {
    // pick slots to free up
    val freeableSlots = suitableSupervisors.map(s => {
      val usedPortsOnSupervisor = cluster.getUsedPorts(s).asScala
      usedPortsOnSupervisor.map(port => new WorkerSlot(s.getId, port))
    }).flatten
    // try to pick slots from different nodes if you can
    val interleavedFreeableSlots = interleaveSlotsByNode(freeableSlots)
    val slotsToFree = interleavedFreeableSlots.take(slotsNeeded)
    // free slots
    slotsToFree.foreach(cluster.freeSlot)
  }

  def getAvailableSlots(suitableSupervisors: Set[SupervisorDetails]): Set[WorkerSlot] = {
    suitableSupervisors.flatMap(s => cluster.getAvailableSlots(s).asScala.toList)
  }
}
