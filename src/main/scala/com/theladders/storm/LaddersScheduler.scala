package com.theladders.storm

import LaddersTopologyRules._
import backtype.storm.scheduler._
import java.util.{Map => JMap}
import org.slf4j.{LoggerFactory, Logger}

class LaddersScheduler extends IScheduler {
  private var logger: Logger = _

  def prepare(map: JMap[_, _]): Unit = {
    logger = LoggerFactory.getLogger(classOf[LaddersScheduler])
  }

  def schedule(topologies: Topologies,
               cluster: Cluster): Unit = {
    try {
      scheduleTopologies(topologies, cluster)
    }
    catch {
      case e: Exception => {
        logger.error("Error scheduling topologies...", e)
        logger.info("Letting EvenScheduler take over")
      }
    }

    // every other topology
    new EvenScheduler().schedule(topologies, cluster)
  }


  private def scheduleTopologies(topologies: Topologies,
                                 cluster: Cluster) {
    val scheduler = new HostBasedScheduler(cluster)
    rules.foreach(rule => {
      val topologyName = rule._1
      val hosts = rule._2

      val topology = topologies.getByName(topologyName)
      if (topology != null && cluster.needsScheduling(topology)) {
        logger.info(s"$topologyName: not scheduled. Scheduling")
        scheduler.scheduleOnHosts(topology, hosts)
      }
    })
  }
}
