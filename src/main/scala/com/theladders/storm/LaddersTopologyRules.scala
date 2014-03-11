package com.theladders.storm

object LaddersTopologyRules {
  private val largeNodes = (1 to 8).map(x => s"storm-large-$x.idc.theladders.com").toSet

  /*
   * Map of topology name to allowed hosts under which that toplogy can run
   *
   * Current implementation only supports one specifc topology within a specific node (or set of nodes). For e.g.
   * storm-document-indexer is the only topology that can be required to run within storm-large-* nodes using this
   * scheduler. That does not mean other topologies can't or won't be scheduled on those nodes, it's just that other
   * topologies can't be mandated to run on those nodes. That's because this representation of rules does not
   * encapsulate any priority so if two topologies are scheduled to run on those same node(s), they will end up
   * taking each other out indefinitely.
   */
  val rules: Map[String, Set[String]] = {
    Map[String, Set[String]](
      "storm-document-indexer" -> largeNodes
    )
  }
}
