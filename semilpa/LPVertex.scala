package  cn.quantgroup.graph.semilpa

class LPVertex extends java.io.Serializable{
  //labels and scores which are injected in the node
  var injectedLabels = Map[String, Double]()
  //labels and scores which are estimated by the algorithm
  var estimatedLabels = Map[String,Double]()
  // set to true if the nodes is injected with seed labels
  var isSeedNode = false
}