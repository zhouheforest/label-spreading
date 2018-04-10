package  cn.quantgroup.graph.semilpa

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeContext
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.TripletFields

/**
  * Implementation of the orignial label propagation by
  * Xiaojin zhu and Zoubin Ghahramani.
  * 《Learning from labeled and unlabeld data with label propagation》.
  * The basic idea of label propagation is that we start with seed users in a network who have some information about
  * the label probabilities. Following the weights in the network, we propagate the label probabilities from the seed
  * user into the network users. It will assign label probabilities for those unknown users. It has a wide application,
  * for example, in churn analysis, we want to know how likely an non-churner will churn if he has a few churner friends.
  * The label propagation algorithm is described as follows:

    1.All nodes propagate labels for one step :Y = TY (T:probabilistic transition matrix based on edge weight)
    2.Row-normalize Y to maintain the class probability interpretation
    3.Clamp the labeled data. Repeat from step 1 until Y converges or max iter.

  */

class LPZhuGhahramani(g:Graph[LPVertex, Double], self_weight:Double=1.0, undirected:Boolean=false, labels:Array[String]) extends java.io.Serializable{
  /**
    * Run label propagation for a fixed number of iterations
    * returning a graph with vertex attributes containing the estimated label probabilities.
    * @param maxIter The number of iteration the label progation to run
    * @return
    */
  def run(maxIter:Int):Graph[LPVertex, Double] = {
    //initialize graph vertices
    val initializer = new VertexInitializer()
    val verts = initializer(g, labels)

    //update the non-seed node estimated labels and keep the seed node estimated labels same as the injected labels
    def updateVertexEstimatedLabes(vert:LPVertex, msgSum:Map[String, Double]): LPVertex = {
      if(vert.isSeedNode){
        vert.estimatedLabels = vert.injectedLabels
      }else{
        var estimatedLabels = vert.estimatedLabels
        estimatedLabels = estimatedLabels.map(x => x._1 -> x._2 * self_weight)
        vert.estimatedLabels = estimatedLabels ++ msgSum.map({case(k,v) => k -> (v + estimatedLabels.getOrElse(k, 0.0))}) //step 1
        vert.estimatedLabels = initializer.normalize(vert.estimatedLabels) //step 2
      }
      vert
    }

    //send messages over undirected graph
    def sendMsg2(et:EdgeContext[LPVertex, Double, Map[String, Double]]){
      et.sendToSrc(et.dstAttr.estimatedLabels.map(x => (x._1, x._2 * et.attr)))
      et.sendToDst(et.srcAttr.estimatedLabels.map(x => (x._1, x._2 * et.attr)))
    }

    //send messages over directed graph
    def sendMsg(et:EdgeContext[LPVertex, Double, Map[String, Double]]){
      et.sendToDst(et.srcAttr.estimatedLabels.map(x => (x._1, x._2 * et.attr)))
    }

    //merge messages
    def mergeMsg(m1:Map[String, Double], m2:Map[String,Double]):Map[String,Double] = {
      m1 ++ m2.map({case(k,v) => k -> (v + m1.getOrElse(k, 0.0))})
    }

    var rankGraph = g.outerJoinVertices(verts){case(vid, oldVert, newVert) => newVert.getOrElse(oldVert)}

    var iteration = 0
    var prevRankGraph:Graph[LPVertex, Double] = null

    while(iteration < maxIter){
      rankGraph.cache()

      var rankUpdates:VertexRDD[Map[String,Double]] = null
      if(undirected){
        rankUpdates = rankGraph.aggregateMessages[Map[String,Double]](sendMsg2, mergeMsg)
      }else{
        rankUpdates = rankGraph.aggregateMessages[Map[String,Double]](sendMsg, mergeMsg, TripletFields.Src)
      }
      prevRankGraph = rankGraph

      //recover the injected labels to estimated labels for the seed nodes and adjust values
      rankGraph = rankGraph.joinVertices(rankUpdates){
        (id, vert, msgSum) => updateVertexEstimatedLabes(vert, msgSum)
      }.cache()

      rankGraph.vertices.foreachPartition(x => {}) //materializeds rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)
      iteration += 1
    }
    rankGraph
  }

  /*

  def runUntilConvergence(tol:Double):Graph[LPVertex, Double] = {
    //initialize graph vertices
    val initializer = new VertexInitializer()
    val verts = initializer(g, labels)
    var rankGraph =
      g.outerJoinVertices(verts){case(vid, oldVert, newVert) => newVert.getOrElse(oldVert)}
        .mapVertices((id, attr) => (attr, 0.0))
    //define the vertex program function needed to implement label propagation in the graph version of Pregel
    def vertexProgram(id:VertexId, attr:(LPVertex, Double), msgSum:Map[String, Double]):(LPVertex, Double) = {
      val (oldEstimatedLabels, lastDelta) = (attr._1.estimatedLabels, attr._2)
      val newEstimatedLabels = oldEstimatedLabels ++ msgSum.map({case(k,v) => k -> (v+oldEstimatedLabels.getOrElse(k, 0.0))})
      val newEstimatedLabelsNorm = initializer.normalize(newEstimatedLabels)
      val delta = differenceNorm2Squarred(oldEstimatedLabels, 1.0, newEstimatedLabelsNorm, 1.0)
      //clamp the seed node
      if(attr._1.isSeedNode){
        attr._1.estimatedLabels = attr._1.injectedLabels
      }else{
        attr._1.estimatedLabels = newEstimatedLabelsNorm
      }
      (attr._1, delta)
    }

    //send messages over undirected graph
    def sendMsg(edge:EdgeTriplet[(LPVertex, Double), Double]) = {
      if(edge.srcAttr._2 > tol) {
        Iterator((edge.srcId, edge.dstAttr._1.estimatedLabels.map(x => (x._1, x._2*edge.attr))),
          (edge.dstId, edge.srcAttr._1.estimatedLabels.map(x => (x._1, x._2*edge.attr))))
      }else{
        Iterator.empty
      }
    }

    //send messages over directed graph
    def sendMsg2(edge:EdgeTriplet[(LPVertex, Double), Double]) = {
      if(edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._1.estimatedLabels.map(x => (x._1, x._2*edge.attr))))
      }else{
        Iterator.empty
      }
    }

    //combine two messages
    def messageCombiner(m1:Map[String, Double], m2:Map[String,Double]):Map[String,Double] = {
      m1 ++ m2.map({case(k,v) => k -> (v + m1.getOrElse(k, 0.0))})
    }

    //initial message received by all vertices in Label propagation
    val initialMessage = Map[String,Double]()
    //execute a dynamic version of Pregel
    if(undirected){
      Pregel(rankGraph, initialMessage, activeDirection = EdgeDirection.Either)(
        vertexProgram, sendMsg, messageCombiner)
        .mapVertices((vid, attr) => attr._1)
    }else{
      Pregel(rankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
        vertexProgram, sendMsg2, messageCombiner)
        .mapVertices((vid, attr) => attr._1)
    }
  }

  def differenceNorm2Squarred(m1:Map[String,Double], m1Mult:Double, m2:Map[String,Double], m2Mult:Double):Double = {
    var diffMap = m1.map(x => (x._1, x._2*m1Mult))
    var tmp = m2.map(x => (x._1, x._2*m2Mult))
    diffMap = diffMap ++ tmp.map({case(k,v) => k -> (v-diffMap.getOrElse(k,0.0))})
    var sum = 0.0
    diffMap.foreach(p => sum += p._2 * p._2)
    math.sqrt(sum)
  }

  */

}