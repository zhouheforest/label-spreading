package  cn.quantgroup.graph.semilpa

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

//import org.apache.spark.graphx.GraphLoader

object Main {

  def main(args:Array[String]) = {
    val graphFile = "/user/he.zhou/data/edgefile_weight_322_2.txt"
    val seedFile = "/user/he.zhou/data/seedfile_black_and_white.txt"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val output_path = "/user/he.zhou/output/LPA_3_22_2"

    val users:RDD[(Long, String, Double)] = sc.textFile(seedFile).map{ x =>
      val parts = x.split("\t")
      (parts(0).toLong, parts(1), parts(2).toDouble)
    }

    //val edges = GraphLoader.edgeListFile(sc, graphFile) // directed???
    val edges = sc.textFile(graphFile).map{ x =>
      val item = x.split("\t")
      Edge(item(0).toLong, item(1).toLong, item(2).toDouble)
    }


  /*
    val edges = sc.parallelize(Array(
      Edge(11111111111111L, 22222222222222L, 1.5),
      Edge(44444444444444L, 33333333333333L, 1.0),
      Edge(44444444444444L, 55555555555555L, 1.0),
      Edge(55555555555555L, 44444444444444L, 2.0),
      Edge(44444444444444L, 66666666666666L, 3.0),
      Edge(11111111111111L, 55555555555555L, 2.0),
      Edge(55555555555555L, 11111111111111L, 1.5),
      Edge(55555555555555L, 22222222222222L, 1.0)
    ))

    val users:RDD[(Long, String, Double)] = sc.parallelize(Array(
      (11111111111111L, "white", 1.0),
      (44444444444444L, "white", 0.0),
      (11111111111111L, "black", 0.0),
      (44444444444444L, "black", 1.0)
    ))
  */

    val labelsArr = users.map(x => (x._2, 1)).reduceByKey(_+_).map(x => x._1).collect() //？？？
    val verts = users.map(x => (x._1, (x._2, x._3))).groupByKey().map(x => {
      val vert = new LPVertex
      vert.isSeedNode =true
      vert.injectedLabels = x._2.toMap
      (x._1, vert)
    })

    val graph = Graph.fromEdges(edges, 1).outerJoinVertices(verts){
      (vid, vdata, vert) => if(vert eq None){val v = new LPVertex; v} else vert.get}
    val lp = new LPZhuGhahramani(graph, undirected=false, labels = labelsArr)
    val rankGraph = lp.run(5)   //lp.runUntilConvergence(0.0001)
    /*
    rankGraph.vertices.collect().foreach(x =>
      println(x._1 + ": " + x._2.estimatedLabels.mkString(","))
    )
    */
    rankGraph.vertices.map(x =>
      x._1 + " " + x._2.estimatedLabels.mkString(" ")
    ).repartition(1).saveAsTextFile(output_path)

   // rankGraph.vertices.collect().map(x => x._1 + "\t" + x._2.estimatedLabels).saveAsTextFile(output_path)
  }
/*
  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]): RDD[(String, Long)] = triplets.flatMap {
    case (src, dst, weight) =>
    Try(Array(src, dst)).getOrElse(Array.empty[String])
  }.distinct().zipWithIndex()
*/
}