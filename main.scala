import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
//import org.apache.spark.Row
import org.graphframes._
import org.apache.spark.sql.functions.{array, lit, when}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{array, collect_list}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

    val graph = GraphLoader.edgeListFile(sc, "file:///Users/charlie/Documents/eisti/spark/com-amazon.ungraph.txt")
	graph.vertices.take(10)
	val g2: GraphFrame = GraphFrame.fromGraphX(graph)
	val g = g2.labelPropagation.maxIter(5).run()
	g.write.parquet("vertices")
	
	val vertices = sqlContext.read.parquet("vertices")        // read back parquet to DF
	vertices.show() 
	
	val v = vertices.withColumn("id", vertices("id").cast(StringType))
	val id = collect_list($"id").alias("id")

	val v2 = v.groupBy($"label").agg(collect_list("id").alias("a"))
	v2.show()	
	val v3 = v2.select(concat_ws(" ", $"a").alias("a"))
	v3.write.format("com.databricks.spark.csv").option("header", "false").save("labelled5.csv")
	  