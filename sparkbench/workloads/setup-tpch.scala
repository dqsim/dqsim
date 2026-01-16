import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs.{FileSystem, Path}

val DATA_PATH = "/data/tpch-data"
val WAREHOUSE_PATH = "/data/spark-warehouse"
val NUM_BUCKETS = 60
val DATABASE = "tpch"

// Create database if not exists
spark.sql(s"CREATE DATABASE IF NOT EXISTS $DATABASE")
spark.sql(s"USE $DATABASE")

// Enable bucketing optimizations
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

// Get Hadoop FileSystem for physical cleanup
val hadoopConf = spark.sparkContext.hadoopConfiguration
val fs = FileSystem.get(hadoopConf)

// Helper function to drop table and remove physical data
def cleanupTable(tableName: String): Unit = {
  // Drop from metastore
  spark.sql(s"DROP TABLE IF EXISTS $tableName")
  
  // Remove physical directory
  val tablePath = new Path(s"$WAREHOUSE_PATH/$DATABASE.db/$tableName")
  if (fs.exists(tablePath)) {
    fs.delete(tablePath, true)
    println(s"  -> Dropped $tableName (metadata + data)")
  } else {
    println(s"  -> Dropped $tableName (metadata only, no data dir)")
  }
}

// Helper function to create hash-bucketed table with compaction (one file per bucket)
def createBucketedTable(
    df: DataFrame,
    tableName: String,
    bucketCol: String,
    numBuckets: Int
): Unit = {
  println(s"Creating bucketed table $tableName with $numBuckets buckets on $bucketCol...")
  // Repartition by bucket column to ensure one file per bucket
  df.repartition(numBuckets, col(bucketCol))
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .bucketBy(numBuckets, bucketCol)
    .sortBy(bucketCol)
    .saveAsTable(tableName)
  println(s"  -> $tableName created with ${spark.table(tableName).count()} rows")
}

// Helper function to create broadcast (non-bucketed) table
def createBroadcastTable(
    df: DataFrame,
    tableName: String
): Unit = {
  println(s"Creating broadcast table $tableName (single partition)...")
  df.coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .saveAsTable(tableName)
  println(s"  -> $tableName created with ${spark.table(tableName).count()} rows")
}

// Read parquet files with schema inference
println("Reading TPC-H parquet files (using schema inference)...")
val customerDf = spark.read.parquet(s"$DATA_PATH/customer.parquet")
val nationDf = spark.read.parquet(s"$DATA_PATH/nation.parquet")
val ordersDf = spark.read.parquet(s"$DATA_PATH/orders.parquet")
val partDf = spark.read.parquet(s"$DATA_PATH/part.parquet")
val partsuppDf = spark.read.parquet(s"$DATA_PATH/partsupp.parquet")
val regionDf = spark.read.parquet(s"$DATA_PATH/region.parquet")
val supplierDf = spark.read.parquet(s"$DATA_PATH/supplier.parquet")
val lineitemDf = spark.read.parquet(s"$DATA_PATH/lineitem.parquet")

// Print inferred schemas
println("\nInferred schemas:")
println("customer:"); customerDf.printSchema()
println("nation:"); nationDf.printSchema()
println("orders:"); ordersDf.printSchema()
println("part:"); partDf.printSchema()
println("partsupp:"); partsuppDf.printSchema()
println("region:"); regionDf.printSchema()
println("supplier:"); supplierDf.printSchema()
println("lineitem:"); lineitemDf.printSchema()

// Drop existing tables and remove physical data to ensure clean setup
println("\nCleaning up existing tables (metadata + physical data)...")
Seq("lineitem", "orders", "customer", "partsupp", "part", "supplier", "nation", "region").foreach { table =>
  cleanupTable(table)
}

println("\nCreating hash-bucketed Hive tables...")
println("=" * 60)

// Hash-partitioned tables (60 buckets)
createBucketedTable(lineitemDf, "lineitem", "l_orderkey", NUM_BUCKETS)
createBucketedTable(ordersDf, "orders", "o_orderkey", NUM_BUCKETS)
createBucketedTable(customerDf, "customer", "c_custkey", NUM_BUCKETS)
createBucketedTable(partsuppDf, "partsupp", "ps_partkey", NUM_BUCKETS)
createBucketedTable(partDf, "part", "p_partkey", NUM_BUCKETS)
createBucketedTable(supplierDf, "supplier", "s_suppkey", NUM_BUCKETS)

// Broadcast tables (single partition, no bucketing)
createBroadcastTable(nationDf, "nation")
createBroadcastTable(regionDf, "region")

println("=" * 60)
println(s"\nTPC-H tables created successfully in database '$DATABASE'")
println("\nTable summary:")
spark.sql("SHOW TABLES").show()

println("\nBucketing info for 'lineitem' table:")
spark.sql("DESCRIBE EXTENDED lineitem").show(100, truncate = false)