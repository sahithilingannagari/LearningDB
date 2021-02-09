# system Dependencies

import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Intialise Spark Session
spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
    
# Read Data from Kafka
lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("startingOffsets", "latest") \
	.option("subscribe","real-time-project")  \
	.option("failOnDataLoss", False) \
	.load() 
	

kafkaDF = lines.selectExpr("cast(value as string)")	

# Defining schema for order
schema = StructType()	\
      .add("invoice_no",LongType()) \
      .add("country",StringType()) \
      .add("timestamp", TimestampType()) \
      .add("type",StringType()) \
      .add("items",ArrayType(StructType([StructField("SKU", StringType()),StructField("title", StringType()),StructField("unit_price",DoubleType()),StructField("quantity",IntegerType())])))



invoiceDF = kafkaDF.select(from_json(col("value"),schema).alias("data")).select("data.*")

# Function for calculating total value for a single order
def get_total_order_value(items,order_type):
    cost=0
    type_multiplier=1 if order_type=='ORDER' else -1
    for item in items:
        cost=cost+(item['unit_price']*item['quantity'])
    
    return cost*type_multiplier

# Function to get the total number of items in a single order
def get_total_item_count(items):
    total_count=0
    for item in items:
        total_count=total_count+item['quantity']
        
    return total_count

# Function for is_order flag value for a single order
def get_order_flag(order_type):
    return 1 if order_type=='ORDER' else 0


# Function for is_return value for a single order
def get_return_flag(order_type):
    return 1 if order_type=='RETURN' else 0 

# Define the UDF's with above functions
add_total_order_value=udf(get_total_order_value,DoubleType())
add_total_item_count=udf(get_total_item_count,IntegerType())
add_order_flag=udf(get_order_flag,IntegerType())
add_return_flag=udf(get_return_flag,IntegerType())

#Adding the additional caluculated columns
FinalDF=invoiceDF\
        .withColumn("total_cost",add_total_order_value(invoiceDF.items,invoiceDF.type))  \
        .withColumn("total_items",add_total_item_count(invoiceDF.items))  \
        .withColumn("is_order",add_order_flag(invoiceDF.type))  \
        .withColumn("is_return",add_return_flag(invoiceDF.type))
                    

query = FinalDF  \
    .select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return")	\
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
    .option("truncate","false")		\
    .trigger(processingTime="1 minute")  \
	.start()
	


# Calculate the time based KPI's
TimeStreamDF=FinalDF  \
           .withWatermark("timestamp","1 minute") \
           .groupBy(window("timestamp","1 minute","1 minute"))  \
           .agg(sum("total_cost"),
                avg("total_cost"),
                count("invoice_no").alias("OPM"),
                avg("is_return"))  \
           .select("window",
                   "OPM",
                   format_number("sum(total_cost)",2).alias("total_sale_volume"),
                   format_number("avg(total_cost)",2).alias("average_transaction_size"),
                   format_number("avg(is_return)",2).alias("rate_of_return"))

# Calculate the time and country based KPI's
TimeCountryStreamDF=FinalDF  \
           .withWatermark("timestamp","1 minute") \
           .groupBy(window("timestamp","1 minute","1 minute"),"country")  \
           .agg(sum("total_cost"),
                count("invoice_no").alias("OPM"),
                avg("is_return"))  \
           .select("window",
                   "country",
                   "OPM",
                   format_number("sum(total_cost)",2).alias("total_sale_volume"),
                   format_number("avg(is_return)",2).alias("rate_of_return"))

# Writing time based KPI values to json files
querybyTime = TimeStreamDF  \
    .select("window","OPM","total_sale_volume","average_transaction_size","rate_of_return")  \
	.writeStream  \
	.outputMode("append")  \
	.format("json")  \
    .option("truncate","false")		\
    .option("path","/user/root/outputFiles")	\
    .option("checkpointLocation","/user/root/checkpoint1")	\
    .trigger(processingTime="1 minute")  \
	.start()

# Writing time and country based KPI values to json files
querybyTimeandCountry = TimeCountryStreamDF  \
    .select("window","country","OPM","total_sale_volume","rate_of_return")  \
	.writeStream  \
	.outputMode("append")  \
	.format("json")  \
    .option("truncate","false")		\
    .option("path","/user/root/outputFiles1")	\
    .option("checkpointLocation","/user/root/checkpoint2")	\
    .trigger(processingTime="1 minute")  \
	.start()
    
querybyTimeandCountry.awaitTermination()
