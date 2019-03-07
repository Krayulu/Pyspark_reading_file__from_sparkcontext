from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

spark = (SparkSession
    .builder
    .appName("Spark Order store master event processing")
    .getOrCreate())
	
sc = spark.sparkContext
input_path = 'D:\My learnings\PJI\sample\global-order_store_order.csv'
output_path = 'D:\My learnings\PJI\sample\master-order_store_order.csv'

#Reading the input file to a RDD
RDD = sc.textFile(input_path)

rec = RDD.map(lambda l : l.split(','))

orders = rec.map(lambda r: Row(insert_timestamp=r[0],event_id=r[0],business_date=r[1],
                               store_order_number=r[2],event_type=r[3],business_date_order_taken=r[4],
                               business_time_order_taken=r[5],tax_amount=r[6],discount_amount=r[7],
                               subtotal_amount=r[8],event_timestamp=r[9],makeline_print_date=r[10]))
							   
#Converting RDD to Datafram 							   
df = spark.createDataFrame(orders)

#Creating a Temp view table which is binded to current spark session
df.createOrReplaceTempView("global_order_store_order_view")


#Removing Duplicates based on combination of (event_id,event_type) over event_timestamp and creating 'master' data frame
master = spark.sql("SELECT insert_timestamp, a.event_id, business_date, a.store_order_number,a.event_type,business_date_order_taken,tax_amount, discount_amount, subtotal_amount,  event_timestamp, makeline_print_date from (SELECT insert_timestamp, event_id, business_date, store_order_number,event_type,business_date_order_taken,tax_amount, discount_amount, subtotal_amount,  event_timestamp, makeline_print_date, ROW_NUMBER() OVER (PARTITION BY event_id, event_type ORDER BY event_timestamp DESC ) AS index from global_order_store_order_view ) a where a.index=1")

#Writing output single file instead of creating mutiple out files
master.coalesce(1).write.format('com.databricks.spark.csv').save(output_path,header = 'true')
#either obove line over below line can be used
master.coalesce(1).write.csv(output,header = 'true')	