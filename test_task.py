from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
from pyspark.sql.window import Window

customer_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('joinDate', DateType(), True),
    StructField('status', StringType(), True)
])

product_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('numberOfProducts', IntegerType(), True)
])

order_schema = StructType([
    StructField('customerID', IntegerType(), True),
    StructField('orderID', IntegerType(), True),
    StructField('productID', IntegerType(), True),
    StructField('numberOfProduct', IntegerType(), True),
    StructField('orderDate', DateType(), True),
    StructField('status', StringType(), True)
])

def top_product_for_customer(customer, product, order):
    
    #находим популярный продукт для клиента
    top_product = order \
        .where(order['status'] == 'delivered') \
        .groupBy(order['customerID'],order['productID']) \
        .agg(f.sum(order['numberOfProduct']).alias("numberOfProduct")) \
        .select(f.col('customerID'),f.col('productID'),f.col('numberOfProduct')) \
        .withColumn('rn', f.row_number().over(Window.partitionBy('customerID').orderBy(f.desc('numberOfProduct')))) \
        .where(f.col('rn') == 1) 
      
    #добавляем информацию о товаре и покупателе
    top_product_with_info = top_product \
        .join(customer, top_product['customerID']==customer['id'], how='left') \
        .select(f.col('name').alias('customerName'),f.col('productID')) \
        .join(product, top_product['productID']==product['id'], how='left') \
        .select(f.col('customerName'),f.col('name').alias('productName'))
    
    return top_product_with_info
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName('test_task').getOrCreate()
    
    customer = spark.read \
        .option("header", "false") \
        .schema(customer_schema) \
        .csv('customer.csv', sep='\t')

    product = spark.read \
        .option("header", "false") \
        .schema(product_schema) \
        .csv('product.csv', sep='\t')
    
    order = spark.read \
        .option("header", "false") \
        .schema(order_schema) \
        .csv('order.csv', sep='\t')     

    top_product_for_customer(customer, product, order) \
       .write.format("csv") \
       .option("header", "true") \
       .save("result.csv")
    