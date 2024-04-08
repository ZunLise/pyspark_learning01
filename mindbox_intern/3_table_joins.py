from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("get product categories") \
    .getOrCreate()

# get data from csv files
products_raw = spark.read.csv('products.csv', header=True, inferSchema=True, escape="\"")
categories_raw = spark.read.csv('categories.csv', header=True, inferSchema=True, escape="\"")
connections_raw = spark.read.csv('connections.csv', header=True, inferSchema=True, escape="\"")

# Create a DataFrame from the sample data
products = spark.createDataFrame(products_raw, ['product_id', 'product_name'])
categories = spark.createDataFrame(categories_raw, ['category_id', 'category_name'])
connections = spark.createDataFrame(connections_raw, ['connection_id', 'product_id', 'category_id'])

# create big dataframe
# it has all products (even those with no connections, but not all categories
working_with = connections.join(categories, 'category_id', 'inner').join(products, 'product_id', 'right')

# all that's left is to get all product-category pairs
working_with.select('product_name', 'category_name').orderBy('product_name').show()






# Group by 'name' and calculate the average age
# grouped_df = df.groupBy('name').agg({'age': 'avg'})

# Show the result
# grouped_df.show()