from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Product_Categories").getOrCreate()

products_path = "./data/products.csv"
cat_path = "./data/categories.csv"
p_c_path = "./data/product_cats.csv"

products = spark.read.csv(products_path, header=True)
cat = spark.read.csv(cat_path, header=True)
p_c = spark.read.csv(p_c_path, header=True)
products.createOrReplaceTempView("products_view")
cat.createOrReplaceTempView("cat_view")
p_c.createOrReplaceTempView("pc_view")

data = spark.sql("select p.product_name, c.category_name from products_view p left join pc_view pc on p.product_id = pc.product_id left join cat_view c on pc.cat_id = c.cat_id order by p.product_name")
data.show()

spark.stop()