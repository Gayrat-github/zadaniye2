from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Product_Categories").getOrCreate()

products_path = "./data/products.csv"
cat_path = "./data/categories.csv"
p_c_path = "./data/product_cats.csv"

products = spark.read.csv(products_path, header=True)
cat = spark.read.csv(cat_path, header=True)
p_c = spark.read.csv(p_c_path, header=True)
data = products.join(p_c, "product_id", "left").join(cat, "cat_id", "left").select("product_name", "category_name").orderBy("product_name")
data.show()

spark.stop()