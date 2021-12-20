# Databricks notebook source
storage_account_name = "myaccount1996"
client_id = "13246d0c-3bea-401b-be7e-bbcb120ce672"
tenant_id = "7857aa44-5e72-40ff-a58f-50ef9e2aca63"
client_secret = "ak57Q~prIUiE5y4kRnPjGgfYZ-wCUSxruUquM"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------


container_name = "hotel-11"
dbutils.fs.unmount(mount_point = f"/mnt/{storage_account_name}/weather-new-data")
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/myaccount1996/hotel-11")

# COMMAND ----------

file_location = "/mnt/myaccount1996/hotel-11/*.csv"

raw = spark.read.csv(file_location,inferSchema = True, header = True)
raw.show(10)

# COMMAND ----------

raw.createOrReplaceTempView("Hotel_data")
count = spark.sql(" select count(*) as count from Hotel_data ")
count.show()

# COMMAND ----------

invalid_data = spark.sql("select * from Hotel_data where Latitude is null or Longitude is null or Latitude rlike 'NA' or Longitude rlike 'NA' ")
invalid_data.show()
invalid_data.count()

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pygeohash

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install opencage

# COMMAND ----------

from opencage.geocoder import OpenCageGeocode
import pygeohash as geohash

key = '77fba6afd5864138a5f575eb3389cb4c'
geocoder = OpenCageGeocode(key)

def geo_lat_lon(name, address, city, country, tag):
    value = geocoder.geocode("{},{},{},{}".format(name,address,city,country))
    return (value[0]['geometry'][tag])

lat_lon = udf(geo_lat_lon)


# COMMAND ----------

def hashfunc(lat,long) : 
    if lat is None or long is None:
      lat = 0.0
      long =0.0
    return (geohash.encode(lat,long,precision=5))

geohash_udf = udf(hashfunc)

# COMMAND ----------

from pyspark.sql.functions import col,lit
modified_data = invalid_data.withColumn("lat", lat_lon(col("name"),col("address"),col("city"),col("country"),lit("lat"))) \
                            .withColumn("long", lat_lon(col("name"),col("address"),col("city"),col("country"),lit("lng"))) \
                            .drop('Latitude','Longitude')
                       
modified_data.show()
                    

# COMMAND ----------

hotel_valid_data = raw.subtract(invalid_data)
hotel_valid_data.count()

# COMMAND ----------

Hotel_data = hotel_valid_data.union(modified_data)
display(Hotel_data)

# COMMAND ----------

Hotel_data.count()

# COMMAND ----------

Hotel_final = Hotel_data.withColumn("geohash",geohash_udf(col("latitude").cast("float"),col("longitude").cast("float")))
display(Hotel_final)

# COMMAND ----------

Hotel_final.write.format('jdbc').options(url='jdbc:sqlserver://synapse1196.sql.azuresynapse.net:1433;database=snynapsepool',dbtable = 'Hotel_Table',user = 'sqladminuser@synapse1196', password = 'Sagar@2021').mode('append').save()


# COMMAND ----------

dbutils.fs.unmount(mount_point = f"/mnt/{storage_account_name}/hotel-11")
container_name = "weather-new-data"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

file_location = "/mnt/myaccount1996/weather-new-data/"

raw = spark.read.parquet(file_location,inferSchema = True, header = True)
raw_df=raw.limit(20000)
raw_df.count()

# COMMAND ----------

display(raw_df)

# COMMAND ----------

weather_df = raw_df.withColumn(("geohash_weather"),geohash_udf(col("lng").cast("float"),col("lat").cast("float")))
weather_df.show(100,False)


# COMMAND ----------

#weather_df.write.format('jdbc').options(url='jdbc:sqlserver://synapse1196.sql.azuresynapse.net:1433;database=snynapsepool',dbtable = 'weather_data',user = 'sqladminuser@synapse1196', password = 'Sagar@2021').mode('append').save()

# COMMAND ----------

weather_final = spark.read.format('jdbc').options(url='jdbc:sqlserver://synapse1196.sql.azuresynapse.net:1433;database=snynapsepool',dbtable = 'weather_data',user = 'sqladminuser@synapse1196', password = 'Sagar@2021').load()

# COMMAND ----------

display(weather_final)

# COMMAND ----------

hotel_final_1 = spark.read.format('jdbc').options(url='jdbc:sqlserver://synapse1196.sql.azuresynapse.net:1433;database=snynapsepool',dbtable = 'Hotel_Table',user = 'sqladminuser@synapse1196', password = 'Sagar@2021').load()

# COMMAND ----------

display(hotel_final_1)

# COMMAND ----------

condition = [(hotel_final_1['geohash'] == weather_final['geohash']) \
            | (hotel_final_1['geohash'][1:4] == weather_final['geohash'][1:4]) \
            | (hotel_final_1['geohash'][1:3] == weather_final['geohash'][1:3])
            ]

# COMMAND ----------

hotel_weather_df = hotel_final_1.join(weather_final, condition, "inner")
hotel_weather_df.show(100,False)

# COMMAND ----------

hotel_weather_precision = hotel_weather_df.withcolumn('precision' \
                                                     when(col))
