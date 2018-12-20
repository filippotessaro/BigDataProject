import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point, Polygon
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# Load the shapefile and change crs
shape = gpd.read_file(shapefile_path)
shape.plot()
shape['geometry'] = shape['geometry'].to_crs({'init': 'epsg:4326'})
shape.plot()

# Define function for getting the location ID from (longitude, latitude) coordinates
def findID(longitude, latitude):
    point = Point(longitude, latitude)
    matches = shape[shape['geometry'].contains(point)]['LocationID']
    if len(matches) > 0:
        return matches.iloc[0].item()
    else:
        # Here you should return the value of a wrong location (264 or something like this?)
        return -1

# Read the taxi data
df = spark.read.csv(taxidata_path, header=True, inferSchema=True)
test = df.select('pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude’)

# Define a udf that uses the function for getting the location ID from longitude and latitude
coord2locUDF = udf(lambda long, lat: findID(x, y), IntegerType())

# Apply the udf to pickup and dropoff coordinates
converted = test.withColumn('pickup_id', coord2locUDF(test['pickup_longitude'], test['pickup_latitude']))
converted = converted.withColumn('dropoff_id', coord2locUDF(test['dropoff_longitude'], test['dropoff_latitude']))\
converted.show()
