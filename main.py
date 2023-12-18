from pyspark.sql import SparkSession
from dataPipeline import cleanData, transformData, restitutionData, sparkSQL
from dataScience import train_linear_model, visualize_predictions
from dataVisualisation import nbTitresPlateformes, nbTitresDateDeSortie, jeuxLesPlusVendusParGenre, nbTitresParRegion, \
    initMap, addMarkers, saveAndOpen, top10TitresPlateformes

# [Init]
spark = SparkSession.builder.appName("VotreApplication").getOrCreate()
salesRDD = spark.read.csv('datasets/Video_Games_Sales_as_at_22_Dec_2016.csv', header=True, inferSchema=True)
detailsRDD = spark.read.csv('datasets/Windows_Games_List.csv', header=True, inferSchema=True)

# [Clean]
salesRDD, detailsRDD = cleanData(salesRDD, detailsRDD)

# [Transform]
cleanedRDD = transformData(salesRDD, detailsRDD)

# [Restitution]
cleanedRDD = restitutionData(cleanedRDD)

# [Spark SQL]
cleanedRDD = sparkSQL(cleanedRDD)

# [DataVisualisation]
# nbTitresPlateformes(spark)
top10TitresPlateformes(spark)
nbTitresDateDeSortie(spark)
jeuxLesPlusVendusParGenre(spark)
sales = nbTitresParRegion(cleanedRDD)

map = initMap()
map = addMarkers(map, sales)
saveAndOpen(map)

# [Data Science]
predictions = train_linear_model(cleanedRDD)
visualize_predictions(predictions)