from pyspark.sql.functions import lit
from pyspark.sql.functions import col


def cleanData(salesRDD, detailsRDD):
    # Vérifier les entêtes de salesRDD et de detailsRDD
    # headerSalesRDD = salesRDD.first()
    # headerDetailsRDD = detailsRDD.first()
    # Renommer les entêtes de detailsRDD par celles de salesRDD
    newHeaderDetailsRDD = ['Name', 'Year_of_Release', 'Developer', 'Publisher', 'Genre']
    detailsRDD = detailsRDD.toDF(*newHeaderDetailsRDD)
    # Ajouter les entêtes manquants
    additional_columns = ['Platform', 'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales', 'Critic_Score',
                          'Critic_Count', 'User_Score', 'User_Count', 'Rating']
    for column in additional_columns:
        detailsRDD = detailsRDD.withColumn(column, lit(None))
    detailsRDD = detailsRDD.withColumn('Platform', lit('PC'))
    # Réorganiser les colonnes
    detailsRDD = detailsRDD.select(
        'Name', 'Platform', 'Year_of_Release', 'Genre', 'Publisher', 'NA_Sales', 'EU_Sales', 'JP_Sales',
        'Other_Sales', 'Global_Sales', 'Critic_Score', 'Critic_Count', 'User_Score', 'User_Count', 'Developer', 'Rating'
    )
    return salesRDD, detailsRDD


def transformData(salesRDD, detailsRDD):
    # Join les deux dans un nouveau RDD
    joinedRDD = salesRDD.union(detailsRDD)
    # Effacer les doublons
    cleanedRDD = joinedRDD.dropDuplicates()
    duplicates = cleanedRDD.groupBy(cleanedRDD.columns).count().filter(col('count') > 1)
    # Contrôler la présence de doublons
    if duplicates.count() > 0:
        print("Le fichier contient des doublons.")
        duplicates.show()
    else:
        print("Le fichier ne contient pas de doublons.")
    return cleanedRDD


def restitutionData(cleanedRDD):
    cleanedRDD.coalesce(1).write.mode('overwrite').csv('datasets/Joined.csv', header=True)
    return cleanedRDD


def sparkSQL(cleanedRDD):
    cleanedRDD.createOrReplaceTempView("TheBigTable")
    return cleanedRDD
