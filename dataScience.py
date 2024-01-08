from matplotlib import pyplot as plt
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.ml.regression import LinearRegression


def train_linear_model(cleaned_rdd):
    df = cleaned_rdd

    # Convertir les colonnes pertinentes en types numériques
    df = df.withColumn("User_Score", col("User_Score").cast("float"))

    # Sélectionner les colonnes pertinentes pour l'entraînement
    feature_columns = ['Genre', 'Developer', 'User_Score', 'Critic_Score', 'JP_Sales', 'EU_Sales', 'NA_Sales',
                       'Other_Sales', 'Global_Sales']
    selected_data = df.select(feature_columns)

    # Filtrer les données avec Year_of_Release non null, différent de 'N/A' ou 'TBA'
    filtered_data = selected_data.dropna()

    # Créer un vecteur de caractéristiques
    assembler = VectorAssembler(inputCols=['User_Score', 'Critic_Score'], outputCol='features')
    assembled_data = assembler.transform(filtered_data)

    # Initialiser le modèle de régression linéaire
    lr = LinearRegression(featuresCol='features', labelCol="JP_Sales")

    # Diviser les données en ensembles de formation et de test
    train_data, test_data = assembled_data.randomSplit([0.7, 0.3], seed=42)

    # Entraîner le modèle
    model = lr.fit(train_data)

    # Faire des prédictions sur l'ensemble de test
    predictions = model.transform(test_data)

    return predictions


def visualize_predictions(predictions):
    # Collecte des valeurs prédites et réelles pour la comparaison
    predictions_collect = predictions.select("Genre", "JP_Sales", "EU_Sales", "NA_Sales", "Other_Sales").collect()

    # Création des dictionnaires pour stocker les ventes par genre pour chaque région
    sales_by_genre = {genre: {"JP_Sales": 0, "EU_Sales": 0, "NA_Sales": 0, "Other_Sales": 0} for genre in
                      set(row.Genre for row in predictions_collect)}

    # Agrégation des ventes par genre pour chaque région
    for row in predictions_collect:
        genre = row.Genre
        sales_by_genre[genre]["JP_Sales"] += row.JP_Sales
        sales_by_genre[genre]["EU_Sales"] += row.EU_Sales
        sales_by_genre[genre]["NA_Sales"] += row.NA_Sales
        sales_by_genre[genre]["Other_Sales"] += row.Other_Sales


    # Extraction des données pour le graphique
    genres = list(sales_by_genre.keys())
    jp_sales = [sales_by_genre[genre]["JP_Sales"] for genre in genres]
    eu_sales = [sales_by_genre[genre]["EU_Sales"] for genre in genres]
    na_sales = [sales_by_genre[genre]["NA_Sales"] for genre in genres]
    other_sales = [sales_by_genre[genre]["Other_Sales"] for genre in genres]

    # Création du graphique en barres empilées
    plt.figure(figsize=(12, 8))

    bar_width = 0.25
    index = range(len(genres))

    plt.bar(index, jp_sales, color='green', width=bar_width, label='JP_Sales')
    plt.bar(index, eu_sales, color='purple', width=bar_width, label='EU_Sales', bottom=jp_sales)
    plt.bar(index, na_sales, color='orange', width=bar_width, label='NA_Sales',
            bottom=[jp + eu for jp, eu in zip(jp_sales, eu_sales)])
    plt.bar(index, other_sales, color='blue', width=bar_width, label='Other_Sales', bottom=[jp + eu + na for jp, eu, na in zip(jp_sales, eu_sales, na_sales)])

    plt.xlabel('Genre')
    plt.ylabel('Ventes éstimées (en millions)')
    plt.title('Ventres estimées par genre et par région')
    plt.xticks(index, genres, rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig("charts/predictionVentesParGenresParRegions.png")
