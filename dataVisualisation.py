import webbrowser

import folium
from matplotlib import pyplot as plt


def nbTitresPlateformes(spark):
    # Visualiser le nombre de titres par plateformes
    platform_counts = spark.sql(
        "SELECT Platform, COUNT(Name) AS Name_Count FROM TheBigTable GROUP BY Platform").collect()
    platforms = [row.Platform for row in platform_counts]
    titles = [row.Name_Count for row in platform_counts]
    plt.figure(figsize=(10, 6))
    plt.bar(platforms, titles)
    plt.xlabel('Plateformes')
    plt.ylabel('Nombre de titres')
    plt.title('Nombre de titres par plateformes')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("charts/nbTitresPlateformes.png")


def top10TitresPlateformes(spark):
    # Visualiser le nombre de titres par plateformes (top10)
    platform_counts2 = spark.sql(
        "SELECT Platform, COUNT(Name) AS Name_Count FROM TheBigTable GROUP BY Platform ORDER BY Name_Count DESC LIMIT 10").collect()
    platforms2 = [row.Platform for row in platform_counts2]
    titles = [row.Name_Count for row in platform_counts2]
    colors = ['#FFFF52', '#5286FF', '#FF5252', '#78A0FF', '#FF7878', '#8EFF52', '#94B4FF', '#B3C9FF', '#BDFF99',
              '#FF9494']

    # Graphique
    plt.figure(figsize=(8, 6))
    pie = plt.pie(titles, labels=platforms2, colors=colors, autopct='%1.1f%%', startangle=90)
    plt.axis('equal')
    plt.title('Nombre de titres par plateformes\n')

    # Légende
    legend_labels = ['Nintendo', 'Sony', 'Microsoft']
    legend_colors = ['red', 'blue', 'green']
    legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in legend_colors]
    plt.legend(legend_handles, legend_labels, loc='best')

    plt.savefig('charts/top10TitresPlateformes.png')
    

def nbTitresDateDeSortie(spark):
    # Visualiser le nombre de titres par date de sortie des 20 dernières années
    years_count = spark.sql(
        "SELECT Year_of_Release, COUNT(Name) AS Name_Count FROM TheBigTable GROUP BY Year_of_Release ORDER BY Year_of_Release DESC LIMIT 20").collect()
    years = [row.Year_of_Release for row in years_count]
    titles = [row.Name_Count for row in years_count]
    plt.figure(figsize=(10, 6))
    plt.bar(years, titles)
    plt.xlabel('Date de sortie')
    plt.ylabel('Nombre de titres')
    plt.title('Nombre de titres par Date de sortie')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("charts/nbTitresDateDeSortie.png")


def jeuxLesPlusVendusParGenre(spark):
    # Les jeux les plus vendus par genre
    best_sellers = spark.sql(
        "SELECT Genre, SUM(Global_Sales) AS Global_Sales FROM TheBigTable GROUP BY Genre ORDER BY Global_Sales DESC LIMIT 10").collect()
    genres = [row.Genre for row in best_sellers]
    sales = [row.Global_Sales for row in best_sellers]
    plt.figure(figsize=(10, 6))
    plt.bar(genres, sales)
    plt.xlabel('Genres')
    plt.ylabel('Ventes globales (en millions)')
    plt.title('Les jeux les plus vendus par genre')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("charts/jeuxLesPlusVendusParGenre.png")


def nbTitresParRegion(cleanedRDD):
    # Visualiser le nombre de titres par régions sur un planisphère
    sales_by_region = cleanedRDD.select('JP_Sales', 'EU_Sales', 'NA_Sales', 'Other_Sales', 'Global_Sales').na.fill(0)
    total_sales = sales_by_region.agg(
        {'JP_Sales': 'sum', 'EU_Sales': 'sum', 'NA_Sales': 'sum', 'Other_Sales': 'sum',
         'Global_Sales': 'sum'}).collect()[0]
    sales = [total_sales['sum(JP_Sales)'], total_sales['sum(EU_Sales)'], total_sales['sum(NA_Sales)'],
             total_sales['sum(Other_Sales)'], total_sales['sum(Global_Sales)']]
    sales = [round(s, 2) for s in sales]
    plt.savefig("charts/nbTitresParRegion.png")
    return sales


def initMap():
    initial_coords = [43.68731135256114, 7.227802126432193]  # Coordonnées IUT
    map_title = "Nombre de titres par régions"
    map = folium.Map(location=initial_coords, zoom_start=2, titles=map_title)
    map
    return map


def addMarkers(map, sales):
    # NA(USA) -> NewYork
    NewYork_Sales = [40.716578240386546, -78.03702818075023]
    NewYork_Sales_Description = ("<h3>Amérique du nord</h3>"
                                 "<br><h5>Millions d'unités vendues</h5>"
                                 f"{sales[2]}")
    folium.Marker(NewYork_Sales, icon=folium.Icon(icon="dollar-sign", color="orange", prefix="fa"),
                  popup=folium.Popup(NewYork_Sales_Description, max_width=200)).add_to(map)

    # EU(France) -> Paris
    Paris = [48.857921976690065, 2.3446292198635126]
    Paris_Description = ("<h3>Europe</h3>"
                         "<br><h5>Millions d'unités vendues</h5>"
                         f"{sales[1]}")
    folium.Marker(Paris, icon=folium.Icon(icon="dollar-sign", color="purple", prefix="fa"),
                  popup=folium.Popup(Paris_Description, max_width=200)).add_to(map)

    # Japan -> Tokyo
    Tokyo = [35.71956933370287, 139.72567169859576]
    Tokyo_Description = ("<h3>Japon</h3>"
                         "<br><h5>Millions d'unités vendues</h5>"
                         f"{sales[0]}")
    folium.Marker(Tokyo, icon=folium.Icon(icon="dollar-sign", color="green", prefix="fa"),
                  popup=folium.Popup(Tokyo_Description, max_width=200)).add_to(map)

    # Africa(Cameroun) -> Douala
    Douala = [4.050286379462756, 9.761888143062599]
    Douala_Description = ("<h3>Afrique</h3>"
                          "<br><h5>Millions d'unités vendues</h5>"
                          f"{sales[3]}")
    folium.Marker(Douala, icon=folium.Icon(icon="dollar-sign", color="blue", prefix="fa"),
                  popup=folium.Popup(Douala_Description, max_width=200)).add_to(map)

    # Loutre
    Loutre = [64.17482132102505, -51.74217411075999]
    Loutre_Description = ("<h3>Loutre</h3>"
                          "<br><h5>Millions d'unités vendues</h5>"
                          f"{sales[4]}")
    folium.Marker(Loutre, icon=folium.Icon(icon="otter", color="lightgray", prefix="fa"),
                  popup=folium.Popup(Loutre_Description, max_width=200)).add_to(map)
    return map


def saveAndOpen(map):
    # Save and open
    map.save("folium_map.html")
    webbrowser.open("index.html")
