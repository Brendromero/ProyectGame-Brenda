from fastapi import FastAPI
import pandas as pd
import numpy as np
from ast import literal_eval
import re

app = FastAPI() #Creacion de una instancia
#Para cambiar el nombre de nuestra aplicacion y descripcion agrego lo siguiente
app.title = "Juegos de Stream"
app.description= 'Proyecto Individual N1'
#Agrego contacto de github y mail
app.contact = {"name": "Brendromero", "url": "https://github.com/Brendromero", "email": "brendromerok@gmail.com"}

df = pd.read_csv('./new_users_item.csv')
df1 = pd.read_csv('./new_users_reviews.csv')
df2 = pd.read_csv('./new_steam_games.csv', low_memory=False)

#creacion de los endpoint, podemos usar los tags para agrupar las rutas de la aplicacion

@app.get('/Usuario/{id}', tags=['General'])
#Necesitamos hacer que nos devuelva la cantidad de dinero gastado por usuario,
#El porcentaje de recomendaciones que lo encontramos en la columna de recommend,
#Y tambien necesitamos que nos devuelva la cantidad de items que tiene el usuario.
def userdata(User_id: str) :
    
    #Lo que hacemos es crear una variable donde llamemos al dataframe donde contenga esas columnas
    usuario = df[['user_id', 'items_count', 'item_id']]
    df_price = df2[['item_id','price']]
    
    #En df_price1 lo que hacemos es copiar los resultados del dataframe
    df_price1 = df_price.copy()
    
    #En las siguientes lineas lo que hacemos es volver el price en tipo string
    #Y lo que nos hace lower es que nos convierte en minuscula.
    df_price1['price'] = df_price1['price'].astype(str)
    df_price1 = df_price1[df_price1['price'].str.lower() != 'free to play']
    df_price1 = df_price1[df_price1['price'].str.lower() != 'free']

    #Filtramos desde el user_id, y con merge unimos los resultados que obtenemos,
    #En on indicaremos cual es la columna que fusionaremos,
    #Y con How esppecifica como se hace la combinacion de los dos dataframe.
    usuario_filtrado = usuario[usuario['user_id'] == User_id]
    usuario_filtrado = pd.merge(usuario_filtrado, df_price1, on='item_id', how='left')

    #Se convierten a flotante, reemplaza los errores con NaN y luego los elimina
    usuario_filtrado['price'] = pd.to_numeric(usuario_filtrado['price'], errors='coerce')
    dinero_gastado = usuario_filtrado['price'].dropna().sum().round(2)


    #Nuevamente para el porcentaje, filtramos por user_id
    porcentaje_recomendacion = df1[df1['user_id'] == User_id]
    
    #Con len se calcula la longuitud de porcentaje_recomendacion y lo guardamos en la variable
    reviews_totales = len(porcentaje_recomendacion)
    
    #Si no hay ningun registro, sera 0
    if reviews_totales == 0:
        porcentaje_total = 0
    #Si hay registros
    else:
        #Entonces se filtra los registros donde la columna de recommend es True y se contara la cantidad de registros
        reviews_positivos = len(porcentaje_recomendacion[porcentaje_recomendacion['recommend'] == True])
        #Calculamos el porcentaje de recomendaciones positivas como la proporcion de la linea anterior y reviews_totales
        #Y se multipicara por 100 para obtener un porcentaje
        porcentaje_total = (reviews_positivos / reviews_totales) * 100

    #Se toma con iloc el primer valor que la columna de items_count aparece para guardarlo en cantidad_items
    cantidad_items = usuario['items_count'].iloc[0]


    resultado = {
    'User_Id' : User_id,
    'Total_Price' : float(dinero_gastado),  # Convirtiendo a float de Python nativo
    'Recommend_Percentaje' : float(porcentaje_total),  # Convirtiendo a float de Python nativo
    'Items_Count' : int(cantidad_items)  # Convirtiendo a int de Python nativo
    }

    return resultado

#necesitamos obtener cantidad de usuarios que realizaron reviews entre las fechas dadas donde lo sacamos de df1
#y un porcentaje de esas recomendaciones que se obtienen de recommend
@app.get('/Reviews', tags=['General'])
def count_reviews(start_date: str, end_date: str):
    
    df_copy = df1[['user_id', 'recommend', 'clean_date']].copy()
    
    # Convertir la columna 'clean_date' al tipo datetime
    df_copy['clean_date'] = pd.to_datetime(df_copy['clean_date'], errors='coerce')
    
    # Filtrar el DataFrame entre start_date y end_date
    filtered_df = df_copy[(df_copy['clean_date'] >= start_date) & (df_copy['clean_date'] <= end_date)]
    
    # Cálculos
    user_count = len(filtered_df['user_id'].unique())
    total_reviews = len(filtered_df)
    
    if total_reviews == 0:
        percentage = 0
    else:
        positive_reviews = len(filtered_df[filtered_df['recommend'] == True])
        percentage = round((positive_reviews / total_reviews) * 100, 2)
    
    result = {
        'cantidad_usuarios': user_count,
        'Porcentaje_recomendacion': percentage
    }
    
    return result




def genre_expan(df2):
    df_genres = df2[['item_id', 'genres']].dropna(subset=['genres'])
    df_genres.dropna(subset=['item_id'], inplace=True)  # Eliminar filas donde 'item_id' es NaN
    df_genres['item_id'] = df_genres['item_id'].astype(int)
    df_genres['genres'] = df_genres['genres'].apply(literal_eval)  # se convierte la representación de cadenas de listas en listas reales

    generos_expandidos = []

    for index, row in df_genres.iterrows():
        item_id = row['item_id']
        genres = row['genres']
        
        if isinstance(genres, (list, tuple)):
            for genre in genres:
                new_row = {
                    'item_id': item_id,
                    'genres': genre
                }
                generos_expandidos.append(new_row)

    generos_expandidos_df = pd.DataFrame(generos_expandidos)
    return generos_expandidos_df

@app.get('/Genero', tags=['General'])
def genre(genre:str):
    usuario = df[['item_id', 'item_name', 'playtime_forever']]
    gen_expan = genre_expan(df2)
    generos = pd.merge(usuario, gen_expan, on='item_id', how='left')
    generos = generos.dropna(subset=['genres', 'playtime_forever'])

    grouped_df = generos.groupby('genres').agg({'playtime_forever': 'sum'}).reset_index()

    
# Se ordena el Dataframe agrupandolo por la columna 'playtime_forever' de manera descendente
    sorted_df = grouped_df.sort_values(by='playtime_forever', ascending=False).reset_index(drop=True) #Se resetea el index para que se visualice de manera adecuada
    sorted_df.index = sorted_df.index + 1 #Se le suma un 1 al index para que comience de ahi y no desde 0.
    ranking = sorted_df[sorted_df['genres'] == genre]
    return ranking.to_dict(orient='records')

@app.get('/Usuario por genero', tags=['General'])
def userforgenre(genero:str):
    # Se crea un dataframe que solo contenga las columnas necesarias.
    usuario = df[['user_id', 'user_url', 'item_id', 'playtime_forever']]

    # Se fuciona el dataframe con el que contiene generos.
    generos = pd.merge(usuario, genre_expan(df2), on='item_id', how='left')

    # Filtramos las filas por el genero ingresado.
    generos = generos[generos['genres'] == genero]

    # Agrupamos el  user_id y sumamos el total de playtime.
    grouped_df = generos.groupby('user_id').agg({'playtime_forever': 'sum'}).reset_index()

    # Se ordena el playtime_forever de manera descendente y reseteamos el index.
    sorted_df = grouped_df.sort_values(by='playtime_forever', ascending=False).reset_index(drop=True)

    # Tomamos los top 5 en usuarios.
    top_5_users = sorted_df.head(5)

    # Se vuelve a fusionar para obtener las URL y los 5 usuarios principales.
    top_5_users_with_url = pd.merge(top_5_users, usuario[['user_id', 'user_url']], on='user_id', how='left').drop_duplicates(subset=['user_id'])

    top_5_users_with_url[['user_id', 'user_url', 'playtime_forever']]
    return top_5_users_with_url.to_dict(orient="records") #El .to_dict nos sirve para que lo devuelva en forma de diccionario


@app.get('/desarrollador', tags=['General'])
def developer(desarrollador:str):
    dataframe_reducido = df2[['developer', 'release_date', 'item_id', 'price']]
    empresa_desarrolladora = dataframe_reducido[dataframe_reducido['developer'] == desarrollador]
    empresa_desarrolladora = df2[df2['developer'] == desarrollador]

    # Se extrae el año desde la columna release_date y se crea una nueva llamada year.
    empresa_desarrolladora['year'] = pd.to_datetime(empresa_desarrolladora['release_date']).dt.year

    # Se crea una lista vacia para almacenar los resultados.
    result_list = []

    # Se agrupa por año e iteramos por cada grupo para realizar operaciones.
    for year, group in empresa_desarrolladora.groupby('year'):
        total_items = len(group)
        free_items = len(group[group['price'] == 'Free to Play'])
        free_percentage = (free_items / total_items) * 100 if total_items > 0 else 0
        
        result_list.append({
            'Year': year,
            'Total_Items': total_items,
            'Free_Percentage': free_percentage
        })

    # Se convierte el dataframe en un diccionario.
    result_df = pd.DataFrame(result_list)
    return result_df.to_dict(orient="records")


def get_review_counts_for_year(df, year):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    df_filtered = df[(df['clean_date'] >= start_date) & (df['clean_date'] <= end_date)]

    positive_reviews = len(df_filtered[df_filtered['sentiment_analysis'] == 2])
    neutral_reviews = len(df_filtered[df_filtered['sentiment_analysis'] == 1])
    negative_reviews = len(df_filtered[df_filtered['sentiment_analysis'] == 0])

    return {
        "Positive Reviews": positive_reviews,
        "Neutral Reviews": neutral_reviews,
        "Negative Reviews": negative_reviews
    }
    
@app.get('/Analisis de sentimiento', tags=['General'])
def sentiment_analysis(anio:str):
    countreviews = df1[['user_id', 'clean_date', 'sentiment_analysis']].copy()
    countreviews['clean_date'] = pd.to_datetime(countreviews['clean_date'], errors='coerce')
    countreviews['clean_date'] = countreviews['clean_date'].dt.strftime('%Y-%m-%d')
    countreviews_clean = countreviews.dropna(subset=['clean_date'])
    review_counts = get_review_counts_for_year(countreviews_clean, anio)

    result = {(f"Positivo: {review_counts['Positive Reviews']}"),
            (f"Neutral: {review_counts['Neutral Reviews']}"),
            (f"Negativo: {review_counts['Negative Reviews']}")}
    return result
