import os

# Variables de entorno
os.environ['REDSHIFT_DBNAME'] = 'data-engineer-database'
os.environ['REDSHIFT_USER'] = 'jenni_salazar01_coderhouse'
os.environ['REDSHIFT_PASSWORD'] = 'N1u27vOhvm'
os.environ['REDSHIFT_HOST'] = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
os.environ['REDSHIFT_PORT'] = '5439'

import os
import requests
import json
import psycopg2
import pandas as pd
from io import StringIO

# Función para conectarse a la base de datos de Redshift
def conectar_redshift():
    print("Conectando a Redshift...")
    try:
        conn = psycopg2.connect(
            dbname=os.environ['REDSHIFT_DBNAME'],
            user=os.environ['REDSHIFT_USER'],
            password=os.environ['REDSHIFT_PASSWORD'],
            host=os.environ['REDSHIFT_HOST'],
            port=os.environ['REDSHIFT_PORT']
        )
        print("Conexión exitosa.")
        return conn
    except Exception as e:
        print("Error al conectar a Redshift:", e)
        return None
    
# Función para crear una tabla en Redshift
def crear_tabla_redshift(conn):
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS data_criptomonedas (
        id VARCHAR(255),
        nombre VARCHAR(100),
        simbolo VARCHAR(10),
        precio FLOAT,
        market_cap_rank INTEGER,
        last_updated TIMESTAMP,
        current_price FLOAT,
        high_24h FLOAT,
        low_24h FLOAT,
        fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id, last_updated)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

# Función para extraer datos de la API de CoinGecko
def extraer_datos_api():
    print("Extrayendo datos de la API de CoinGecko...")
    try:
        response = requests.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd')
        data = response.json()
        print("Extracción exitosa.")
        return data
    except Exception as e:
        print("Error al extraer datos de la API:", e)
        return None
    
# Función para cargar los datos en Redshift
def cargar_datos_redshift(conn, data):
    if data:
        try:
            # Convertir los datos en un DataFrame
            df = pd.DataFrame(data)

            # Renombrar columnas para que coincidan con los nombres en la base de datos
            df.rename(columns={
                'id': 'id',
                'name': 'nombre',
                'symbol': 'simbolo',
                'current_price': 'precio',
                'market_cap_rank': 'market_cap_rank',
                'last_updated': 'last_updated',
                'high_24h': 'high_24h',
                'low_24h': 'low_24h'
            }, inplace=True)

            # Mostrar los primeros registros del DataFrame para verificar
            print("Datos extraídos:")
            print(df.head())

            cursor = conn.cursor()

            for index, row in df.iterrows():
                try:
                    # Actualizar registros existentes
                    update_query = """
                        UPDATE data_criptomonedas
                        SET nombre = %s,
                            simbolo = %s,
                            precio = %s,
                            market_cap_rank = %s,
                            current_price = %s,
                            high_24h = %s,
                            low_24h = %s,
                            fecha_ingesta = CURRENT_TIMESTAMP
                        WHERE id = %s AND last_updated = %s
                    """
                    cursor.execute(update_query, (
                        row['nombre'], row['simbolo'], row['precio'], row['market_cap_rank'],
                        row['precio'], row['high_24h'], row['low_24h'], row['id'], row['last_updated']
                    ))

                    # Insertar nuevos registros
                    insert_query = """
                        INSERT INTO data_criptomonedas (id, nombre, simbolo, precio, market_cap_rank, last_updated, current_price, high_24h, low_24h)
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM data_criptomonedas WHERE id = %s AND last_updated = %s
                        )
                    """
                    cursor.execute(insert_query, (
                        row['id'], row['nombre'], row['simbolo'], row['precio'], row['market_cap_rank'],
                        row['last_updated'], row['precio'], row['high_24h'], row['low_24h'],
                        row['id'], row['last_updated']
                    ))
                except Exception as e:
                    print("Error al actualizar/insertar datos:", e)
                    conn.rollback()
                    continue

            conn.commit()
            print("Datos cargados en Redshift correctamente.")
            cursor.close()
        except Exception as e:
            print("Error al cargar datos en Redshift:", e)
    else:
        print("No se pudieron extraer datos de la API.")

# Ejecutar el script
conn = conectar_redshift()
if conn:
    crear_tabla_redshift(conn)  # Asegurar que la tabla exista
    data = extraer_datos_api()
    if data:
        cargar_datos_redshift(conn, data)
    else:
        print("No se pudieron extraer datos de la API.")
    conn.close()
else:
    print("No se pudo conectar a Redshift.")