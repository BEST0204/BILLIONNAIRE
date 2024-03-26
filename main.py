#importation des modules
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Chargement des données
billionnaire_data = pd.read_csv('df_ready.csv')

print(billionnaire_data.head())

# Transformation portant sur les noms des colonnes
colonnes = ['position', 'wealth', 'industry', 'full_name', 'age', 'country_of_residence', 'city_of_residence', 'source', 'citizenship', 'gender', 'birth_date', 'last_name', 'first_name', 'residence_state', 'residence_region', 'birth_year', 'birth_month', 'birth_day', 'cpi_country', 'cpi_change_country', 'gdp_country', 'g_tertiary_ed_enroll', 'g_primary_ed_enroll', 'life_expectancy', 'tax_revenue', 'tax_rate', 'country_pop', 'country_lat', 'country_long', 'continent']
billionnaire_data.columns = colonnes
print('--------------------------------------------------------')
print(billionnaire_data.columns)

# Chargement du dataframe dans MYSQL

try:
    connexion = mysql.connector.connect(host='localhost', 
                                        database='db_billionnaire', 
                                        user='root', 
                                        password='')
    if connexion.is_connected():
        print('Connexion à MySQLréussie')
except Error as e:
    print(f"Erreur lors de la connexion à MySQL: {e}")

try:
    cursor = connexion.cursor()

    for i,row in billionnaire_data.iterrows():
        sql ="""INSERT INTO billionnaire (position, wealth, industry, full_name, age, country_of_residence, city_of_residence, source, citizenship, gender, birth_date, last_name, first_name, residence_state, residence_region, birth_year, birth_month, birth_day, cpi_country, cpi_change_country, gdp_country, g_tertiary_ed_enroll, g_primary_ed_enroll, life_expectancy, tax_revenue, tax_rate, country_pop, country_lat, country_long, continent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        #print(sql)
        cursor.execute(sql, tuple(row))

    connexion.commit()
    connexion.close()
    print("Dataframe chargé dans MySQL avec succès!")
except Exception as e:
    print(f"Erreur lors du chargement du DataFrame dans MySQL: {e}")







