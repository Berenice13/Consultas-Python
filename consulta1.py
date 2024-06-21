#------Ganacias por autor
import pandas as pd
from database_connection import DatabaseConnection
from tabulate import tabulate

# Establece la conexión a la base de datos MySQL
db = DatabaseConnection('localhost', 'root', '', 'pubs')

mysqlConnection = db.connect()
#-------------CONSULTA BASE-------------#
query = """
SELECT a.au_id, CONCAT(a.au_fname, ' ', a.au_lname) autor,
       t.title_id, t.title, 
       t.price, COALESCE(ta.royaltyper, 100) as royaltyper, s.qty
FROM sales s
    LEFT JOIN titles t ON t.title_id = s.title_id
    LEFT JOIN titleauthor ta ON ta.title_id = t.title_id
    LEFT JOIN authors a ON ta.au_id = a.au_id
"""

execute_select = pd.read_sql(query, mysqlConnection)
mysqlConnection = db.close()
print("Datos originales:")
print(execute_select)




#-------------AGRUPAR POR AUTOR-------------#
# Rellenar los valores nulos
execute_select['au_id'] = execute_select['au_id'].fillna('')

# Agrupar por au_id y obtener los datos a mostrar
agrupa_autor = execute_select.groupby(['au_id']).agg({
    'autor': 'first',
    'title_id': 'first',
    'title': 'first',
    'price': 'first',
    'royaltyper': 'max',
    'qty': 'sum'
}).reset_index()

# Ordenar por 'title'
agrupa_autor = agrupa_autor.sort_values(by='title')

print("\nConsulta agrupada por autor")
print(agrupa_autor)




#-------------TRAER TITULOS FALTANTES-------------#
agrupa_titulo = agrupa_autor.groupby('title').agg({
    'title_id': 'first',
    'price': 'first',  
    'qty': 'sum',      
    'royaltyper': lambda x: 100 - x.sum()  # Calculamos '100 - SUM(royaltyper)'
}).reset_index()

# Filtrar aquellos donde 'royaltyper > 0'
agrupa_titulo = agrupa_titulo[agrupa_titulo['royaltyper'] > 0]

# Agregar columnas 'au_id' y 'autor' con valores nulos
agrupa_titulo['au_id'] = None
agrupa_titulo['autor'] = None

# Reordenar las columnas
agrupa_titulo = agrupa_titulo[['au_id', 'autor', 'title_id', 'title', 'price', 'royaltyper', 'qty']]

print("\nTitulos restantes:")
print(agrupa_titulo)



#-------------UNIR AUTORES CON FALTANTES-------------#
union = pd.concat([agrupa_autor, agrupa_titulo], ignore_index=True)
union = union.sort_values(by='title')

print("\nResultado final combinado:")
print(union)


#-------------UNIR AUTORES CON FALTANTES-------------#
# Reemplazar NULL en 'autor' por 'restante'
union['autor'] = union['autor'].fillna('restante')

# Calcular el total ((price * qty) * royaltyper) / 100
union['total'] = (union['price'] * union['qty'] * union['royaltyper']) / 100


total_por_autor = union.groupby('autor')['total'].sum().reset_index()

table = tabulate(total_por_autor, headers='keys', tablefmt='pretty', showindex=True)
total_por_autor.to_excel("ganancia_por_autor.xlsx", index=False, engine='openpyxl')
print("\nResultado final combinado con total por autor:")
print(table)



#-------------SUMA TOTAL DE LOS TOTALES POR AUTOR------------#
suma_total = total_por_autor['total'].sum()

print(f"\nSuma total de todos los totales: {suma_total}")
