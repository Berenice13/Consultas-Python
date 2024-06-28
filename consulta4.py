#------Productos mas comprado por categoria y año y quien cmpro mas y menos ese producto
import pandas as pd
from database_connection import DatabaseConnection
from tabulate import tabulate

db = DatabaseConnection('localhost', 'root', '', 'northwind')

mysqlConnection = db.connect()
#-------------CONSULTA BASE-------------#
query = """
SELECT o.OrderID, 
	o.CustomerID, c.CompanyName,
    year(o.OrderDate) as ordenYear, 
    od.ProductID, p.ProductName, p.CategoryID, cat.CategoryName,
	od.Quantity, od.UnitPrice, (od.Quantity * od.UnitPrice) as total
FROM orderdetails od
JOIN orders o on o.OrderID = od.OrderID
JOIN customers c ON c.CustomerID = o.CustomerID
JOIN products p ON p.ProductID = od.ProductID
JOIN categories cat ON cat.CategoryID = p.CategoryID
"""

execute_select = pd.read_sql(query, mysqlConnection)
mysqlConnection = db.close()

execute_select = execute_select.sort_values(by=['CategoryID', 'ordenYear', 'total'], ascending=[True, False, False])
#print("Datos ordenados:")
#print(execute_select)

#-------------CANTIDAD MAS VENDIDA POR CATEGORIA Y AÑO-------------#
group_cant_max = execute_select.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
    'total': 'max'
}).reset_index()

group_cant_max = group_cant_max.sort_values(by=['CategoryID', 'ordenYear', 'total'], ascending=[True, False, False])
#print("Cantidad mas vendida:")
#print(group_cant_max)


#-------------PRODUCTO MAS VENDIDO POR CATEGORIA Y AÑO-------------#
product_max = pd.merge(execute_select, group_cant_max, on=['CategoryID', 'CategoryName', 'ordenYear', 'total'])
product_max = product_max[['ProductID', 'ProductName', 'CategoryName', 'ordenYear', 'total']]

#print("\nProducto más vendido:")
#print(product_max)



#-------------CLIENTES QUE COMPRARON PRODUCTO MAS VENDIDO-------------#
clientes_producto_max = pd.merge(execute_select, product_max, on=['ProductID', 'CategoryName', 'ordenYear'])

clientes_producto_max = clientes_producto_max[['CompanyName', 'ProductID', 'ProductName_y', 'CategoryID', 'CategoryName', 'ordenYear', 'total_x']]
clientes_producto_max = clientes_producto_max.rename(columns={'ProductName_y': 'ProductName', 'total_x': 'total'})
clientes_producto_max = clientes_producto_max.drop_duplicates()

#print("\nClientes que compraron el producto más vendido:")
#print(clientes_producto_max)

 
#-------------MENOR TOTAL DE ESE PRODUCTO-------------#
total_min = clientes_producto_max.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
    'ProductID': 'first',
    'ProductName': 'first',
    'total': 'min'
}).reset_index()

total_min = total_min.sort_values(by=['CategoryID', 'ordenYear', 'total'], ascending=[True, False, False])
#print("Cantidad mas vendida:")
#print(total_min)


#-------------MAYOR TOTAL DE ESE PRODUCTO-------------#
total_max = clientes_producto_max.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
    'ProductID': 'first',
    'ProductName': 'first',
    'total': 'max'
}).reset_index()

total_max = total_max.sort_values(by=['CategoryID', 'ordenYear', 'total'], ascending=[True, False, False])
#print("Cantidad mas vendida:")
#print(total_max)


#-------------CLIENTES QUE MAS COMPRRAON PRODUCTO MAS VENDIDO-------------#
clientes_total_max = pd.merge(clientes_producto_max, total_max, on=['CategoryName', 'ordenYear', 'total'])
clientes_total_max = clientes_total_max[['CompanyName', 'ProductID_y', 'ProductName_y', 'CategoryName', 'ordenYear', 'total']]
clientes_total_max = clientes_total_max.rename(columns={'ProductID_y' : 'ProductID', 'ProductName_y': 'ProductName', 'total_x': 'total'})

#print("\nClientes que mas compraron el producto más vendido:")
#print(clientes_total_max)


#-------------MENOR TOTAL DE ESE PRODUCTO-------------#
total_min = clientes_producto_max.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
    'ProductID': 'first',
    'ProductName': 'first',
    'total': 'min'
}).reset_index()

total_min = total_min.sort_values(by=['CategoryID', 'ordenYear', 'total'], ascending=[True, False, False])
#print("Cantidad mas vendida:")
#print(total_max)


#-------------CLIENTES QUE MENOS COMPRRAON PRODUCTO MAS VENDIDO-------------#
clientes_total_min = pd.merge(clientes_producto_max, total_min, on=['CategoryName', 'ordenYear', 'total'])
clientes_total_min = clientes_total_min[['CompanyName', 'ProductID_y', 'ProductName_y', 'CategoryName', 'ordenYear', 'total']]
clientes_total_min = clientes_total_min.rename(columns={'ProductID_y' : 'ProductID', 'ProductName_y': 'ProductName', 'total_x': 'total'})

#print("\nClientes que menos compraron el producto más vendido:")
#print(clientes_total_min)


#-------------CLIENTES QUE MAS Y MENOS COMPRRAON PRODUCTO MAS VENDIDO-------------#
clientes_combinados = pd.concat([clientes_total_min, clientes_total_max], ignore_index=True)
clientes_combinados = clientes_combinados.sort_values(by=['CategoryName', 'ordenYear', 'total'], ascending=[True, False, False])
clientes_combinados = clientes_combinados.drop_duplicates()

print("\nClientes que mas y menos compraron el producto más vendido:")
print(clientes_combinados)



#-------------ULTIMOS 3 AÑOS-------------#
año_maximo = clientes_combinados['ordenYear'].max()
ultimos_3_años = [año_maximo - i for i in range(3)]
clientes_ultimos_3_años = clientes_combinados[clientes_combinados['ordenYear'].isin(ultimos_3_años)]

print("\nClientes que mas y menos compraron el producto más vendido los ultimos 3 años:")
print(clientes_ultimos_3_años)


#---------------CONCATENACION---------------#
def concat_vals(group):
    return ', '.join(f"{row['ProductName']} {row['CompanyName']}-{row['total']}" for index, row in group.iterrows())

resultado = clientes_ultimos_3_años.groupby(['CategoryName', 'ordenYear']).apply(concat_vals).reset_index(name='clientes')
resultado = resultado.sort_values(by=['CategoryName', 'ordenYear'], ascending=[True, False])

print(resultado)

# Año más alto en `resultado`
año_mas_alto = resultado['ordenYear'].max()

# Siguientes dos años
año_siguiente = año_mas_alto - 1
dos_años_después = año_mas_alto - 2

columnas_años = [f'AÑO1 ({año_mas_alto})', f'AÑO2 ({año_siguiente})', f'AÑO3 ({dos_años_después})']
columnas_finales = ['CATEGORIA'] + columnas_años
matriz_final = pd.DataFrame(columns=columnas_finales)

categorias_unicas = resultado['CategoryName'].unique()

for categoria in categorias_unicas:
    df_categoria = resultado[resultado['CategoryName'] == categoria]
    
    fila = {'CATEGORIA': categoria}
    for año in ['AÑO1 (1999)', 'AÑO2 (1998)', 'AÑO3 (1997)']:
        año_num = int(año.split(' ')[1].strip('()'))
        valor = df_categoria[df_categoria['ordenYear'] == año_num]['clientes'].values
        fila[año] = valor[0] if len(valor) > 0 else ''
    
    matriz_final = pd.concat([matriz_final, pd.DataFrame([fila])], ignore_index=True)


matriz_final.to_excel("examen.xlsx", index=False)
table = tabulate(matriz_final, headers='keys', tablefmt='outline', showindex=True)
print(table)