#------Clientes junto a el producto que menos compraron en cada region
import pandas as pd
from database_connection import DatabaseConnection
from tabulate import tabulate

db = DatabaseConnection('localhost', 'root', '', 'northwind')

mysqlConnection = db.connect()
#-------------CONSULTA BASE-------------#
query = """
SELECT c.CustomerID, c.CompanyName, YEAR(o.OrderDate) AS ordenYear,
    p.ProductID, p.ProductName, od.UnitPrice, od.Quantity,
    (od.UnitPrice * od.Quantity) as total,
    tb.RegionDescription, tb.RegionID
FROM orders o
LEFT JOIN orderdetails od ON od.OrderID = o.OrderID
LEFT JOIN products p ON p.ProductID = od.ProductID
LEFT JOIN (
    SELECT DISTINCT r.RegionID, r.RegionDescription, et.EmployeeID 
    FROM region r
    JOIN territories t on r.RegionID = t.RegionID 
    JOIN employeeterritories et on et.TerritoryID = t.TerritoryID
    ORDER BY et.EmployeeID, r.RegionID 
) AS tb ON tb.EmployeeID = o.EmployeeID
LEFT JOIN customers c ON c.CustomerID = o.CustomerID
"""

execute_select = pd.read_sql(query, mysqlConnection)
mysqlConnection = db.close()
print("Datos originales:")
print(execute_select)



#-------------CALCULAMOS EL TOTAL-------------#
execute_select['total'] = execute_select['UnitPrice'] * execute_select['Quantity']

print("\nDatos con el campo 'total':")
print(execute_select)



#-------------AGRUPAMOS POR CLIENTE, REGION, PRODUCTO Y AÑO-------------#
group = execute_select.groupby(['CustomerID', 'RegionID', 'ordenYear', 'ProductID']).agg({
    'CompanyName': 'first',
    'RegionDescription': 'first',
    'total': 'sum',
    'ProductName': 'first'
}).reset_index()

group =  group[['CustomerID', 'CompanyName', 'ordenYear', 'RegionID', 'RegionDescription', 'ProductID', 'ProductName', 'total']]
group_orden = group.sort_values(by=['CustomerID', 'RegionID', 'total'], ascending=[True, True, True])
print("\nDatos ordenados:")
print(group_orden)



#-------------AGRUPAMOS POR CLIENTE y REGION-------------#
cliente_region = group_orden.groupby(['CustomerID', 'RegionID']).agg({
    'CompanyName': 'first',
    'RegionDescription': 'first',
    'ProductName': 'first',
    'total': 'first'
}).reset_index()

cliente_region =  cliente_region[['CustomerID', 'CompanyName', 'RegionDescription', 'ProductName', 'total']]

print("\nClientes y producto menos comprado region:")
print(cliente_region)



#-------------AÑO(S) QUE COMPRARON ESE PRODUCTO EN ESA REGION-------------#
producto_year = group_orden.merge(cliente_region, on=['CustomerID', 'CompanyName', 'RegionDescription', 'total', 'ProductName'])

producto_year['ordenYear'] = producto_year['ordenYear'].astype(str)
producto_year_grouped = producto_year.groupby(['CompanyName', 'RegionDescription', 'ProductName']).agg({
    'ordenYear': ', '.join,
}).reset_index()

print("\nDatos filtrados:")
print(producto_year_grouped)



#-------------CONCATENAR PRODUCTOS CON SUS AÑOS-------------#
producto_year_grouped = producto_year.groupby(['CompanyName', 'RegionDescription', 'ProductName']).agg({
    'ordenYear': ', '.join,
}).reset_index()


producto_year_grouped['ProductYear'] = producto_year_grouped['ProductName'] + ' - ' + producto_year_grouped['ordenYear']
producto_year_grouped =  producto_year_grouped[['CompanyName','RegionDescription', 'ProductYear']]

print("\nDatos filtrados:")
print(producto_year_grouped)


 
#-------------MATRIZ FINAL-------------#
matriz = producto_year_grouped.pivot(index='CompanyName', columns='RegionDescription', values='ProductYear')
matriz = matriz.fillna('null')

table = tabulate(matriz, headers='keys', tablefmt='simple', showindex=True)
print("\nDatos filtrados:")
print(table)


 