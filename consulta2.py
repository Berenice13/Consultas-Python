#------Ganancias por cliente, por año y region
import pandas as pd
from database_connection import DatabaseConnection
from tabulate import tabulate

db = DatabaseConnection('localhost', 'root', '', 'northwind')

mysqlConnection = db.connect()
#-------------CONSULTA BASE-------------#
query = """
SELECT o.OrderID, o.CustomerID, c.CompanyName, YEAR(o.OrderDate) AS ordenYear,
	od.UnitPrice, od.Quantity,
	tb.RegionDescription, tb.RegionID
FROM orders o
LEFT JOIN orderdetails od ON od.OrderID = o.OrderID
LEFT JOIN products p ON p.ProductID = od.ProductID
LEFT JOIN customers c ON c.CustomerID = o.CustomerID
LEFT JOIN (
	SELECT DISTINCT r.RegionID, r.RegionDescription, et.EmployeeID 
	FROM region r
	JOIN territories t on r.RegionID = t.RegionID 
	JOIN employeeterritories et on et.TerritoryID = t.TerritoryID
	ORDER BY et.EmployeeID, r.RegionID 
) AS tb ON tb.EmployeeID = o.EmployeeID
"""

execute_select = pd.read_sql(query, mysqlConnection)
mysqlConnection = db.close()
print("Datos originales:")
print(execute_select)


#-------------CALCULAMOS EL TOTAL-------------#
execute_select['total'] = execute_select['UnitPrice'] * execute_select['Quantity']

print("\nDatos con el campo 'total':")
print(execute_select)



#-------------AGRUPAMOS POR CLIENTE, AÑO Y REGION-------------#
group = execute_select.groupby(['CustomerID', 'RegionID', 'ordenYear']).agg({
    'CompanyName': 'first',
    'RegionDescription': 'first',
    'total': 'sum'
}).reset_index()

group =  group[['CustomerID', 'CompanyName', 'ordenYear', 'RegionID', 'RegionDescription', 'total']]
group_orden = group.sort_values(by=['RegionID', 'ordenYear', 'total'], ascending=[True, True, False])
print("\nDatos ordenados:")
print(group_orden)



#-------------OBTENEMOS EL MAYOR TOTAL POR REGION Y AÑO-------------#
group_max_total = group_orden.groupby(['ordenYear', 'RegionID']).agg({
    'RegionDescription': 'first',
    'total': 'max'
}).reset_index()

group_max_total = group_max_total[['RegionID', 'RegionDescription', 'ordenYear', 'total']]
max_total_orden = group_max_total.sort_values(by=['RegionID', 'ordenYear'])

print("\nMayor ganancia por año y region:")
print(max_total_orden)



#-------------CLIENTES QUE MAS COMPRARON POR AÑO Y REGION-------------#
clientes_max = group_orden.merge(max_total_orden, on=['RegionID', 'RegionDescription', 'ordenYear', 'total'])

print("\nDatos filtrados:")
print(clientes_max)



#-------------MATRIZ FINAL-------------#
def concat_company(series):
    return ', '.join(series)

matriz = clientes_max.pivot_table(
    index='ordenYear',
    columns='RegionDescription',
    values='CompanyName',
    aggfunc=concat_company,
    fill_value=''
).reset_index()


matriz.columns.name = None

table = tabulate(matriz, headers='keys', tablefmt='simple', showindex=False)

# Mostrar la tabla formateada
print("Matriz final:")
print(table)



