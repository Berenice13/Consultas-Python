import pandas as pd
from database_connection import DatabaseConnection
from tabulate import tabulate

class Consulta4:
    def __init__(self, db_config):
        self.db = DatabaseConnection(**db_config)
        self.connection = self.db.connect()
        self.execute_select = None

    def fetch_data(self):
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
        self.execute_select = pd.read_sql(query, self.connection)
        self.db.close()

    def process_data(self):
        self.execute_select = self.execute_select.sort_values(by=['CategoryID', 'ordenYear', 'total'], ascending=[True, False, False])
        self.group_cant_max = self.calculate_group_max()
        self.product_max = self.calculate_product_max()
        self.clientes_producto_max = self.calculate_clients_product_max()
        self.total_min = self.calculate_total_min()
        self.total_max = self.calculate_total_max()
        self.clientes_total_max = self.calculate_clients_total_max()
        self.clientes_total_min = self.calculate_clients_total_min()
        self.clientes_combinados = self.combine_clients()
        self.clientes_ultimos_3_años = self.filter_last_3_years()
        self.resultado = self.concatenate_results()
        self.matriz_final = self.generate_final_matrix()

    def calculate_group_max(self):
        return self.execute_select.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
            'total': 'max'
        }).reset_index()

    def calculate_product_max(self):
        product_max = pd.merge(self.execute_select, self.group_cant_max, on=['CategoryID', 'CategoryName', 'ordenYear', 'total'])
        return product_max[['ProductID', 'ProductName', 'CategoryName', 'ordenYear', 'total']]

    def calculate_clients_product_max(self):
        clients_product_max = pd.merge(self.execute_select, self.product_max, on=['ProductID', 'CategoryName', 'ordenYear'])
        clients_product_max = clients_product_max[['CompanyName', 'ProductID', 'ProductName_y', 'CategoryID', 'CategoryName', 'ordenYear', 'total_x']]
        clients_product_max = clients_product_max.rename(columns={'ProductName_y': 'ProductName', 'total_x': 'total'})
        return clients_product_max.drop_duplicates()

    def calculate_total_min(self):
        return self.clientes_producto_max.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
            'ProductID': 'first',
            'ProductName': 'first',
            'total': 'min'
        }).reset_index()

    def calculate_total_max(self):
        return self.clientes_producto_max.groupby(['CategoryID', 'CategoryName', 'ordenYear']).agg({
            'ProductID': 'first',
            'ProductName': 'first',
            'total': 'max'
        }).reset_index()

    def calculate_clients_total_max(self):
        clients_total_max = pd.merge(self.clientes_producto_max, self.total_max, on=['CategoryName', 'ordenYear', 'total'])
        clients_total_max = clients_total_max[['CompanyName', 'ProductID_y', 'ProductName_y', 'CategoryName', 'ordenYear', 'total']]
        return clients_total_max.rename(columns={'ProductID_y': 'ProductID', 'ProductName_y': 'ProductName', 'total_x': 'total'})

    def calculate_clients_total_min(self):
        clients_total_min = pd.merge(self.clientes_producto_max, self.total_min, on=['CategoryName', 'ordenYear', 'total'])
        clients_total_min = clients_total_min[['CompanyName', 'ProductID_y', 'ProductName_y', 'CategoryName', 'ordenYear', 'total']]
        return clients_total_min.rename(columns={'ProductID_y': 'ProductID', 'ProductName_y': 'ProductName', 'total_x': 'total'})

    def combine_clients(self):
        combined_clients = pd.concat([self.clientes_total_min, self.clientes_total_max], ignore_index=True)
        combined_clients = combined_clients.sort_values(by=['CategoryName', 'ordenYear', 'total'], ascending=[True, False, False])
        return combined_clients.drop_duplicates()

    def filter_last_3_years(self):
        año_maximo = self.clientes_combinados['ordenYear'].max()
        ultimos_3_años = [año_maximo - i for i in range(3)]
        return self.clientes_combinados[self.clientes_combinados['ordenYear'].isin(ultimos_3_años)]

    def concatenate_results(self):
        def concat_vals(group):
            return ', '.join(f"{row['ProductName']} {row['CompanyName']}-{row['total']}" for index, row in group.iterrows())

        resultado = self.clientes_ultimos_3_años.groupby(['CategoryName', 'ordenYear']).apply(concat_vals).reset_index(name='clientes')
        return resultado.sort_values(by=['CategoryName', 'ordenYear'], ascending=[True, False])

    def generate_final_matrix(self):
        año_mas_alto = self.resultado['ordenYear'].max()
        año_siguiente = año_mas_alto - 1
        dos_años_después = año_mas_alto - 2

        columnas_años = [f'AÑO1 ({año_mas_alto})', f'AÑO2 ({año_siguiente})', f'AÑO3 ({dos_años_después})']
        columnas_finales = ['CATEGORIA'] + columnas_años
        matriz_final = pd.DataFrame(columns=columnas_finales)

        categorias_unicas = self.resultado['CategoryName'].unique()

        for categoria in categorias_unicas:
            df_categoria = self.resultado[self.resultado['CategoryName'] == categoria]

            fila = {'CATEGORIA': categoria}
            for año in columnas_años:
                año_num = int(año.split(' ')[1].strip('()'))
                valor = df_categoria[df_categoria['ordenYear'] == año_num]['clientes'].values
                fila[año] = valor[0] if len(valor) > 0 else ''

            matriz_final = pd.concat([matriz_final, pd.DataFrame([fila])], ignore_index=True)

        matriz_final.to_excel("examen.xlsx", index=False)
        return matriz_final

    def print_results(self):
        table = tabulate(self.matriz_final, headers='keys', tablefmt='outline', showindex=True)
        print(table)

if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'northwind'
    }

    analyzer = Consulta4(db_config)
    analyzer.fetch_data()
    analyzer.process_data()
    analyzer.print_results()
