# TP1 - BANCO DE DADOS 1 - 2024/2
#
# Acadêmicas:
#     Maria Gabriela Morais de Sá - 22250537
#     Maria Giovanna Gonçalves Sales - 22251138
#     Juíle Yoshie Sarkis Hanada - 22251135 
#
# SCRIPT 2 - Consulta ao banco de dados

# VARIAVEIS PARA CONEXÃO COM O BANCO DE DADOS

DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'tp1'
DB_USER = 'postgres'
DB_PASS = 'postgres'

import re
import psycopg2

class Database:
  
  def __init__(self):
    self.dbname = DB_NAME
    self.dbuser = DB_USER
    self.dbpass = DB_PASS
    self.dbhost = DB_HOST
    self.dbport = DB_PORT
    
    self.connection = None
    self.cursor = None
    
  def connect(self):
    try:
      self.connection = psycopg2.connect(
        dbname = self.dbname,
        user = self.dbuser,
        password = self.dbpass,
        host = self.dbhost, 
        port = self.dbport
      )
      self.cursor = self.connection.cursor()
    except Exception as e:
      print('Não foi possível estabelecer a conexão com o banco de dados. \n', e)
      exit()
  
  def close(self):
    if (self.cursor):
      self.cursor.close()
    if (self.connection):
      self.connection.close()
    print('Encerrada conexão com o banco de dados.')
    
  def opcao_a(self):
    try:
      product_id = input("\nVocê escolheu a opção a! Digite o id do produto a ser consultado: ")

      self.cursor.execute("SELECT reviews.customer, date, rating, votes, helpful FROM reviews, customers WHERE reviews.customer=customers.customer AND product_id=(%s) ORDER BY helpful DESC, rating DESC LIMIT 5", (product_id,))
      results = self.cursor.fetchall()

      if results:
        print("\nOs 5 comentários mais úteis e com maior avaliação:\n\ncustomer          date    rating votes helpful")
        for result in results:
          print(result[0], result[1], "  ", result[2], "    ", result[3], "    ", result[4])
      else:
        print("Nenhum resultado encontrado.")

      self.cursor.execute("SELECT reviews.customer, date, rating, votes, helpful FROM reviews, customers WHERE reviews.customer=customers.customer AND product_id=(%s) ORDER BY helpful DESC, rating ASC LIMIT 5", (product_id,))
      results = self.cursor.fetchall()

      if results:
        print("\nOS 5 comentários mais úteis e com menor avaliação:\n\ncustomer          date    rating  votes  helpful")
        for result in results:
          print(result[0], result[1], "  ", result[2], "    ", result[3], "    ", result[4])
      else:
        print("Nenhum resultado encontrado.")

    except Exception as e:
      print(f"Não foi possível realizar a consulta a. Erro: {e}")

  def opcao_b(self):
    try:
      product_id = input("\nVocê escolheu a opção b! Digite o id do produto a ser consultado: ")

      self.cursor.execute("SELECT p2.asin, p2.title, p2.salesrank FROM products p1 JOIN similar_products sp ON p1.id = sp.product_id JOIN products p2 ON p2.asin = sp.similar_asin WHERE p1.id = (%s) AND p2.salesrank < p1.salesrank ORDER BY p2.salesrank ASC;", (product_id,))      
      results = self.cursor.fetchall()

      if results:
        print(f"\nEsses são os produtos similares com maiores vendas que o produto {product_id}:\n\nasin    title   salesrank")

        for result in results:
          print(result[0], result[1], result[2])
      else:
        print("Nenhum resultado encontrado.")

    except Exception as e:
      print(f"Não foi possível realizar a consulta a. Erro: {e}")

  def opcao_c(self):
    try:
      product_id = input("\nVocê escolheu a opção c! Digite o id do produto a ser consultado: ")

      self.cursor.execute("SELECT date, AVG(rating) AS avg_rating FROM reviews WHERE product_id=(%s) GROUP BY date ORDER BY date ASC;", (product_id,))
      results = self.cursor.fetchall()

      if results:
        print("\ndate           avg_rating")
        for result in results:
          print(result[0], result[1])
      else:
        print("Nenhum resultado encontrado.")

    except Exception as e:
      print(f"Não foi possível realizar a consulta c. Erro: {e}")

  def opcao_d(self):
    print("\nVocê escolheu a opção d!")
    try:
        self.cursor.execute("""
            SELECT group_title, title, salesrank
            FROM (
                SELECT group_title, title, salesrank,
                    ROW_NUMBER() OVER (PARTITION BY group_title ORDER BY salesrank ASC) as rn
                FROM products
            ) as ranked
            WHERE rn <= 10
            ORDER BY group_title, salesrank ASC;
        """)
        top_products = self.cursor.fetchall()

        print("\nOs 10 produtos líderes de venda em cada grupo:\n")
        for product in top_products:
            print(product)

    except Exception as e:
        print(f"Não foi possível realizar a consulta d. Erro: {e}")    

  def opcao_e(self):
    print("\nVocê escolheu a opção e!")
    try:
        self.cursor.execute("""
            SELECT p.title, ROUND(AVG(r.helpful),2) AS avg_helpful
            FROM reviews r
            JOIN products p ON r.product_id = p.id
            GROUP BY p.title
            ORDER BY avg_helpful DESC
            LIMIT 10
        """)
        helpful_products = self.cursor.fetchall()

        print("\nOs 10 produtos com a maior média de avaliações úteis positivas:\n")
        for product in helpful_products:
            print(product)

    except Exception as e:
        print(f"Não foi possível realizar a consulta e. Erro: {e}")

        
  def opcao_f(self):
    print("\nVocê escolheu a opção f!")

    try:
        self.cursor.execute("""
            SELECT c.title, ROUND(AVG(r.helpful),2) AS avg_helpful
            FROM reviews r
            JOIN products_categories pc ON r.product_id = pc.product_id
            JOIN categories c ON pc.category_id = c.id
            GROUP BY c.title
            ORDER BY avg_helpful DESC
            LIMIT 5
        """)
        helpful_categories = self.cursor.fetchall()

        print("\nAs 5 categorias de produto com a maior média de avaliações úteis positivas:\n")
        for category in helpful_categories:
            print(category)

    except Exception as e:
        print(f"Não foi possível realizar a consulta f. Erro: {e}")


  def opcao_g(self):
    print("\nVocê escolheu a opção g!")
    try:
        self.cursor.execute("""
            SELECT customer, group_title, review_count
            FROM (
                SELECT c.customer, p.group_title, COUNT(*) AS review_count,
                    ROW_NUMBER() OVER (PARTITION BY p.group_title ORDER BY COUNT(*) DESC) as rn
                FROM reviews r
                JOIN customers c ON r.customer = c.customer
                JOIN products p ON r.product_id = p.id
                GROUP BY c.customer, p.group_title
            ) AS ranked
            WHERE rn <= 10
            ORDER BY group_title, review_count DESC;
        """)

        top_customers = self.cursor.fetchall()

        print("\nOs 10 clientes que mais fizeram comentários por grupo de produto:\n")
        for customer in top_customers:
            print(customer)

    except Exception as e:
        print(f"Não foi possível realizar a consulta g. Erro: {e}")

def main():
  
    database = Database()
  
    print('Conectando com o Banco de Dados...')
    database.connect()

    print("\nDASHBOARD MENU\n")
    print("a) Consultar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação de um produto.") 
    print("b) Consultar os produtos similares com maiores vendas do que o produto.")
    print("c) Consultar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.")
    print("d) Consultar os 10 produtos líderes de venda em cada grupo de produtos.")
    print("e) Consultar os 10 produtos com a maior média de avaliações úteis positivas por produto.")
    print("f) Consultar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto.")
    print("g) Consultar os 10 clientes que mais fizeram comentários por grupo de produto.")

    while True:
      opcao = input("\nEscolha uma opção entre a e g (ou 's' para sair): ").lower()

      if opcao == 'a':
        database.opcao_a()
      elif opcao == 'b':
        database.opcao_b()
      elif opcao == 'c':
        database.opcao_c()
      elif opcao == 'd':
        database.opcao_d()
      elif opcao == 'e':
        database.opcao_e()
      elif opcao == 'f':
        database.opcao_f()
      elif opcao == 'g':
        database.opcao_g()
      elif opcao == 's':
        print("Encerrando o programa.")
        break  
      else:
        print("Você escolheu uma opção inválida!")

if __name__ == '__main__':
  main()
