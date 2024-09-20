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
      product_asin = input("\nVocê escolheu a opção a! Digite o ASIN do produto a ser consultado: ")

      self.cursor.execute("SELECT id FROM products WHERE asin=(%s)", (product_asin,))
      product_id=self.cursor.fetchone()[0]

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
    product_asin = input("\nVocê escolheu a opção b! Digite o ASIN do produto a ser consultado: ")

  def opcao_c(self):
    try:
      product_asin = input("\nVocê escolheu a opção c! Digite o ASIN do produto a ser consultado: ")

      self.cursor.execute("SELECT id FROM products WHERE asin=(%s)", (product_asin,))
      product_id=self.cursor.fetchone()[0]

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

  def opcao_d():
    print("\nVocê escolheu a opção d!")

  def opcao_e():
    print("\nVocê escolheu a opção e!")

  def opcao_f():
    print("\nVocê escolheu a opção f!")

  def opcao_g():
    print("\nVocê escolheu a opção g!")

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
