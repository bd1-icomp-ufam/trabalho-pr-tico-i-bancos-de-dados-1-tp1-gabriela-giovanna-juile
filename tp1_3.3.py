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
      product_id = 15
      print(self.cursor.execute("SELECT title FROM products WHERE id = %s", (product_id,)))
      result = self.cursor.fetchone()

      if result:
        print(result[0])
      else:
        print("No product found with this ID.")

    except Exception as e:
      print('Não foi possível realizar a consulta a')
      self.connection.commit()

  def opcao_b(self):
    
  
  def opcao_c(self):

  def opcao_d(self):

  def opcao_e(self):

  def opcao_f(self):

  def opcao_g(self):

  
  

def main():
    # prune_file(FILE_PATH, 'amazon-meta-light.txt')
  
    database = Database()
  
    print('Conectando com o Banco de Dados...')
    database.connect()

    print("DASHBOARD MENU\n")
    print("a) Consultar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação de um produto.") 
    print("b) Consultar os produtos similares com maiores vendas do que o produto.")
    print("c) Consultar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.")
    print("d) Consultar os 10 produtos líderes de venda em cada grupo de produtos.")
    print("e) Consultar os 10 produtos com a maior média de avaliações úteis positivas por produto.")
    print("f) Consultar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto.")
    print("g) Consultar os 10 clientes que mais fizeram comentários por grupo de produto.")

    database.opcao_a()

if __name__ == '__main__':
  main()
