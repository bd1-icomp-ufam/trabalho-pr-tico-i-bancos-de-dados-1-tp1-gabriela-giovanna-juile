# TP1 - BANCO DE DADOS 1 - 2024/2
#
# Acadêmicas:
#     Maria Gabriela Morais de Sá - 22250537
#     Maria Giovanna Gonçalves Sales - 22251138
#     Juíle Yoshie Sarkis Hanada - 22251135 
#
# SCRIPT 1 - Criação de tabelas, processamento do arquivo e inserção de dados

# VARIAVEIS PARA CONEXÃO COM O BANCO DE DADOS

DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'tp1'
DB_USER = 'postgres'
DB_PASS = 'postgres'

# ARGUMENTOS PARA EXECUÇÃO DO SCRIPT

FILE_PATH = 'amazon-meta-10000.txt'
SKIP_ID = 0
LOG_INTERVAL = 100

# QUERIES SQL PARA CRIAÇÃO DE TABELAS (DDLs)

DROP_TABLES = """
  DROP TABLE IF EXISTS reviews;
  DROP TABLE IF EXISTS customers;
  DROP TABLE IF EXISTS products_categories;
  DROP TABLE IF EXISTS categories;
  DROP TABLE IF EXISTS similar_products;
  DROP TABLE IF EXISTS products;
  DROP TABLE IF EXISTS groups;
"""

CREATE_TABLE_GROUPS = """
  CREATE TABLE groups (
    title VARCHAR(15) NOT NULL,
    PRIMARY KEY (title)
  );
"""

CREATE_TABLE_PRODUCTS = """
  CREATE TABLE products (
    id INTEGER NOT NULL,
    asin VARCHAR(15) NOT NULL UNIQUE,
    title VARCHAR(250) NOT NULL,
    group_title VARCHAR(15) NOT NULL,
    salesrank INTEGER NOT NULL, 
    PRIMARY KEY (id),    
    CONSTRAINT fk_product_group
      FOREIGN KEY (group_title) REFERENCES groups(title)
  );
"""

CREATE_TABLE_SIMILAR_PRODUCTS = """
  CREATE TABLE similar_products (
    product_id INTEGER NOT NULL, 
    similar_asin VARCHAR(15) NOT NULL, 
    PRIMARY KEY (product_id, similar_asin),
    CONSTRAINT fk_similar_product
      FOREIGN KEY (product_id) REFERENCES products(id)
  );
"""

CREATE_TABLE_CATEGORIES = """
  CREATE TABLE categories (
    id INTEGER NOT NULL, 
    title VARCHAR(100) NOT NULL,
    PRIMARY KEY (id)
  );
"""

CREATE_TABLE_PRODUCTS_CATEGORIES = """
  CREATE TABLE products_categories (
    product_id INTEGER NOT NULL, 
    category_id INTEGER NOT NULL, 
    PRIMARY KEY (product_id, category_id),
    CONSTRAINT fk_product
      FOREIGN KEY (product_id) REFERENCES products(id),
    CONSTRAINT fk_category
      FOREIGN KEY (category_id) REFERENCES categories(id)
  );
"""

CREATE_TABLE_CUSTOMERS = """
  CREATE TABLE customers (
    customer VARCHAR(20) NOT NULL,
    PRIMARY KEY (customer)
  );
"""

CREATE_TABLE_REVIEWS= """
  CREATE TABLE reviews (
    id SERIAL,
    product_id INTEGER NOT NULL, 
    customer VARCHAR (20) NOT NULL, 
    date DATE NOT NULL, 
    rating INTEGER NOT NULL, 
    votes INTEGER NOT NULL, 
    helpful INTEGER NOT NULL, 
    PRIMARY KEY (id),
    CONSTRAINT fk_review_product
      FOREIGN KEY (product_id) REFERENCES products(id),
    CONSTRAINT fk_review_customer
      FOREIGN KEY (customer) REFERENCES customers(customer)  
  );
"""

# CONSTANTS

REGEX_DATE_PATTERN = r'^\d{4}-\d{1,2}-\d{1,2}'
REGEX_NUMBER_PATTERN = r'\d+'

# IMPORTS

import re
import psycopg2

# FUNÇÕES AUXILIARES

# para poda de arquivos
def prune_file(input, output, lines):
  with open(input, 'r', encoding = "utf8") as entrada, open(output, 'w') as saida:
    for i, linha in enumerate(entrada):
      if (i > lines):
        break
      saida.write(linha)

# parse string review
def parse_review_string(review_str):
    pattern = r'(?P<date>\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(?P<customer>\S+)\s+rating:\s+(?P<rating>\d+)\s+votes:\s+(?P<votes>\d+)\s+helpful:\s+(?P<helpful>\d+)'
    match = re.search(pattern, review_str)
    if match:
        review_dict = match.groupdict()        
        return review_dict
    else:
        raise ValueError(f"String '{review_str}' does not match the pattern")
      
# parse string category
def parse_category_string(category_str):
    match = re.match(r'^(.*)\[(\d+)\]$', category_str)
    category_dict = {}
    if match:
        category_dict['title'] = match.group(1).strip()  
        category_dict['id'] = match.group(2)          
        return category_dict
    else:
        raise ValueError(f"String '{category_str}' does not match the pattern")

# DATABASE CLASS

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
  
  def create_tables(self):
    try:
      self.cursor.execute(DROP_TABLES)     
      self.cursor.execute(CREATE_TABLE_GROUPS)
      self.cursor.execute(CREATE_TABLE_PRODUCTS)
      self.cursor.execute(CREATE_TABLE_SIMILAR_PRODUCTS)
      self.cursor.execute(CREATE_TABLE_CATEGORIES)
      self.cursor.execute(CREATE_TABLE_PRODUCTS_CATEGORIES)
      self.cursor.execute(CREATE_TABLE_CUSTOMERS)
      self.cursor.execute(CREATE_TABLE_REVIEWS)
    except Exception as e:
      print('Não foi possível realizar a criação das tabelas. \n', e)
      exit()
    self.connection.commit()
    
  def insert_group(self, group):
    try:
      self.cursor.execute("INSERT INTO groups (title) VALUES (%s)", (group,))
    except Exception as e:
      print('Não foi possível realizar a inserção do grupo '+group+'. \n', e)
    self.connection.commit()
      
  def insert_product(self, product):
    try:
      self.cursor.execute("""
            INSERT INTO products (id, asin, title, group_title, salesrank)
            VALUES (%s, %s, %s, %s, %s)
        """, (product['id'], product['asin'], product['title'], product['group_title'], product['salesrank']))
    except Exception as e:
      print('Não foi possível realizar a inserção do produto '+product['id']+'. \n', e)
    self.connection.commit()
  
  def insert_similar_product(self, product_id, similar_asin):
    try:
      self.cursor.execute("INSERT INTO similar_products (product_id, similar_asin) VALUES (%s , %s)", (product_id, similar_asin))
    except Exception as e:
      print('Não foi possível realizar a inserção do relacionamento de produto similar '+product_id +'+'+similar_asin+'. \n', e)
    self.connection.commit()
    
  def insert_customer(self, customer):
    try:
      self.cursor.execute("INSERT INTO customers (customer) VALUES (%s) ON CONFLICT (customer) DO NOTHING", (customer,))
    except Exception as e:
      print('Não foi possível realizar a inserção do customer '+customer+'. \n', e)
    self.connection.commit()
    
  def insert_review(self, product_id, review):
    try:
      self.cursor.execute("""
            INSERT INTO reviews (product_id, customer, date, rating, votes, helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (product_id, review['customer'], review['date'], review['rating'], review['votes'], review['helpful']))
    except Exception as e:
      print('Não foi possível realizar a inserção da review '+review+'. \n', e)
    self.connection.commit()
  
  def insert_category(self, category):
    try:
      self.cursor.execute("INSERT INTO categories (id, title) VALUES (%s , %s)", (category['id'], category['title']))
    except Exception as e:
      print('Não foi possível realizar a inserção da categoria '+category+'. \n', e)
    self.connection.commit()
  
  def insert_product_category(self, product_id, category_id):
    try:
      self.cursor.execute("INSERT INTO products_categories (product_id, category_id) VALUES (%s , %s)", (product_id, category_id))
    except Exception as e:
      print('Não foi possível realizar a inserção do relacionamento produto e categoria '+product_id +'+'+category_id+'. \n', e)
    self.connection.commit()
      
# MAIN

def main():
  # prune_file(FILE_PATH, 'input/amazon-meta-10000.txt', 10000)
  
  database = Database()
  
  print('Conectando com o Banco de Dados...')
  database.connect()
  
  print('Criando tabelas...')
  database.create_tables()
  
  print('Realizando a leitura do arquivo',FILE_PATH,'e inserção no Banco de Dados...')
  print('Produtos lidos: 0...')
  
  groups = []
  product = {}
  similars = []
  product_categories = []
  all_categories = []
  reviews = []
  valid_product = False
  skip = True
  cont_product = 0  
  
  with open(FILE_PATH, 'r', encoding = "utf8") as f:
    for linha in f:
      
      stripped_line = linha.strip()
      
      if (stripped_line.startswith('Id')):
        # início de um novo produto
        valid_product = True
        cont_product+=1
        product = {}
        similars = []
        product_categories = []
        reviews = []
        product['id'] = linha.split(':')[1].strip()
        skip = int(product['id']) < SKIP_ID
        if (cont_product % LOG_INTERVAL == 0):
          print('Produtos lidos: '+str(cont_product)+'...')
        continue
      
      if (stripped_line.startswith('discontinued')):
        valid_product = False
        continue
      
      if (stripped_line.startswith('ASIN')):
        product['asin'] = linha.split(':')[1].strip()
        continue
        
      if (stripped_line.startswith('title')):
        product['title'] = linha.split(':')[1].strip()
        continue
      
      if (stripped_line.startswith('group')):
        group = linha.split(':')[1].strip()
        
        if (group not in groups):
          if (not skip):
            database.insert_group(group)
            groups.append(group)
          
        product['group_title'] = group
        continue
      
      if (stripped_line.startswith('salesrank')):
        product['salesrank'] = linha.split(':')[1].strip()
        continue       
        
      if (stripped_line.startswith('similar')):
        cont_product_similar = int(linha.split()[1].strip())
        if (cont_product_similar > 0):
          similars = stripped_line.split()[2:]
      
      if (stripped_line.startswith('categories: ')):
        continue
    
      if (stripped_line.startswith('|')):
        categories_str = stripped_line.strip('|').split('|')
        
        categories = [parse_category_string(category_str) for category_str in categories_str]
        
        for category in reversed(categories):
          if category not in all_categories:
            if (not skip):
              database.insert_category(category)
              all_categories.append(category)  
          else: 
            break
          
        for category in reversed(categories):
          if category not in product_categories:
            product_categories.append(category)  
          else: 
            break   
        continue
      
      if (stripped_line.startswith('reviews')):
        continue
      
      if (len(stripped_line.split()) > 0):
        if (re.match(REGEX_DATE_PATTERN,stripped_line.split()[0])):
          review = parse_review_string(stripped_line)
          reviews.append(review)
        continue
      
      if (stripped_line == ''):
        # fim de um produto
        product['valid'] = valid_product
        if (valid_product and not skip):
          database.insert_product(product)
          
          for similar in similars:
            database.insert_similar_product(product['id'], similar)
                    
          for review in reviews:
            database.insert_customer(review['customer'])
            database.insert_review(product['id'], review)
          
          for category in product_categories:
            database.insert_product_category(product['id'], category['id'])
        
    # save last product
    if (valid_product and not skip):
      database.insert_product(product)
      
      for similar in similars:
        database.insert_similar_product(product['id'], similar)
        
      for review in reviews:
        database.insert_customer(review['customer'])
        database.insert_review(product['id'], review)
        
      for category in product_categories:
        database.insert_product_category(product['id'], category['id']) 
        
  print('Arquivo processado.')
  database.close()
  print('Total de produtos lidos:', cont_product)
  print('Grupos encontrados: ',len(groups), groups)
  print('Categorias encontradas:', len(all_categories))
    
if __name__ == '__main__':
  main()
