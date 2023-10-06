#!/usr/bin/env python
# coding: utf-8

# In[26]:


pip install mysql-connector-python


# In[27]:


import mysql.connector
from mysql.connector import Error
import pandas as pd


# In[28]:


def create_server_connection(host_name,user_name,user_password):
    
    global connection
    connection = None
    
    try:
        connection = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = user_password
        )
        print("MySQL connection Successfull")
        
    except Error as er:
        print(f"Error: {er}")
        
    return connection

ps = "hpsweta@1123"

db = "Internship"

create_server_connection("127.0.0.1","root",ps)


# In[29]:


#create database Internship

def create_database(connection,query):
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        print("Database Created Successfully")
        
    except Error as er:
        print(f"Error:{er}")
         
create_database_query = "create Database Internship"
create_database(connection,create_database_query)


# In[30]:


# connect to database
def create_db_connection(host_name,user_name,user_password,db_name):
      connection = None
        
      try:
          connection = mysql.connector.connect(
          host = host_name,
          user = user_name,
          password = user_password,
          database = db_name
          )
        
          print("Mysql Database connection Successful")
        
      except Error as er:
          print(f"Error: {er}")
    
      return connection


# In[31]:


# Execute SQL Queries

global cursor

def execute_query(connection,query):
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        connection.commit()
        print("Query is Successful")
        
    except Error as er:
        print(f"Error:{er}")


# In[32]:


# Create order table

create_orders_table = """
Create table orders(
order_id int primary key,
customer_name varchar(30) not null,
product_name varchar(30) not null,
date_ordered date,
quantity int,
unit_price float,
phone_number varchar(30));
"""

# connect to the database
connection = create_db_connection("127.0.0.1","root",ps,db)
execute_query(connection,create_orders_table)


# In[33]:


# insert data

data_orders = """
insert into orders values
(101, 'Jonson' ,'laptop', '2019-10-12',1, 500, '9199292938'),
(102, 'Romeo', 'books', '1019-01-13',4, 200, '9199292238'),
(103, 'Jos', 'mobile', '1019-10-28',6, 340, '9102292932'),
(104, 'Marry', 'shoes', '2019-06-24',5, 600, '9199296631'),
(105, 'Steve', 'Tie', '2017-12-02',8, 200, '9197792900'),
(106, 'Dolly', 'T-Shirt', '2022-09-18',9, 100, '9128292944'),
(107, 'Dassy', 'cosmatic', '1999-10-09',11, 670,'9199292990'),
(108, 'John', 'Books', '2000-11-07',10, 200,'9199292956'),
(109, 'Nancy', 'Headphone', '2019-12-29',20, 200, '9199292944'),
(110, 'jonny', 'Shirt', '2011-04-29',17 ,290, '9199292932');
"""

connection = create_db_connection("127.0.0.1","root",ps,db)
execute_query(connection,data_orders)


# In[34]:


def read_query(connection,query):
    cursor = connection.cursor()
    results = None
    
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as er:
        print(f"Error: {er}")


# In[35]:


# Usinf Select statement

Q1 = """
Select * from orders;
"""

connection = create_db_connection("127.0.0.1","root",ps,db)
results = read_query(connection,Q1)

for result in results:
    print(result)


# In[36]:


from_db = []

for result in results:
    result = list(result)
    from_db.append(result)
    
columns = ["order_id", "customer_name", "product_name", "date_ordered", "quantity",
"unit_price", "phone_number"]

df = pd.DataFrame(from_db,columns = columns)
display(df)


# In[37]:


Q2 = """
SELECT DISTINCT year(date_ordered) FROM orders;
"""

connection = create_db_connection("127.0.0.1","root",ps,db)
results = read_query(connection,Q2)

for result in results:
    print(result)


# In[38]:


1- delete a row 2- upadate a record 3- fetch some condtion ( unit price is greater or smaller ) 4- query with order by 5- select * from orders;


# In[41]:


Q3 = """
SELECT COUNT(*) FROM orders WHERE unit_price BETWEEN 300 AND 600;
"""

connection = create_db_connection("127.0.0.1","root",ps,db)
results = read_query(connection,Q3)

for result in results:
    print(result)


# In[40]:


Q4 = """
DELETE FROM orders WHERE product_name = 'cosmatic';

"""

connection = create_db_connection("127.0.0.1","root",ps,db)
results = read_query(connection,Q4)

for result in results:
    print(result)


# In[39]:


Q5 = """
Select * from orders;
"""

connection = create_db_connection("127.0.0.1","root",ps,db)
results = read_query(connection,Q5)

for result in results:
    print(result)


# In[ ]:




