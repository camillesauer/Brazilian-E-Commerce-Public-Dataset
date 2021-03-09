import pandas as pd
import mysql.connector
import sqlalchemy
import pymysql



engine = sqlalchemy.create_engine("mysql+pymysql://student:PanapoiC19!@localhost/brazilian_e_commerce")

mydb = mysql.connector.connect(
    host="localhost",
    user="student",
    password="psw",
    database="brazilian_e_commerce"
)

mycursor = mydb.cursor()
print(mydb)

#I create my database
create_database= """
_______________
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE brazilian_e_commerce")
"""

# I connect my database
connect_database="""
mydb = mysql.connector.connect(
    host="localhost",
    user="student",
    password="password",
    database="brazilian_e_commerce"
)
"""

# I create tables
create_tables="""
mycursor.execute("CREATE TABLE Customer (customer_id INT PRIMARY KEY, customer_unique_id INT, customer_zip_code_prefix INT(64), customer_city VARCHAR(40), customer_state  VARCHAR(10), FOREIGN KEY(customer_zip_code_prefix) REFERENCES Geolocation (geolocation_zip_code_prefix))")
mycursor.execute("CREATE TABLE Product (product_id INT PRIMARY KEY, product_category_name VARCHAR(100), product_name_lenght FLOAT, product_description_lenght FLOAT, product_photos_qty FLOAT, product_weight_g FLOAT, product_length_cm FLOAT, product_height_cm FLOAT, product_width_cm FLOAT)")
mycursor.execute("CREATE TABLE Seller (seller_id INT PRIMARY KEY, seller_zip_code_prefix INT, seller_city VARCHAR(40), seller_state  VARCHAR(40), FOREIGN KEY(seller_zip_code_prefix) REFERENCES Geolocation(geolocation_zip_code_prefix))")
mycursor.execute("CREATE TABLE ProductCategory (id INT AUTO_INCREMENT PRIMARY KEY, product_category_name VARCHAR(40), product_category_name_english VARCHAR(40))")
mycursor.execute("CREATE TABLE Geolocation (geolocation_zip_code_prefix INT(64) PRIMARY KEY, geolocation_lat FLOAT, geolocation_lng FLOAT, geolocation_city VARCHAR(40), geolocation_state VARCHAR(40))")
mycursor.execute("CREATE TABLE Orders (order_id INT PRIMARY KEY, customer_id INT, order_status VARCHAR(40), order_approved_at  DATETIME, order_purchase_timestamp DATETIME, order_delivered_customer_date DATE, order_delivered_carrier_date DATE, order_estimated_delivery_date DATE, FOREIGN KEY(customer_id) REFERENCES Orders(order_id))"))
mycursor.execute("CREATE TABLE OrderPayment (id INT AUTO_INCREMENT PRIMARY KEY, payment_sequential INT(64), order_id INT, payment_type VARCHAR(40), payment_installments  INT (64), payment_value  FLOAT)")
mycursor.execute("CREATE TABLE OrderReview (review_id INT PRIMARY KEY, order_id INT, review_comment_title VARCHAR(40), review_comment_message TEXT, review_creation_date  DATE, review_answer_timestamp DATETIME, review_score INT(64))")
mycursor.execute("CREATE TABLE OrderItem (order_item_id INT PRIMARY KEY, order_id INT(64),  product_id INT, seller_id  INT, shipping_limit_date DATE, price FLOAT, freight_value FLOAT)")

"""

# I create foreign key and PFK table
create_fk_pfk="""

FK:
mycursor.execute("ALTER TABLE OrderItem ADD FOREIGN KEY(seller_id) REFERENCES OrderItem(order_item_id)")
mycursor.execute("ALTER TABLE OrderItem ADD  FOREIGN KEY(product_id) REFERENCES OrderItem(order_item_id)")
mycursor.execute("ALTER TABLE OrderReview ADD FOREIGN KEY(order_id) REFERENCES OrderReview(review_id)")
mycursor.execute("ALTER TABLE OrderPayment ADD FOREIGN KEY(order_id) REFERENCES OrderPayment(id)")

PFK Table: 

"""

df = pd.read_csv("/home/apprenant/Documents//Brief2/olist_customers_dataset.csv")
df1 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_order_items_dataset.csv")
df2 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_order_payments_dataset.csv")
df3 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_order_reviews_dataset.csv")
df4 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_orders_dataset.csv")
df5 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_products_dataset.csv")
df6 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_sellers_dataset.csv")
df7 = pd.read_csv("/home/apprenant/Documents//Brief2/product_category_name_translation.csv")
df8 = pd.read_csv("/home/apprenant/Documents//Brief2/olist_geolocation_dataset.csv")

df8.drop_duplicates(subset=['geolocation_zip_code_prefix'], inplace= True)

list_df = [df,df1,df2,df3,df4,df5,df6,df7,df8]
list_noms = ['Customer', 'OrderItem', 'OrderPayment', 'OrderReview', 'Order', 'Product', 'Seller', 'ProductCategory', 'Geolocalisation']

count = 0

"""
for i in list_df:
    print(list_noms[count])
    print(i.dtypes)
    count = count + 1
"""

df.to_sql('Customer', engine, if_exists='replace', index=False)
df1.to_sql('OrderItem', engine, if_exists='replace', index=False)
df2.to_sql('OrderPayment', engine, if_exists='replace', index=False)
df3.to_sql('OrderReview', engine, if_exists='replace', index=False)
df4.to_sql('Orders', engine, if_exists='replace', index=False)
df5.to_sql('Product', engine, if_exists='replace', index=False)
df6.to_sql('Seller', engine, if_exists='replace', index=False)
df7.to_sql('ProductCategory', engine, if_exists='replace', index=False)
df8.to_sql('Geolocation', engine, if_exists='replace', index=False)

# #Nombre de client total
# mycursor.execute("SELECT COUNT(*) FROM Customer")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de produit total
# mycursor.execute("SELECT COUNT(*) FROM Product")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de produit par catégorie
# mycursor.execute("SELECT COUNT(*) FROM ProductCategory")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de commande total
# mycursor.execute("SELECT COUNT(*) FROM Orders")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de commande selon leurs états (en cours de livraison etc...)
# mycursor.execute("SELECT COUNT(*) FROM Orders WHERE order_status")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de commande par mois
# mycursor.execute("SELECT EXTRACT(MONTH FROM order_purchase_timestamp) AS MONTH, COUNT(DISTINCT order_id)  FROM Orders GROUP BY MONTH")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Prix moyen d'une commande (panier moyen)
# mycursor.execute("""SELECT AVG(prix_total) FROM (
# SELECT order_id, SUM(payment_value) AS prix_total
# FROM OrderPayment
# GROUP BY order_id) AS test_p""")
# # myresult = mycursor.fetchall()
# # print(myresult)
#
#
# #Score de satisfaction moyen (notation sur la commande)
# mycursor.execute("SELECT AVG(review_score) FROM OrderReview")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de vendeur
# mycursor.execute("SELECT COUNT(*) FROM Seller")
# myresult = mycursor.fetchall()
# print(myresult)
#
# #Nombre de vendeur par région
# mycursor.execute("SELECT COUNT(*) FROM Seller GROUP BY seller_state")
# myresult = mycursor.fetchall()
# print(myresult)

# Nombre de commande par jours
# mycursor.execute("SELECT EXTRACT(DAY FROM order_purchase_timestamp) AS DAY, COUNT(DISTINCT order_id)  FROM Orders GROUP BY DAY")
# myresult = mycursor.fetchall()
# print(myresult)

