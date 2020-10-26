from pyspark import SparkConf
from pyspark.sql import SparkSession

def main():
 spark=(SparkSession.builder.appName('Retail Analytics').config("hive.metastore.uris","thrift://localhost:9083",conf=SparkConf()).enableHiveSupport().getOrCreate())
 spark.sparkContext.setLogLevel("ERROR")
 db1 = getRdbmsData(spark,"empoffice","employees","employeeNumber")
 db1.createOrReplaceTempView("employee")
 print(" employee data pulled from mysql !! ")

 db2 = getRdbmsData(spark,"empoffice","offices","officecode")
 db2.createOrReplaceTempView("offices")
 print(" offices data pulled from mysql !!")
 
 custSql="""(select c.customerNumber, upper(c.customerName)as customerName, c.contactFirstName, c.contactLastName, c.phone, c.addressLine1, c.city as city, c.state, c.postalCode, c.country, c.salesRepEmployeeNumber, c.creditLimit, p.checkNumber, p.paymentDate, p.amount from customers c inner join payments p on c.customerNumber = p.customerNumber) as cust_pay"""

 db3 = getRdbmsData(spark,"custpayments",custSql,"city")
 db3.createOrReplaceTempView("cust_payment")
 print("customer and payment data joined successfully pulled from mysql ")

 ordsql = """(select o.customerNumber, o.orderNumber, o.orderDate as orderdate, o.shippedDate, o.status, o.comments, od.quantityOrdered, od.priceEach, od.orderLineNumber, p.productCode, p.productName, p.productLine, p.productScale, p.productVendor, p.productDescription, p.quantityInStock, p.buyPrice, p.MSRP from orders o inner join orderdetails od on o.orderNumber = od.orderNumber inner join products p on od.productCode = p.productCode) as order_prod"""
 db4 = getRdbmsData(spark,"ordersproducts",ordsql,"orderDate")
 db4.createOrReplaceTempView("order_products")
 print(" order data successfully pulled from mysql !! ")

 processCustPayData(spark)

 print("Customer data has been processed successfully and loaded into retail_mart.custordfinal Hive table")
 print("Cutomer payment details availabe in /user/hduser/output/CustPayment file")

def writetoes(df):
  df.write.mode("overwrite").format("org.elasticsearch.spark.sql").option("es.resource","custfinales2/doc1").save()
  

def getRdbmsData(spark,DatabaseName, TableName, PartitionColumn):
 Driver = "com.mysql.jdbc.Driver"
 Host = "localhost"
 Port = "3306"
 User = "root"
 Pass = "root"
 url = "jdbc:mysql://{0}:{1}/{2}".format(Host, Port, DatabaseName)
 df_db = spark.read.format("jdbc") \
 .option("driver", Driver) \
 .option("url", url) \
 .option("user", User) \
 .option("lowerBound", 1) \
 .option("upperBound", 10000) \
 .option("numPartitions", 4) \
 .option("partitionColumn", PartitionColumn) \
 .option("password", Pass) \
 .option("dbtable", TableName).load()
 return df_db

def processCustPayData(spark):
 spark.sql("""select customerNumber,customerName,concat(contactFirstName,',',contactLastName) as contactfullname,addressLine1,city,state,postalCode,country,phone,creditLimit,checkNumber,amount,paymentDate from cust_payment""").createOrReplaceTempView("custdetcomplextypes")

 spark.sql("""select customerNumber, orderNumber, shippedDate, status, comments, productCode, quantityOrdered, priceEach, orderLineNumber, productName, productLine, productScale, productVendor, productDescription, quantityInStock, buyPrice,MSRP, orderDate from order_products""").createOrReplaceTempView("orddetcomplextypes")

 custpaymentcomplextypes = spark.sql("""select CONCAT(customerNumber,'~',checkNumber,'~',CONCAT(creditLimit,'$',amount),'~',paymentDate) from cust_payment""")
 custpaymentcomplextypes.write.mode("overwrite").option("header","true").csv("hdfs://localhost:54310/user/hduser/output/CustPayment")

 custfinal = spark.sql("""insert into retail_mart.custordfinal select cd.customerNumber customernumber,cd.customerName customername,cd.contactfullname contactfullname,cd.addressLine1 addressLine1,cd.city city,cd.state state,cd.country country,cast(cd.phone as bigint) phone,cast(cd.creditLimit as float) creditlimit,cd.checkNumber checknum,cast(cd.amount as int) checkamt,o.orderNumber ordernumber,cast(o.shippedDate as date) shippeddate,o.status,o.comments,o.productCode,cast(o.quantityOrdered as int) quantityordered,cast(o.priceEach as double)priceeach,cast(o.orderLineNumber as int) orderlinenumber,o.productName,o.productLine,productScale,o.productVendor,o.productDescription,cast(o.quantityInStock as int) quantityInStock,cast(o.buyPrice as double) buyPrice,cast(o.MSRP as double) MSRP,cast(o.orderDate as date) orderdate from custdetcomplextypes cd inner join orddetcomplextypes o on cd.customernumber=o.customernumber""")
 
 custfinales = spark.sql("""select cd.customerNumber customernumber,cd.customerName customername,cd.city city,cast(o.quantityInStock as int) quantityInStock,cast(o.MSRP as double) MSRP,cast(o.orderDate as date) orderdate from custdetcomplextypes cd inner join orddetcomplextypes o on cd.customerNumber=o.customerNumber""")
 writetoes(custfinales)
 print("""Calling the writetoes function to load the custfinales dataframe into Elastic search index""")


if __name__ == "__main__":
 main()

 

