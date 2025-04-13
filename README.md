Bronze Layer: Handles raw data ingestion.​
GitHub
+11
GitHub
+11
GitHub
+11

Bronze_layer_DB_creation.py: Sets up the database for the Bronze layer.​
GitHub
+2
GitHub
+2
GitHub
+2

Bronze_layer_customer_load.py: Loads raw customer data.​
GitHub
+3
GitHub
+3
GitHub
+3

Bronze_layer_product_catlog_load.py: Loads raw product catalog data.​
GitHub
+10
GitHub
+10
GitHub
+10

Bronze_layer_transaction.py: Loads raw transaction data.​
GitHub
+8
GitHub
+8
GitHub
+8

Silver Layer: Processes and cleanses data from the Bronze layer.​
GitHub
+4
GitHub
+4
GitHub
+4

silver_layer_DB.py: Sets up the database for the Silver layer.​
GitHub
+5
GitHub
+5
GitHub
+5

Silverlayer_customer_load.py: Processes customer data.​
GitHub

silverlayer_product_load.py: Processes product data.​
GitHub
+9
GitHub
+9
GitHub
+9

silverlayer_transaction_load.py: Processes transaction data.​
GitHub
+9
GitHub
+9
GitHub
+9

Gold Layer: Aggregates and prepares data for analytics and reporting.​
GitHub
+1
GitHub
+1

Goldlayer_DB.py: Sets up the database for the Gold layer.​

Goldlayer_DailySales.py: Generates daily sales metrics.​
GitHub

Goldlayer_salesby_category.py: Generates sales metrics by category.​

🛠️ Technologies Used
Databricks: For developing and orchestrating the data pipeline.​
GitHub
+2
GitHub
+2
GitHub
+2

Delta Lake: Provides ACID transactions and scalable metadata handling.​
GitHub
+1
GitHub
+1

PySpark: Used for writing the ETL processes and data transformations.​
GitHub
+1
GitHub
+1

🚀 Getting Started
Clone the Repository:

bash
Copy
Edit
git clone https://github.com/Atharva1700/DatabricksBronzesilvergold.git
Set Up Databricks Environment:

Create a new Databricks workspace or use an existing one.​

Import the .py files as notebooks or scripts into your workspace.​

Execute the Pipelines:

Run the Bronze layer scripts to ingest raw data.​
GitHub
+2
GitHub
+2
GitHub
+2

Proceed with the Silver layer scripts for data cleansing and transformation.​
GitHub
+5
GitHub
+5
GitHub
+5

Finally, execute the Gold layer scripts to generate analytical datasets.​
GitHub
+4
