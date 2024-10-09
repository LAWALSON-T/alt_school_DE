# ETL PROJECT

## Objectives

- Download data from kaggle
- Create docker containers for airflow, DBT , POSTGRES
- Ingest data from local storage to PostgreSQL database
- Load the transformed data into Google BigQuery.
- Transform the data using dbt and prepare it for analytics.
- Analyze the data to answer specific business questions.

## Technology Stack

- Docker: To containerize and orchestrate the Airflow and dbt services.
- Airflow: For orchestrating the ETL processes.
- PostgreSQL: The source database for raw eCommerce data.
- Google BigQuery: The destination where transformed data is loaded and stored.
- dbt (Data Build Tool): To handle the transformations and modeling of the data.
- Python: Used in Airflow for automation scripts.
- GitHub: To manage code and version control.

---

## Steps to Run the Project 

### Prerequisites:
- Docker installed on your local machine.
- A Google Cloud Platform (GCP) project with BigQuery enabled.
- Service account credentials for BigQuery with access to the dataset.

### Steps:

1. Clone the repository:
   bash
   git [clone (https://github.com/LAWALSON-T/alt_school_DE.git)]
   cd alt_school_DE
   

2. Set up environment variables:
   Create an .env file in the root directory and add the following variables:
   bash
   AIRFLOW_UID=50000
   AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3
   also among files provided is an env file containing tokens and passwords
   

4. Configure Google Cloud Credentials:
   In event there is an issue with the api key , please Sign up , create dataset and get API KEY
   Ensure your BigQuery service account key is located within you project folder
   

5. Build the Docker images:
   bash
   docker compose build
   

6. Start the services:
   bash
   docker compose up -d
   

7. Run the ETL pipeline:
   - Open Airflow at http://localhost:8080 and trigger the DAG that runs the ETL process (PostgreSQL to BigQuery) if it didnt trigger automatically.
   
8. Run dbt transformations:
   bash
   docker compose run dbt dbt run
   

9. Inspect Results:
   - Visit your BigQuery console and verify that the transformed data is loaded into the correct dataset.

---

##  dbt Models and Transformations

1. Raw Models:
   - raw_orders.sql: Extracts data from the PostgreSQL orders table and stores it in BigQuery.
   
2. Staging Models:
   - stg_orders.sql: Cleans and standardizes the raw orders data,making it easier to work with in subsequent transformations
   - stg_order_items.sql: Cleans and standardizes the order items data, which includes calculating item totals.
   - stg_products.sql: This staging model organizes data from the products table, making product details easier to join with other models.
   - stg_customers.sql: This staging model organizes data from the customers table, making customers details easier to join with other models.
     
3. Intermediate Models:
   - int_sales_by_category.sql: This intermediate model aggregates the sales data by product category. It joins the stg_orders_items and stg_products (dimension table) to 
     get the total sales for each product category.
   - int_avg_deliver_time.sql: This intermediate model calculates the delivery time for each order and then calculates the average delivery time per customer.
   - int_orders_by_state.sql: This intermediate model joins orders and customers table to get state information and counts orders per state

3. Final Models:
   - fct_sales_by_category.sql: This final model pulls data from the intermediate model int_sales_by_category to create a materialized view that holds the total sales by 
     product category.
   - fct_avg_delivery_time.sql: This final model calculates the overall average delivery time across all customers, using the intermediate model int_avg_delivery_time.
   - fct_orders_by_state.sql: This final model aggregates the total number of orders by customer state.
   
4. dbt Macros and Utils:
   - dbt_utils: A collection of utilities to simplify the development of complex SQL transformations.

---

## Analytical Questions Answered

- The average delivery time for orders across all locations is approximately 12.09 days
  
- Which products are selling the most?
  Beleza _saude is the product category with the most sales followed closely by relogos_presentes

- Sau Paulo with state code SP has the most orders , followed by Rio de Janeiro with code RJ

---


