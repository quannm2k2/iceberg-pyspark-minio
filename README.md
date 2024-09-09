# **Apache Iceberg on MinIO**

This project demonstrates how to use **Apache Iceberg** with **MinIO** through **PySpark** for managing data storage in **Parquet** format, with **Avro** handling schema and version control.

## **Features**
- Data storage format: **Parquet**
- Schema and version control: **Avro**
- Partitioning techniques for efficient query performance

## **Requirements**
Before you begin, make sure you have the following:
- Python 3.8+
- PySpark 3.2+
- Docker
- MinIO instance running locally
- Hadoop AWS 3.3.2 package for S3 connectivity
- Apache Iceberg 1.6.1

## **Installation and Setup**

### 1. **Clone the Repository**
```bash
git clone https://github.com/NgMQ1111/iceberg-pyspark-minio.git
cd your-repo-name
```

### 2. **Set up MinIO**
MinIO is used as the object storage service in this project.
1. Open the folder containing the `docker-compose.yml` configuration file for MinIO

2. Run MinIO with Docker
- Run the following command in the directory containing `docker-compose.yml`
```bash
docker-compose up -d
```
- Access `http://localhost:9000` with:
   - username `minioadmin`
   - password `minioadmin`

3. Create S3 Bucket
- In the MinIO dashboard, create an S3 bucket to store Apache Iceberg files name.
- Upload file `data-csv/supermarket_sales.csv` to your bucket.

### 3. **Install Apache Iceberg and Configuration**
**Step 1: Install Iceberg**
- You can use **PySpark** or **Java** for **Apache Iceberg**. Here is an example using **PySpark**.
- First, let's install **Apache Iceberg** with **PySpark**:
  
  ```bash
  pip install pyspark
  pip install iceberg
  ```
**Step 2: Configure Environment Variables**
- Create a `.env` file in the project root directory with your MinIO credentials
  ```bash
  MINIO_ACCESS_KEY=minioadmin
  MINIO_SECRET_KEY=minioadmin
  ```

**Step 3: Configure Apache Iceberg with S3 (MinIO)**
- It is the file `supermarket/spark_config.py`

**Step 4: Create Iceberg Table with Parquet and Avro**
- Apache Iceberg defaults to Parquet caching and Avro version control. If you want to change this default configuration, see `https://iceberg.apache.org/docs/latest/configuration`

***Note: Spark and Hadoop Configuration***
- The project uses PySpark and Hadoop's S3A file system to connect to MinIO. The `spark_config.py` contains the configuration for the Spark session. Ensure that you configure your environment properly

### 4. Run Project
- Perform `INSERT`, `UPDATE`, `DELETE` and some basic queries
- In the MinIO dashboard, access the data storage bucket to view information about the `data files`, `manifest files`, and `metadata file`.



