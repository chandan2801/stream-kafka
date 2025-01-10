# stream-kafka
# Data Pipeline Project using GCP

## Project Overview
This project implements a robust data pipeline leveraging Google Cloud Platform (GCP) services to handle streaming and batch data processing. The pipeline fetches JSON data from the Wikipedia Recent Changes API, processes it through Kafka and Spark Streaming, and ultimately stores it in BigQuery for analytics and reporting.

## Pipeline Architecture
1. **Data Ingestion**:
   - The pipeline fetches real-time JSON data from the [Wikipedia Recent Changes API](https://stream.wikimedia.org/v2/stream/recentchange).
   - The fetched data is pushed to a Kafka topic hosted on multiple VM instances in GCP.

2. **Real-Time Processing**:
   - A Spark Streaming job is created using a Dataproc cluster.
   - The Spark job continuously consumes data from the Kafka topic.
   - The data is then pushed to a Hive directory stored in Google Cloud Storage (GCS).

3. **Batch Processing**:
   - A batch job runs on top of the Hive table.
   - The job fetches data, performs transformations and aggregations, and loads the processed data into BigQuery.

## Components Used
### GCP Services:
- **Dataproc**: For running the Spark Streaming job and batch processing.
- **Google Cloud Storage (GCS)**: For storing raw and intermediate data in Hive directories.
- **BigQuery**: For storing transformed and aggregated data for analytics.
- **Composer**: For orchestration of kafka ingestion, spark streaming and bigquery batch processing job.

### Other Tools:
- **Wikipedia Recent Changes API**: The source of streaming JSON data.
- **Kafka**: Hosted on multiple VM instances for real-time data ingestion.
- **Hive**: For structured storage of streaming data in GCS.
- **Spark**: For streaming and batch data processing.
- **Airflow**: For orchestration of all jobs.

## Setup Instructions
### Prerequisites
1. A GCP account with the following enabled:
   - Dataproc
   - Cloud Storage
   - BigQuery
2. Kafka setup on multiple VM instances.
3. Hive metastore setup.

### Steps
1. **Kafka Setup:**
   - Create a Kafka cluster with at least three VM instances. In current scenario, 2 instances have been used.
   - Configure the Kafka topic to receive streaming data.

2. **Data Ingestion:**
   - Write a Python script to fetch data from the Wikipedia Recent Changes API and push it to the Kafka topic.

3. **Dataproc Cluster:**
   - Create a Dataproc cluster to run Spark jobs.

4. **Spark Streaming Job:**
   - Write a Spark Streaming job to consume data from Kafka and store it in a Hive directory on GCS.
   - Deploy the job on the Dataproc cluster.

5. **Batch Processing:**
   - Write a batch job to fetch data from Hive, perform transformations and aggregations, and load it into BigQuery.
   - Schedule the batch job using Airflow on Cloud Composer.

## Notes
 - Detailed explanation of each utility is mentioned within the codebase.
   
## Future Improvements
- Implement data quality checks during ingestion.
- Add monitoring and alerting for Kafka and Spark jobs.
- Optimize batch jobs for faster processing and lower cost.

## Contact
For any queries or issues, please reach out to [cprusty58@gmail.com].


