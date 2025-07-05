🎵 YouTube Global Music ETL with Apache Airflow
This project builds a complete ETL pipeline to extract global trending music videos from YouTube, transform and clean the data, and load it into a structured MySQL database using Apache Airflow.

It demonstrates data engineering skills using modular Python scripts, task scheduling, and Dockerized orchestration.

## 📌 Project Highlights

- 🔄 **ETL Workflow**: Python scripts to extract, transform, and load data
- ⏱ **Scheduled Automation**: DAGs managed via Apache Airflow
- 🐳 **Containerized Setup**: All services run via Docker Compose
- 🌍 **Global Coverage**: Supports videos across continents and regions
- 🧊 **Snowflake Schema**: Well-structured relational schema for scalability

📂 Project Structure
.
├── dags/
│   ├── youtube_dag.py          # Airflow DAG definition
│   └── scripts/
│       ├── youtube_extract.py  # Extracts trending videos via YouTube API
│       ├── youtube_transform.py# Transforms and cleans data
│       └── youtube_load.py     # Loads data into MySQL
├── output/                     # Stores intermediate JSON and CSVs
├── docker-compose.yml          # Docker environment for Airflow, MySQL, PostgreSQL
└── README.md

🔁 ETL Pipeline Overview
1. 🛠 Extract (youtube_extract.py)
Fetches top trending music videos from YouTube API for multiple countries grouped by continent.

Retrieves metadata like:

Video ID, title, description, channel, thumbnail

View count, like count, comment count

Tags and region data

Saves raw JSON to:
/opt/airflow/output/youtube_raw.json

2. 🧹 Transform (youtube_transform.py)
Reads the raw JSON and loads it into a pandas DataFrame.

Cleans missing/null values, standardizes types.

Splits the data into four logical tables:

video_details.csv

video_metrics.csv

video_regions.csv

tags_table.csv

Saves intermediate cleaned files to /opt/airflow/output/

3. 🧱 Load (youtube_load.py)
Connects to MySQL database.

Inserts the cleaned data into the following tables:

video_details

video_metrics

video_regions

tags_table

Uses foreign keys to map video relationships (e.g., region and metrics).

🧊 Database Schema — Snowflake Design
The schema follows a snowflake design with normalized tables and foreign keys.

ER Diagram (Logical View):

video_details
    ├── video_details_id (PK)
    ├── ⬑── video_regions
    │         └── country_name, country_code, continent_region
    ├── ⬑── tags_table
    │         └── tag
    ├── ⬑── video_metrics
              └── view_count, like_count, comment_count
✅ DDL (MySQL)

CREATE TABLE video_details (
  video_details_id INT AUTO_INCREMENT PRIMARY KEY,
  processed_at DATE DEFAULT (CURRENT_DATE),
  video_id VARCHAR(500),
  published_at DATETIME,
  channel_id VARCHAR(500),
  song_title LONGTEXT,
  song_description LONGTEXT,
  song_thumbnail LONGTEXT,
  channel_title VARCHAR(500)
) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE TABLE video_regions (
  video_region_id INT AUTO_INCREMENT PRIMARY KEY,
  processed_at DATE DEFAULT (CURRENT_DATE),
  video_details_id INT,
  country_name VARCHAR(300),
  country_code VARCHAR(200),
  continent_region VARCHAR(300),
  FOREIGN KEY (video_details_id) REFERENCES video_details(video_details_id)
) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE TABLE tags_table (
  video_tag_id INT AUTO_INCREMENT PRIMARY KEY,
  video_details_id INT,
  processed_at DATE DEFAULT (CURRENT_DATE),
  tag TEXT,
  FOREIGN KEY (video_details_id) REFERENCES video_details(video_details_id)
) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE TABLE video_metrics (
  video_metrics_id INT AUTO_INCREMENT PRIMARY KEY,
  video_details_id INT,
  video_region_id INT,
  processed_at DATE DEFAULT (CURRENT_DATE),
  view_count BIGINT,
  like_count BIGINT,
  favourite_count BIGINT,
  comment_count BIGINT,
  FOREIGN KEY (video_details_id) REFERENCES video_details(video_details_id),
  FOREIGN KEY (video_region_id) REFERENCES video_regions(video_region_id)
) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

⚙️ Airflow DAG (youtube_dag.py)
Defines a scheduled Airflow DAG that runs daily at 7:30 AM.

Pipeline tasks:

run_youtube_extract

run_youtube_transform

run_youtube_load

extract >> transform >> load


🐳 Docker Setup
Services:
webserver: Apache Airflow UI and executor

scheduler: Airflow scheduler

postgres: Metadata DB for Airflow

mysql: Target DB for storing YouTube music data

🧑‍💻 Technologies Used
Python

Apache Airflow

Docker Compose

YouTube Data API v3

MySQL

pandas / pycountry / requests

✅ To Do / Improvements
Add deduplication & idempotency in loading step

Add unit tests for transformation logic

Enhance logging & error tracking

Create dashboard

🙌 Acknowledgements
YouTube Data API

Apache Airflow

Docker Community

🙌 Author
Ashutosh Ghosh
📫 coldaghosh@gmail.com
