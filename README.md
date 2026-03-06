# Global Air Quality Data Engineering Pipeline

## Project Overview

This project develops an end-to-end data engineering pipeline to process and analyze global air quality data. The system streams raw environmental data, performs data cleaning and validation, and stores the processed data in both document and graph databases for analysis. The project supports Sustainable Development Goals (SDG) 3 (Good Health and Well-being) and SDG 11 (Sustainable Cities and Communities) by providing structured air quality data for environmental monitoring and decision-making.

## Dataset
The dataset used in this project was obtained from Kaggle, titled “Global Urban Air Quality Index Dataset (2015–2025)”.
The dataset contains air quality measurements from multiple global cities, including:

* AQI (Air Quality Index)
* PM2.5
* PM10
* NO₂
* SO₂
* CO
* O₃
* Temperature
* Humidity
* Wind Speed

These data sources are collected from environmental monitoring agencies such as EPA, EEA, OpenAQ, and WAQI.

## System Architecture

The pipeline includes the following stages:

1. **Data Streaming**

   * Apache Kafka is used to stream raw air quality data from CSV files.

2. **Data Processing**

   * Python scripts perform data cleaning, validation, standardization, and duplicate removal.

3. **Data Storage**

   * **MongoDB** stores structured air quality documents.
   * **Neo4j** stores relationships between cities, pollutants, and environmental factors.

4. **Real-Time Processing**

   * Spark Structured Streaming processes incoming data streams and performs aggregations.

## Technologies Used

* Python
* Apache Kafka
* Spark Structured Streaming
* MongoDB
* Neo4j
* HDFS

## Project Structure

```
data engineering
│
├── Task1StreamRawData
│   ├── producer.py
│   └── consumer.py
│   └── global_air_quality.csv
│
├── Task2ProcessData
│   ├── _init_.py
│   └── noise.csv
│   └── Task2Noise.py
│   └── runPipeline.py
│   └── preprocess
│       └── _init_.py
│       └── enrich.py
│       └── fill_AQI.py
│       └── fillers.py
│       └── imputers.py
│       └── rangeCheck.py
│       └── removeDuplicate.py
│       └── standardizer.py
│   └── validate
│       └── _init_.py
│       └── error_labeler.py
│       └── validation_config.py
│       └── validation_predicates.py
│       └── validator_pipeline.py
│       └── validity_rules.py
│
├── Task3Mongo
│   ├── pymongo_utils.py
│   └── transform_and_load.py
│   └── analytics_queries.py
│   └── create_indexes.py
│   └── Task3.py
│
├── Task4Neo4j
│   ├── load_to_neo4j.py
│   └── loader.py
│   └── neo4j_queries.py
│   └── neo4j_writer.py
│   └── Task4.py
│
├─ Task5Kafka
│   ├── dashboard_streamer.py
│   └── main_streaming.py
│   └── streaming_processor.py
│
└── README.md
```

## Key Features

* Real-time air quality data streaming using Kafka
* Automated data cleaning and validation pipeline
* Storage using both document database (MongoDB) and graph database (Neo4j)
* Streaming analytics using Spark Structured Streaming

## Authors

Group Project – Data Engineering

* Tay Zhuang Yin
* Kam Win Ni
* Lee Qian Hui
* Yaw Wei Ying
* Yoo Xin Wei
