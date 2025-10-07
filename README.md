🏎️ Formula One Data Analytics Platform
A dynamic, metadata-driven data pipeline that transforms historical Formula One data into actionable insights through automated ETL processes and interactive visualizations.

https://img.shields.io/badge/Airflow-DAGs-orange
https://img.shields.io/badge/PostgreSQL-Database-blue
https://img.shields.io/badge/Tableau-Visualization-orange
https://img.shields.io/badge/Python-3.11-green

📊 Overview
This project implements a sophisticated data engineering platform that processes decades of Formula One historical data through a metadata-driven architecture. Unlike traditional static ETL systems, this platform dynamically discovers data schemas, automatically adapts to changes, and generates analytical datamarts without manual intervention.

🚀 Features
🔍 Dynamic Schema Discovery: Automatically detects and processes new CSV files without code changes

📈 Metadata-Driven ETL: Centralized metadata management enables adaptive data processing

⚡ Parallel Processing: Efficient chunked loading for large datasets with configurable parallelism

📊 Interactive Dashboards: Tableau-powered visualizations with dynamic filtering and performance analytics

🔧 Automated Orchestration: Apache Airflow-managed workflows with comprehensive monitoring

💾 Multi-Format Output: PostgreSQL storage with automated CSV exports for BI tools

🏗️ Architecture
text
Raw CSV Files → Schema Discovery → Metadata Repository → Data Lake → Datamart Transformation → Visualization
System Components
Data Ingestion Layer: Dynamic CSV processing with automatic type inference

Metadata Management: Centralized schema registry and transformation rules

Transformation Engine: SQL-based datamart creation with business logic

Visualization Layer: Tableau dashboards with interactive analytics

🛠️ Installation & Setup
Prerequisites
Docker & Docker Compose

Python 3.11+

PostgreSQL 15+

Tableau Public (for visualization)

Quick Start
Clone the repository

bash
git clone https://github.com/yourusername/formulaone-analytics.git
cd formulaone-analytics
Start the services

bash
docker-compose up -d
Initialize Airflow

bash
docker exec -it airflow-webserver airflow db init
docker exec -it airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
Access the applications

Airflow UI: http://localhost:8080

PostgreSQL: localhost:5432

pgAdmin: http://localhost:5050

📁 Project Structure
text
formulaone-analytics/
├── dags/
│   ├── lake/                    # Data ingestion DAGs
│   │   ├── historical_load.py   # Main data loading DAG
│   │   └── populate_metadata.py # Metadata management DAG
│   ├── datamart/               # Transformation DAGs
│   │   └── formulaone_datamart.py
│   └── scripts/                # SQL transformation files
│       ├── Driver_Career_Stats.sql
│       └── ... (more datamarts)
├── data/
│   ├── historical/             # Source CSV files
│   └── datamart/              # Exported CSV files
├── docker/
│   └── docker-compose.yml      # Container configuration
├── docs/
│   └── technical-documentation.md
└── README.md
🎯 Usage
1. Data Ingestion
Place CSV files in data/historical/ and trigger the historical_load DAG in Airflow. The system will:

Automatically discover all CSV files

Infer schemas and data types

Load data into PostgreSQL with chunked processing

Populate metadata repository

2. Datamart Creation
Execute the formulaone_datamart DAG to:

Transform raw data into analytical datamarts

Generate driver performance statistics

Export results to CSV for Tableau

Update all metadata relationships

3. Visualization
Connect Tableau to the exported CSV files in data/datamart/ to create interactive dashboards featuring:

Driver career performance trends

Season-by-season comparisons

Constructor team analytics

Interactive filtering and parameter controls

📊 Sample Dashboards
Driver Performance Dashboard
Career points progression across seasons

Win percentage analysis

Performance consistency metrics

Head-to-head driver comparisons

Season Analytics
Race performance heatmaps

Constructor championship standings

Circuit performance analysis

Historical trend visualization

🔧 Configuration
Environment Variables
bash
POSTGRES_DB=formulaone_db
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
AIRFLOW_UID=50000
YAML Configuration
yaml
tables:
  - lake_races
  - lake_drivers
  - lake_constructors
  - lake_results
  - lake_circuits
🧪 Testing
Run the complete pipeline:

bash
# Trigger data ingestion
docker exec airflow-webserver airflow dags trigger historical_load

# Execute datamart transformations
docker exec airflow-webserver airflow dags trigger formulaone_datamart
📈 Performance
Processing Speed: Handles 100,000+ records with chunked loading

Memory Efficiency: Optimized for systems with 16GB RAM

Parallel Execution: Configurable task parallelism (default: 2 concurrent tasks)

Storage: Efficient PostgreSQL indexing and partitioning

🤝 Contributing
We welcome contributions! Please see our Contributing Guidelines for details.

Fork the repository

Create a feature branch (git checkout -b feature/amazing-feature)

Commit your changes (git commit -m 'Add amazing feature')

Push to the branch (git push origin feature/amazing-feature)

Open a Pull Request

📄 License
This project is licensed under the MIT License - see the LICENSE file for details.

🙏 Acknowledgments
Formula One World Championship Limited for historical data

Apache Airflow community for workflow orchestration

Tableau for visualization capabilities

PostgreSQL community for database management

📞 Support
For support, please open an issue in the GitHub issue tracker or contact the development team.

Built with ❤️ for the F1 analytics community