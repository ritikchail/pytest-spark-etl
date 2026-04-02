# PySpark ETL Pipeline with PostgreSQL

A comprehensive ETL (Extract, Transform, Load) pipeline built with Apache Spark and PostgreSQL, featuring automated testing and data validation.

## Overview

This project demonstrates a complete data engineering workflow:
- **Extract**: Read data from PostgreSQL using Spark JDBC
- **Transform**: Aggregate and process seller data
- **Load**: Collect results for further analysis
- **Test**: Comprehensive unit and integration testing

## Features

- 🚀 **Apache Spark** for distributed data processing
- 🐘 **PostgreSQL** (Neon cloud) database integration
- ✅ **Pytest** with fixtures for robust testing
- 🔧 **Environment-based configuration** for security
- 📊 **Data validation** and quality checks
- 🎯 **Modular architecture** with clear separation of concerns

## Prerequisites

- Python 3.8+
- Apache Spark
- PostgreSQL database (Neon recommended)
- Git

## Quick Start

### 1. Clone and Setup
```bash
git clone <your-repo-url>
cd pytest-spark-etl
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Environment Configuration
```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Database Setup
```bash
python run_schema.py  # Create tables
python generate_data.py  # Insert sample data
```

### 4. Run ETL Pipeline
```bash
python simple_etl.py
```

### 5. Run Tests
```bash
# Unit tests only (fast)
pytest -m "not integration"

# Integration tests (requires database)
pytest -m integration

# All tests
pytest
```

## Project Structure

```
├── simple_etl.py          # Main ETL pipeline
├── generate_data.py       # Data generation script
├── run_schema.py          # Database schema setup
├── check_conn.py          # Connection testing
├── tests/
│   ├── conftest.py        # Test fixtures and configuration
│   ├── test_unit.py       # Unit tests
│   └── test_integration.py # Integration tests
├── .env.example           # Environment template
└── requirements.txt       # Python dependencies
```

## Key Components

### ETL Pipeline (`simple_etl.py`)
- Connects to PostgreSQL via JDBC
- Extracts seller data
- Transforms data (user-level aggregation)
- Loads results to Python collections

### Testing Strategy
- **Unit Tests**: Test Spark operations in isolation
- **Integration Tests**: Test full pipeline with real database
- **Fixtures**: Shared Spark sessions and DB connections

### Data Flow
```
PostgreSQL → Spark JDBC → DataFrame → Transform → Aggregate → Python List
```

## Configuration

Environment variables in `.env`:
- `UPSTREAM_DATABASE`: Database name
- `UPSTREAM_USERNAME`: DB username
- `UPSTREAM_PASSWORD`: DB password
- `UPSTREAM_HOST`: DB host
- `UPSTREAM_PORT`: DB port (default: 5432)


## License

MIT License - see LICENSE file for details