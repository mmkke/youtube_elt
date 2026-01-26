# youtube_elt


### To do list:
    - finish testing
        - unit
        - integration
        - end-to-end
    - Refactor code and create package source code
    - Add timestamps to ingestion pipeline
    - incorporate slowly changing dimensions



/opt/airflow/
├── dags/
│   └── youtube_api_ingestion.py
│
├── src/
│   └── youtube_elt/
│       ├── __init__.py
│       ├── api/
│       │   ├── __init__.py
│       │   └── extract.py
│       ├── warehouse/
│       │   ├── __init__.py
│       │   ├── data_utils.py
│       │   ├── data_modification.py
│       │   └── data_transformations.py
│       └── config.py
│
├── tests/
│   ├── unit/
│   │   └── test_data_modification.py
│   └── integration/
│       └── test_db_roundtrip.py
│
├── pyproject.toml
└── README.md