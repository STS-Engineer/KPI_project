## Project Structure

- `api_server.py` - API server implementation for data management
- `process_data_to_db.py` - Script for processing Excel data and storing it in the database
- `requirements.txt` - List of Python dependencies required for the project

## Usage

1. Run the API server:
```bash
python api_server.py
```

2. Process data from Excel files to database:
```bash
python process_data_to_db.py
```

## API Endpoints

### Get Plant KPI Data
Retrieves KPI data for a specific plant, including both analysis and export data.

- **URL**: `/kpi-data`
- **Method**: GET
- **Query Parameters**: 
  - `plant` (required): Name of the plant (e.g., POITIERS, ANHUI, CYCLAM, etc.)

Example request:
```
http://127.0.0.1:5000/kpi-data?plant=POITIERS
```

- `500 Internal Server Error`: Server error

