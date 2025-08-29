# Flask API server that retrieves KPI data from a PostgreSQL database
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote_plus

app = Flask(__name__)

# Database connection parameters
DB_HOST = 'avo-adb-001.postgres.database.azure.com'
DB_PORT = '5432'
DB_NAME = 'KPI_files'
DB_USER = 'adminavo'
DB_PASSWORD = '$#fKcdXPg4@ue8AW'

def create_db_connection():
    password_encoded = quote_plus(DB_PASSWORD)
    connection_string = f"postgresql+psycopg2://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine

@app.route('/kpi-data', methods=['GET'])
def get_plant_data():
    # Get plant parameter
    plant_name = request.args.get('plant')
    
    if not plant_name:
        return jsonify({"error": "Plant parameter is required"}), 400
    
    try:
        engine = create_db_connection()
        
        # Get analysis data
        analysis_query = """
        SELECT * FROM analysis WHERE LOWER(plant) = LOWER(%(plant_name)s)
        """
        analysis_df = pd.read_sql_query(analysis_query, engine, params={'plant_name': plant_name})
        
        # Get export data
        export_query = """
        SELECT * FROM export WHERE LOWER(plant) = LOWER(%(plant_name)s)
        """
        export_df = pd.read_sql_query(export_query, engine, params={'plant_name': plant_name})
        
        # Check if plant exists
        if analysis_df.empty and export_df.empty:
            return jsonify({"error": "Plant not found"}), 404
        
        # Convert to JSON
        response = {
            "plant": plant_name,
            "analysis": analysis_df.to_dict('records'),
            "export": export_df.to_dict('records')
        }
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)