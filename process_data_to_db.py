import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
import re
from urllib.parse import quote_plus

DB_HOST = 'avo-adb-001.postgres.database.azure.com'
DB_PORT = '5432'
DB_NAME = 'KPI_files'
DB_USER = 'adminavo'
DB_PASSWORD ='$#fKcdXPg4@ue8AW'

def create_db_connection():
    try:
        password_encoded = quote_plus(DB_PASSWORD)
        connection_string = f"postgresql+psycopg2://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return None

def extract_plant_name(file_name):
    base_name = os.path.splitext(file_name)[0]
    
    match = re.search(r'KPI\s+([A-Za-z]+)', base_name)
    if match:
        return match.group(1)
    else:
        return base_name.split('_')[0].split(' ')[0]

def process_excel_files(folder_path):
    """
    Process all Excel files in the folder and extract KFS data
    Returns: list of tuples (file_name, plant_name, kfs_table, limit_check_table, last_week_col)
    """
    excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx") and not f.startswith("~$")]
    print(f"Fichiers Excel trouvés : {excel_files}")
    
    processed_files = []
    
    for file_name in excel_files:
        file_path = os.path.join(folder_path, file_name)
        plant_name = extract_plant_name(file_name)

        print(f"Traitement du fichier : {file_path}")
        print(f"Nom de la plante : {plant_name}")

        # ==================== FIRST: READ EXPORT SHEET TO GET LAST WEEK COLUMN ====================
        export_df = pd.read_excel(file_path, sheet_name='EXPORT')
        export_columns = export_df.columns.tolist()
        week_columns = [col for col in export_columns if isinstance(col, str)
                        and col.startswith('w')
                        and not col.startswith('Target')
                        and not col.startswith('Gap')]
        
        # Get the last week column name from EXPORT sheet (e.g., 'w34/25')
        last_week_col = week_columns[-1] if week_columns else None
        print(f"Dernière colonne de semaine : {last_week_col}")

        # ==================== ANALYSE FEUILLE 'ANALYSIS' ====================
        analysis_df = pd.read_excel(file_path, sheet_name='Analysis', header=None)

        def normalize_week_label(label):
            try:
                num = float(label)
                return f"{num:.1f}"
            except (ValueError, TypeError):
                return str(label)

        def clean_numeric_value(value):
            if pd.isna(value):
                return None
            if isinstance(value, str):
                value = str(value).replace(',', '')
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        # Extract week labels from Analysis sheet (columns 2 to 9, row 1)
        week_labels = [normalize_week_label(l) for l in analysis_df.iloc[1, 2:10]]
        kfs_data = []

        for i in range(2, min(analysis_df.shape[0], 202)):
            row = analysis_df.iloc[i]
            if row[1] == 'KFS':
                kfs_name = row[0]
                # Clean numeric values
                standard_values = [clean_numeric_value(val) for val in row[2:10].tolist()]

                min_values = [None] * len(week_labels)
                max_values = [None] * len(week_labels)

                min_row, max_row = None, None
                for j in range(i+1, min(i+6, analysis_df.shape[0])):
                    next_row = analysis_df.iloc[j]
                    if next_row[0] == f"{kfs_name} Min":
                        min_row = [clean_numeric_value(val) for val in next_row[2:10].tolist()]
                    elif next_row[0] == f"{kfs_name} Max":
                        max_row = [clean_numeric_value(val) for val in next_row[2:10].tolist()]
                    if min_row and max_row:
                        break

                # Min values handling - set to 0 if min_row exists
                if min_row:
                    min_values = [0] * len(week_labels)
                if max_row:
                    max_values = max_row

                kfs_data.append((kfs_name, 'Standard', standard_values))
                kfs_data.append((kfs_name, 'Min', min_values))
                kfs_data.append((kfs_name, 'Max', max_values))

        formatted_kfs_data = []
        for name, status, values in kfs_data:
            formatted_kfs_data.append([name, status] + values)

        # Create DataFrame for analysis table
        kfs_table = pd.DataFrame(formatted_kfs_data, columns=['kfs_name', 'status'] + week_labels)
        kfs_table['plant'] = plant_name
        
        # ADD THE LAST_WEEK_LABEL COLUMN TO ANALYSIS TABLE - FIXED
        kfs_table['last_week_label'] = last_week_col if last_week_col else None
        
        # Reorder columns to match DB (with original week labels)
        analysis_db_columns = ['plant', 'kfs_name', 'status'] + week_labels + ['last_week_label']
        kfs_table = kfs_table.reindex(columns=analysis_db_columns)

        kfs_names = sorted(set(name for name, status, values in kfs_data if status == 'Standard'))
        print("✅ Feuille 'Analysis' analysée.")

        # ==================== ANALYSE FEUILLE 'EXPORT' ====================
        limit_check_data = []

        for kfs_name in kfs_names:
            kfs_row = export_df[export_df['KPI_Name'] == kfs_name]
            if not kfs_row.empty:
                resp = kfs_row['Resp'].values[0] if pd.notna(kfs_row['Resp'].values[0]) else None
                trend = kfs_row['TrendToLookFor'].values[0] if pd.notna(kfs_row['TrendToLookFor'].values[0]) else None
                min_val = kfs_row['Minimum'].values[0] if pd.notna(kfs_row['Minimum'].values[0]) else None
                max_val = kfs_row['Maximum'].values[0] if pd.notna(kfs_row['Maximum'].values[0]) else None
                
                # Get last week value if column exists
                if last_week_col and last_week_col in export_df.columns:
                    last_week_value = kfs_row[last_week_col].values[0] if pd.notna(kfs_row[last_week_col].values[0]) else None
                else:
                    last_week_value = None
                    print(f"Warning: Column {last_week_col} not found in EXPORT sheet")

                # Check if value respects limits
                try:
                    if pd.notna(last_week_value) and pd.notna(min_val) and pd.notna(max_val):
                        val = float(last_week_value)
                        min_val = float(min_val)
                        max_val = float(max_val)
                        respect = 'Yes' if min_val <= val <= max_val else 'No'
                    else:
                        respect = 'Not checked'
                except (ValueError, TypeError):
                    respect = 'Not checked'
            else:
                resp = trend = min_val = max_val = last_week_value = None
                respect = 'Not checked'
                print(f"Warning: KFS {kfs_name} not found in EXPORT sheet")

            limit_check_data.append([
                kfs_name, resp, trend, min_val, max_val, last_week_value, last_week_col, respect
            ])

        # Create DataFrame for export table
        limit_check_table = pd.DataFrame(limit_check_data, columns=[
            'kfs_name', 'resp', 'trend_to_look_for', 'minimum', 'maximum', 'last_week_value', 'last_week_label', 'respect_limit'
        ])
        limit_check_table['plant'] = plant_name
        
        # Reorder columns to match DB
        limit_check_table = limit_check_table.reindex(columns=[
            'plant', 'kfs_name', 'resp', 'trend_to_look_for', 'minimum', 'maximum', 'last_week_value', 'last_week_label', 'respect_limit'
        ])
        
        processed_files.append((file_name, plant_name, kfs_table, limit_check_table, last_week_col))
        print(f"✅ Fichier {file_name} traité avec succès.")
        print(f"   - Analysis table shape: {kfs_table.shape}")
        print(f"   - Export table shape: {limit_check_table.shape}")
        print(f"   - Last week label: {last_week_col}")
    
    return processed_files

def save_to_database(processed_files):
    """
    Save processed data to database
    """
    db_engine = create_db_connection()
    if db_engine is None:
        print("Failed to connect to database. Cannot save to DB.")
        return False
    
    print("\n💾 Sauvegarde vers la base de données...")
    
    for file_name, plant_name, kfs_table, limit_check_table, last_week_col in processed_files:
        try:
            # Save analysis data
            kfs_table.to_sql('analysis', db_engine, if_exists='append', index=False)
            print(f"✅ Données 'Analysis' de {file_name} insérées dans la base de données.")
            
            # Save export data
            limit_check_table.to_sql('export', db_engine, if_exists='append', index=False)
            print(f"✅ Données 'EXPORT' de {file_name} insérées dans la base de données.")
            
        except Exception as e:
            print(f"❌ Erreur lors de l'insertion des données de {file_name}: {str(e)}")
            return False
    
    print("✅ Toutes les données ont été sauvegardées en base de données.")
    return True

def save_to_excel_files(processed_files, output_folder_path):
    """
    Save processed data to individual Excel files
    """
    os.makedirs(output_folder_path, exist_ok=True)
    
    print(f"\n📁 Sauvegarde vers des fichiers Excel dans : {output_folder_path}")
    
    for file_name, plant_name, kfs_table, limit_check_table, last_week_col in processed_files:
        base_filename = os.path.splitext(file_name)[0]
        output_path = os.path.join(output_folder_path, f"{base_filename}_KFS_Analysis.xlsx")
        
        try:
            # Create Excel writer
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Save analysis table
                kfs_table.to_excel(writer, sheet_name='Analysis', index=False)
                
                # Save export table
                limit_check_table.to_excel(writer, sheet_name='EXPORT', index=False)
            
            print(f"📄 Fichier consolidé généré : {output_path}")
            
        except Exception as e:
            print(f"❌ Erreur lors de la génération du fichier Excel {output_path}: {str(e)}")
    
    print("✅ Tous les fichiers Excel ont été générés.")

def main():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    print(f"Chemin du fichier actuel : {folder_path}")
    
    output_folder_path = os.path.join(folder_path, "KPI_Files_Analysis")
    print(f"Chemin du dossier de sortie : {output_folder_path}")
    
    # Process all Excel files
    processed_files = process_excel_files(folder_path)
    
    if not processed_files:
        print("Aucun fichier à traiter.")
        return
    
    save_to_database(processed_files)
    save_to_excel_files(processed_files, output_folder_path)
    print("\n🎉 Traitement terminé!")

if __name__ == "__main__":
    main()