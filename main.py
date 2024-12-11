# main.py
from taipy.gui import Gui, notify, State
from google.oauth2 import service_account
from google.cloud import storage
import duckdb
import tempfile
import os
import json
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io

class DuckDBGDriveManager:
    def __init__(self):
        self._temp_dir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._temp_dir, 'pricecatcher.duckdb')
        self._connection = None
        self.FOLDER_ID = "1L0E2fSEAYrpzHV3Jwt1nznjTUJAKQcV_"
        self.DB_NAME = "pricecatcher.duckdb"
        self.table_info = None

    def initialize_connection(self, credentials_dict):
        try:
            # Create temporary credentials file
            creds_path = os.path.join(self._temp_dir, 'creds.json')
            with open(creds_path, 'w') as f:
                json.dump(credentials_dict, f)

            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )

            drive_service = build('drive', 'v3', credentials=credentials)
            query = f"name = '{self.DB_NAME}' and '{self.FOLDER_ID}' in parents"
            results = drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            files = results.get('files', [])

            if not files:
                raise FileNotFoundError(f"Could not find {self.DB_NAME} in the specified folder")

            file_id = files[0]['id']
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.FileIO(self._db_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)
            
            done = False
            while not done:
                _, done = downloader.next_chunk()

            self._connection = duckdb.connect(self._db_path, read_only=True)
            self._connection.execute("SET memory_limit='4GB'")
            self._connection.execute("SET threads TO 4")
            
            self.table_info = self._get_table_info()
            return True
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False

    def _get_table_info(self):
        tables = self._connection.execute("SHOW TABLES").fetchdf()
        table_info = {}
        for table_name in tables['name']:
            columns = self._connection.execute(f"DESCRIBE {table_name}").fetchdf()
            table_info[table_name] = columns
        return table_info

    def execute_query(self, query):
        try:
            if not self._connection:
                raise Exception("Database not connected")
            return self._connection.execute(query).fetchdf()
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")

    def cleanup(self):
        if self._connection:
            self._connection.close()
        if os.path.exists(self._temp_dir):
            import shutil
            shutil.rmtree(self._temp_dir)

# Global variable for database manager
db_manager = DuckDBGDriveManager()

# Initial state
state = State()
state.credentials_content = None
state.connection_status = False
state.query = "SELECT *\nFROM your_table\nLIMIT 5;"
state.results = None
state.table_info = None
state.execution_time = None
state.error_message = None

def on_file_upload(state, file):
    try:
        content = json.loads(file)
        state.credentials_content = content
        return content
    except Exception as e:
        notify(state, "error", f"Invalid credentials file: {str(e)}")
        return None

def connect_database(state):
    if state.credentials_content:
        if db_manager.initialize_connection(state.credentials_content):
            state.connection_status = True
            state.table_info = db_manager.table_info
            notify(state, "success", "Connected successfully!")
        else:
            notify(state, "error", "Connection failed")
    else:
        notify(state, "error", "Please upload credentials first")

def execute_query(state):
    if not state.connection_status:
        notify(state, "error", "Please connect to database first")
        return
    
    try:
        start_time = datetime.now()
        state.results = db_manager.execute_query(state.query)
        end_time = datetime.now()
        state.execution_time = (end_time - start_time).total_seconds()
        state.error_message = None
        notify(state, "success", f"Query executed in {state.execution_time:.2f} seconds")
    except Exception as e:
        state.error_message = str(e)
        notify(state, "error", str(e))

def download_results(state):
    if state.results is not None:
        return state.results.to_csv(index=False)
    return None

# Taipy page layout
page = """
# PriceCatcher Database Query Interface

<|layout|columns=300px 1|
<|part|class_name=sidebar|
### Configuration
<|file_selector|on_change=on_file_upload|label=Upload Service Account JSON|extensions=.json|>

<|{connection_status}|indicator|value=Connection Status|color=green|>

<|Connect to Database|button|on_action=connect_database|>

### Database Schema
<|{table_info}|table|show_all=true|width=100%|>
|>

<|part|
### Query Editor
<|{query}|text_area|height=200px|>

<|Execute Query|button|on_action=execute_query|>

<|part|render={execution_time is not None}|
Query executed in {execution_time:.2f} seconds
|>

<|part|render={error_message is not None}|class_name=error|
Error: {error_message}
|>

<|part|render={results is not None}|
### Results
<|{results}|table|width=100%|>

<|Download Results|button|on_action=download_results|>
|>
|>
|>
"""

# CSS styles
css = """
.sidebar {
    padding: 20px;
    background-color: #f8f9fa;
}
.error {
    color: red;
    padding: 10px;
    margin: 10px 0;
    background-color: #ffebee;
    border-radius: 4px;
}
"""

# Initialize and run the Taipy application
if __name__ == "__main__":
    gui = Gui(page=page, css=css)
    gui.run(dark_mode=False)