# clickhouse_flatfile_tool

Develop a web-based application with a simple user interface (UI) that facilitates data 
ingestion between a ClickHouse database and the Flat File platform. The application 
must support bidirectional data flow (ClickHouse to Flat File and Flat File to 
ClickHouse), handle JWT token-based authentication for ClickHouse as a source, 
allow users to select specific columns for ingestion, and report the total number of 
records processed upon completion.

# Frontend Implementation (React)

# 1. Set up React project

In PyCharm terminal:
```bash
npx create-react-app frontend
cd frontend
npm install axios react-bootstrap bootstrap @fortawesome/react-fontawesome @fortawesome/free-solid-svg-icons

ctrl+c main.js and ctrl+v into /frontend/src/App.js


# Running the Application

1. **Start the backend**:
   - In PyCharm terminal:
     ```bash
     cd backend
     python app.py
     ```
   - The backend should start on `http://localhost:5000`

2. **Start the frontend**:
   - Open another terminal in PyCharm
   - Navigate to the frontend directory:
     ```bash
     cd frontend
     ```
   - Start the React development server:
     ```bash
     npm start
     ```
   - The frontend should open in your browser at `http://localhost:3000`

# Testing the Application

# 1. File to ClickHouse
1. Select "File to ClickHouse" tab
2. Upload a file (CSV, Excel, or JSON) from data -> pp-monthly-update-new-version.csv
3. Enter ClickHouse connection details
4. Specify a new table name
5. Click "Start Ingestion"

# 2. ClickHouse to File
1. Select "ClickHouse to File" tab
2. Enter ClickHouse connection details
3. Click "Connect"
4. Select a table and click "Load Columns"
5. Select columns to export
6. Configure file output settings
7. Click "Start Ingestion"





