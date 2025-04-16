from flask import Flask, request, jsonify
from flask_cors import CORS
from utils.clickhouse_utils import ClickHouseConnector
from utils.file_utils import FileHandler
import jwt
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

# Configuration
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB


@app.route('/api/connect/clickhouse', methods=['POST'])
def connect_clickhouse():
    try:
        data = request.json
        connector = ClickHouseConnector(
            host=data['host'],
            port=data['port'],
            user=data.get('user'),
            password=data.get('password'),
            jwt_token=data.get('jwt_token'),
            database=data.get('database')
        )

        # Test connection
        tables = connector.get_tables()
        return jsonify({
            'status': 'success',
            'tables': tables,
            'message': 'Connected to ClickHouse successfully'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400


@app.route('/api/clickhouse/columns', methods=['POST'])
def get_clickhouse_columns():
    try:
        data = request.json
        connector = ClickHouseConnector(
            host=data['host'],
            port=data['port'],
            user=data.get('user'),
            password=data.get('password'),
            jwt_token=data.get('jwt_token'),
            database=data.get('database')
        )

        columns = connector.get_table_columns(data['table'])
        return jsonify({
            'status': 'success',
            'columns': columns
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400


@app.route('/api/ingest/clickhouse-to-file', methods=['POST'])
def clickhouse_to_file():
    try:
        data = request.json
        connector = ClickHouseConnector(
            host=data['host'],
            port=data['port'],
            user=data.get('user'),
            password=data.get('password'),
            jwt_token=data.get('jwt_token'),
            database=data.get('database')
        )

        file_handler = FileHandler()
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], data['filename'])

        # Get data from ClickHouse
        query = f"SELECT {', '.join(data['columns'])} FROM {data['table']}"
        if data.get('where_clause'):
            query += f" WHERE {data['where_clause']}"

        if data.get('limit'):
            query += f" LIMIT {data['limit']}"

        df = connector.execute_query(query)

        # Save to file
        file_handler.save_to_file(df, file_path, data['file_type'])

        return jsonify({
            'status': 'success',
            'message': f'Data exported to {file_path}',
            'record_count': len(df)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400


@app.route('/api/ingest/file-to-clickhouse', methods=['POST'])
def file_to_clickhouse():
    try:
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'No file uploaded'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'status': 'error', 'message': 'No selected file'}), 400

        data = request.form.to_dict()

        # Save uploaded file
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)

        # Read file
        file_handler = FileHandler()
        df = file_handler.read_file(file_path, data.get('delimiter', ','))

        # Connect to ClickHouse
        connector = ClickHouseConnector(
            host=data['host'],
            port=data['port'],
            user=data.get('user'),
            password=data.get('password'),
            jwt_token=data.get('jwt_token'),
            database=data.get('database')
        )

        # Create table if not exists
        if data.get('create_table', 'false').lower() == 'true':
            connector.create_table_from_df(
                df,
                data['table_name'],
                primary_key=data.get('primary_key')
            )

        # Insert data
        record_count = connector.insert_dataframe(df, data['table_name'])

        return jsonify({
            'status': 'success',
            'message': f'Data imported to ClickHouse table {data["table_name"]}',
            'record_count': record_count
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400


@app.route('/api/clickhouse/join-tables', methods=['POST'])
def join_tables():
    try:
        data = request.json
        connector = ClickHouseConnector(
            host=data['host'],
            port=data['port'],
            user=data.get('user'),
            password=data.get('password'),
            jwt_token=data.get('jwt_token'),
            database=data.get('database')
        )

        # Construct JOIN query
        join_clause = f" FROM {data['main_table']}"
        for join in data['joins']:
            join_clause += f" {join['type']} JOIN {join['table']} ON {join['condition']}"

        query = f"SELECT {', '.join(data['columns'])} {join_clause}"
        if data.get('where_clause'):
            query += f" WHERE {data['where_clause']}"

        df = connector.execute_query(query)
        return jsonify({
            'status': 'success',
            'data': df.to_dict('records'),
            'query': query
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400


if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)
