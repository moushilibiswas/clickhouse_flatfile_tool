import pandas as pd
import os


class FileHandler:
    def read_file(self, file_path, delimiter=','):
        try:
            if file_path.endswith('.csv'):
                return pd.read_csv(file_path, delimiter=delimiter)
            elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                return pd.read_excel(file_path)
            elif file_path.endswith('.json'):
                return pd.read_json(file_path)
            else:
                raise ValueError("Unsupported file format")
        except Exception as e:
            raise Exception(f"Error reading file: {str(e)}")

    def save_to_file(self, df, file_path, file_type='csv'):
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            if file_type == 'csv':
                df.to_csv(file_path, index=False)
            elif file_type == 'excel':
                df.to_excel(file_path, index=False)
            elif file_type == 'json':
                df.to_json(file_path, orient='records')
            else:
                raise ValueError("Unsupported file format")
        except Exception as e:
            raise Exception(f"Error saving file: {str(e)}")

    def get_file_preview(self, file_path, delimiter=',', rows=100):
        try:
            df = self.read_file(file_path, delimiter)
            return df.head(rows).to_dict('records')
        except Exception as e:
            raise Exception(f"Error getting file preview: {str(e)}")
