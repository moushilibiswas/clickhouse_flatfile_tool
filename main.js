import React, { useState } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Card, Tab, Tabs, Alert, Spinner, Table } from 'react-bootstrap';
import { faDatabase, faFile, faExchangeAlt, faCheckCircle, faExclamationTriangle } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import 'bootstrap/dist/css/bootstrap.min.css';

function App() {
  const [activeTab, setActiveTab] = useState('clickhouseToFile');
  const [clickhouseConfig, setClickhouseConfig] = useState({
    host: 'localhost',
    port: '8123',
    database: 'default',
    user: '',
    password: '',
    jwt_token: ''
  });
  const [fileConfig, setFileConfig] = useState({
    filename: 'output.csv',
    fileType: 'csv',
    delimiter: ','
  });
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [status, setStatus] = useState({ loading: false, message: '', error: false });
  const [result, setResult] = useState(null);
  const [previewData, setPreviewData] = useState([]);

  const handleClickhouseConfigChange = (e) => {
    setClickhouseConfig({
      ...clickhouseConfig,
      [e.target.name]: e.target.value
    });
  };

  const handleFileConfigChange = (e) => {
    setFileConfig({
      ...fileConfig,
      [e.target.name]: e.target.value
    });
  };

  const connectClickhouse = async () => {
    setStatus({ loading: true, message: 'Connecting to ClickHouse...', error: false });
    try {
      const response = await axios.post('http://localhost:5000/api/connect/clickhouse', clickhouseConfig);
      setTables(response.data.tables);
      setStatus({ loading: false, message: response.data.message, error: false });
    } catch (error) {
      setStatus({ loading: false, message: error.response?.data?.message || error.message, error: true });
    }
  };

  const loadColumns = async () => {
    if (!selectedTable) return;

    setStatus({ loading: true, message: 'Loading columns...', error: false });
    try {
      const response = await axios.post('http://localhost:5000/api/clickhouse/columns', {
        ...clickhouseConfig,
        table: selectedTable
      });
      setColumns(response.data.columns);
      setSelectedColumns(response.data.columns.map(col => col.name));
      setStatus({ loading: false, message: 'Columns loaded successfully', error: false });
    } catch (error) {
      setStatus({ loading: false, message: error.response?.data?.message || error.message, error: true });
    }
  };

  const handleColumnSelection = (columnName, isSelected) => {
    if (isSelected) {
      setSelectedColumns([...selectedColumns, columnName]);
    } else {
      setSelectedColumns(selectedColumns.filter(col => col !== columnName));
    }
  };

  const executeIngestion = async () => {
    setStatus({ loading: true, message: 'Starting data ingestion...', error: false });
    setResult(null);

    try {
      let response;
      if (activeTab === 'clickhouseToFile') {
        response = await axios.post('http://localhost:5000/api/ingest/clickhouse-to-file', {
          ...clickhouseConfig,
          table: selectedTable,
          columns: selectedColumns,
          filename: fileConfig.filename,
          file_type: fileConfig.fileType
        });
      } else {
        // For file upload, we need to handle it differently
        const formData = new FormData();
        const fileInput = document.querySelector('#fileUpload');
        if (fileInput.files.length === 0) {
          throw new Error('Please select a file to upload');
        }
        formData.append('file', fileInput.files[0]);

        // Add other configs
        Object.entries({
          ...clickhouseConfig,
          table_name: selectedTable,
          create_table: true,
          delimiter: fileConfig.delimiter
        }).forEach(([key, value]) => {
          formData.append(key, value);
        });

        response = await axios.post('http://localhost:5000/api/ingest/file-to-clickhouse', formData, {
          headers: {
            'Content-Type': 'multipart/form-data'
          }
        });
      }

      setResult(response.data);
      setStatus({ loading: false, message: 'Ingestion completed successfully', error: false });
    } catch (error) {
      setStatus({ loading: false, message: error.response?.data?.message || error.message, error: true });
    }
  };

  const getPreview = async () => {
    if (activeTab === 'fileToClickhouse') {
      const fileInput = document.querySelector('#fileUpload');
      if (fileInput.files.length === 0) return;

      const file = fileInput.files[0];
      const reader = new FileReader();

      reader.onload = (e) => {
        try {
          const text = e.target.result;
          const lines = text.split('\n').slice(0, 101); // Get first 100 rows + header
          const headers = lines[0].split(fileConfig.delimiter);
          const data = lines.slice(1).map(line => {
            const values = line.split(fileConfig.delimiter);
            return headers.reduce((obj, header, i) => {
              obj[header] = values[i];
              return obj;
            }, {});
          });
          setPreviewData(data);
        } catch (error) {
          setStatus({ loading: false, message: 'Error parsing file preview', error: true });
        }
      };

      reader.readAsText(file);
    } else {
      // ClickHouse preview
      try {
        const response = await axios.post('http://localhost:5000/api/ingest/clickhouse-to-file', {
          ...clickhouseConfig,
          table: selectedTable,
          columns: selectedColumns,
          filename: 'preview_temp.csv',
          file_type: 'csv',
          limit: 100
        }, {
          params: {
            preview: true
          }
        });
        setPreviewData(response.data.preview);
      } catch (error) {
        setStatus({ loading: false, message: error.response?.data?.message || error.message, error: true });
      }
    }
  };

  return (
    <Container className="mt-4">
      <h1 className="text-center mb-4">
        <FontAwesomeIcon icon={faExchangeAlt} className="me-2" />
        Bidirectional Data Ingestion Tool
      </h1>

      <Tabs
        activeKey={activeTab}
        onSelect={(k) => setActiveTab(k)}
        className="mb-3"
      >
        <Tab eventKey="clickhouseToFile" title="ClickHouse to File">
          <Card>
            <Card.Body>
              <h4><FontAwesomeIcon icon={faDatabase} className="me-2" />ClickHouse Configuration</h4>
              <Form className="mb-4">
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Host</Form.Label>
                      <Form.Control
                        type="text"
                        name="host"
                        value={clickhouseConfig.host}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Port</Form.Label>
                      <Form.Control
                        type="text"
                        name="port"
                        value={clickhouseConfig.port}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                </Row>
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Database</Form.Label>
                      <Form.Control
                        type="text"
                        name="database"
                        value={clickhouseConfig.database}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>User</Form.Label>
                      <Form.Control
                        type="text"
                        name="user"
                        value={clickhouseConfig.user}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                </Row>
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Password</Form.Label>
                      <Form.Control
                        type="password"
                        name="password"
                        value={clickhouseConfig.password}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>JWT Token</Form.Label>
                      <Form.Control
                        type="text"
                        name="jwt_token"
                        value={clickhouseConfig.jwt_token}
                        onChange={handleClickhouseConfigChange}
                        placeholder="Optional"
                      />
                    </Form.Group>
                  </Col>
                </Row>
                <Button variant="primary" onClick={connectClickhouse} disabled={status.loading}>
                  {status.loading ? <Spinner animation="border" size="sm" /> : 'Connect'}
                </Button>
              </Form>

              {tables.length > 0 && (
                <>
                  <h5>Tables</h5>
                  <Form.Select
                    className="mb-3"
                    value={selectedTable}
                    onChange={(e) => setSelectedTable(e.target.value)}
                  >
                    <option value="">Select a table</option>
                    {tables.map(table => (
                      <option key={table} value={table}>{table}</option>
                    ))}
                  </Form.Select>

                  <Button
                    variant="secondary"
                    onClick={loadColumns}
                    disabled={!selectedTable || status.loading}
                    className="mb-3"
                  >
                    Load Columns
                  </Button>
                </>
              )}

              {columns.length > 0 && (
                <>
                  <h5>Select Columns</h5>
                  <div className="mb-3" style={{ maxHeight: '300px', overflowY: 'auto' }}>
                    {columns.map(column => (
                      <Form.Check
                        key={column.name}
                        type="checkbox"
                        id={`col-${column.name}`}
                        label={`${column.name} (${column.type})`}
                        checked={selectedColumns.includes(column.name)}
                        onChange={(e) => handleColumnSelection(column.name, e.target.checked)}
                      />
                    ))}
                  </div>
                </>
              )}

              <h4><FontAwesomeIcon icon={faFile} className="me-2" />File Configuration</h4>
              <Form className="mb-4">
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Filename</Form.Label>
                      <Form.Control
                        type="text"
                        name="filename"
                        value={fileConfig.filename}
                        onChange={handleFileConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>File Type</Form.Label>
                      <Form.Select
                        name="fileType"
                        value={fileConfig.fileType}
                        onChange={handleFileConfigChange}
                      >
                        <option value="csv">CSV</option>
                        <option value="excel">Excel</option>
                        <option value="json">JSON</option>
                      </Form.Select>
                    </Form.Group>
                  </Col>
                </Row>
              </Form>

              <div className="d-flex justify-content-between">
                <Button
                  variant="info"
                  onClick={getPreview}
                  disabled={selectedColumns.length === 0 || status.loading}
                >
                  Preview Data
                </Button>

                <Button
                  variant="success"
                  onClick={executeIngestion}
                  disabled={selectedColumns.length === 0 || status.loading}
                >
                  {status.loading ? <Spinner animation="border" size="sm" /> : 'Start Ingestion'}
                </Button>
              </div>
            </Card.Body>
          </Card>
        </Tab>

        <Tab eventKey="fileToClickhouse" title="File to ClickHouse">
          <Card>
            <Card.Body>
              <h4><FontAwesomeIcon icon={faFile} className="me-2" />File Configuration</h4>
              <Form className="mb-4">
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>File Upload</Form.Label>
                      <Form.Control
                        type="file"
                        id="fileUpload"
                        accept=".csv,.xlsx,.xls,.json"
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Delimiter (for CSV)</Form.Label>
                      <Form.Control
                        type="text"
                        name="delimiter"
                        value={fileConfig.delimiter}
                        onChange={handleFileConfigChange}
                        placeholder=","
                      />
                    </Form.Group>
                  </Col>
                </Row>
              </Form>

              <h4><FontAwesomeIcon icon={faDatabase} className="me-2" />ClickHouse Configuration</h4>
              <Form className="mb-4">
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Host</Form.Label>
                      <Form.Control
                        type="text"
                        name="host"
                        value={clickhouseConfig.host}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Port</Form.Label>
                      <Form.Control
                        type="text"
                        name="port"
                        value={clickhouseConfig.port}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                </Row>
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Database</Form.Label>
                      <Form.Control
                        type="text"
                        name="database"
                        value={clickhouseConfig.database}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Table Name</Form.Label>
                      <Form.Control
                        type="text"
                        value={selectedTable}
                        onChange={(e) => setSelectedTable(e.target.value)}
                        placeholder="Enter new table name"
                      />
                    </Form.Group>
                  </Col>
                </Row>
                <Row>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>User</Form.Label>
                      <Form.Control
                        type="text"
                        name="user"
                        value={clickhouseConfig.user}
                        onChange={handleClickhouseConfigChange}
                      />
                    </Form.Group>
                  </Col>
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>JWT Token</Form.Label>
                      <Form.Control
                        type="text"
                        name="jwt_token"
                        value={clickhouseConfig.jwt_token}
                        onChange={handleClickhouseConfigChange}
                        placeholder="Optional"
                      />
                    </Form.Group>
                  </Col>
                </Row>
              </Form>

              <div className="d-flex justify-content-between">
                <Button
                  variant="info"
                  onClick={getPreview}
                  disabled={status.loading}
                >
                  Preview Data
                </Button>

                <Button
                  variant="success"
                  onClick={executeIngestion}
                  disabled={!selectedTable || status.loading}
                >
                  {status.loading ? <Spinner animation="border" size="sm" /> : 'Start Ingestion'}
                </Button>
              </div>
            </Card.Body>
          </Card>
        </Tab>
      </Tabs>

      {status.message && (
        <Alert variant={status.error ? 'danger' : 'info'} className="mt-3">
          {status.error ? (
            <FontAwesomeIcon icon={faExclamationTriangle} className="me-2" />
          ) : status.loading ? (
            <Spinner animation="border" size="sm" className="me-2" />
          ) : (
            <FontAwesomeIcon icon={faCheckCircle} className="me-2" />
          )}
          {status.message}
        </Alert>
      )}

      {result && (
        <Alert variant="success" className="mt-3">
          <FontAwesomeIcon icon={faCheckCircle} className="me-2" />
          {result.message} - {result.record_count} records processed
        </Alert>
      )}

      {previewData.length > 0 && (
        <Card className="mt-3">
          <Card.Header>Data Preview (First 100 rows)</Card.Header>
          <Card.Body style={{ maxHeight: '400px', overflowY: 'auto' }}>
            <Table striped bordered hover size="sm">
              <thead>
                <tr>
                  {previewData.length > 0 && Object.keys(previewData[0]).map(header => (
                    <th key={header}>{header}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {previewData.map((row, i) => (
                  <tr key={i}>
                    {Object.values(row).map((value, j) => (
                      <td key={j}>{value}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </Table>
          </Card.Body>
        </Card>
      )}
    </Container>
  );
}

export default App;
