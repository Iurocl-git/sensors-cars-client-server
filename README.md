# Raspberry Pi Sensor Data Collection System

This project implements a data collection and visualization system for environmental sensors connected to a Raspberry Pi. The system consists of a Flask backend server and a client application that collects and sends sensor data.

## Features

- Real-time sensor data collection
- PostgreSQL database storage
- RESTful API endpoints
- Data visualization capabilities
- Docker containerization support

## Prerequisites

- Python 3.x
- PostgreSQL database
- Docker (optional)

## Project Structure

- `main.py` - Flask backend server
- `client.py` - Data collection client
- `data.csv` - Historical data storage
- `Dockerfile` - Container configuration
- `requirements.txt` - Python dependencies

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure database connection in `main.py`:
```python
db_config = {
    'dbname': 'sensors-camera_data',
    'user': 'postgres',
    'password': '1234',
    'host': '127.20.0.12',
    'port': '5432'
}
```

3. Run the server:
```bash
python main.py
```

Or using Docker:
```bash
docker build -t sensor-server .
docker run -p 3000:3000 sensor-server
```

## API Endpoints

### GET /data
Retrieves sensor data based on specified date and time range.

### POST /data
Sends new sensor data to the server.

### POST /data/add
Adds new sensor readings to the database.

## Sensor Data Types

The system collects the following sensor data:
- Temperature (°C)
- Pressure (hPa)
- Humidity (%)
- Light (Lux)
- Oxidized gases (kO)
- Reduced gases (kO)
- NH3 (kO)
- PM1 particles (ug/m3)
- PM2.5 particles (ug/m3)
- PM10 particles (ug/m3)

## License

This project is licensed under the MIT License. 
