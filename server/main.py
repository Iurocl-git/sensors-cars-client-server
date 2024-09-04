import pandas as pd
from flask import Flask, jsonify, request
import psycopg2
import datetime
from psycopg2 import sql
from decimal import Decimal

db_config = {
    'dbname': 'sensors_camera',    # Имя базы данных
    'user': 'postgres',           # Имя пользователя
    'password': 'admin',       # Пароль
    'host': 'localhost',               # Хост (обычно localhost)
    'port': '5432'                     # Порт подключения к PostgreSQL (по умолчанию 5432)
}
def get_db_connection():
    """Создание подключения к базе данных."""
    try:
        connection = psycopg2.connect(**db_config)
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


app = Flask(__name__)


df = pd.read_csv('data.csv')
# print(df['Time'])
# Преобразование колонки 'Time' в формат datetime
# df['Time'] = pd.to_datetime(df['Time'], format='%d-%m-%Y %H:%M:%S')


# Răspunde la cererea GET la root ('/')
@app.route('/')
def home():
    return "Salut! Acesta este un server backend simplu pe Python."

# Răspunde la cererea GET la /data
@app.route('/data', methods=['GET'])
def get_data():
    data = {
        "mesaj": "Aceasta este o cerere GET",
        "status": "succes",
    }
    return jsonify(data)

# Răspunde la cererea POST la /data
@app.route('/data', methods=['POST'])
def extract_data():
    data = request.json  # Preia datele trimise în corpul cererii POST
    date_filter = data.get('date')
    time_start = data.get('time_start')
    time_end = data.get('time_end')

    date_start = date_filter + ' ' + time_start
    date_end = date_filter + ' ' + time_end
    date_filter = date_filter.replace("-", '/')
    date_filter = date_filter[8:10] + date_filter[4:8] + date_filter[0:4]

    filtered_df = df[(df['Time'] >= f'{date_filter} {time_start}') & (df['Time'] <= f'{date_filter} {time_end}')]
    filtered_df = filtered_df[['Time', 'Nr. Inmatriculare', 'Tip']]

    type_counts = pd.DataFrame(filtered_df['Tip']).value_counts()

    cars = []
    # for index, value in type_counts.items():
    #     item = {"name": index, "value": value};
    #     cars.append(item)

    for index, value in type_counts.items():
        # item = {"name": index, "value": value};
        cars.append([index, value])

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()

            # cursor.execute('''
            #     SELECT MAX(scan_time) FROM sensor_data WHERE scan_time < %s
            # ''', (date_end,))

            # max_timestamp = cursor.fetchone()[0]

            # cursor.execute('''
            #     SELECT name, value, unit FROM sensor_data
            #     WHERE scan_time = %s
            # ''', (max_timestamp,))

            # print(date_start)
            # date_start = '2024-07-01 00:00'
            # date_end = '2024-12-01 00:00'

            # New version of query
            cursor.execute('''
                            SELECT name, value, unit
                            FROM sensor_data
                            WHERE scan_time > %s AND scan_time < %s;
                        ''', (date_start, date_end))

            rows = cursor.fetchall()

            average_df = pd.DataFrame(rows, columns=['name', 'value', 'unit'])
            average_values = average_df.groupby('name')['value'].mean().reset_index()
            average_result = [(row['name'], round(row['value'], 2), average_df[average_df['name'] == row['name']]['unit'].iloc[0]) for
                              _, row in average_values.iterrows()]

            # print(average_result)
            # print(rows)
            values = []
            for i in range(len(average_result)):
                name, value, unit = average_result[i]
                values.append({"name": name, "value": value, "unit": unit})
                # values.append({"scan_time": scan_time.time().isoformat()[0:8], "name": name, "value": value, "unit": unit})
            # print(values)
            # print(len(values))
            # Сохранение изменений
            connection.commit()
            cursor.close()
            connection.close()

            data = {
                "message": "Data extracted successfully!",
                "row_count": len(filtered_df),
                # "data": rows,
                "values": values,
                "data_cars": cars,
            }

            return jsonify(data), 200
        except Exception as e:
            print(f"Error extracting data: {e}")
            return jsonify({'error': 'Failed to extract data'}), 500
    else:
        return jsonify({'error': 'Failed to connect to database'}), 500

    # values = [
    #     {"name": "temp", "value": 20.0},  # Температура в градусах Цельсия
    #     {"name": "pres", "value": 913.4},  # Давление в гПа (гектопаскаль)
    #     {"name": "humi", "value": 30.4},  # Влажность в процентах
    #     {"name": "ligh", "value": 1299.9},  # Освещенность в люксах
    #     {"name": "oxid", "value": 1.5},  # Значение концентрации окислителя в кОм
    #     {"name": "redu", "value": 211.8},  # Значение концентрации восстановителя в кОм
    #     {"name": "nh3", "value": 2.4},  # Концентрация аммиака (NH3) в кОм
    #     {"name": "pm1", "value": 20.0},  # Концентрация мелких частиц (PM1) в мкг/м3
    #     {"name": "pm25", "value": 38.0},  # Концентрация частиц (PM2.5) в мкг/м3
    #     {"name": "pm10", "value": 44.0}  # Концентрация крупных частиц (PM10) в мкг/м3
    # ]
    # data = {
    #     "status": "success",
    #     "row_count": len(filtered_df),
    #     # "data": rows,
    #     "values": values,
    #     "data_cars": cars,
    # }
    # return jsonify(data), 200

@app.route('/data/add', methods=['POST'])
def add_data():
    to_old_query = "DELETE FROM WHERE sensorsdata < NOW() - INTERVAL 3 DAY;"
    # test_data_old = {0: [1725350414.244102, 'temperature', 81.08137488888889, 'C'], 1: [1725350414.244102, 'pressure', 3671.674325666667, 'hPa'], 2: [1725350414.244102, 'humidity', 95.32543433333333, '%'], 3: [1725350414.244102, 'light', 251.57104711111108, 'Lux'], 4: [1725350414.244102, 'oxidised', 84.48153144444444, 'kO'], 5: [1725350414.244102, 'reduced', 1014.962227, 'kO'], 6: [1725350414.244102, 'nh3', 1057.5502482222223, 'kO'], 7: [1725350414.244102, 'pm1', 74.32111111111111, 'ug/m3'], 8: [1725350414.244102, 'pm25', 111.49444444444444, 'ug/m3'], 9: [1725350414.244102, 'pm10', 111.49444444444444, 'ug/m3']}
    # test_data = {0: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'temperature', 127.24303050802469, 'C'], 1: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'pressure', 5498.587387441235, 'hPa'], 2: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'humidity', 132.49145035814814, '%'], 3: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'light', 348.9750323693827, 'Lux'], 4: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'oxidised', 239.46771395913584, 'kO'], 5: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'reduced', 1365.8151879817285, 'kO'], 6: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'nh3', 1200.95911562, 'kO'], 7: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'pm1', 108.28034444444444, 'ug/m3'], 8: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'pm25', 181.42057654320988, 'ug/m3'], 9: [datetime.datetime(2024, 9, 3, 10, 36, 46, 113943), 'pm10', 191.43058148148148, 'ug/m3']}
    # test_list = list(dict.values(test_data));

    data_list = request.json
    data_list = list(dict.values(data_list))

    # print(list(dict.values(data_list['data'])))
    # print(test_list)
    if not isinstance(data_list, list):
        return jsonify({'error': 'Input should be a list'}), 400

    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()

            insert_query = ''' INSERT INTO sensor_data (scan_time, name, value, unit) 
            VALUES (%s, %s, %s, %s) '''

            data_tuples = [(d[0], d[1], d[2], d[3]) for d in data_list]

            cursor.executemany(insert_query, data_tuples)

            connection.commit()
            cursor.close()
            connection.close()

            return jsonify({'message': 'All data added successfully!'}), 201
        except Exception as e:
            print(f"Error inserting data: {e}")
            return jsonify({'error': 'Failed to add data'}), 500
    else:
        return jsonify({'error': 'Failed to connect to database'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='192.168.168.6', port='16948')