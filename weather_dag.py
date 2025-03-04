from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

def kelvin_to_fahrenheit(temp):
    return (temp - 273.15) * (9 / 5) + 32

def transform_and_send_to_kafka(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_far = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_far = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_far = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_far = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone']).isoformat()
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']).isoformat()
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']).isoformat()

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_far,
        "Feels Like (F)": feels_like_far,
        "Minimum Temp (F)": min_temp_far,
        "Maximum Temp (F)": max_temp_far,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('weather_topic', transformed_data)
    producer.flush()
    print(f"Sent data to Kafka: {transformed_data}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email':['ak1401296@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_kafka_dag', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Pune&appid=4880b8a14a2812da4e928ec1598669fb'
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Pune&appid=4880b8a14a2812da4e928ec1598669fb',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_and_send = PythonOperator(
        task_id='transform_and_send_to_kafka',
        python_callable=transform_and_send_to_kafka
    )

    is_weather_api_ready >> extract_weather_data >> transform_and_send
