import os
import time
import json
import boto3
import requests
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

#load environment variables
load_dotenv()

class WeatherDashboard:
    def __init__(self):
        self.api_key = os.getenv('OPEN_WEATHER_API_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.s3_client = boto3.client('s3')

    def create_bucket_if_not_exists(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            # First check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} already exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    location = {'LocationConstraint': 'eu-north-1'}
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration=location
                    )
                    print(f"Successfully created bucket {self.bucket_name}")
                except Exception as e:
                    print(f"Error creating bucket: {e}")


    def get_weather_data(self, city):
        # fetch weather data from open weather api
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric"
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching weather data: {e}")
            return None

    def save_to_s3(self, weather_data, city):
        # save weather data to s3 bucket
        if not weather_data:
            return False

        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"weather-data/{city}--{timestamp}.json"

        try:
            weather_data['timestamp'] = timestamp
            # Convert dictionary to JSON string
            json_data = json.dumps(weather_data)
            self.s3_client.put_object(
                Bucket = self.bucket_name,
                Key = file_name,
                Body = json_data,
                ContentType = 'application/json'
            )
            print(f"Weather data for {city} saved to s3: {file_name}")
            return True
        except Exception as e:
            print(f"Error saving data to s3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()

    # create a bucket if it does not exist
    dashboard.create_bucket_if_not_exists()

    # cities = ["Nairobi", "Mombasa", "Kisumu", "Eldoret", "Nakuru"]


    while True:
        os.system('cls')
        print("Welcome to the Weather Dashboard!".center(60, "-"))
        city = input("Enter City Name: ").title()
        if city == "Exit":
            break

        os.system("cls")
        for x in range(5):
            print(f"Fetching weather data for {city}{x * '.'}")
            time.sleep(0.5)
            os.system('cls')
        weather_data = dashboard.get_weather_data(city)
        if weather_data:
            print(f"The Weather for {city.capitalize()}".center(50, "-"))
            temp = weather_data['main']['temp']
            feels_like = weather_data['main']['feels_like']
            humidity = weather_data['main']['humidity']
            description = weather_data['weather'][0]['description']

            print(f"Temperature: {temp}°C")
            print(f"Feels like: {feels_like}°C")
            print(f"Humidity: {humidity}%")
            print(f"Conditions: {description}")

            # Save to S3
            success = dashboard.save_to_s3(weather_data, city)
            if success:
                print(f"Weather data for {city} saved to S3!")
        else:
            print(f"Failed to fetch weather data for {city}")

        time.sleep(5)



if __name__ == "__main__":
    main()