import requests 
import json
import logging
import time
from quixstreams import Application 

def get_wheater():
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":5.1151,
            "longitude":101.8892,
            "current": "temperature_2m"
        })
    return response.json()

def main():
    app = Application(broker_address="localhost:9092",
                    loglevel="DEBUG",
                    )

    with app.get_producer() as producer:
        while True:
            weather = get_wheater()
            logging.debug("Got weather : %s", weather)
            producer.produce(
                topic="weather_data_demo",
                key="Kelantan",
                value=json.dumps(weather),
            )
            logging.info("produced. Sleeping...")
            time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()