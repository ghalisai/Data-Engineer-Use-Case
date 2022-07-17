import os
import logging
import requests
import pandas as pd
import boto3

s3 = boto3.client('s3',
                  region_name="us-east-2",
                  aws_access_key_id=os.getenv("AWS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET")

                  )


# getting the corona data from the coronatracker api
def call_corona_api():
    logging.info("------------calling the api to get the corona data-----------")
    # params = {"countryCode": 10}
    url = "http://api.coronatracker.com/v3/stats/worldometer/country?countryCode="
    try:
        res = requests.get(url)
        return res.json()
    except Exception as e:
        logging.info(e)


# uploading the csv file to the s3 bucket
def upload_to_s3(data, bucket, file_name):
    s3.upload_file(data, bucket, file_name)



def main():
    try:
        all_country_corona = pd.DataFrame.from_dict(call_corona_api())
        all_country_corona.to_csv("/tmp/corona/worldometer.csv", sep=";", header=True)
        upload_to_s3("/tmp/corona/worldometer.csv", "cron-uploads-sai", "corona_data.csv")

    except Exception as e:
        print(e)


if __name__ == '__main__':
    if not os.path.exists("/tmp/corona/"):
        os.makedirs("/tmp/corona/")
    main()
