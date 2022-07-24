import os
import io
import logging
import requests
import pandas as pd
import boto3
import sqlalchemy
import psycopg2
import paramiko

# connecting to the s3 bucket using boto3
s3 = boto3.client('s3',
                  region_name="us-east-2",
                  aws_access_key_id=os.getenv("AWS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET")

                  )

hostname = '192.168.121.128'
# connection to postgres db
conn = psycopg2.connect(
    host=hostname,
    database="postgres",
    user="postgres",
    password="root",
    port="5432")


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


def upload_the_file_to_folder_in_ubuntu():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=hostname, password='Gundu@135')
    ftp_client = ssh_client.open_sftp()
    ftp_client.put('/tmp/corona/corona.csv', '/home/ghalisai/Documents/crons-uploads/worldcorona.csv')
    ftp_client.close()


def connect_postgres_db():
    cur = conn.cursor()
    return cur


def insert_into_db(df):
    #engine = sqlalchemy.create_engine('postgresql://postgres:root@192.168.121.128:5432/postgres')
    cur = connect_postgres_db()
    cur.execute("TRUNCATE TABLE public.cron2")
    conn.commit()
    #df.to_sql('cron2', engine, index=False)
    output = io.StringIO()
    df.to_csv(output, sep=';', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'cron2',  null="", sep=';')  # null values become ''
    conn.commit()


def main():
    all_country_corona = pd.DataFrame.from_dict(call_corona_api())
    all_country_corona.columns = all_country_corona.columns.str.lower()
    all_country_corona.to_csv("/tmp/corona/corona.csv", sep=";", header=True, index=False)
    insert_into_db(all_country_corona)
    # upload_to_s3("/tmp/corona/worldometer.csv", "cron-uploads-sai", "corona_data.csv")
    upload_the_file_to_folder_in_ubuntu()


if __name__ == '__main__':
    if not os.path.exists("/tmp/corona/"):
        os.makedirs("/tmp/corona/")
    main()
