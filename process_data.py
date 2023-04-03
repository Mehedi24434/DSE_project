import requests
from datetime import datetime
import time
import pandas as pd
from bs4 import BeautifulSoup
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS, WritePrecision
from influxdb_client.rest import ApiException
import re
url= 'https://www.dsebd.org/'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
def writter():
    url= 'https://www.dsebd.org/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    def Time_checker(Date= False, Current_Time= False, Market_Status= False):
        div1 = soup.find('div', {'class': 'containbox'})
        div2 = div1.find('div',{'class':'col-md-12 col-xs-12 col-sm-12'})
        div3 = div2.find('div',{'class':'_row'})
        head1 = div3.find('header',{'class':'Header'})
        Hdiv = head1.find('div',{'class':'HeaderTop'})
        span_all = Hdiv.find_all('span',{'class':'time'})
        text = []
        for s in span_all:
            span_text = s.text.strip()
            text.append(span_text)
        if Date == True:
            return datetime.strptime(text[0], "%A, %B %d, %Y").date()
        if Current_Time == True:
            time_str = text[1].split(": ")[1].strip("(BST)")
            time_obj = datetime.strptime(time_str, "%H:%M:%S %p ").time()
            return time_obj
        if Market_Status == True:
            return text[2].split(": ")[1]
    def Dse_rolling_ticker_scrapper():
        df = pd.DataFrame()
        if df.empty:
            div1 = soup.find('div', {'class': 'containbox'})
            div2 = div1.find('div',{'class':'col-md-12 col-xs-12 col-sm-12'})
            div3 = div2.find('div',{'class':'_row'})
            head1 = div3.find('header',{'class':'Header'})
            Hdiv = head1.find('div',{'class':'HeaderBottom'})
            Hdiv2 = Hdiv.find('div',{'class':'Scroller'})
            Hdiv3 = Hdiv2.find('div',{'class':'scroll-item'})
            marq = Hdiv3.find_next('marquee',{'id':'mq2'})
            tab1 = marq.find_next('table')
            tr1 = tab1.find('tr')
            tds = tr1.find_all('td')
            data_list = []
            seen_data = set() # to keep track of already seen data
            for td in tds:
                a = td.find('a',{'class': 'abhead'})
                if a is not None:
                    data = a.text.strip()
                    if data not in seen_data: # check if data has not been added before
                        row = {'Company': data.split()[0], 'Price': float(data.split()[1]), 'Change': float(data.split()[2])}
                        data_list.append(row)
                        seen_data.add(data) # add data to seen_data set
            df = pd.DataFrame(data_list)
            current_time = Time_checker(Current_Time=True)
            current_date = Time_checker(Date=True)
            datetime_index = pd.to_datetime(current_date.strftime('%Y-%m-%d') + ' ' + current_time.strftime('%H:%M:%S'))
            df["DateTime"]= datetime_index
            # df.set_index("DateTime", inplace=True) # set "DateTime" column as the index
            # df.index.name = 'Datetime' # set the name of the index
        return df   
    def ready_influx (write = False, query = False, bucket = 'mybuc'):
        url = "http://localhost:8086"
        token = "fDcO5JGNd1vCPTb0vWM84xX20fTG-2MGo3dHIoJ3qa17NQhhFa6Crb97IIkdxN8MvMG5WFNQfrmBn-FnjfkLZA=="
        org = "my-org"
        bucket = bucket
        client = InfluxDBClient(url=url, token=token)
        if write:
            write_api = client.write_api(write_options=ASYNCHRONOUS,write_precision=WritePrecision.S)
            return write_api, bucket, org       
        if query:
            query_api = client.query_api()
            return query_api, bucket, org
    def data_writter():
        x = Dse_rolling_ticker_scrapper()
        ListA = []
        for index, row in x.iterrows():
            data = Point("stocks").tag("ticker", row["Company"]).field("Price", row["Price"]).field("Change", row["Change"]).tag("time-zone", str(row["DateTime"]))
            ListA.append(data)
        write_api, bucket, org  = ready_influx(write = True)
        write_api.write(bucket=bucket, org=org, record=ListA)
    data_writter()
                 


def reader():
    
    def ready_influx (write = False, query = False, bucket = 'mybuc'):
        url = "http://localhost:8086"
        token = "fDcO5JGNd1vCPTb0vWM84xX20fTG-2MGo3dHIoJ3qa17NQhhFa6Crb97IIkdxN8MvMG5WFNQfrmBn-FnjfkLZA=="
        org = "my-org"
        bucket = bucket
        client = InfluxDBClient(url=url, token=token)
        if write:
            write_api = client.write_api(write_options=ASYNCHRONOUS,write_precision=WritePrecision.S)
            return write_api, bucket, org       
        if query:
            query_api = client.query_api()
            return query_api, bucket, org
    def data_reader():
        query_api, bucket, org = ready_influx (query = True)
        query = f'from(bucket:"{bucket}") \
            |> range(start: 0) \
            |> filter(fn: (r) => r._measurement == "stocks")'
        li = []
        result = query_api.query(query, org=org)
        for table in result:
            for record in table.records:
                df = record.values
                li.append(df)
        df = pd.DataFrame(li)
        return df
    return data_reader()    
    
    
def Time_checker(Date= False, Current_Time= False, Market_Status= False):
    url= 'https://www.dsebd.org/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    div1 = soup.find('div', {'class': 'containbox'})
    div2 = div1.find('div',{'class':'col-md-12 col-xs-12 col-sm-12'})
    div3 = div2.find('div',{'class':'_row'})
    head1 = div3.find('header',{'class':'Header'})
    Hdiv = head1.find('div',{'class':'HeaderTop'})
    span_all = Hdiv.find_all('span',{'class':'time'})
    text = []
    for s in span_all:
        span_text = s.text.strip()
        text.append(span_text)
    if Date == True:
        return datetime.strptime(text[0], "%A, %B %d, %Y").date()
    if Current_Time == True:
        time_str = text[1].split(": ")[1].strip("(BST)")
        time_obj = datetime.strptime(time_str, "%H:%M:%S %p ").time()
        return time_obj
    if Market_Status == True:
        return text[2].split(": ")[1]

  

