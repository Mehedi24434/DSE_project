# DSE_project
The application will scrape (using BeautifulSoup/Selenium) the rolling ticker displayed at the top of the Dhaka Stock Exchange website and collect prices of each stock at each minute throughout the trading day (9AM - 3PM). 
This data will then be sent to an InfluxDB database and stored in a bucket. 
An analytics dashboard (using Dash) will be updated in real time which will include tables, stats and graphs including Sector-by-sector daily movement, top 10 stock movement, etc. 
Additionally, two FastAPI REST endpoints will make the raw data and dashboard available to anyone through a url. 
