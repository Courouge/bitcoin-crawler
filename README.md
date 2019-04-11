This is a simple python app working with kafka and cassandra

You need to add your client info in crawler_coinbase in line 9 <br/>
Check the website https://developers.coinbase.com/ <br/>

This app crawl the current price of Bitcoin in currency pair BTC-EUR (crawler_coinbase.py) <br/>
The price is stored in Cassandra database (persistdata.py) <br/>
The data is shown in simple webpage (showdata.py) <br/>
