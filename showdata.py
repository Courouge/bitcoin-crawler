import logging
import json
import time
import datetime
import math
import numpy as np
import matplotlib.pyplot as plt

from cassandra.cluster import Cluster

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from flask import Flask
from flask import Markup
from flask import Flask
from flask import render_template
from plotly.offline import plot
from plotly.graph_objs import Scatter

dates = []
amounts = []

app = Flask(__name__)

@app.route('/')
def results():
    error = None

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    KEYSPACE = "BTC"
    rows = session.execute('SELECT date, amount FROM BTC.USD')
    for row in rows:
        if not row.date == None:
            dates.append(datetime.datetime.fromtimestamp(float(row.date)).strftime('%Y-%m-%d %H:%M:%S'))
        if not row.amount == None:
            amounts.append(str(row.amount))
    my_plot_div = plot([Scatter(x=dates, y=amounts)], output_type='div')
    return render_template('results.html',
                           div_placeholder=Markup(my_plot_div)
                           )

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=80)