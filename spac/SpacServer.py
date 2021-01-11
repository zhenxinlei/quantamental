from flask import Flask, render_template
import spac.SpacWatcher as SpacWatcher
import json
import plotly
import plotly.graph_objs as go
import dash

import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import yfinance as yf
import time

app = Flask(__name__)
app.debug = True


@app.route('/2')
def index():
    rng = pd.date_range('1/1/2011', periods=7500, freq='H')
    ts = pd.Series(np.random.randn(len(rng)), index=rng)

    graphs = [
        dict(
            data=[
                dict(
                    x=[1, 2, 3],
                    y=[10, 20, 30],
                    type='scatter'
                ),
            ],
            layout=dict(
                title='first graph'
            )
        ),

        dict(
            data=[
                dict(
                    x=[1, 3, 5],
                    y=[10, 50, 30],
                    type='bar'
                ),
            ],
            layout=dict(
                title='second graph'
            )
        ),

        dict(
            data=[
                dict(
                    x=ts.index,  # Can use the pandas data structures directly
                    y=ts
                )
            ]
        )
    ]

    # Add "ids" to each of the graphs to pass up to the client
    # for templating
    ids = ['graph-{}'.format(i) for i, _ in enumerate(graphs)]

    # Convert the figures to JSON
    # PlotlyJSONEncoder appropriately converts pandas, datetime, etc
    # objects to their JSON equivalents
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('index.html',
                           ids=ids,
                           graphJSON=graphJSON)



@app.route('/sample1')
def line():
    count = 500
    xScale = np.linspace(0, 100, count)
    yScale = np.random.randn(count)

    # Create a trace
    trace = go.Scatter(
        x=xScale,
        y=yScale
    )

    data = [trace]
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('index2.html',
                           graphJSON=graphJSON)


last_download_time =0

yf_raw_data = None

def genVolInUsdDf(raw_ohlc_df ):
    df = raw_ohlc_df.copy()
    avg_price = df[['Close', 'Open', 'High', 'Low']].mean(axis=1, level=1)
    volume = df['Volume']
    volume_in_usd = avg_price * volume
    # print(volume_in_usd.columns,avg_price.columns )
    new_col_name = "vol_usd"
    price_df = df.drop(new_col_name, axis=1, level=0)  # drop col if exist
    for col_name in volume_in_usd.columns:
        volume_in_usd = volume_in_usd.rename(columns={col_name: (new_col_name, col_name)})

    df = df.join(volume_in_usd)

    return df

@app.route('/')
def getRealTimeDayVolume():
    global yf_raw_data
    global last_download_time
    print( "last download time ",last_download_time)
    symbols=["AAPL", "NIO"]
    if time.time()+60 < last_download_time or yf_raw_data is None:
        yf_raw_data = yf.download(symbols,period='1Y',interval='1d')
        last_download_time = time.time()

    df = genVolInUsdDf(yf_raw_data)

    print("vol", df['vol_usd'].tail())

    tmp =np.log10(df['vol_usd'].tail())
    print( tmp)

    #df['log_vol_usd'] = np.log10(df['vol_usd'])
    fig = make_subplots()
    traces = []
    for symbol in df['vol_usd'].columns:

        traces.append(go.Scatter(mode='lines', x=df.index, y=df['vol_usd'][symbol], opacity=1, showlegend=True,name=symbol
                ))

    graphJSON = json.dumps(traces, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('index2.html',
                           graphJSON=graphJSON)


if __name__ == '__main__':
    app.run()