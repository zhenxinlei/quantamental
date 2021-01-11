import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import datetime
import pandas as pd
import time
import yfinance as yf
import numpy as np
from flask import Flask,render_template
import plotly
import json
from flask import current_app as app

from spac.SpacWatcher import SpacWatcher

from plotly.subplots import make_subplots

from spac.custom_flask_ngrok import run_with_ngrok
from dash.dependencies import Input, Output



class SpacWebGrapher():
    def __init__(self):
        self.last_download_time =0
        self.yf_raw_data = None


    def genVolInUsdDf(self, raw_ohlc_df ):
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

    def getRealTimeDayVolume(self,spacWatcher,period="3mo", interval ="1d",volume_usd_thd=10000000):

        print( "last download time ",self.last_download_time," ", time.time()-60)
        symbols=["BTC-USD","ETH-USD"]
        #print(symbols)

        if time.time() > self.last_download_time+60 or self.yf_raw_data is None:
            symbols = spacWatcher.gs.read(spacWatcher.sheet_id, "all_spac!B2:B")
            symbols = [x[0] for x in symbols]
            self.yf_raw_data = yf.download(symbols,period=period,interval=interval)
            self.last_download_time = time.time()
            print(" download new bar  ")

        df = self.genVolInUsdDf(self.yf_raw_data)

        last_row = df['vol_usd'].iloc[-1].dropna().sort_values(ascending=False)
        sorted_column =last_row[last_row>=volume_usd_thd].index

        fig = make_subplots()
        fig.update_layout( autosize=False, width=1000, height=1000,)
        traces = []
        for symbol in sorted_column:

            fig.add_trace(go.Scatter(mode='lines+markers', x=df.index, y=df['vol_usd'][symbol], opacity=0.9, showlegend=True,
                                     name=symbol,marker=dict(size=7,opacity=0.5), line=dict(width=2)
                    ))


        fig.update_yaxes(type='log')

        return fig

def createDashboardAppServer(server,spacgraper ,spacwatcher ):


    app = dash.Dash(server= server)

    app.layout = html.Div(children=[
        html.H1(children='Log Volume USD '),
        html.Div(id='lastupdate'),
        dcc.Graph(
            id='example-graph',
            figure=spacgraper.getRealTimeDayVolume(spacwatcher),
            animate=True,
        ),
        dcc.Interval(
            id='interval-component',
            interval=60 * 5 * 1000,  # in milliseconds
            n_intervals=0
        )

    ])

    @app.callback([Output('example-graph', 'figure'),Output('lastupdate', component_property='children')],
                  Input('interval-component', 'n_intervals'))
    def update_metrics(n):
        print("callback ",n)
        return spacgrapher.getRealTimeDayVolume(spacwatcher), "Last Update:{}".format(datetime.datetime.now())


    return app.server




def initFlaskApp( spacgrapher, spacwatcher):
    app = Flask(__name__)
    app = createDashboardAppServer(app, spacgrapher, spacwatcher)
    app.debug = True
    run_with_ngrok(app)


    return app


if __name__ == '__main__':
    spacwatcher = SpacWatcher()
    spacgrapher = SpacWebGrapher()
    app = initFlaskApp(spacgrapher= spacgrapher, spacwatcher= spacwatcher)
    app.run()

    while True:

        time.sleep(60*30)
        now = datetime.datetime.now()
        if (now.hour >=16 and now.minute>=30) or now.hour>16:
            print(" market closeed ", now)
            break

    exit()

