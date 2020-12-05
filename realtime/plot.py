import yfinance as yf
import dash
from dash.dependencies import Output, Input, State
import dash_core_components as dcc
import dash_html_components as html
import talib
import plotly.graph_objs as go
from collections import deque
from plotly.subplots import make_subplots
import pandas as pd
import cronjob.factors_monitor as fm
import factors.factory as fty

from datetime import datetime



app = dash.Dash(__name__)
app.layout = html.Div(
    [

        dcc.Input(id="symbol", type="text", value="AAPL",debounce=True),
        dcc.Input(id="period", type="text", value="2Y",debounce=True),
        dcc.Input(id="interval", type="text", value="1d",debounce=True),
        html.Button('Refresh', id='refresh_but', n_clicks=0),
        html.Br(),
        html.Div(id='lastupdate'),
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='interval-component',
            interval=5*60*1000,
            n_intervals=3
        ),

    ]
)

class GraphObject:
    m_symbol = ""
    m_period=""
    m_interval =""
    m_lastupdate = datetime.now()
    m_is_working = False

    def isInputUpdated(self, symbol, period, interval, force_update=False):
        if force_update or symbol != self.m_symbol or period != self.m_period or interval != self.m_interval:
            self.m_symbol = symbol
            self.m_period = period
            self.m_interval = interval
            return True
        return False

    def generate_fig(self, df, symbols):
        res = []

        high_name = 'High'
        low_name = 'Low'
        open_name = 'Open'
        close_name = 'Close'
        vol_name = 'Volume'
        st_buy_sig, st_sell_sig, super_trend_line, df = fty.super_trend_vpt(df, close_name)

        # super trend upper|lower level
        high_low = (df[high_name] - df[low_name]) * 100
        open_close = (df[close_name] - df[open_name]) * 100
        spread_vol = open_close * df[vol_name] / high_low
        vpt = spread_vol + spread_vol.cumsum()

        price_spread = (df[high_name] - df[low_name]).rolling(28).std()
        avg_vpt = vpt.rolling(14).mean()
        shadow = price_spread * (vpt - avg_vpt) / (vpt - avg_vpt).rolling(28).std()

        tmp_high = df[high_name][shadow > 0] + shadow[shadow > 0]

        tmp_low = df[low_name][shadow < 0] + shadow[shadow < 0]
        tmp = tmp_high.combine_first(tmp_low)

        vpt = tmp.ewm(alpha=2 / (10 + 1), adjust=False).mean()

        atr = fty.get_atr(df)
        up_lev = vpt - 1 * atr
        dn_lev = vpt + 1 * atr

        df = fty.append_macd_cols(df, 'Close')
        df = fty.append_rsi_cols(df, 'Close')

        df = fty.append_bollinger_band_cols(df, 'Close')
        df = fty.append_bollinger_band_cols(df, 'Close', timeperiod=60)

        # indiviual symbol
        for sym_index in range(len(symbols)):

            symbol = symbols[sym_index]
            price_df = pd.DataFrame()
            price_df['price'] = (df['Close'] + df['Open'])[symbol] / 2
            for i in [5, 10, 14, 21, 30, 60, 120, 250]:
                price_df['ema_%s' % i] = talib.EMA(df['Close', symbol], timeperiod=i)

            price_df['st_line'] = super_trend_line[symbol][super_trend_line[symbol] != 0]
            low_band = price_df['st_line'][price_df['price'] > price_df['st_line']]
            up_band = price_df['st_line'][price_df['price'] < price_df['st_line']]

            # macd
            price_df['macd'] = df['MACD_12_26_9'][symbol]
            price_df['macd_sig'] = df['MACD_SIG_12_26_9'][symbol]

            # rsi
            price_df['rsi'] = df['RSI_14'][symbol]

            # bb_band
            price_df['bbu_slw'] = df['BB_UP_120'][symbol]
            price_df['bbm_slw'] = df['BB_MID_120'][symbol]
            price_df['bbl_slw'] = df['BB_LOW_120'][symbol]

            price_df['bbu_fst'] = df['BB_UP_60'][symbol]
            price_df['bbm_fst'] = df['BB_MID_60'][symbol]
            price_df['bbl_fst'] = df['BB_LOW_60'][symbol]

            tmp = price_df[['ema_5', 'ema_10', 'ema_14', 'ema_21', 'ema_30', 'ema_60', 'ema_120', 'ema_250']]
            # fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig = make_subplots(specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]],
                                rows=3, cols=1, row_heights=[1.6, 0.5, 0.5])
            fig.update_layout(title="{}:{}".format(sym_index, symbol), autosize=False, width=1000, height=1000,
                              xaxis_rangeslider_visible=False)

            fig.add_trace(go.Candlestick(x=df.index,
                                         open=df['Open', symbol],
                                         high=df['High', symbol],
                                         low=df['Low', symbol],
                                         close=df['Close', symbol]))

            fig.add_trace(
                go.Bar(x=df.index, y=df['Volume', symbol], name='volume', opacity=0.3, marker=dict(color="#396afa"))
                , secondary_y=True)

            # Add ema trace
            for col in tmp.columns:
                color = '#ffbaba'
                fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=tmp[col], name=col, opacity=0.3,
                                         line=dict(color=color)))

            # low/up band stline

            fig.add_trace(go.Scatter(mode='markers', x=low_band.index, y=low_band, name='low_line', opacity=0.8,
                                     marker=dict(color="#00A51D", symbol="triangle-up", size=8)))
            fig.add_trace(go.Scatter(mode='markers', x=up_band.index, y=up_band, name='up_line', opacity=0.8,
                                     marker=dict(color="#001aff", symbol="triangle-down", size=8)))

            fig.add_trace(go.Scatter(mode='markers', x=up_lev.index, y=up_lev[symbol], name='up_lev', opacity=0.5,
                                     marker=dict(color="#719D78", symbol='cross')))

            fig.add_trace(go.Scatter(mode='markers', x=dn_lev.index, y=dn_lev[symbol], name='dn_lev', opacity=0.5,
                                     marker=dict(color="#001aff", symbol='cross')))

            # macd
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['macd'], name='macd', opacity=1,
                                     line=dict(color="#ff2600")), row=2, col=1)
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['macd_sig'], name='macd_sig', opacity=1,
                                     line=dict(color="#ffc800")), row=2, col=1)

            # rsi
            fig.add_trace(
                go.Scatter(mode='lines', x=price_df.index, y=[75] * len(price_df.index), opacity=1, showlegend=False,
                           line=dict(color="#9e9e9e", dash='dash')), row=3, col=1)
            fig.add_trace(
                go.Scatter(mode='lines', x=price_df.index, y=[25] * len(price_df.index), opacity=1, showlegend=False,
                           line=dict(color="#9e9e9e", dash='dash')), row=3, col=1)
            fig.add_trace(
                go.Scatter(mode='lines', x=price_df.index, y=[50] * len(price_df.index), opacity=1, showlegend=False,
                           line=dict(color="#9e9e9e", dash='dash')), row=3, col=1)
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['rsi'], name='rsi', opacity=1,
                                     line=dict(color="#ff2600")), row=3, col=1)

            # bb_band
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['bbu_slw'], name='bbu_slw', opacity=1,
                                     line=dict(color="#64b0a5")), )
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['bbm_slw'], name='bbm_slw', opacity=1,
                                     line=dict(color="#64b0a5", dash='dash')), )
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['bbl_slw'], name='bbl_slw', opacity=1,
                                     line=dict(color="#64b0a5")), )

            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['bbu_fst'], name='bbu_fst', opacity=1,
                                     line=dict(color="#cf9667")), )
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['bbm_fst'], name='bbm_fst', opacity=1,
                                     line=dict(color="#cf9667", dash='dash')), )
            fig.add_trace(go.Scatter(mode='lines', x=price_df.index, y=price_df['bbl_fst'], name='bbl_fst', opacity=1,
                                     line=dict(color="#cf9667")), )

            res.append(fig)

        print("Finish generate graph.")
        return fig

graph_object = GraphObject()

@app.callback([Output('live-graph', 'figure'),
                Output('lastupdate', component_property='children')
              ],
              [Input('interval-component', 'n_intervals'),
               Input('refresh_but','n_clicks')],
              state=[State('symbol', 'value'),
                     State('period', 'value'),
                     State('interval', 'value')]
              )
def update_graph_scatter(input,n_clicks,  symbol, period, interval):
    print("input ", input, n_clicks, symbol, period, interval)
    if graph_object.m_is_working:
        return None,None

    graph_object.isInputUpdated(symbol,period,interval,True)


    if(symbol=="SPY"):
        return None,None

    graph_object.m_is_working=True
    symbols = ["SPY"]
    symbols.append(symbol.upper())
    #print("input ", input, symbols, period, interval)
    price_data = yf.download(symbols,period=period,interval=interval)
    fig= graph_object.generate_fig(price_data,symbols)
    graph_object.m_is_working = False

    return fig, "Last Update:{}".format(datetime.now())
    #return generate_fig(price_data, symbols)



if __name__ == '__main__':

    app.run_server(debug=True)

