from factors import factory as fty
from data.symbollist import CUS_SPY_SYMBOLS
from utils import emailhelper

import yfinance as yf
import pandas as pd
import talib
import webbrowser
from glob import glob
import time

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from spac.config import Secret, Config
import os

CWD = os.path.dirname(os.path.realpath(__file__)) + "/"


def generate_super_trend_graph(org_df, super_trend_line, cross_up, cross_dn, open_position_symbols=[],
                               filename="graph.html"):
    file = open(CWD + filename, 'w+')
    for i in range(2):
        side_sig = [cross_up, cross_dn][i]
        side = "BUY" if i == 0 else "SELL"
        file.write("<h1> {0}</h1>  ".format(side))
        # print(side)

        for item in side_sig[side_sig == True].iloc[-1].dropna().iteritems():
            # print("item",item)
            symbol = item[0]
            print("%s: %s" % (side, symbol))
            file.write("<h2> %s: %s</h2>" % (side, symbol))
            buy = org_df['Close', symbol][cross_up[symbol] == True]
            sell = org_df['Close', symbol][cross_dn[symbol] == True]

            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # Add traces
            fig.add_trace(go.Scatter(x=org_df.index, y=org_df['Close', symbol], name=symbol), secondary_y=False)
            fig.add_trace(
                go.Scatter(x=org_df.index, y=super_trend_line[symbol][super_trend_line[symbol] != 0], name='st_line'),
                secondary_y=False)

            fig.add_trace(go.Scatter(x=buy.index, y=buy, name='buy'), secondary_y=False)
            fig.add_trace(go.Scatter(x=sell.index, y=sell, name='sell'), secondary_y=False)
            # fig.show()

            file.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
    file.close

    open_position_file = "open_position.html"
    file = open(CWD + open_position_file, 'w+')
    # plot has position symbol
    for symbol in open_position_symbols:
        print("open position %s" % (symbol))
        file.write("%s" % (symbol))
        buy = org_df['Close', symbol][cross_up[symbol] == True]
        sell = org_df['Close', symbol][cross_dn[symbol] == True]

        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Add traces
        fig.add_trace(go.Scatter(x=org_df.index, y=org_df['Close', symbol], name=symbol), secondary_y=False)
        fig.add_trace(
            go.Scatter(x=org_df.index, y=super_trend_line[symbol][super_trend_line[symbol] != 0], name='st_line'),
            secondary_y=False)

        fig.add_trace(go.Scatter(x=buy.index, y=buy, name='buy'), secondary_y=False)
        fig.add_trace(go.Scatter(x=sell.index, y=sell, name='sell'), secondary_y=False)
        # fig.show()

        file.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))

    file.close()

    return [filename, open_position_file]



def generate_nice_graph(df, symbols, filename="graph.html"):
    file_path = CWD + filename
    file = open(file_path, 'w+')
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
                                 marker=dict(color="#36ff00", symbol="triangle-up", size=8)))
        fig.add_trace(go.Scatter(mode='markers', x=up_band.index, y=up_band, name='up_line', opacity=0.8,
                                 marker=dict(color="#001aff", symbol="triangle-down", size=8)))

        fig.add_trace(go.Scatter(mode='markers', x=up_lev.index, y=up_lev[symbol], name='up_lev', opacity=0.5,
                                 marker=dict(color="#36ff00", symbol='cross')))

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


        # fig.show()
        file.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))  # write plot to file

        if sym_index % 10 == 0 and sym_index != 0:
            file.close()
            print(" open new file ", CWD + "{}_{}".format(sym_index, filename))
            file = open(CWD + "{}_{}".format(sym_index, filename), 'w+')

    file.close()

    for file in glob(CWD + '*.html'):
        now = time.time()
        modi_time = os.path.getmtime(file)
        if modi_time + 24 * 3600 < now:
            continue
        webbrowser.open('file://' + file)
    return [file]


def send_email(filename):
    pass


if __name__ == '__main__':
    '''
    open_symbols = ["ZYXI","MSFT","AAPL","SPY","VXX"]
    symbols = []
    symbols.extend(open_symbols)#CUS_SPY_SYMBOLS
    symbols.extend(CUS_SPY_SYMBOLS)
    symbols = sorted(list(set(symbols)))
    print(symbols)
    
    data = yf.download(symbols, period='2Y', interval='1d')
    df = data.copy()
    cross_up,cross_dn,st_line,df =fty.super_trend_vpt(df)
    files = generate_super_trend_graph(df, st_line, cross_up, cross_dn, open_symbols)
    emailhelper.send_email("Super Trend Watch List","", files, CWD)
    '''

    symbols = ["ZYXI", "MSFT", "AAPL", "SPY", "VXX", "BABA", "NVDA", "BYND","CRSR",
               "NIO", "TSLA", "DIS", "WMT", "BILI", "SQ", "XLNX", "AMD", "SPG", "O",
               "BAC", "JPM", "MSFT", "FB", "ADSK", "ADBE", "MRK", "MDB", "COF",
               "VZ", "M", "APO", "COST", "QCOM", "MU", "LMT", "SBUX", "DIS", "ASML",
               "DADA", "TAL", "FSR", "SE", "TDOC", "SDC", "AXP", "MA", "UAL","U",
               "JKS","NKLA","ZTO","SLQT","PLTR","NIO","BYDDF","LI","XPEV","CIIC",
               "EDU"]

    # symbols = ["ZYXI", "MSFT", "AAPL", "SPY", "VXX", "BABA", "NVDA", "BYND"]

    symbols = sorted(list(set(symbols)))
    print(symbols)
    print('len ', len(symbols))

    data = yf.download(symbols, period='2Y', interval='1d')
    plot_files = generate_nice_graph(data, symbols)
