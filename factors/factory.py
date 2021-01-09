import talib
import pandas as pd
import numpy as np

'''
org_df axis=1 (columns ), level=0 ( OHLCV) , level=1 ( stocks )
SMA return
'''


def append_return_cols(org_df, col_name, window, look_back, axis=1, level=0):
    new_col_name = "{COL}_PAST_RETURN_MA{WIN}_LB{LB}".format(COL=col_name, LB=look_back, WIN=window)
    org_df = org_df.drop(new_col_name, axis=axis, level=level)  # drop if exist

    rolling_mean = org_df.xs(col_name, axis=axis, level=level, drop_level=False).rolling(window=window).mean()
    return_cols = rolling_mean / rolling_mean.shift(look_back) - 1

    return_cols = return_cols.rename(columns={col_name: new_col_name})
    return_cols.tail()

    org_df = org_df.join(return_cols)

    return org_df


def append_fwd_return_cols(org_df, col_name, window, look_fwd, axis=1, level=0):
    new_col_name = "{COL}_FWD_RETURN_MA{WIN}_FWD{FWD}".format(COL=col_name, FWD=look_fwd, WIN=window)
    org_df = org_df.drop(new_col_name, axis=axis, level=level)  # drop if exist

    rolling_mean = org_df.xs(col_name, axis=axis, level=level, drop_level=False).rolling(window=window).mean()
    return_cols = rolling_mean.shift(-abs(look_fwd)) / rolling_mean - 1

    return_cols = return_cols.rename(columns={col_name: new_col_name})
    return_cols.tail()

    org_df = org_df.join(return_cols)

    return org_df

''' sma lines'''
def append_sma_cols(org_df, col_name, window, axis=1, level=0):
    new_col_name = "{COL}_SMA{WIN}".format(COL=col_name,  WIN=window)
    org_df = org_df.drop(new_col_name, axis=axis, level=level)  # drop if exist

    rolling_mean = org_df.xs(col_name, axis=axis, level=level, drop_level=False).rolling(window=window).mean()
    return_cols = rolling_mean.rename(columns={col_name: new_col_name})


    org_df = org_df.join(return_cols)

    return org_df

''' ema lines '''
def append_ema_cols(org_df, col_name, window, axis=1, level=0):
    new_col_name = "{COL}_EMA{WIN}".format(COL=col_name,  WIN=window)
    org_df = org_df.drop(new_col_name, axis=axis, level=level)  # drop if exist

    tmp = org_df.xs(col_name, axis=axis, level=level, drop_level=False)
    ema_tmp = tmp.copy()
    for symbol in tmp.columns.levels[1]:
        try:
            ema = talib.EMA(tmp[col_name, symbol],window)

            ema_tmp[col_name, symbol] = ema
        except:
            pass

    ema_tmp = ema_tmp.rename(columns={col_name: new_col_name})
    org_df = org_df.join(ema_tmp)

    return org_df


'''ema macd'''
def append_macd_cols(org_df, col_name, fastperiod=12, slowperiod=26, signalperiod=9
                     , fastmatype=1, slowmatype=1,
                     signalmatype=1, axis=1, level=0):
    macd_name = "MACD_{FT}_{SL}_{SIG}".format(COL=col_name, FT=fastperiod, SL=slowperiod, SIG=signalperiod)
    sig_name = "MACD_SIG_{FT}_{SL}_{SIG}".format(COL=col_name, FT=fastperiod, SL=slowperiod, SIG=signalperiod)
    org_df = org_df.drop(macd_name, axis=axis, level=level)  # drop if exist
    org_df = org_df.drop(sig_name, axis=axis, level=level)  # drop if exist

    tmp = org_df.xs(col_name, axis=axis, level=level, drop_level=False)
    macd_tmp = tmp.copy()
    sig_tmp = tmp.copy()
    for symbol in tmp.columns.levels[1]:
        macd, macd_sig, _ = talib.MACDEXT(tmp[col_name, symbol],
                                          fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod,
                                          fastmatype=fastmatype, slowmatype=slowmatype, signalmatype=signalmatype)

        macd_tmp[col_name, symbol] = macd
        sig_tmp[col_name, symbol] = macd_sig

    macd_tmp = macd_tmp.rename(columns={col_name: macd_name})
    sig_tmp = sig_tmp.rename(columns={col_name: sig_name})

    org_df = org_df.join(macd_tmp)
    org_df = org_df.join(sig_tmp)

    return org_df


def append_bollinger_band_cols(org_df, col_name, timeperiod=120, nbdevup=2, nbdevdn=2, matype=1, axis=1, level=0):
    bb_up_name = "BB_UP_{TP}".format(COL=col_name, TP=timeperiod)
    bb_mid_name = "BB_MID_{TP}".format(COL=col_name, TP=timeperiod)
    bb_low_name = "BB_LOW_{TP}".format(COL=col_name, TP=timeperiod)
    bb_diff_name = "BB_DIFF_{TP}".format(COL=col_name, TP=timeperiod)

    org_df = org_df.drop(bb_up_name, axis=axis, level=level)  # drop if exist
    org_df = org_df.drop(bb_mid_name, axis=axis, level=level)  # drop if exist
    org_df = org_df.drop(bb_low_name, axis=axis, level=level)  # drop if exist
    org_df = org_df.drop(bb_diff_name, axis=axis, level=level)

    tmp = org_df.xs(col_name, axis=axis, level=level, drop_level=False)
    bb_up_tmp = tmp.copy()
    bb_mid_tmp = tmp.copy()
    bb_low_tmp = tmp.copy()
    bb_diff_tmp = tmp.copy()

    for symbol in tmp.columns.levels[1]:
        bb_up, bb_mid, bb_low = talib.BBANDS(tmp[col_name, symbol],
                                             timeperiod=timeperiod,
                                             nbdevup=nbdevup, nbdevdn=nbdevdn, matype=matype)
        bb_up_tmp[col_name, symbol] = bb_up
        bb_mid_tmp[col_name, symbol] = bb_mid
        bb_low_tmp[col_name, symbol] = bb_low
        bb_diff_tmp[col_name, symbol] = bb_up - bb_low

    bb_up_tmp = bb_up_tmp.rename(columns={col_name: bb_up_name})
    bb_mid_tmp = bb_mid_tmp.rename(columns={col_name: bb_mid_name})
    bb_low_tmp = bb_low_tmp.rename(columns={col_name: bb_low_name})
    bb_diff_tmp = bb_diff_tmp.rename(columns={col_name: bb_diff_name})

    org_df = org_df.join(bb_up_tmp)
    org_df = org_df.join(bb_mid_tmp)
    org_df = org_df.join(bb_low_tmp)
    org_df = org_df.join(bb_diff_tmp)

    return org_df


def append_rsi_cols(org_df, col_name, timeperiod=14, axis=1, level=0):
    rsi_name = "RSI_{TP}".format(COL=col_name, TP=timeperiod)

    org_df = org_df.drop(rsi_name, axis=axis, level=level)  # drop if exist

    tmp = org_df.xs(col_name, axis=axis, level=level, drop_level=False)
    rsi_tmp = tmp.copy()

    for symbol in tmp.columns.levels[1]:
        rsi = talib.RSI(tmp[col_name, symbol], timeperiod=timeperiod)

        rsi_tmp[col_name, symbol] = rsi

    rsi_tmp = rsi_tmp.rename(columns={col_name: rsi_name})
    org_df = org_df.join(rsi_tmp)

    return org_df


# SuperTrend VPT

def super_trend_vpt(org_df, timeperiod=14, axis=1, level=0,
                    high_name='High', low_name='Low', vol_name='Volume',
                    open_name='Open', close_name='Close'):
    high_low = (org_df[high_name] - org_df[low_name]) * 100
    open_close = (org_df[close_name] - org_df[open_name]) * 100
    spread_vol = open_close * org_df[vol_name] / high_low
    vpt = spread_vol + spread_vol.cumsum()

    price_spread = (org_df[high_name] - org_df[low_name]).rolling(28).std()
    avg_vpt = vpt.rolling(14).mean()
    shadow = price_spread * (vpt - avg_vpt) / (vpt - avg_vpt).rolling(28).std()

    tmp_high = org_df[high_name][shadow > 0] + shadow[shadow > 0]

    tmp_low = org_df[low_name][shadow < 0] + shadow[shadow < 0]
    tmp = tmp_high.combine_first(tmp_low)

    vpt = tmp.ewm(alpha=2 / (10 + 1), adjust=False).mean()

    atr = get_atr(org_df)
    up_lev = vpt - 1 * atr
    dn_lev = vpt + 1 * atr

    up_trend = up_lev.copy()
    up_trend[:] = 0
    dn_trend = dn_lev.copy()
    dn_trend[:] = 0
    for i in range(1, len(up_trend)):
        tmp = org_df[close_name].iloc[i - 1] > up_trend.iloc[i - 1]
        tmp_2 = pd.concat([up_lev.iloc[i], up_trend.iloc[i - 1]], keys=range(2)).groupby(level=1).max()
        up_trend.iloc[i] = np.where(tmp, tmp_2, up_lev.iloc[i])
        # print(  up_trend.iloc[i])

        tmp = org_df[close_name].iloc[i - 1] < dn_trend.iloc[i - 1]
        tmp_2 = pd.concat([dn_lev.iloc[i], dn_trend.iloc[i - 1]], keys=range(2)).groupby(level=1).min()
        dn_trend.iloc[i] = np.where(tmp, tmp_2, dn_lev.iloc[i])

    trend = up_trend.copy()
    trend[:] = 0
    for i in range(1, len(trend)):
        tmp_up = org_df[close_name].iloc[i] > dn_trend.iloc[i - 1]
        tmp_out = np.where(tmp_up, 1, trend.iloc[i])
        # print("UP ",tmp_out)

        tmp_dn = org_df[close_name].iloc[i] < up_trend.iloc[i - 1]
        tmp_out = np.where(tmp_dn, -1, tmp_out)
        # print("DN ", tmp_out)

        tmp_pre = (org_df[close_name].iloc[i] >= up_trend.iloc[i - 1]) & \
                  (org_df[close_name].iloc[i] <= dn_trend.iloc[i - 1])
        tmp_out = np.where(tmp_pre, trend.iloc[i - 1], tmp_out)

        trend.iloc[i] = tmp_out

        # print("ALL ", tmp_out, tmp_up, tmp_dn, tmp_pre)

    # trend[org_df[close_name]> dn_trend.shift()]=1
    # trend[org_df[close_name]< up_trend.shift()]=-1

    st_line = up_trend.copy()
    # st_line[:]=np.NaN
    st_line[trend == 1] = up_trend
    st_line[trend != 1] = dn_trend

    '''
    #print(pd.concat([org_df[close_name]['CTSH'],dn_trend['CTSH'],up_trend['CTSH'],trend['CTSH'] ], axis=1)['2020-06'])

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(go.Scatter(x=org_df[close_name].index,y=org_df[close_name]['CTSH'],name='CLOSE'),secondary_y=False)

    fig.add_trace(go.Scatter(x=dn_trend.index,y=dn_trend['CTSH'],name='DN'),secondary_y=False)
    fig.add_trace(go.Scatter(x=up_trend.index,y=up_trend['CTSH'],name='UP'),secondary_y=False)
    fig.add_trace(go.Scatter(x=st_line.index,y=st_line['CTSH'],name='st_line'),secondary_y=False)
    fig.show()
    '''

    # crossover
    buy = (org_df[close_name] > st_line) & (org_df[close_name].shift() <= st_line.shift())
    sell = (org_df[close_name] < st_line) & (org_df[close_name].shift() >= st_line.shift())

    # append st_line to org_df
    vpt_name = "VPT"
    org_df = org_df.drop(vpt_name, axis=axis, level=level)  # drop if exist
    tmp = org_df.xs(close_name, axis=axis, level=level, drop_level=False)
    vpt_tmp = tmp.copy()

    for symbol in tmp.columns.levels[1]:
        vpt_tmp[close_name, symbol] = st_line[symbol]

    vpt_tmp = vpt_tmp.rename(columns={close_name: vpt_name})

    org_df = org_df.join(vpt_tmp)

    return buy, sell, st_line, org_df


def get_atr(org_df, timeperiod=100, axis=1, level=0,
            high_name='High', low_name='Low', vol_name='Volume',
            open_name='Open', close_name='Close'):
    data = org_df[[close_name, high_name]].copy()
    high = org_df[high_name]
    low = org_df[low_name]
    close = org_df[close_name]
    hl = abs(high - low)
    hc = abs(high - close.shift())
    lc = abs(low - close.shift())
    tr = pd.concat([hl, hc, lc], keys=range(3)).groupby(level=1).max()

    # print( tr.tail() , hl.tail(), hc.tail(), lc.tail() )

    atr = tr.ewm(alpha=1 / timeperiod, adjust=False).mean()
    # print(atr.tail())
    return atr

if __name__ == '__main__':
    import yfinance as yf
    symbols =['AAPL','SPG']
    data = yf.download(symbols,period='2Y',interval='1d')


    df = data.copy()

    #df = append_return_cols(df, 'Close', 1, 1)
    #df = append_return_cols(df, 'Close', 5, 5)
    #df = append_return_cols(df, 'Close', 5, 30)
    #df = append_return_cols(df, 'Volume', 5, 10)
    #df = append_return_cols(df, 'Volume', 5, 30)
    #df = append_macd_cols(df, 'Close')
    #df = append_bollinger_band_cols(df, 'Close')
    #df = append_rsi_cols(df, 'Close')
    #df = append_fwd_return_cols(df, 'Close', 5, 30)

    # df[sig_buy['AAPL']]['Close']['AAPL'].plot()

    #print(df['Close'])
    _,_,st_line,df =super_trend_vpt(df)
    print(df.columns)
    print(df.tail())
    print(st_line.tail())







