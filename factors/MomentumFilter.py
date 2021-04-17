import yfinance as yf
from factors import factory as fty

def mom_entry_filter(input_df, long_ma=30, long_win=60, long_pct=0.20,
                     short_ma=5, short_win=10, short_pct=0.02,
                     col_name='Close'):
    df = fty.append_ema_cols(input_df, col_name, long_ma)
    long_ma_col_name = "{col}_EMA{win}".format(col=col_name, win=long_ma)

    long_ma_pct = df[long_ma_col_name].iloc[-1] / df[long_ma_col_name].iloc[-long_win] - 1

    buy_scores = {}
    sell_scores = {}
    short_ma_list = [5, 10, 30, 60, 120]
    for i in range(len(short_ma_list)):
        df = fty.append_ema_cols(df, col_name, short_ma_list[i])
        short_ma_col_name = "{col}_EMA{win}".format(col=col_name, win=short_ma_list[i])

        close_short_ma_pct = df[col_name].iloc[-1] / df[short_ma_col_name].iloc[-1] - 1

        buy_condition = (abs(close_short_ma_pct) < short_pct) & (long_ma_pct > long_pct)
        sell_condition = (abs(close_short_ma_pct) < short_pct) & (long_ma_pct < -long_pct)
        for symbol in buy_condition[buy_condition == True].index:
            if symbol not in buy_scores:
                buy_scores[symbol] = {"ma_list": [short_ma_list[i]], "score": 1}
            else:
                buy_scores[symbol]["ma_list"].append(short_ma_list[i])
                buy_scores[symbol]["score"] += 1 + 0.5 * i

        for symbol in sell_condition[sell_condition == True].index:
            if symbol not in sell_scores:
                sell_scores[symbol] = 1
                sell_scores[symbol] = {"ma_list": [short_ma_list[i]], "score": 1}
            else:
                sell_scores[symbol]["score"] += (1 + i * 0.5)
                sell_scores[symbol]["ma_list"].append(short_ma_list[i])


    return buy_scores, sell_scores


if __name__ == '__main__':
    # symbols =["AAPL", "SPY", "BCEI", "CTSH","NIO"]

    symbols = ["MSFT", "AAPL", "SPY", "VXX", "BABA", "NVDA", "BYND","CRSR",
                   "NIO", "TSLA", "DIS", "WMT", "BILI", "SQ", "XLNX", "AMD", "SPG", "O",
                   "BAC", "JPM", "MSFT", "FB", "ADSK", "ADBE", "MRK", "MDB", "COF",
                   "VZ", "M", "APO", "COST", "QCOM", "MU", "LMT", "SBUX", "DIS", "ASML",
                   "DADA", "TAL", "SE", "TDOC", "SDC", "AXP", "MA", "UAL","U","SHLX",
                   "SLQT","PLTR","NIO","BYDDF","LI","XPEV","CIIC","TSM","RXT","NKE",
                   "EDU","KKR","FTCH","NRG","OPEN","FROG"]

    crypto_symbol=["SI","MARA","CAN","GBTC","NCTY","RIOT","BK","MSTR","FTFY"]

    symbols.extend(crypto_symbol)

    yf_raw_data = yf.download(symbols, period='1Y', interval='1d')
    df = yf_raw_data.copy()
    print(mom_entry_filter(df))
