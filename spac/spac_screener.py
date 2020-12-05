import pandas as pd
import yfinance as yf

import time
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE

from spac.config import Secret,Config
import os
CWD  = os.path.dirname(os.path.realpath(__file__))+"/"



def load_spac_info(path_to_csv="active_spacs_clean.csv"):
    df = pd.read_csv(path_to_csv,encoding = "ISO-8859-1")
    return df


def load_hist_data(tickers, start=None, end=None, interval='1d', valid_data_window=10):
    price_df = None
    if start is None or end is None:
        price_df = yf.download(tickers, period='1Y', interval=interval)
    else:
        price_df = yf.download(tickers, start, end, interval=interval)

    invalid_tickers = price_df['Close'].iloc[-valid_data_window - 1].isnull()
    invalid_tickers = invalid_tickers[invalid_tickers == True]

    price_df = price_df.drop(invalid_tickers.index.tolist(), axis=1, level=1)

    return price_df


'''
return dataframe columns name like "[('col_1', 'ticker_1'),('col_1','ticker_2'),...]"
'''


def gen_factor_1_df(price_df, close_col='Close', volume_col='Volume', look_back=10, ma_window=10):
    df = pd.DataFrame(index=price_df.index)

    rolling_mean = price_df.xs(close_col, axis=1, level=0, drop_level=False).rolling(window=ma_window).mean()
    close_price = price.xs(close_col, axis=1, level=0, drop_level=False)
    return_cols = close_price / rolling_mean.shift(look_back) - 1
    return_cols = return_cols.rename(columns={close_col: 'price_chg_pct'})
    df = df.join(return_cols)

    # yesterday return
    close_price = price.xs(close_col, axis=1, level=0, drop_level=False)
    return_cols = close_price / close_price.shift(1) - 1
    return_cols = return_cols.rename(columns={close_col: 'ytday_price_chg_pct'})

    df = df.join(return_cols)


    vol_rolling_mean = price_df.xs(volume_col, axis=1, level=0, drop_level=False).rolling(window=ma_window).mean()
    vol_chg_pct = vol_rolling_mean / vol_rolling_mean.shift(look_back) - 1
    vol_chg_pct = vol_chg_pct.rename(columns={volume_col: 'vol_chg_pct'})
    df = df.join(vol_chg_pct)

    avg_price = price_df[['Close', 'Open', 'High', 'Low']].mean(axis=1, level=1)
    volume = price_df['Volume']
    volume_in_usd = avg_price * volume
    # print(volume_in_usd.columns,avg_price.columns )
    for col_name in volume_in_usd.columns:
        volume_in_usd = volume_in_usd.rename(columns={col_name: ('volume_in_usd', col_name)})
    df = df.join(volume_in_usd)

    return df


def get_fct1_flter1_tickers(tickers, f1_df):
    abnormal_tickers = []
    for ticker in tickers:
        is_abnormal_return = f1_df[('price_chg_pct', ticker)].iloc[-1] > Config.price_abnormal
        is_abnormal_volume = f1_df[('vol_chg_pct', ticker)].iloc[-1] > Config.volume_abnormal
        if is_abnormal_return and is_abnormal_volume:
            # print( ticker, is_abnormal_return)
            abnormal_tickers.append(ticker)

    return abnormal_tickers

def get_fct1_flter2_tickers(tickers, fct1_df):
    abnormal_tickers = []
    for ticker in tickers:
        is_abnormal_return = (fct1_df[('price_chg_pct', ticker)].iloc[-1] <= Config.price_abnormal) & (
                    fct1_df[('price_chg_pct', ticker)].iloc[-1] >= -Config.price_abnormal)
        is_abnormal_volume = fct1_df[('vol_chg_pct', ticker)].iloc[-1] > Config.volume_abnormal

        if is_abnormal_return and is_abnormal_volume:
            # print( ticker, is_abnormal_return)
            abnormal_tickers.append(ticker)

    return abnormal_tickers

def gen_fct1_to_send(price, spac_info, filename='abnormal_spac.txt'):
    df = gen_factor_1_df(price)
    print( " check this ",df.index[-1].strftime('%Y-%m-%d'))
    last_date = df.index[-1].strftime('%Y-%m-%d')
    abnormal_tickers_filters=[]

    filter1 = get_fct1_flter1_tickers(price['Close'].columns, df)
    abnormal_tickers_filters.append(filter1)
    filter2 = get_fct1_flter2_tickers(price['Close'].columns, df)
    abnormal_tickers_filters.append(filter2)

    filenames=[]
    for i in range(len(abnormal_tickers_filters)):
        filename="abnormal_spac_filter"+str(i)+"_"+last_date+".csv"
        to_send = {}
        #print("preparing file "+filename , str(abnormal_tickers_filters[i]))
        #print(df.columns)
        for ticker in abnormal_tickers_filters[i]:

            tmp = pd.DataFrame()
            to_send[ticker] = tmp
            # tmp = df[[('price_chg_pct',ticker), ('vol_chg_pct',ticker),('volume_in_usd',ticker)]]
            tmp['price change(%)'] = (df[('price_chg_pct', ticker)] * 100).apply(lambda x: '{:,.2f}%'.format(x))
            tmp['ytday price change(%)'] = (df[('ytday_price_chg_pct', ticker)] * 100).apply(lambda x: '{:,.2f}%'.format(x))
            tmp['volume change(%)'] = (df[('vol_chg_pct', ticker)] * 100).apply(lambda x: '{:,.2f}%'.format(x))
            tmp['volume USD($MM)'] = (df[('volume_in_usd', ticker)] / 1000000).apply(lambda x: '${:,.2f}MM'.format(x))
            tmp['Close'] = price['Close'][ticker].apply(lambda x: '{:,.2f}'.format(x))

            # has_target = (spac_info[spac_info['Symbol']==ticker]['Merger Target?']).values
            # ipo_date = spac_info[spac_info['Symbol']==ticker]['IPO Date'].values
            liq_date = spac_info[spac_info['Symbol'] == ticker][
                ['Symbol', 'Merger Target?', 'IPO Date', 'Liquidation Date']]

        #filename = 'sample.txt'
        file_full_name = CWD + filename
        desc =""
        if i ==0:
            desc="Volatile Price, Volatile Volume"
        elif i==1:
            desc="Stable Price, Volatile Volume"
        with open(file_full_name, 'w') as outfile:
            outfile.write('\n/** \n %s  \n**/\n' % (desc))
            for ticker, value in to_send.items():
                outfile.write('\n======== %s ========\n' % (ticker))
                header = spac_info[spac_info['Symbol'] == ticker][
                    ['Symbol', 'Merger Target?', 'IPO Date', 'Liquidation Date']]
                header.to_string(outfile)
                outfile.write('\n')
                to_send[ticker].tail(10).to_string(outfile)
                outfile.write('\n\n')
        filenames.append(filename)
    
        tickers_filename = "filter%s_tickers_%s.csv"%(i,last_date)
        with open (CWD+tickers_filename,'w') as f:
            for ticker in abnormal_tickers_filters[i]:
                f.write("%s\n"%ticker)             
        filenames.append(tickers_filename)
    return filenames

def send_email(filenames = ['abnormal_spac.txt'], path_to_file=CWD ):
    try:

        server = smtplib.SMTP("smtp.gmail.com", "587")
        # self.server.ehlo() # Can be omitted
        server.starttls()
        # self.server.ehlo() # Can be omitted
        server.login(Secret.EMAILFROM,Secret.EMAILPW)

        message = MIMEMultipart()
        message['From'] = Secret.EMAILFROM
        message['Bcc'] = COMMASPACE.join(Secret.EMAILDICT['bcc'])
        message['Subject'] = "SPAC watch list"

        sent = 0
        retry = 0
        while sent == 0:
            try:
                for filename in filenames:
                    file_full_name = path_to_file + filename
                    with open(file_full_name, "rb") as attachment:
                        # Add file as application/octet-stream
                        # Email client can usually download this automatically as attachment
                        part = MIMEApplication(
                            attachment.read(),
                            Name=filename
                        )

                    # Add header as key/value pair to attachment part
                    part['Content-Disposition'] = 'attachment; filename="%s"' % (filename)

                    # Add attachment to message and convert message to string
                    message.attach(part)
                text = message.as_string()

                server.sendmail(Secret.EMAILFROM, Secret.EMAILDICT['bcc'], text)
                print('sent email ')
                sent += 1
            except Exception as e:
                time.sleep(1)  # TODO not good
                retry += 1
                if retry == 2:
                    raise Exception("Failed to send Email after retry %s" % e)
                    break
    except Exception as e:
        print('Send email Exception', e)
    finally:
        server.quit()

if __name__ == '__main__':
    spac_info = load_spac_info("/Users/ZhenxinLei/MyWork/quantamental/spac/active_spacs_clean.csv")
    tickers = list(spac_info['Symbol'])
    price = load_hist_data(tickers)
    filenames = gen_fct1_to_send(price, spac_info)

    print('before send email '+CWD," to send files ", filenames)
    send_email(filenames)
    quit()
