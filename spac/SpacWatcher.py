import pandas as pd
import re
import logging
import operator
import requests
import argparse
import datetime
import sys
from lxml import html
from user_agent import generate_user_agent
from utils.GoogleSheetController import GoogleSheetController

OPS= {">": operator.gt, ">=": operator.ge,
        "<": operator.lt, "<=": operator.le, }


SPREADSHEET_ID = '1_96TAcYvT07e9Lriur6YTQhTL30_G2bF8jM3_4tgNkc'
#COPY_SHEET_ID = '1rz44BsYNqdF-NCacWgmblrfr0kuR_oR4yO4Bd1O4UGo'
#SPREADSHEET_ID = COPY_SHEET_ID

from spac.spac_screener import load_hist_data

class FilterCondition():

    def __init__(self,field, name, operator, threshold, benchmark=None):
        self.name = name
        self.threshold = threshold
        self.field = field
        self.benchmark = benchmark
        self.operator = OPS[operator]

    def gen_field_chagne_pct_df(self, price_df,field_col='Close',look_back=10, ma_window=10):
        if  self.benchmark !=None and re.match(r'(?i)ma_{0,9}.+', self.benchmark):
            ma_window=int(self.benchmark.split('_')[1])
            #print("ma window ",ma_window)

        df = pd.DataFrame(index=price_df.index)

        rolling_mean = price_df.xs(field_col, axis=1, level=0, drop_level=False).rolling(window=ma_window).mean()
        close_price = price.xs(field_col, axis=1, level=0, drop_level=False)
        return_cols = close_price / rolling_mean.shift(look_back) - 1
        return_cols = return_cols.rename(columns={field_col: self.field})
        df = df.join(return_cols)
        return df


    def gen_volume_usd_df(self, price_df,field_col='Volume',look_back=10, ma_window=10):
        df = pd.DataFrame(index=price_df.index)

        avg_price = price_df[['Close', 'Open', 'High', 'Low']].mean(axis=1, level=1)
        volume = price_df[field_col]
        volume_in_usd = avg_price * volume
        #print(volume_in_usd.columns,avg_price.columns )
        new_col_name = self.field+'_'+self.benchmark
        price_df = price_df.drop(new_col_name, axis=1, level=0)  # drop col if exist
        for col_name in volume_in_usd.columns:
            volume_in_usd = volume_in_usd.rename(columns={col_name: (new_col_name, col_name)})

        df = price_df.join(volume_in_usd)
        #print(df.tail())

        return df

    def gen_benchmark_df(self, price_df,field_col='Close',look_back=10, ma_window=10):
        #print( " field name ", field_col)
        if  self.benchmark !=None and re.match(r'(?i)ma_{0,9}.+', self.benchmark):
            ma_window=int(self.benchmark.split('_')[1])
            #print("ma window ",ma_window)

        df = pd.DataFrame(index=price_df.index)
        new_col_name = self.field+"_"+self.benchmark
        price_df = price_df.drop(new_col_name, axis=1, level=0) #drop col if exist

        rolling_mean = price_df.xs(field_col, axis=1, level=0, drop_level=False).rolling(window=ma_window).mean()
        #close_price = price.xs(field_col, axis=1, level=0, drop_level=False)

        return_cols = rolling_mean.rename(columns={field_col: new_col_name})
        df = price_df.join(return_cols)

        #print(df.tail())

        return df


class Filter():
    def __init__(self, name, interval):
        self.name = name
        self.interval = interval
        pass

    def parseStringToFormula(self, formula_str, benchmark_val,):
        new_str = formula_str.format(benchmark=benchmark_val)
        #eval may not works for all
        new_benchmark = eval(new_str)
        return new_benchmark


class AndFilter(Filter):


    def __init__(self, name,interval):
        super(AndFilter,self).__init__(name,interval)
        self.conditions = []

    def addCondition(self, condition:FilterCondition):
        self.conditions.append(condition)

    #AND conditions only
    def checkCondition(self,df, ticker):
        is_match = False
        try:
            for condition in self.conditions:
                if condition.benchmark is None:
                    continue
                benchmark_col = condition.field+"_"+condition.benchmark
                #print( 'benchmark col ',benchmark_col, (benchmark_col in df))
                if benchmark_col in df:
                    #print(benchmark_col,ticker,df[condition.field+"_"+condition.benchmark][ticker].tail())
                    if condition.field == "Volume" and condition.name == "usd_val":
                        field_val=  df[condition.field+"_"+condition.benchmark][ticker].iloc[-1]
                        benchmark_val = float(condition.benchmark)
                    else:
                        benchmark_val = df[condition.field+"_"+condition.benchmark][ticker].iloc[-1]
                        field_val = df[condition.field][ticker].iloc[-1]
                        benchmark_val = self.parseStringToFormula(condition.threshold, benchmark_val = benchmark_val)
                    #print(' field val ', field_val, 'ben_vl', benchmark_val)
                    condition_result = condition.operator(field_val, benchmark_val)

                    if not condition_result:
                        #print('Failed ',benchmark_col, condition.threshold, field_val, condition.operator,benchmark_val)
                        return False
                    #print("Passed ",benchmark_col, condition.threshold, field_val, condition.operator,benchmark_val)

                    #price_df[('price_change_pct', ticker)].iloc[-1] > Config.price_abnormal

                else:
                    logging.warning("Cant find col name "+benchmark_col)


            is_match = True
        except Exception as e:
            print("Failed parse ticker ", ticker,"\n ", e)



        return is_match




class NewsFtecher():
    STOCK_URL = 'https://finviz.com/quote.ashx'
    NEWS_URL = 'https://finviz.com/news.ashx'
    CRYPTO_URL = 'https://finviz.com/crypto_performance.ashx'
    STOCK_PAGE = {}

    def http_request_get(self,url, session=None, payload=None, parse=True):
        """ Sends a GET HTTP request to a website and returns its HTML content and full url address. """

        if payload is None:
            payload = {}

        try:
            if session:
                content = session.get(url, params=payload, verify=False, headers={'User-Agent': generate_user_agent()})
            else:
                content = requests.get(url, params=payload, verify=False, headers={'User-Agent': generate_user_agent()})

            content.raise_for_status()  # Raise HTTPError for bad requests (4xx or 5xx)
            if parse:
                return html.fromstring(content.text), content.url
            else:
                return content.text, content.url
        except Exception as e :
            logging.exception(" error in get http request ")
            return None, None


    def get_page(self,ticker):
        global STOCK_PAGE

        if ticker not in self.STOCK_PAGE:

            self.STOCK_PAGE[ticker], _ = self.http_request_get(
                url=self.STOCK_URL, payload={'t': ticker}, parse=True)


    def get_news(self,ticker):
        """
        Returns a list of sets containing news headline and url
        :param ticker: stock symbol
        :return: list
        """

        self.get_page(ticker)
        page_parsed = self.STOCK_PAGE[ticker]
        if page_parsed is None:
            return [("","","")]

        news_table = page_parsed.cssselect('[id="news-table"]')
        last_date = None

        times=[]
        headers =[]
        urls=[]
        try:
            for row in news_table[0].cssselect('tr'):

                time_text = row.cssselect('td')[0].text_content().replace(u'\xa0','')
                date_time = time_text.split(" ")
                if len(date_time)==2:
                    last_date = date_time[0]

                else:
                    time_text = last_date+" "+time_text

                title  = row.cssselect('td')[1].cssselect('a[class="tab-link-news"]')[0].xpath(
            'text()')[0]
                link = row.cssselect('td')[1].cssselect('a[class="tab-link-news"]')[0].get('href')

                times.append(time_text)
                headers.append(title)
                urls.append(link)
        except (Exception):
            logging.error("Error in fetch news "+ticker)


        return list(zip(times,headers,urls))


class SpacWatcher():
    filters = {}

    def __init__(self):
        self.gs = GoogleSheetController()
        self.sheet_id= SPREADSHEET_ID

    def gen_filtered_df(self,price_df, filter, close_col='Close', volume_col='Volume', look_back=10, ma_window=10, tab_prefix=None):
        today = datetime.datetime.today().date()
        if price_df.index[-1].date() != today and filter.interval=='1h':
            print("Didn't get price update for today ", today)
            return

        tab_name = "{0}_{1}".format(tab_prefix, filter.name) if tab_prefix is not None else filter.name

        #get yester tickker
        data = self.gs.read(self.sheet_id,tab_name+"!A2:G")
        old_data = []
        if data != None:
            for d in data:
                if len(d)==0:
                    continue
                else:
                    old_data=d

        #print("yester day data ",old_data)
        old_tickers =[]

        if len(old_data)>2:
            if filter.interval == '1d' and old_data[0] == today.strftime('%m/%d/%Y'):
                return
            elif filter.interval == '1d' or (filter.interval=='1h' and old_data[0] == today.strftime('%m/%d/%Y')):
                old_tickers.extend([x.strip() for x in old_data[1].split(',')])#stay
                old_tickers.extend([x.strip() for x in old_data[2].split(',')])#new

        print(" last day ticker ", old_tickers)


        #generate data frame
        for condition in filter.conditions:
            #print(condition.field, condition.benchmark)
            if re.match(r'change_pct',condition.name):
                price_df = condition.gen_benchmark_df(price_df, field_col=condition.field)
            if re.match(r'usd_val',condition.name) and condition.field == "Volume":
                #print(condition.name,condition.benchmark)
                price_df = condition.gen_volume_usd_df(price_df, field_col=condition.field)

        filtered_tickers = []

        for ticker in price_df.columns.levels[1]:
            is_match = filter.checkCondition(price_df,ticker)
            if is_match:
                filtered_tickers.append(ticker)
                print(" ticker match ",filter.name, ticker)

        new_tickers= []
        removed_tickers=[]
        stay_tickers = list(set(filtered_tickers)&set(old_tickers))
        for ticker in filtered_tickers:
            if ticker not in old_tickers and ticker !='':
                new_tickers.append(ticker)
        for ticker in old_tickers:
            if ticker not in filtered_tickers and ticker !='':
                removed_tickers.append(ticker)

        now = datetime.datetime.now()
        sheet_values = [
            [now.strftime("%m/%d/%Y"), ",".join(stay_tickers),",".join(new_tickers), ",".join(removed_tickers),now.strftime("%m/%d/%Y %H:%M:%S")]
                        ]

        #print(" new ", sheet_values )

        self.gs.append(self.sheet_id,tab_name+"!A2:G",sheet_values )




    def write_news_to_sheet(self,news_dict, clear_sheet=False, max_news_number=10):
        range = "news!A2:E"
        values =[]
        #print( news_dict)
        if clear_sheet :
            self.gs.clear(self.sheet_id,range)

        for ticker, all_news in news_dict.items():

            if len(all_news) ==0:
                continue
            is_first_news = True
            count = 0
            for news in all_news:
                tmp=[ticker if is_first_news else "", news[0],news[1],news[2]]
                is_first_news=False
                values.append(tmp)
                count +=1
                if count>= max_news_number:
                    break

        self.gs.write(self.sheet_id, range,values)

    def get_news_symbols(self):
        range = "news_watchlist!B2:B"
        news_symbols = self.gs.read(self.sheet_id, range)
        tickers=[]
        for symbol in news_symbols:
            if len(symbol[0])==0:
                continue
            tickers.append(symbol[0])
        tickers = sorted(set(tickers))

        return tickers


    def get_filter_from_gs(self, tab_prefix=None):
        range="setup!A:E"
        data = self.gs.read(self.sheet_id, range)
        filter = None
        for d in data:
            if len(d) >0:
                if re.match(r'@.+',d[0]): #header
                    continue
                if re.match(r'#.+',d[0]):
                    name = d[0].replace('#','')
                    interval = d[1]
                    filter = AndFilter(name, interval)
                    self.filters[name]= filter
                    tab_name = "{0}_{1}".format(tab_prefix,name) if tab_prefix is not None else name
                    self.gs.get_or_create_tab(self.sheet_id,tab_name,['Date','Stay','New','Removed','Last Update Time'])
                else:
                    cond = FilterCondition(d[0], d[1], d[2], d[3], d[4])
                    filter.addCondition(cond)

    def get_filter_from_gs2(self, watchlist_name):
        range="setup!A:E"
        data = self.gs.read(self.sheet_id, range)
        filter = None
        for d in data:
            if len(d) >0:
                if re.match(r'@.+',d[0]): #header
                    continue
                if re.match(r'#.+',d[0]):
                    name = d[0].replace('#','')
                    interval = d[1]


                    is_allowed_filter = False
                    if(len(d)>=3):
                        allowed_watchlist = d[2]
                        if allowed_watchlist.lower() == "all":
                            is_allowed_filter = True
                        elif watchlist_name is not None and watchlist_name in allowed_watchlist:
                            is_allowed_filter = True
                    else:
                        is_allowed_filter = True


                    if is_allowed_filter:
                        filter = AndFilter(name, interval)
                        self.filters[name]= filter
                        tab_name = "{0}_{1}".format(watchlist_name,name)
                        self.gs.get_or_create_tab(self.sheet_id,tab_name,['Date','Stay','New','Removed','Last Update Time'])
                        print("Create or Get tab name ", tab_name)

                else:
                    cond = FilterCondition(d[0], d[1], d[2], d[3], d[4])
                    filter.addCondition(cond)


    # def create_archived_tab(self, tab_prefix="archived"):
    #     tab_name = "{0}_{1}".format(tab_prefix, name) if tab_prefix is not None else name
    #     self.gs.get_or_create_tab(self.sheet_id, tab_name, ['Date', 'Stay', 'New', 'Removed', 'Last Update Time'])

    def screen_archived_tickers(self, tab_prefix= "archived"):
        archived_tickers = spacWatcher.gs.read(spacWatcher.sheet_id, tab_prefix+"_spac!E2:E")
        archived_tickers = [i for i in archived_tickers if i]  # drop empty ticker string
        archived_tickers = [x[0] for x in archived_tickers]  # drop duplicate tickers
        spacWatcher.get_filter_from_gs(tab_prefix=tab_prefix)
        #spacWatcher.create_archived_tab(tab_prefix)
        print(tab_prefix+"  tickers ", archived_tickers)

        price = load_hist_data(archived_tickers)
        count= 0
        for name, filter in spacWatcher.filters.items():
            if filter.interval == args.m:
                count += 1
                spacWatcher.gen_filtered_df(price, filter, tab_prefix=tab_prefix)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Spac Watcher')
    parser.add_argument('-m', type=str, help='mode')

    args = parser.parse_args()

    print("arg mode ",args.m)

    spacWatcher = SpacWatcher()


    if "1h" in args.m or "1d" in args.m:
        col_names = spacWatcher.gs.read(spacWatcher.sheet_id, "watchlist!A1:Z1")[0]
        tickers = spacWatcher.gs.read(spacWatcher.sheet_id, "watchlist!A2:Z")

        print(" ticker list \n",col_names," ticker \n", tickers)

        col_to_tickers ={}

        for row in tickers:
            for col in range(len(col_names)):

                if col_names[col] not in col_to_tickers.keys():
                    col_to_tickers[col_names[col]]=[]

                if col >= len(row):
                    break
                ticker = row[col]
                col_to_tickers[col_names[col]].append(ticker)

        print(" col to tickers ", col_to_tickers)

        for watchlist_name,tickers in col_to_tickers.items():
            print("ticker to get price ",tickers)
            price = load_hist_data(tickers)
            spacWatcher.get_filter_from_gs2(watchlist_name = watchlist_name)

            count = 0
            for name,filter in spacWatcher.filters.items():
                if filter.interval==args.m:
                    count += 1
                    spacWatcher.gen_filtered_df(price, filter, tab_prefix= watchlist_name)

    elif "news" in args.m:
        tickers = spacWatcher.get_news_symbols()
        print(" get news ",tickers)
        news_dict = {}
        for ticker in tickers:
            news = NewsFtecher().get_news(ticker)

            news_dict[ticker] = news
        spacWatcher.write_news_to_sheet(news_dict,True, 10)
        pass


    #
    #
    # if "news" in args.m:
    #     tickers = spacWatcher.get_news_symbols()
    #     print(" get news ",tickers)
    #     news_dict = {}
    #     for ticker in tickers:
    #         news = NewsFtecher().get_news(ticker)
    #
    #         news_dict[ticker] = news
    #     spacWatcher.write_news_to_sheet(news_dict,True, 10)
    #     pass
    # elif "1h" in args.m:
    #     tickers = spacWatcher.gs.read(spacWatcher.sheet_id, "active_spac!B2:B")
    #     tickers = [x[0] for x in tickers]
    #
    #     #tickers =["KCAC", "TDAC","CIIC"]
    #     print("active tickers\n", tickers)
    #
    #     price = load_hist_data(tickers)
    #
    #     spacWatcher.get_filter_from_gs()
    #     count = 0
    #     for name, filter in spacWatcher.filters.items():
    #         if filter.interval==args.m:
    #             count += 1
    #             spacWatcher.gen_filtered_df(price, filter)
    #
    #
    #     spacWatcher.screen_archived_tickers()
    #
    # elif "1d" in args.m:
    #
    #     tickers = spacWatcher.gs.read(spacWatcher.sheet_id,"active_spac!B2:B")
    #     tickers = [x[0] for x in tickers]
    #
    #     #tickers =["KCAC","CIIC","TDAC"]
    #     print(tickers)
    #
    #     price = load_hist_data(tickers)
    #     #print(price[['Close',"Volume"]])
    #
    #     spacWatcher.get_filter_from_gs()
    #     count =0
    #     for name,filter in spacWatcher.filters.items():
    #         if filter.interval==args.m:
    #             count += 1
    #             spacWatcher.gen_filtered_df(price, filter)
    #
    #     spacWatcher.screen_archived_tickers()

    print("Existing... ")
    exit(0)



