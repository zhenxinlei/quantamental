import yfinance as yf

def create_cross_section_mom_df(price_df):
    price_df['return']= price_df['Close']/price_df.iloc[0]['Close']-1



def gen_cross_section_mom_graph(data, symbols):
    pass

if __name__ == '__main__':
    symbols=['NIO','LI','XPEV']
    symbols = sorted(list(set(symbols)))
    print(symbols)
    print('len ', len(symbols))

    data = yf.download(symbols, period='2Y', interval='1d')
    plot_files = create_cross_section_mom_df(data)