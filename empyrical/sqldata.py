"""
1. 仅仅用于收益率计算，无需太多列(减少数据传输，提高速度)
2. 不使用close， 原始价格因停牌，价格标记为0，第一行设定为Nan?
3. 直接调用涨跌幅
"""
# TODO:需要再确认！！！
import pandas as pd

from cnswd.utils import sanitize_dates
from cnswd.sql.base import session_scope
from cnswd.sql.szsh import StockDaily, IndexDaily, Treasury

DAILY_COLS = ['date', 'change_pct']
TREASURY_COLS = ['date', '1month', '3month', '6month',
                 '1year', '2year', '3year',
                 '5year', '7year', '10year', '20year', '30year']


def get_single_stock_equity(symbol, start_date, end_date):
    """
    从本地数据库读取单个股票期间非累计收益率

    Parameters
    ----------
    symbol : str
        要获取数据的股票代码
    start_date : datetime-like
        自开始日期
    end_date : datetime-like
        至结束日期

    return
    ----------
    DataFrame: DataFrame对象。
        date列与change_pct列
    
    **注意** 其实返回的是涨跌幅，并非百分比

    Examples
    --------
    >>> symbol = '000333'
    >>> start_date = '2017-9-4'
    >>> end_date = '2017-9-8'
    >>> df = get_single_stock_equity(symbol, start_date, end_date)
    >>> df
            date  change_pct
    0  2017-09-04    0.012733
    1  2017-09-05    0.001209
    2  2017-09-06   -0.010625
    3  2017-09-07    0.000000
    4  2017-09-08    0.000488
    """
    start_date, end_date = sanitize_dates(start_date, end_date)
    with session_scope('cn') as sess:
        query = sess.query(
            StockDaily.日期,
            StockDaily.涨跌幅,
        ).filter(
            StockDaily.股票代码 == symbol
        ).filter(
            StockDaily.日期.between(start_date, end_date)
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = DAILY_COLS
        df['change_pct'] = df['change_pct'] / 100.0
        return df


def get_single_index_equity(symbol, start_date, end_date):
    """
    从本地数据库读取单个指数期间非累计收益率

    Parameters
    ----------
    symbol : str
        指数代码
    start_date : datetime-like
        开始日期
    end_date : datetime-like
        结束日期

    return
    ----------
    DataFrame: DataFrame对象。
        date列与change_pct列

    **注意** 其实返回的是涨跌幅，并非百分比

    Examples
    --------
    >>> symbol = '000300'
    >>> start_date = '2017-9-4'
    >>> end_date = pd.Timestamp('2017-9-8')
    >>> df = get_single_index_equity(symbol, start_date, end_date)
    >>> df
            date  change_pct
    0  2017-09-04    0.003936
    1  2017-09-05    0.002972
    2  2017-09-06   -0.001970
    3  2017-09-07   -0.005086
    4  2017-09-08   -0.001014
    """
    start_date, end_date = sanitize_dates(start_date, end_date)
    with session_scope('cn') as sess:
        query = sess.query(
            IndexDaily.日期,
            IndexDaily.涨跌幅,
        ).filter(
            IndexDaily.指数代码 == symbol
        ).filter(
            IndexDaily.日期.between(start_date, end_date)
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = DAILY_COLS
        df['change_pct'] = df['change_pct'] / 100.0
        return df


def get_treasury_data(start_date, end_date):
    """读取期间资金成本数据

    Parameters
    ----------
    start_date : datetime-like
        开始日期
    end_date : datetime-like
        结束日期

    return
    ----------
    DataFrame: OHLCV列的DataFrame对象。

    Examples
    --------
    >>> start_date = '2017-9-4'
    >>> end_date = pd.Timestamp('2017-9-18')
    >>> df = get_treasury_data(start_date, end_date)
    >>> df
                1month    3month    6month     1year     2year     3year  \
    2017-09-04  0.028337  0.029352  0.033742  0.033959  0.034888  0.035816
    2017-09-05  0.028273  0.029284  0.033743  0.034214  0.035029  0.035843
    2017-09-06  0.028668  0.029742  0.033744  0.035062  0.035391  0.035719
    2017-09-07  0.028365  0.030045  0.033154  0.034837  0.035129  0.035420
    2017-09-08  0.028892  0.029667  0.033479  0.034625  0.034873  0.035120
    2017-09-11  0.029164  0.029970  0.033299  0.034771  0.034981  0.035191
    2017-09-12  0.029427  0.030094  0.033382  0.034832  0.034996  0.035161
    2017-09-13  0.029439  0.030429  0.033779  0.034912  0.035088  0.035264
    2017-09-14  0.029557  0.030342  0.033924  0.034803  0.035017  0.035230
    2017-09-15  0.030139  0.030126  0.033787  0.034759  0.034988  0.035217
    2017-09-18  0.030052  0.030303  0.033672  0.034695  0.035015  0.035334
                5year     7year    10year    20year    30year
    2017-09-04  0.036235  0.036345  0.036454  0.041561  0.042308
    2017-09-05  0.036403  0.036522  0.036641  0.041661  0.042409
    2017-09-06  0.036314  0.036434  0.036554  0.041599  0.042372
    2017-09-07  0.036056  0.036105  0.036155  0.041331  0.042236
    2017-09-08  0.035811  0.035908  0.036005  0.041256  0.042237
    2017-09-11  0.035918  0.036037  0.036156  0.041357  0.042338
    2017-09-12  0.035895  0.036038  0.036181  0.041382  0.042363
    2017-09-13  0.036220  0.036288  0.036356  0.041480  0.042461
    2017-09-14  0.035968  0.035975  0.035982  0.041244  0.042366
    2017-09-15  0.035995  0.035988  0.035982  0.041419  0.042716
    2017-09-18  0.036289  0.036255  0.036220  0.041473  0.042769
    """
    start_date, end_date = sanitize_dates(start_date, end_date)
    with session_scope('cn') as sess:
        query = sess.query(
            Treasury.date,
            Treasury.m1,
            Treasury.m3,
            Treasury.m6,
            Treasury.y1,
            Treasury.y2,
            Treasury.y3,
            Treasury.y5,
            Treasury.y7,
            Treasury.y10,
            Treasury.y20,
            Treasury.y30
        ).filter(
            Treasury.date.between(start_date, end_date)
        )
        df = pd.DataFrame.from_records(query.all())
        df.columns = TREASURY_COLS
        # 缺少2/7年数据，使用简单平均插值
        df['2year'] = (df['1year'] + df['3year']) / 2
        df['7year'] = (df['5year'] + df['10year']) / 2
        df.index = pd.to_datetime(df['date'].values)
        df.drop('date', axis=1, inplace=True)
        return df
