"""
本地数据库查询单个股票或指数代码的期间收益率、国库券利率
"""
import pandas as pd

from cnswd.utils import sanitize_dates
from cnswd.mongodb import get_db
from cnswd.websource.wy import get_main_index

DAILY_COLS = ['date', 'change_pct']
TREASURY_COL_MAPS = {
    'm0': 'cash',
    'm1': '1month',
    'm2': '2month',
    'm3': '3month',
    'm6': '6month',
    'm9': '9month',
    'y1': '1year',
    'y3': '3year',
    'y5': '5year',
    'y7': '7year',
    'y10': '10year',
    'y15': '15year',
    'y20': '20year',
    'y30': '30year',
    'y40': '40year',
    'y50': '50year',
}


def query(collection, start, end):
    predicate = {'日期': {'$gte': start, '$lte': end}}
    projection = {'日期': 1, '涨跌幅': 1, '_id': 0}
    sort = [('日期', 1)]
    cursor = collection.find(predicate, projection, sort=sort)
    df = pd.DataFrame.from_records(cursor)
    return df


def _get_single_stock_equity(symbol, start_date, end_date, is_index,
                             index_name):
    start_date, end_date = sanitize_dates(start_date, end_date)
    db_name = 'wy_index_daily' if is_index else 'wy_stock_daily'
    db = get_db(db_name)
    collection = db[symbol]
    df = query(collection, start_date, end_date)
    df.columns = DAILY_COLS
    df['change_pct'] = df['change_pct'] / 100.0
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)
    res = df.tz_localize('utc')['change_pct']
    res.name = index_name
    # 原始数据中含nan
    res.fillna(0.0, inplace=True)
    return res


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
    Series: Series对象。

    **注意** 返回涨跌幅，非百分比

    Examples
    --------
    >>> symbol = '000333'
    >>> start_date = '2020-05-15'
    >>> end_date = '2020-05-25'
    >>> s = get_single_stock_equity(symbol, start_date, end_date)
    >>> s
    date
    2020-05-15 00:00:00+00:00   -0.018103
    2020-05-18 00:00:00+00:00    0.009482
    2020-05-19 00:00:00+00:00    0.022091
    2020-05-20 00:00:00+00:00    0.004595
    2020-05-21 00:00:00+00:00   -0.005252
    2020-05-22 00:00:00+00:00   -0.027248
    2020-05-25 00:00:00+00:00   -0.004902
    Name: 000333, dtype: float64
    """
    return _get_single_stock_equity(symbol, start_date, end_date, False,
                                    symbol)


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
    Series: Series对象。

    **注意** 返回涨跌幅，非百分比

    Examples
    --------
    >>> symbol = '000300'
    >>> start_date = '2020-05-15'
    >>> end_date = '2020-05-25'
    >>> s = get_single_index_equity(symbol, start_date, end_date)
    >>> s
    date
    2020-05-15 00:00:00+00:00   -0.003160
    2020-05-18 00:00:00+00:00    0.002580
    2020-05-19 00:00:00+00:00    0.008498
    2020-05-20 00:00:00+00:00   -0.005315
    2020-05-21 00:00:00+00:00   -0.005445
    2020-05-22 00:00:00+00:00   -0.022927
    2020-05-25 00:00:00+00:00    0.001376
    Name: 沪深300, dtype: float64
    """
    names = get_main_index()
    try:
        index_name = names.loc[symbol, 'name']
    except KeyError:
        index_name = symbol
    return _get_single_stock_equity(symbol, start_date, end_date, True,
                                    index_name)


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
    DataFrame: DataFrame对象。

    Examples
    --------
    >>> start_date = '2020-05-15'
    >>> end_date = '2020-05-25'
    >>> df = get_treasury_data(start_date, end_date)
    >>> df.iloc[:5, :5]
        cash	1month	2month	3month	6month
    date					
    2020-05-15 00:00:00+00:00	0.006838	0.009496	0.009506	0.010076	0.011570
    2020-05-18 00:00:00+00:00	0.006838	0.009369	0.009611	0.010414	0.011701
    2020-05-19 00:00:00+00:00	0.009838	0.009425	0.010490	0.010307	0.012016
    2020-05-20 00:00:00+00:00	0.008188	0.009084	0.010712	0.011012	0.012378
    2020-05-21 00:00:00+00:00	0.007028	0.008569	0.010695	0.011032	0.012465
    """
    start, end = sanitize_dates(start_date, end_date)
    db = get_db()
    collection = db['国债利率']
    predicate = {'date': {'$gte': start, '$lte': end}}
    projection = {'_id': 0}
    sort = [('date', 1)]
    cursor = collection.find(predicate, projection, sort=sort)
    df = pd.DataFrame.from_records(cursor)
    # 缺少2年数据，使用简单平均插值
    value = (df['y1'] + df['y3']) / 2
    df.insert(7, '2year', value)
    df.rename(columns=TREASURY_COL_MAPS, inplace=True)
    df.set_index('date', inplace=True)
    return df.tz_localize('UTC')