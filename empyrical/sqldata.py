"""
本地数据库查询单个股票或指数代码的期间收益率、国库券利率
"""
import pandas as pd

from cnswd.utils import sanitize_dates
from cnswd.reader import daily_history, treasury
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


def _get_single_stock_equity(symbol, start_date, end_date, is_index,
                             index_name):
    start_date, end_date = sanitize_dates(start_date, end_date)
    df = daily_history(symbol, start_date, end_date, is_index)[['日期', '涨跌幅']]
    df.columns = DAILY_COLS
    df['change_pct'] = df['change_pct'] / 100.0
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
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

    **注意** 返回涨跌幅（浮点小数），非百分比

    Examples
    --------
    >>> symbol = '000333'
    >>> start_date = '2017-9-4'
    >>> end_date = '2017-9-8'
    >>> s = get_single_stock_equity(symbol, start_date, end_date)
    >>> s
    date
    2017-09-04 00:00:00+00:00    0.012733
    2017-09-05 00:00:00+00:00    0.001209
    2017-09-06 00:00:00+00:00   -0.010625
    2017-09-07 00:00:00+00:00    0.000000
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

    **注意** 其实返回的是涨跌幅，并非百分比

    Examples
    --------
    >>> symbol = '000300'
    >>> start_date = '2017-9-4'
    >>> end_date = pd.Timestamp('2017-9-8')
    >>> s = get_single_index_equity(symbol, start_date, end_date)
    >>> s
    date
    2017-09-04 00:00:00+00:00    0.003936
    2017-09-05 00:00:00+00:00    0.002972
    2017-09-06 00:00:00+00:00   -0.001970
    2017-09-07 00:00:00+00:00   -0.005086
    2017-09-08 00:00:00+00:00   -0.001014
    Name: 000300, dtype: float64
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
    >>> start_date = '2017-9-4'
    >>> end_date = pd.Timestamp('2017-9-18')
    >>> df = get_treasury_data(start_date, end_date)
    >>> df.iloc[:5, :5]
        cash	1month	2month	3month	6month
    date					
    2017-09-04 00:00:00+00:00	0.024586	0.028337	0.029225	0.029352	0.033742
    2017-09-05 00:00:00+00:00	0.024269	0.028273	0.029292	0.029284	0.033743
    2017-09-06 00:00:00+00:00	0.024364	0.028668	0.029463	0.029742	0.033744
    2017-09-07 00:00:00+00:00	0.023983	0.028365	0.029749	0.030045	0.033154
    2017-09-08 00:00:00+00:00	0.023839	0.028892	0.029418	0.029667	0.033479
    """
    df = treasury(start_date, end_date)
    # 缺少2年数据，使用简单平均插值
    value = (df['y1'] + df['y3']) / 2
    df.insert(7, '2year', value)
    df.rename(columns=TREASURY_COL_MAPS, inplace=True)
    df.index = pd.DatetimeIndex(df.index)
    return df.tz_localize('UTC')