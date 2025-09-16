"""
General constant enums used in the trading platform.
"""

from enum import Enum
import pandas as pd
from functools import total_ordering
import math


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "Long"
    SHORT = "Short"
    NET = "Net"


class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "Open"
    CLOSE = "Close"
    CLOSETODAY = "Close Today"
    CLOSEYESTERDAY = "Close Yesterday"


class OrderStatus(Enum):
    """
    Order status.
    """
    SUBMITTING = "Submitting"
    NOTTRADED = "Not Traded"
    PARTTRADED = "Partially Traded"
    ALLTRADED = "Fully Traded"
    PARTCANCELLED = "Partially Cancelled"
    ALLCANCELLED = "Cancelled"
    REJECTED = "Rejected"
    PENDING = "Pending Submit"
    UNKNOWN = "Unknown"
    MODIFIED = "Modified"


class Product(Enum):
    """
    Product class.
    """
    EQUITY = "Equity"
    FUTURES = "Futures"
    OPTION = "Option"
    INDEX = "Index"
    FOREX = "Forex"
    SPOT = "Spot"
    ETF = "ETF"
    BOND = "Bond"
    WARRANT = "Warrant"
    SPREAD = "Spread"
    FUND = "Fund"
    CFD = "CFD"
    SWAP = "Swap"


class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "Limit"
    MARKET = "Market"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"
    RFQ = "Request for Quote"
    ETF = "ETF"
    STP_LMT = "STP_LMT"  # Stop Limit
    STP_MKT = "STP_MKT"  # Stop Market
    ABS_LMT = "ABS_LMT"  # 绝对限价单


class OptionType(Enum):
    """
    Option type.
    """
    CALL = "Call Option"
    PUT = "Put Option"


class Exchange(Enum):
    """
    Exchange.
    """
    # Chinese
    CFFEX = "CFFEX"  # China Financial Futures Exchange
    SHFE = "SHFE"  # Shanghai Futures Exchange
    CZCE = "CZCE"  # Zhengzhou Commodity Exchange
    DCE = "DCE"  # Dalian Commodity Exchange
    INE = "INE"  # Shanghai International Energy Exchange
    GFEX = "GFEX"  # Guangzhou Futures Exchange
    SSE = "SSE"  # Shanghai Stock Exchange
    SZSE = "SZSE"  # Shenzhen Stock Exchange
    BSE = "BSE"  # Beijing Stock Exchange
    SHHK = "SHHK"  # Shanghai-HK Stock Connect
    SZHK = "SZHK"  # Shenzhen-HK Stock Connect
    SGE = "SGE"  # Shanghai Gold Exchange
    WXE = "WXE"  # Wuxi Steel Exchange
    CFETS = "CFETS"  # CFETS Bond Market Maker Trading System
    XBOND = "XBOND"  # CFETS X-Bond Anonymous Trading System

    # Global
    SMART = "SMART"  # Smart Router for US stocks
    NYSE = "NYSE"  # New York Stock Exchnage
    NASDAQ = "NASDAQ"  # Nasdaq Exchange
    ARCA = "ARCA"  # ARCA Exchange
    EDGEA = "EDGEA"  # Direct Edge Exchange
    ISLAND = "ISLAND"  # Nasdaq Island ECN
    BATS = "BATS"  # Bats Global Markets
    IEX = "IEX"  # The Investors Exchange
    AMEX = "AMEX"  # American Stock Exchange
    TSE = "TSE"  # Toronto Stock Exchange
    NYMEX = "NYMEX"  # New York Mercantile Exchange
    COMEX = "COMEX"  # COMEX of CME
    GLOBEX = "GLOBEX"  # Globex of CME
    IDEALPRO = "IDEALPRO"  # Forex ECN of Interactive Brokers
    CME = "CME"  # Chicago Mercantile Exchange
    ICE = "ICE"  # Intercontinental Exchange
    SEHK = "SEHK"  # Stock Exchange of Hong Kong
    HKFE = "HKFE"  # Hong Kong Futures Exchange
    SGX = "SGX"  # Singapore Global Exchange
    CBOT = "CBOT"  # Chicago Board of Trade
    CBOE = "CBOE"  # Chicago Board Options Exchange
    CFE = "CFE"  # CBOE Futures Exchange
    DME = "DME"  # Dubai Mercantile Exchange
    EUREX = "EUX"  # Eurex Exchange
    APEX = "APEX"  # Asia Pacific Exchange
    LME = "LME"  # London Metal Exchange
    BMD = "BMD"  # Bursa Malaysia Derivatives
    TOCOM = "TOCOM"  # Tokyo Commodity Exchange
    EUNX = "EUNX"  # Euronext Exchange
    KRX = "KRX"  # Korean Exchange
    OTC = "OTC"  # OTC Product (Forex/CFD/Pink Sheet Equity)
    IBKRATS = "IBKRATS"  # Paper Trading Exchange of IB

    # Special Function
    LOCAL = "LOCAL"  # For local generated data
    GLOBAL = "GLOBAL"  # For those exchanges not supported yet

    UNKNOWN = "UNKNOWN"


class Currency(Enum):
    """
    Currency.
    """
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"
    CAD = "CAD"


class LogLevel(Enum):
    """
    Log Level.
    """
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARNING = "WARNING"
    ERROR = "ERROR"

class CMD(Enum):
    """
    CMD.
    """
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARNING = "WARNING"
    ERROR = "ERROR"


@total_ordering
class Interval(Enum):
    """
    Interval of bar data.
    """
    NONE = math.nan
    TICK = 0.1
    K_1M = 60
    K_3M = 180
    K_5M = 300
    K_15M = 900
    K_30M = 1800
    K_1H = 3600
    K_2H = 7200
    K_3H = 10800
    K_4H = 14400
    K_DAY = 86400
    K_WEEK = 604800
    K_MON = 2592000
    K_QUARTER = 7776000
    K_YEAR = 31104000

    def __lt__(self, other: "Interval") -> bool:
        if isinstance(other, Interval):
            return float(self.value) < float(other.value)
        return NotImplemented
