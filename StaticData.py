SYMBOLS_LIST =  [
                    "MSFT", 
                    "GOOGL", 
                    "AMZN",
                    "AAPL",
                    "MMM",
                    "AXP",
                    "BA",
                    "CAT",
                    "CVX",
                    "CSCO",
                    "KO",
                    "DIS",
                    "DOW",
                    "XOM",
                    "GS",
                    "HD",
                    "IBM",
                    "INTC",
                    "JNJ",
                    "JPM",
                    "MCD",
                    "MRK",
                    "NKE",
                    "PFE",
                    "PG",
                    "TRV",
                    "UTX",
                    "UNH",
                    "VZ",
                    "V",
                    "WMT",
                    "WBA",
                ]

def batchSymbolString():
    batchStr = ""
    for symbol in SYMBOLS_LIST:
        batchStr += symbol 
        batchStr += ","
        
    return batchStr