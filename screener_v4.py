"""
Stock Screener v5.0 - Pure Technical (Price Data Only)
=======================================================
Yahoo Finance .info は401エラーで使用不可のため、
yf.download()の価格データのみでスクリーニングする。

テクニカル指標:
  RS    = percentrank(TICKER/SPY, period) ← TradingView準拠
  Stage2 = Weinstein Stage 2 MA条件スコア (0-9)
  VCS   = Volatility Contraction Score (ATR比率)
  ADR%  = Average Daily Range %

スコアリング（最大10点、全テクニカル）:
  RS_P2 ≥ 90  → +3
  RS_P2 ≥ 70  → +2（≥90未満）
  RS_P2 ≥ 50  → +1（≥70未満）
  VCS ≥ 80    → +3
  VCS ≥ 60    → +1（≥80未満）
  Stage2 = 9  → +2
  Stage2 ≥ 7  → +1（9未満）
  EMA≤3%以内  → +1

出力: docs/data.json
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import time
import random
import warnings
import requests as req_lib
from io import StringIO
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

JST  = timezone(timedelta(hours=9))
NOW  = datetime.now(JST)
DOCS = Path("docs")
DOCS.mkdir(exist_ok=True)

CONFIG = {
    "rs_periods":      {"p1": 5, "p2": 21, "p3": 63, "p4": 126},
    "rs_benchmark":    "SPY",
    "rs_min":          50,      # RS P2 最低ライン
    "stage2_min":      5,       # Stage2スコア最低（9段階）
    "vcs_entry":       60,      # VCS エントリー基準（≥60で収縮中）
    "vcs_best":        80,      # VCS ベスト（≥80で高圧縮）
    "high_52w_pct_max": 35,     # 52週高値からの乖離率上限(%)
    "price_min":       10.0,    # 株価下限（ドル）
    "vix_full":        20,
    "vix_half":        25,
    "stop_loss_pct":   7.0,
    "batch_size":      50,
    "price_period":    "2y",    # 200日MA計算に2年必要
    "max_output":      200,     # 最大出力銘柄数
}

MACRO_TICKERS = ["SPY", "QQQ", "IWM", "^VIX"]

# ============================================================
# ユニバース取得
# ============================================================
SP500_FALLBACK = [
    "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB","AKAM","ALB","ARE",
    "ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN","AMCR","AEE","AAL","AEP","AXP","AIG",
    "AMT","AWK","AMP","AME","AMGN","APH","ADI","ANSS","AON","APA","AAPL","AMAT","APTV","ACGL",
    "ADM","ANET","AJG","AIZ","T","ATO","ADSK","ADP","AZO","AVB","AVY","AXON","BKR","BALL","BAC",
    "BK","BBWI","BAX","BDX","BRK-B","BBY","BIIB","BLK","BX","BA","BMY","AVGO","BR","BRO","BLDR",
    "BF-B","BURL","CHRW","CZR","CPT","CPB","COF","CAH","KMX","CCL","CARR","CAT","CBOE","CBRE",
    "CDW","CE","COR","CNC","CNP","CF","CRL","SCHW","CHTR","CVX","CMG","CB","CHD","CI","CINF",
    "CTAS","CSCO","C","CFG","CLX","CME","CMS","KO","CTSH","CL","CMCSA","CAG","COP","ED","STZ",
    "CEG","COO","CPRT","GLW","CPAY","CTVA","CSGP","COST","CTRA","CCI","CSX","CMI","CVS","DHR",
    "DRI","DVA","DAY","DECK","DE","DELL","DAL","DVN","DXCM","FANG","DLR","DFS","DG","DLTR","D",
    "DPZ","DOV","DOW","DHI","DTE","DUK","DD","EMN","ETN","EBAY","ECL","EIX","EW","EA","ELV",
    "LLY","EMR","ENPH","ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ESS","EL","ETSY","EG","ES",
    "EXC","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO","FAST","FRT","FDX","FIS","FITB","FSLR",
    "FE","FI","FMC","F","FTNT","FTV","FOXA","FOX","BEN","FCX","GRMN","IT","GE","GEHC","GEN",
    "GPC","GILD","GIS","GM","GNRC","GD","GS","GWW","HAL","HIG","HAS","HCA","DOC","HSIC","HSY",
    "HES","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ","HUBB","HUM","HBAN","HII","IBM",
    "IEX","IDXX","ITW","INCY","IR","PODD","INTC","ICE","IFF","IP","IPG","INTU","ISRG","IVZ",
    "INVH","IQV","IRM","JBHT","JBL","JKHY","J","JNJ","JCI","JPM","JNPR","K","KVUE","KDP","KEY",
    "KEYS","KMB","KIM","KMI","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LIN",
    "LYV","LKQ","LMT","L","LOW","LULU","LYB","MTB","MPC","MKTX","MAR","MMC","MLM","MAS","MA",
    "MTCH","MKC","MCD","MCK","MDT","MRK","META","MET","MTD","MGM","MCHP","MU","MSFT","MAA",
    "MRNA","MHK","MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP",
    "NFLX","NWL","NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH","NRG",
    "NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS","PCAR","PKG",
    "PANW","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PNC","POOL",
    "PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PTC","PSA","PHM","PWR","QCOM","DGX","RL",
    "RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY","ROK","ROL","ROP","ROST","RCL","SPGI",
    "CRM","SBAC","SLB","STX","SRE","NOW","SHW","SPG","SWKS","SJM","SNA","SOLV","SO","LUV","SWK",
    "SBUX","STT","STLD","STE","SYK","SYF","SNPS","SYY","TMUS","TROW","TTWO","TPR","TRGP","TGT",
    "TEL","TDY","TFX","TER","TSLA","TXN","TXT","TMO","TJX","TSCO","TT","TDG","TRV","TRMB","TFC",
    "TYL","TSN","USB","UDR","ULTA","UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VLTO","VRSN",
    "VRSK","VZ","VRTX","VTRS","VICI","V","VST","WAB","WBA","WMT","DIS","WBD","WM","WAT","WEC",
    "WFC","WELL","WST","WDC","WY","WHR","WMB","WTW","WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZTS",
    # NASDAQ100追加
    "ABNB","ADSK","ALGN","ANSS","ASML","ATVI","BIIB","CDNS","CMCSA","CPRT","CRWD","CSGP","CTSH",
    "DDOG","DLTR","DXCM","EA","EBAY","ENPH","FAST","FISV","GILD","HOOD","IDXX","ILMN","INCY",
    "INTU","ISRG","KDP","KLAC","LRCX","LULU","MAR","MELI","MNST","MRNA","MU","NFLX","NVDA",
    "NXPI","OKTA","ORLY","PANW","PAYX","PCAR","PYPL","QCOM","REGN","RIVN","ROST","SBUX","SNPS",
    "SNOW","TEAM","TMUS","TSLA","TXN","UBER","VRSK","VRSN","VRTX","WDAY","XEL","ZM","ZS",
    "APP","TTD","AXON","CAVA","ONON","EME","COIN","PLTR","AMD","AVGO",
]

RUSSELL2000_FALLBACK = [
    "ACLS","ACMR","AGYS","ALRM","AMSC","AOSL","APPF","BAND","BLKB","CALX","CAMT","CEVA","CLFD",
    "COHU","CPSI","CSWI","DFIN","DGII","DIOD","DOMO","EMKR","EXTR","FIVN","FORM","FOUR","HLIT",
    "ICFI","INFA","INFN","INMD","KTOS","LSCC","LSTR","MGNI","MLAB","MMSI","MODN","MRTN","MTSI",
    "MYRG","NABL","NCNO","NTGR","OMCL","ONTO","OPCH","QTWO","RCKT","RCUS","RDFN","RDNT","RICK",
    "RMNI","ROCK","ROLL","RXRX","SDGR","SEMR","SHLS","SILC","SIMO","SITM","SKYW","SLGN","SMPL",
    "SNEX","SPSC","SQSP","SRDX","SSYS","SUPN","TALO","TASK","TCMD","TELA","TENB","TMDX","TNDM",
    "TNET","TPIC","TREX","TRMK","TRNS","TRUP","TTMI","TWST","TXMD","TZOO","UFPI","UMBF","UNFI",
    "UNIT","UPST","USLM","USNA","USPH","UTMD","VCYT","VIAV","VRRM","VSEC","WDFC","WERN","WETF",
    "WOLF","WSFS","WULF","XPEL","XPOF","YELP","ZETA","ZEUS","ZUMZ","ZYXI","DOCN","MSTR","SYNA",
    "NTNX","LCUT","ACCD","ADMA","AHCO","AKRO","ALEC","AMPH","ARDX","ARQT","ATRC","AVNS","AXGN",
    "AXSM","BCYC","BFLY","BLFS","BNGO","CARA","CDMO","CERT","CGEM","CHRS","CLDX","CNMD","CODA",
    "COGT","CORT","CPRX","CRSP","CVAC","CYRX","DNLI","DVAX","EDIT","ELVN","ERAS","EVBG","EVLO",
    "FATE","FGEN","FLGT","FOLD","FULC","GALT","GDRX","GERN","GOSS","HALO","HCAT","HRTX","HTBX",
    "ICAD","IDYA","IMGN","IMMP","IOVA","ITCI","KNSA","KPTI","KROS","KRTX","LBPH","LEGN","LGND",
    "LIVN","LMAT","MDXG","MGNX","MGTX","MIRM","MNKD","MORF","NKTR","NTRA","NUVA","NVAX","NVCR",
    "OCGN","OFIX","OMER","OPTN","ORIC","ORGO","ORTX","OSUR","OVID","PAHC","PCRX","PCVX","PRCT",
    "PRDO","PRTK","PTCT","PTGX","QDEL","RDUS","RVNC","SAGE","SANA","TGTX","TBPH","ZGNX","ABCB",
    "ACNB","AMNB","BHLB","BRKL","BSVN","BUSE","CATC","CBAN","CCBG","CFFI","CLBK","CNOB","CTBI",
    "DCOM","EBTC","EFSC","EGBN","ESSA","EVTC","EXPI","FBMS","FFIC","FFWM","FISI","FLIC","FMCB",
    "FMNB","FNLC","FRME","FSBW","GABC","GBCI","GCBC","GFED","GNTY","HAFC","HBCP","HBIA","HFWA",
    "HMST","HOPE","HONE","HRZN","HTBK","HTLF","IBTX","INBK","INDB","KELYA","KFRC","KLIC","KVHI",
    "LKFN","LGIH","LNDC","LOVE","LPSN","LQDT","MATX","MBUU","MCRI","MFIN","MHO","MOFG","MPAA",
    "MSEX","NBHC","NBTB","OCFC","ORRF","PATK","PEBO","PERI","PFBC","PFIS","PGNY","PLCE","PLMR",
    "PNFP","POWL","PPBI","PRAA","PRFT","PRGS","QCRH","RUSHA","RUTH","RYAM","SFBS","SFNC","SMBC",
    "SMBK","SPFI","STBA","TCBI","TCBK","THFF","TILE","UCBI","UMPQ","WSFS","AR","CIVI","CNX",
    "CRGY","MGY","MTDR","NOG","RRC","SBOW","SM","VTLE","ARCH","CEIX","HCC","ARLP","AAON","APOG",
    "ARKO","ASIX","ATNI","CADE","CASH","DXPE","ERII","FCFS","FLNC","FWRG","GRND","GTLS","HWKN",
    "IMAX","IMXI","INSG","INSW","IOSP","KALU","KIDS","KOPN","LLNW","MANT","MIDD","MLNK","MRAM",
    "NARI","NNBR","NOMD","NRDS","NWLI","OMAB","OPRX","OSBC","PACK","PDFS","PLAB","PLXS","RCKT",
    "RMNI","ROLL","SDGR","SEMR","SHLS","SILC","SKYW","SLGN","SMID","SMPL","SNEX","SPSC","SQSP",
    "SRDX","SSYS","TALO","TASK","TCMD","TELA","TENB","TMDX","TNDM","TNET","TPIC","TREX","TRNS",
    "TTMI","TWST","UFPI","UNIT","UPST","USLM","USNA","VSEC","WDFC","WERN","WETF","WOLF","XPEL",
    "YELP","ZETA","ZEUS",
]

def fetch_sp500():
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        resp = req_lib.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=30)
        tables = pd.read_html(StringIO(resp.text))
        tickers = [t.replace(".", "-") for t in tables[0]["Symbol"].tolist()]
        if len(tickers) > 400:
            print(f"  S&P500 (Wikipedia): {len(tickers)}銘柄")
            return tickers
    except Exception as e:
        print(f"  S&P500 Wikipedia失敗: {e}")
    tickers = list(set(SP500_FALLBACK))
    print(f"  S&P500 (ハードコード): {len(tickers)}銘柄")
    return tickers

def fetch_nasdaq100():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = req_lib.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=30)
        tables = pd.read_html(StringIO(resp.text))
        for t in tables:
            if "Ticker" in t.columns:
                tickers = t["Ticker"].dropna().tolist()
                if len(tickers) > 50:
                    print(f"  NASDAQ100 (Wikipedia): {len(tickers)}銘柄")
                    return tickers
    except Exception as e:
        print(f"  NASDAQ100 Wikipedia失敗: {e}")
    return []

def fetch_russell2000():
    local_path = Path("russell2000.txt")
    if local_path.exists():
        raw = [t.strip() for t in local_path.read_text().splitlines() if t.strip()]
        tickers = [t for t in raw if 1 <= len(t.replace("-","")) <= 5 and t.replace("-","").isalpha()]
        if len(tickers) > 500:
            print(f"  Russell 2000 (russell2000.txt): {len(tickers)}銘柄")
            return tickers
    tickers = list(set(RUSSELL2000_FALLBACK))
    print(f"  Russell 2000 (ハードコード): {len(tickers)}銘柄")
    return tickers

def build_universe(mode="full"):
    tickers = set()
    if mode in ["full", "sp500"]:       tickers.update(fetch_sp500())
    if mode in ["full", "nasdaq"]:      tickers.update(fetch_nasdaq100())
    if mode in ["full", "russell2000"]: tickers.update(fetch_russell2000())
    exclude = {"SPY","QQQ","IWM","DIA","RSP","GLD","SLV","TLT","HYG","VXX",
               "IEMG","EFA","AGG","BND","VEA","VWO","IEFA","ITOT","IVV","VOO"}
    tickers -= exclude
    tickers = {t for t in tickers if len(t) <= 5 and t.replace("-","").isalpha()}
    result = list(tickers)
    random.shuffle(result)
    print(f"  ユニバース合計: {len(result)}銘柄")
    return result

# ============================================================
# 価格データ一括取得
# ============================================================
def fetch_prices(tickers: list, period="2y") -> dict:
    results = {}
    bs = CONFIG["batch_size"]
    for i in range(0, len(tickers), bs):
        batch = tickers[i:i+bs]
        n = min(i+bs, len(tickers))
        print(f"  価格取得: {i+1}〜{n}/{len(tickers)}...")
        try:
            raw = yf.download(
                batch, period=period, progress=False,
                auto_adjust=True, group_by="ticker", threads=True,
            )
            for t in batch:
                try:
                    df = raw[t].dropna(how="all") if len(batch) > 1 else raw.dropna(how="all")
                    if len(df) >= 130:
                        results[t] = df
                except Exception:
                    pass
        except Exception as e:
            print(f"  バッチエラー: {e}")
        time.sleep(1.5)
    return results

# ============================================================
# RS計算: percentrank(TICKER/SPY, period) — TradingView準拠
# ============================================================
def percentrank(series: pd.Series, period: int):
    if len(series) < period:
        return None
    window = series.iloc[-period:].values.astype(float)
    valid  = window[~np.isnan(window)]
    if len(valid) < 2:
        return None
    current = valid[-1]
    below   = int(np.sum(valid < current))
    return round(below / (period - 1) * 100, 2)

def calc_rs(ticker_df, spy_df) -> dict | None:
    try:
        tc = ticker_df["Close"].squeeze().dropna()
        sc = spy_df["Close"].squeeze().dropna()
        common = tc.index.intersection(sc.index)
        if len(common) < 130:
            return None
        ratio = (tc[common] / sc[common]).dropna()
        rs = {}
        for name, period in CONFIG["rs_periods"].items():
            rs[name] = percentrank(ratio, period)
        vals = [rs[k] for k in ["p2","p3","p4"] if rs.get(k) is not None]
        rs["avg"] = round(sum(vals)/len(vals), 2) if vals else None
        return rs
    except Exception:
        return None

# ============================================================
# Stage2 (Weinstein)
# ============================================================
def calc_stage2(df) -> dict | None:
    try:
        c = df["Close"].squeeze()
        if len(c) < 200:
            return None
        e21  = c.ewm(span=21, adjust=False).mean()
        s50  = c.rolling(50).mean()
        s150 = c.rolling(150).mean()
        s200 = c.rolling(200).mean()
        cl, v21, v50, v150, v200 = (
            float(c.iloc[-1]), float(e21.iloc[-1]),
            float(s50.iloc[-1]), float(s150.iloc[-1]), float(s200.iloc[-1])
        )
        if any(np.isnan([cl, v21, v50, v150, v200])):
            return None
        conds = [
            cl > v21, cl > v50, cl > v150, cl > v200,
            v50 > v150, v50 > v200, v150 > v200,
            v50  > float(s50.iloc[-10])  if len(s50.dropna())  >= 10 else False,
            v200 > float(s200.iloc[-20]) if len(s200.dropna()) >= 20 else False,
        ]
        h52 = float(c.tail(252).max())
        return {
            "score":         sum(conds),
            "close":         round(cl, 2),
            "pct_from_high": round((cl - h52) / h52 * 100, 1),
            "ema21":         round(v21, 2),
            "sma50":         round(v50, 2),
        }
    except Exception:
        return None

# ============================================================
# VCS (Volatility Contraction Score) — Pine Script完全再現
# ============================================================
def calc_vcs(df) -> dict:
    """
    高スコア(≥80)=良い、低スコア=ルーズ
    """
    try:
        c = df["Close"].squeeze()
        h = df["High"].squeeze()
        l = df["Low"].squeeze()
        v = df["Volume"].squeeze()
        if len(c) < 70:
            return {"score": 0, "vol_shrink": False}

        lenShort=13; lenLong=63; lenVol=50
        sensitivity=2.0; bonusMax=15
        penaltyFactor=0.75; trendPenaltyWeight=1.0; hlLookback=63

        tr = pd.concat([(h-l),(h-c.shift(1)).abs(),(l-c.shift(1)).abs()],axis=1).max(axis=1)
        trShort  = tr.rolling(lenShort).mean()
        trLong   = tr.rolling(lenLong).mean()
        ratioATR = trShort / trLong.clip(lower=1e-6)
        stdShort = c.rolling(lenShort).std()
        stdLong  = c.rolling(lenLong).std()
        ratioStd = stdShort / stdLong.clip(lower=1e-6)
        volAvg      = v.rolling(lenVol).mean()
        volShortAvg = v.rolling(5).mean()
        volRatio    = volShortAvg / volAvg.clip(lower=1.0)
        netChange   = (c - c.shift(lenShort)).abs()
        totalTravel = tr.rolling(lenShort).sum()
        efficiency  = netChange / totalTravel.clip(lower=1e-6)
        trendFactor = (1.0 - efficiency * trendPenaltyWeight).clip(lower=0.0)
        s_atr = (1.0-ratioATR).clip(lower=0.0)*sensitivity
        s_std = (1.0-ratioStd).clip(lower=0.0)*sensitivity
        s_vol = (1.0-volRatio).clip(lower=0.0)
        rawScore     = s_atr*0.4 + s_std*0.4 + s_vol*0.2
        physicsScore = (rawScore * trendFactor * 100).clip(upper=100)
        smoothPhysics= physicsScore.ewm(span=3, adjust=False).mean()
        lowRecent   = l.rolling(lenShort).min()
        lowBase     = l.shift(lenShort).rolling(hlLookback).min()
        isHigherLow = lowRecent >= lowBase.fillna(0)
        isTight   = smoothPhysics >= 70
        groups    = (isTight != isTight.shift()).cumsum()
        daysTight = isTight.groupby(groups).cumcount().where(isTight,0) + isTight.astype(int)
        daysTight = daysTight.where(isTight, 0)
        totalScore = smoothPhysics*0.85 + daysTight.clip(upper=bonusMax)
        finalScore = totalScore.where(isHigherLow, totalScore*penaltyFactor).fillna(0.0)

        return {
            "score":      round(float(finalScore.iloc[-1]), 1),
            "vol_shrink": bool(volRatio.iloc[-1] < 1.0),
        }
    except Exception:
        return {"score": 0, "vol_shrink": False}

def calc_adr(df, period=20) -> float | None:
    try:
        return round(float(((df["High"].squeeze()/df["Low"].squeeze()).tail(period).mean()-1)*100), 2)
    except Exception:
        return None

def calc_momentum(df) -> dict:
    try:
        c = df["Close"].squeeze().dropna()
        def ret(days):
            if len(c) < days + 1:
                return None
            return round((float(c.iloc[-1]) / float(c.iloc[-days]) - 1) * 100, 1)
        return {"m1": ret(21), "m3": ret(63), "m6": ret(126)}
    except Exception:
        return {"m1": None, "m3": None, "m6": None}

# ============================================================
# マクロ環境判定
# ============================================================
def assess_market(price_data: dict) -> dict:
    etfs = {}
    for t in ["SPY","QQQ","IWM"]:
        df = price_data.get(t)
        if df is None:
            continue
        c   = df["Close"].squeeze()
        e21 = c.ewm(span=21, adjust=False).mean()
        etfs[t] = {
            "above":  bool(c.iloc[-1] > e21.iloc[-1]),
            "rising": bool(e21.iloc[-1] > e21.iloc[-5]),
            "close":  round(float(c.iloc[-1]), 2),
            "ema21":  round(float(e21.iloc[-1]), 2),
        }
    vix_df = price_data.get("^VIX")
    vix = round(float(vix_df["Close"].iloc[-1]), 2) if vix_df is not None else 20.0
    bulls = sum(1 for v in etfs.values() if v["above"] and v["rising"])
    n = len(etfs)
    if vix > 30:                                    mode, icon = "全休止",          "🔴"
    elif vix > CONFIG["vix_half"]:                  mode, icon = "防御モード",       "🟠"
    elif bulls == n and vix <= CONFIG["vix_full"]:  mode, icon = "フルスロットル",   "🟢"
    elif bulls >= n // 2:                           mode, icon = "ハーフスロットル", "🟡"
    else:                                           mode, icon = "防御モード",       "🟠"
    return {"mode": mode, "icon": icon, "vix": vix, "etfs": etfs}

# ============================================================
# スクリーニング（純粋テクニカル）
# ============================================================
def run_screening(price_data: dict, spy_df) -> list:
    candidates = []
    universe_tickers = [t for t in price_data if t not in MACRO_TICKERS]

    cnt = {
        "total": len(universe_tickers),
        "price_filter": 0, "rs_fail": 0,
        "stage2_fail": 0, "high_fail": 0, "pass": 0
    }

    for ticker in universe_tickers:
        try:
            df = price_data[ticker]

            # ─ 株価フィルター（$10以上）─
            current_price = float(df["Close"].iloc[-1])
            if current_price < CONFIG["price_min"]:
                cnt["price_filter"] += 1
                continue

            # ─ RS ─
            rs = calc_rs(df, spy_df)
            if rs is None or (rs.get("p2") or 0) < CONFIG["rs_min"]:
                cnt["rs_fail"] += 1
                continue

            # ─ Stage2 ─
            s2 = calc_stage2(df)
            if s2 is None or s2["score"] < CONFIG["stage2_min"]:
                cnt["stage2_fail"] += 1
                continue
            if abs(s2["pct_from_high"]) > CONFIG["high_52w_pct_max"]:
                cnt["high_fail"] += 1
                continue

            # ─ 21EMA Low%（フィルターなし・表示・スコア用）─
            c_s   = df["Close"].squeeze()
            ema21_val = float(c_s.ewm(span=21, adjust=False).mean().iloc[-1])
            cl    = float(c_s.iloc[-1])
            ema_low_pct = round((cl - ema21_val) / ema21_val * 100, 2)

            # ─ ATR%50SMA（フィルターなし・表示用）─
            # (close - SMA50) × 100 / ATR14
            h_s   = df["High"].squeeze()
            l_s   = df["Low"].squeeze()
            tr    = pd.concat([(h_s-l_s),(h_s-c_s.shift(1)).abs(),(l_s-c_s.shift(1)).abs()],axis=1).max(axis=1)
            atr14 = float(tr.rolling(14).mean().iloc[-1])
            sma50_val = float(c_s.rolling(50).mean().iloc[-1])
            atr_sma50 = round((cl - sma50_val) * 100 / atr14, 2) if atr14 > 0 else None

            # ─ VCS / ADR / Momentum ─
            vcs  = calc_vcs(df)
            adr  = calc_adr(df)
            mom  = calc_momentum(df)

            # ─ テクニカルスコア（最大10点）─
            sc = 0
            rs_p2 = rs.get("p2") or 0
            if rs_p2 >= 90:   sc += 3
            elif rs_p2 >= 70: sc += 2
            elif rs_p2 >= 50: sc += 1
            if vcs["score"] >= CONFIG["vcs_best"]:    sc += 3   # ≥80: 高圧縮
            elif vcs["score"] >= CONFIG["vcs_entry"]: sc += 1   # ≥60: 収縮中
            if s2["score"] == 9:   sc += 2
            elif s2["score"] >= 7: sc += 1
            if ema_low_pct <= 3:   sc += 1  # EMA超タイト(3%以内)

            cnt["pass"] += 1
            candidates.append({
                "ticker":      ticker,
                "close":       s2["close"],
                "pct_high":    s2["pct_from_high"],
                "stage2":      s2["score"],
                "ema21":       s2["ema21"],
                "sma50":       s2["sma50"],
                "ema_low_pct": ema_low_pct,
                "atr_sma50":   atr_sma50,
                "rs_p1":       rs.get("p1"),
                "rs_p2":       rs.get("p2"),
                "rs_p3":       rs.get("p3"),
                "rs_p4":       rs.get("p4"),
                "rs_avg":      rs.get("avg"),
                "m1":          mom["m1"],
                "m3":          mom["m3"],
                "m6":          mom["m6"],
                "vcs":         vcs["score"],
                "vol_shrink":  vcs["vol_shrink"],
                "adr":         adr,
                "score":       sc,
                "stop":        round(s2["close"] * (1 - CONFIG["stop_loss_pct"]/100), 2),
                "tp1":         round(s2["close"] * 1.10, 2),
                "tp2":         round(s2["close"] * 1.20, 2),
            })
        except Exception:
            pass

    print(f"  [診断] 合計:{cnt['total']} | $10未満:{cnt['price_filter']} | "
          f"RS<50:{cnt['rs_fail']} | Stage2<5:{cnt['stage2_fail']} | "
          f"52W超:{cnt['high_fail']} | 通過:{cnt['pass']}")

    return sorted(candidates, key=lambda x: (x["score"], x["rs_p2"] or 0), reverse=True)

# ============================================================
# JSON出力
# ============================================================
def save_json(candidates, market, universe_size):
    out = candidates[:CONFIG["max_output"]]
    data = {
        "updated":       NOW.isoformat(),
        "updated_jst":   NOW.strftime("%Y-%m-%d %H:%M JST"),
        "universe_size": universe_size,
        "total_found":   len(candidates),
        "rs_formula":    "percentrank(TICKER/SPY daily close, period)",
        "rs_periods":    CONFIG["rs_periods"],
        "market":        market,
        "stocks":        out,
    }
    path = DOCS / "data.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    print(f"  → docs/data.json 出力 ({len(out)}銘柄 / 候補{len(candidates)}銘柄)")
    return data

def save_html(json_data=None):
    index_path = Path("docs") / "index.html"
    if index_path.exists():
        print(f"  → docs/index.html は既存ファイルを使用")
    else:
        print(f"  ⚠️ docs/index.html が見つかりません")

# ============================================================
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="full",
                   choices=["full","sp500","nasdaq","russell2000"])
    args = p.parse_args()

    print(f"\n{'='*60}")
    print(f"Stock Screener v5.0 — Pure Technical")
    print(f"RS = percentrank(TICKER/SPY, period) — TradingView準拠")
    print(f"{NOW.strftime('%Y-%m-%d %H:%M JST')} | mode={args.mode}")
    print(f"{'='*60}\n")

    print("📋 Step1: ユニバース構築...")
    universe = build_universe(args.mode)

    print(f"\n💹 Step2: 価格データ取得（2年分）...")
    price_data = fetch_prices(MACRO_TICKERS, period="2y")
    price_data.update(fetch_prices(universe, period=CONFIG["price_period"]))

    spy_df = price_data.get("SPY")
    if spy_df is None:
        print("❌ SPYデータ取得失敗")
        return

    print(f"  価格データ取得完了: {len(price_data) - len(MACRO_TICKERS)}銘柄")

    print(f"\n🌐 Step3: マクロ環境判定...")
    market = assess_market(price_data)
    print(f"  {market['icon']} {market['mode']} | VIX: {market['vix']}")

    print(f"\n🔍 Step4: テクニカルスクリーニング...")
    candidates = run_screening(price_data, spy_df)
    print(f"  候補: {len(candidates)}銘柄")

    print(f"\n💾 Step5: JSON出力...")
    json_data = save_json(candidates, market, len(universe))
    save_html(json_data)

    print(f"\n✅ 完了 | {len(candidates)}候補 / {len(universe)}銘柄スキャン")
    print(f"   🌐 {NOW.strftime('%Y-%m-%d')} | https://choco00chip.github.io/a9XvQ2lR7nP4k/\n")

if __name__ == "__main__":
    main()
