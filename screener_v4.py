"""
Stock Screener v4.0 - Accurate RS (percentrank)
=================================================
RS計算式: ta.percentrank(TICKER/SPY の日次終値比率, period)
TradingViewのCockpit v2と同一ロジック

出力: docs/data.json (GitHub Pages経由でReactアプリが取得)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import time
import warnings
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

JST  = timezone(timedelta(hours=9))
NOW  = datetime.now(JST)
DOCS = Path("docs")
DOCS.mkdir(exist_ok=True)

CONFIG = {
    "rs_periods": {"p1": 5, "p2": 21, "p3": 63, "p4": 126},
    "rs_benchmark":      "SPY",
    "market_cap_min":    5e8,
    "eps_growth_min":    20,
    "revenue_growth_min": 15,
    "roe_min":           12,
    "vcs_entry":         60,
    "vcs_best":          40,
    "high_52w_pct_max":  35,
    "vix_full":          20,
    "vix_half":          25,
    "stop_loss_pct":     7.0,
    "max_workers":       6,
    "batch_size":        50,
    "price_period":      "1y",
    "max_output":        100,
}

MACRO_TICKERS = ["SPY", "QQQ", "IWM", "^VIX"]

# ============================================================
# ユニバース取得（複数フォールバック付き）
# ============================================================

# S&P500 完全ハードコードリスト（Wikipediaが取得できない時の最終手段）
SP500_FALLBACK = [
    "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB","AKAM","ALB","ARE",
    "ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN","AMCR","AEE","AAL","AEP","AXP","AIG",
    "AMT","AWK","AMP","AME","AMGN","APH","ADI","ANSS","AON","APA","AAPL","AMAT","APTV","ACGL",
    "ADM","ANET","AJG","AIZ","T","ATO","ADSK","ADP","AZO","AVB","AVY","AXON","BKR","BALL","BAC",
    "BK","BBWI","BAX","BDX","BRK-B","BBY","BIO","TECH","BIIB","BLK","BX","BA","BCR","BMY","AVGO",
    "BR","BRO","BLDR","BF-B","BURL","CHRW","CZR","CPT","CPB","COF","CAH","KMX","CCL","CARR","CTLT",
    "CAT","CBOE","CBRE","CDW","CE","COR","CNC","CNP","CF","CRL","SCHW","CHTR","CVX","CMG","CB",
    "CHD","CI","CINF","CTAS","CSCO","C","CFG","CLX","CME","CMS","KO","CTSH","CL","CMCSA","CMA",
    "CAG","COP","ED","STZ","CEG","COO","CPRT","GLW","CPAY","CTVA","CSGP","COST","CTRA","CCI",
    "CSX","CMI","CVS","DHR","DRI","DVA","DAY","DECK","DE","DELL","DAL","DVN","DXCM","FANG","DLR",
    "DFS","DG","DLTR","D","DPZ","DOV","DOW","DHI","DTE","DUK","DD","EMN","ETN","EBAY","ECL","EIX",
    "EW","EA","ELV","LLY","EMR","ENPH","ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ESS","EL",
    "ETSY","EG","EVRST","ES","EXC","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO","FAST","FRT",
    "FDX","FIS","FITB","FSLR","FE","FI","FMC","F","FTNT","FTV","FOXA","FOX","BEN","FCX","GRMN",
    "IT","GE","GEHC","GEN","GPC","GILD","GIS","GM","GPC","GNRC","GD","GS","GWW","HAL","HIG","HAS",
    "HCA","DOC","HSIC","HSY","HES","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ","HUBB",
    "HUM","HBAN","HII","IBM","IEX","IDXX","ITW","INCY","IR","PODD","INTC","ICE","IFF","IP","IPG",
    "INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT","JBL","JKHY","J","JNJ","JCI","JPM","JNPR","K",
    "KVUE","KDP","KEY","KEYS","KMB","KIM","KMI","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS",
    "LDOS","LEN","LNC","LIN","LYV","LKQ","LMT","L","LOW","LULU","LYB","MTB","MPC","MKTX","MAR",
    "MMC","MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK","META","MET","MTD","MGM","MCHP",
    "MU","MSFT","MAA","MRNA","MHK","MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI",
    "NDAQ","NTAP","NFLX","NWL","NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC",
    "NCLH","NRG","NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS",
    "PCAR","PKG","PANW","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PXD",
    "PNC","POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PTC","PSA","PHM","QRVO","PWR",
    "QCOM","DGX","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY","ROK","ROL","ROP",
    "ROST","RCL","SPGI","CRM","SBAC","SLB","STX","SRE","NOW","SHW","SPG","SWKS","SJM","SNA",
    "SOLV","SO","LUV","SWK","SBUX","STT","STLD","STE","SYK","SYF","SNPS","SYY","TMUS","TROW",
    "TTWO","TPR","TRGP","TGT","TEL","TDY","TFX","TER","TSLA","TXN","TXT","TMO","TJX","TSCO",
    "TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UDR","ULTA","UNP","UAL","UPS","URI","UNH",
    "UHS","VLO","VTR","VLTO","VRSN","VRSK","VZ","VRTX","VTRS","VICI","V","VST","WAB","WBA","WMT",
    "DIS","WBD","WM","WAT","WEC","WFC","WELL","WST","WDC","WRK","WY","WHR","WMB","WTW","GWW",
    "WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZTS",
    # NASDAQ100追加
    "ADSK","ALGN","ALXN","ANSS","ASML","ATVI","BIDU","BIIB","CDNS","CERN","CHKP","CHTR","CMCSA",
    "CPRT","CRWD","CSGP","CTSH","DLTR","DXCM","EA","EBAY","ENPH","EXC","FAST","FISV","GILD",
    "IDXX","ILMN","INCY","INTC","INTU","ISRG","JD","KDP","KLAC","LCID","LRCX","LULU","MAR",
    "MELI","MNST","MRNA","MSFT","MU","NFLX","NTES","NVDA","NXPI","OKTA","ORLY","PANW","PAYX",
    "PCAR","PDD","PYPL","QCOM","REGN","ROST","SBUX","SGEN","SIRI","SNPS","SPLK","TCOM","TEAM",
    "TMUS","TSLA","TXN","VRSK","VRSN","VRTX","WBA","WDAY","XEL","ZM","ZS","ABNB","DDOG","HOOD",
    "LCID","RIVN","SNOW","UBER","COIN","PLTR","APP","TTD","AXON","CAVA","ONON","EME",
]

import requests as req_lib

def fetch_sp500():
    """S&P500銘柄リスト取得（3段階フォールバック）"""

    # 方法1: requests + User-Agent偽装
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }
        resp = req_lib.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=30
        )
        tables = pd.read_html(resp.text)
        tickers = [t.replace(".", "-") for t in tables[0]["Symbol"].tolist()]
        if len(tickers) > 400:
            print(f"  S&P500 (Wikipedia): {len(tickers)}銘柄")
            return tickers
    except Exception as e:
        print(f"  Wikipedia方法1失敗: {e}")

    # 方法2: pandas直接
    try:
        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            storage_options={"User-Agent": "Mozilla/5.0"}
        )
        tickers = [t.replace(".", "-") for t in tables[0]["Symbol"].tolist()]
        if len(tickers) > 400:
            print(f"  S&P500 (pandas): {len(tickers)}銘柄")
            return tickers
    except Exception as e:
        print(f"  Wikipedia方法2失敗: {e}")

    # 方法3: ハードコードリスト（最終手段）
    tickers = list(set(SP500_FALLBACK))
    print(f"  S&P500 (ハードコード): {len(tickers)}銘柄")
    return tickers

def fetch_nasdaq100():
    """NASDAQ100銘柄リスト取得"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = req_lib.get(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            headers=headers, timeout=30
        )
        tables = pd.read_html(resp.text)
        for t in tables:
            if "Ticker" in t.columns:
                tickers = t["Ticker"].dropna().tolist()
                if len(tickers) > 50:
                    print(f"  NASDAQ100: {len(tickers)}銘柄")
                    return tickers
    except Exception as e:
        print(f"  NASDAQ100取得失敗: {e}")
    return []

def fetch_russell2000():
    """Russell 2000銘柄リスト取得（iShares IWM ETF経由）"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        # iShares IWM の保有銘柄CSVを取得
        url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?tab=all&fileType=csv"
        resp = req_lib.get(url, headers=headers, timeout=30)
        from io import StringIO
        # 最初の数行はメタデータなのでスキップ
        lines = resp.text.split("\n")
        start = next(i for i, l in enumerate(lines) if "Ticker" in l or "ISIN" in l)
        df = pd.read_csv(StringIO("\n".join(lines[start:])))
        col = [c for c in df.columns if "Ticker" in c or "ticker" in c][0]
        tickers = df[col].dropna().tolist()
        tickers = [t.strip().replace(".", "-") for t in tickers if isinstance(t, str) and len(t) <= 6 and t.strip().isalpha()]
        if len(tickers) > 500:
            print(f"  Russell 2000: {len(tickers)}銘柄")
            return tickers
    except Exception as e:
        print(f"  iShares取得失敗: {e}")

    # フォールバック: Wikipedia
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = req_lib.get("https://en.wikipedia.org/wiki/Russell_2000_Index", headers=headers, timeout=30)
        tables = pd.read_html(resp.text)
        for t in tables:
            cols = [c for c in t.columns if "ticker" in str(c).lower() or "symbol" in str(c).lower()]
            if cols and len(t) > 100:
                tickers = t[cols[0]].dropna().tolist()
                tickers = [str(t).strip().replace(".", "-") for t in tickers]
                print(f"  Russell 2000 (Wikipedia): {len(tickers)}銘柄")
                return tickers
    except Exception as e:
        print(f"  Wikipedia Russell 2000失敗: {e}")

    print("  Russell 2000取得失敗 → S&P500のみ使用")
    return []

def build_universe(mode="full"):
    tickers = set()
    if mode in ["full", "sp500"]:
        tickers.update(fetch_sp500())
    if mode in ["full", "nasdaq"]:
        tickers.update(fetch_nasdaq100())
    if mode in ["full", "russell2000"]:
        tickers.update(fetch_russell2000())
    exclude = {"SPY","QQQ","IWM","DIA","RSP","GLD","SLV","TLT","HYG","VXX",
               "IEMG","EFA","AGG","BND","VEA","VWO","IEFA","ITOT","IVV","VOO"}
    tickers -= exclude
    # 小型株フィルター: 記号が長すぎるもの・数字含むものを除外
    tickers = {t for t in tickers if len(t) <= 5 and t.replace("-","").isalpha()}
    result = sorted(list(tickers))
    print(f"  ユニバース合計: {len(result)}銘柄")
    return result

# ============================================================
# 価格データ一括取得
# ============================================================
def fetch_prices(tickers: list, period="1y") -> dict:
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
# RS計算: percentrank(TICKER/SPY, period)
# Pine Script: ta.percentrank(source, length)
# = 過去length本のうち、現在値より「小さい」値の数 / (length-1) * 100
# ============================================================
def percentrank(series: pd.Series, period: int):
    """
    TradingView ta.percentrank の完全再現。
    - window[-1] が現在値
    - (length-1) で割る（Pine Scriptの定義）
    """
    if len(series) < period:
        return None
    window = series.iloc[-period:].values.astype(float)
    valid  = window[~np.isnan(window)]
    if len(valid) < 2:
        return None
    current = valid[-1]
    below   = int(np.sum(valid < current))
    return round(below / (period - 1) * 100, 2)

def calc_rs(ticker_df: pd.DataFrame, spy_df: pd.DataFrame) -> dict | None:
    """
    全ピリオドのRS値を計算。
    ratio = TICKER日次終値 / SPY日次終値
    RS_P2 = percentrank(ratio, 21)
    """
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

        # Avg = (p2 + p3 + p4) / 3  ← Pine Scriptデフォルト m1,m3,m6 表示に対応
        vals = [rs[k] for k in ["p2","p3","p4"] if rs.get(k) is not None]
        rs["avg"] = round(sum(vals) / len(vals), 2) if vals else None
        return rs
    except Exception:
        return None

# ============================================================
# テクニカル指標
# ============================================================
def calc_stage2(df: pd.DataFrame) -> dict | None:
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
            "score": sum(conds),
            "close": round(cl, 2),
            "pct_from_high": round((cl - h52) / h52 * 100, 1),
            "ema21": round(v21, 2),
            "sma50": round(v50, 2),
        }
    except Exception:
        return None

def calc_vcs(df: pd.DataFrame) -> dict:
    try:
        c, h, l, v = (
            df["Close"].squeeze(), df["High"].squeeze(),
            df["Low"].squeeze(), df["Volume"].squeeze()
        )
        tr    = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr   = tr.rolling(14).mean()
        apct  = (atr / c) * 100
        score = round(float(apct.iloc[-1]) / float(apct.tail(252).mean()) * 100, 1)
        return {"score": score, "vol_shrink": bool(v.tail(5).mean() < v.tail(20).mean())}
    except Exception:
        return {"score": 100, "vol_shrink": False}

def calc_adr(df: pd.DataFrame, period=20) -> float | None:
    try:
        return round(float(((df["High"].squeeze() / df["Low"].squeeze()).tail(period).mean() - 1) * 100), 2)
    except Exception:
        return None

# ============================================================
# ファンダメンタル並列取得
# ============================================================
def fetch_fundamentals(tickers: list) -> dict:
    def _get(t):
        try:
            info = yf.Ticker(t).info
            return t, {
                "market_cap":      info.get("marketCap", 0) or 0,
                "market_cap_b":    round((info.get("marketCap") or 0) / 1e9, 1),
                "roe":             round((info.get("returnOnEquity") or 0) * 100, 1),
                "revenue_growth":  round((info.get("revenueGrowth") or 0) * 100, 1),
                "earnings_growth": round((info.get("earningsGrowth") or 0) * 100, 1),
                "profit_margin":   round((info.get("profitMargins") or 0) * 100, 1),
                "sector":          info.get("sector", "Unknown"),
                "industry":        info.get("industry", "Unknown"),
                "name":            info.get("shortName", t),
            }
        except Exception:
            return t, None

    print(f"  ファンダメンタル並列取得: {len(tickers)}銘柄...")
    results = {}
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as ex:
        futures = {ex.submit(_get, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            t, data = f.result()
            if data:
                results[t] = data
            done += 1
            if done % 50 == 0:
                print(f"    {done}/{len(tickers)}完了")
            time.sleep(0.15)
    return results

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
    if vix > 30:                                   mode, icon = "全休止",          "🔴"
    elif vix > CONFIG["vix_half"]:                 mode, icon = "防御モード",       "🟠"
    elif bulls == n and vix <= CONFIG["vix_full"]: mode, icon = "フルスロットル",   "🟢"
    elif bulls >= n // 2:                          mode, icon = "ハーフスロットル", "🟡"
    else:                                          mode, icon = "防御モード",       "🟠"
    return {"mode": mode, "icon": icon, "vix": vix, "etfs": etfs}

# ============================================================
# スクリーニング
# ============================================================
def run_screening(price_data, funda_data, spy_df) -> list:
    candidates = []
    universe_tickers = [t for t in price_data if t not in MACRO_TICKERS]

    for ticker in universe_tickers:
        try:
            df = price_data[ticker]
            f  = funda_data.get(ticker)
            if f is None:
                continue

            # ─ RS（実計算）─
            rs = calc_rs(df, spy_df)
            if rs is None or (rs.get("p2") or 0) < 50:
                continue

            # ─ Stage2 ─
            s2 = calc_stage2(df)
            if s2 is None or s2["score"] < 7:
                continue
            if abs(s2["pct_from_high"]) > CONFIG["high_52w_pct_max"]:
                continue

            # ─ VCS / ADR ─
            vcs = calc_vcs(df)
            adr = calc_adr(df)

            # ─ スコア ─
            sc = 0
            if f["earnings_growth"] >= CONFIG["eps_growth_min"]:       sc += 2
            if f["revenue_growth"]  >= CONFIG["revenue_growth_min"]:   sc += 2
            if f["roe"]             >= CONFIG["roe_min"]:               sc += 1
            if (rs.get("p2") or 0) >= 80:                              sc += 2
            if vcs["score"]         <= CONFIG["vcs_entry"]:             sc += 2
            if vcs["score"]         <= CONFIG["vcs_best"]:              sc += 1
            if s2["score"]          >= 8:                               sc += 1

            candidates.append({
                "ticker":     ticker,
                "name":       f["name"],
                "sector":     f["sector"],
                "industry":   f["industry"],
                "market_cap_b": f["market_cap_b"],
                "close":      s2["close"],
                "pct_high":   s2["pct_from_high"],
                "stage2":     s2["score"],
                "ema21":      s2["ema21"],
                "sma50":      s2["sma50"],
                # ─ RS（TradingView準拠の実計算値）─
                "rs_p1":      rs.get("p1"),
                "rs_p2":      rs.get("p2"),
                "rs_p3":      rs.get("p3"),
                "rs_p4":      rs.get("p4"),
                "rs_avg":     rs.get("avg"),
                # ─ ファンダ ─
                "eps":        f["earnings_growth"],
                "rev":        f["revenue_growth"],
                "roe":        f["roe"],
                "margin":     f["profit_margin"],
                # ─ テクニカル ─
                "vcs":        vcs["score"],
                "vol_shrink": vcs["vol_shrink"],
                "adr":        adr,
                "score":      sc,
                # ─ トレード管理 ─
                "stop":       round(s2["close"] * (1 - CONFIG["stop_loss_pct"] / 100), 2),
                "tp1":        round(s2["close"] * 1.10, 2),
                "tp2":        round(s2["close"] * 1.20, 2),
            })
        except Exception:
            pass

    return sorted(candidates, key=lambda x: (x["score"], x["rs_p2"] or 0), reverse=True)

# ============================================================
# JSON出力
# ============================================================
def save_json(candidates, market, universe_size):
    data = {
        "updated":       NOW.isoformat(),
        "updated_jst":   NOW.strftime("%Y-%m-%d %H:%M JST"),
        "universe_size": universe_size,
        "total_found":   len(candidates),
        "rs_formula":    "percentrank(TICKER/SPY daily close, period) — TradingView準拠",
        "rs_periods":    CONFIG["rs_periods"],
        "market":        market,
        "stocks":        candidates[:CONFIG["max_output"]],
    }
    path = DOCS / "data.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    print(f"  → docs/data.json 出力 ({len(candidates[:CONFIG['max_output']])}銘柄)")
    return data

# ============================================================
# HTML アプリ生成（docs/index.html）
# data.jsonと同一ドメインなのでCORSなし・完全自動
# ============================================================
def save_html(json_data):
    data_js = json.dumps(json_data, ensure_ascii=False, default=str)
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Stock Screener — oratnek</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/react/18.2.0/umd/react.production.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/react-dom/18.2.0/umd/react-dom.production.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#060a10;color:#ddeeff;font-family:monospace;min-height:100vh}}
::-webkit-scrollbar{{width:3px;height:3px}}
::-webkit-scrollbar-thumb{{background:#1a2840;border-radius:2px}}
.row:hover{{background:rgba(0,180,255,0.06)!important;cursor:pointer}}
input[type=range]{{accent-color:#00e5ff;cursor:pointer;width:100%}}
button{{cursor:pointer}}
</style>
</head>
<body>
<div id="root"></div>
<script>
const RAW_DATA = {data_js};

const {{useState, useMemo, useCallback}} = React;

const rsCol = v => v>=90?"#00ff88":v>=80?"#7dff9e":v>=70?"#f0d060":v>=55?"#ff9933":"#ff4444";
const numCol = v => v>2?"#00e676":v>0?"#69f0ae":v<-2?"#ff5252":v<0?"#ff8a80":"#607d8b";
const fmt = (v,d=1) => v==null?"—":`${{v>0?"+":""}}${{Number(v).toFixed(d)}}%`;
const fmtRS = v => v==null?"—":Number(v).toFixed(1);
const SCORE_DEF = [
  {{min:9,label:"◎ 最有力",col:"#00ff88"}},
  {{min:7,label:"○ 有力",col:"#7dff9e"}},
  {{min:5,label:"△ 候補",col:"#f0d060"}},
  {{min:0,label:"✕",col:"#4a6a88"}},
];
const TD = {{padding:"7px 9px",borderBottom:"1px solid rgba(14,28,48,.6)",whiteSpace:"nowrap"}};

function ScoreBadge({{score}}){{
  const d = SCORE_DEF.find(x=>score>=x.min)||SCORE_DEF[3];
  return React.createElement("span",{{style:{{
    fontSize:10,padding:"2px 7px",borderRadius:4,fontWeight:700,
    background:`${{d.col}}18`,color:d.col,border:`1px solid ${{d.col}}40`
  }}}},d.label);
}}

function RSCell({{val,isSort}}){{
  if(val==null) return React.createElement("td",{{style:{{...TD,textAlign:"right",color:"#2a4a6a"}}}},"—");
  const col=rsCol(val);
  return React.createElement("td",{{style:{{...TD,textAlign:"right"}}}},
    React.createElement("span",{{style:{{
      color:col,fontWeight:val>=80?700:400,
      background:isSort?`${{col}}14`:"transparent",
      borderRadius:3,padding:isSort?"1px 5px":0
    }}}},fmtRS(val))
  );
}}

function App(){{
  const [sortK,setSortK]=useState("rs_p2");
  const [sortD,setSortD]=useState("desc");
  const [search,setSearch]=useState("");
  const [minRS,setMinRS]=useState(0);
  const [minEPS,setMinEPS]=useState(-999);
  const [maxVCS,setMaxVCS]=useState(100);
  const [minStage,setMinStage]=useState(0);
  const [selected,setSelected]=useState(null);
  const [showP1,setShowP1]=useState(false);
  const [showP2,setShowP2]=useState(true);
  const [showP3,setShowP3]=useState(true);
  const [showP4,setShowP4]=useState(true);
  const [showAvg,setShowAvg]=useState(true);

  const stocks = RAW_DATA.stocks||[];

  const rows = useMemo(()=>{{
    return stocks.filter(s=>{{
      if(search){{const q=search.toUpperCase();if(!s.ticker?.includes(q)&&!s.name?.toUpperCase().includes(q)&&!s.sector?.toUpperCase().includes(q))return false;}}
      if((s.rs_p2??0)<minRS)return false;
      if((s.eps??0)<minEPS)return false;
      if((s.vcs??100)>maxVCS)return false;
      if((s.stage2??0)<minStage)return false;
      return true;
    }}).sort((a,b)=>{{
      const av=a[sortK]??-999,bv=b[sortK]??-999;
      return sortD==="desc"?bv-av:av-bv;
    }});
  }},[search,sortK,sortD,minRS,minEPS,maxVCS,minStage]);

  const handleSort=useCallback(k=>{{
    if(k===sortK)setSortD(d=>d==="desc"?"asc":"desc");
    else{{setSortK(k);setSortD("desc");}}
  }},[sortK]);

    const top=[...rows].sort((a,b)=>(b.score??0)-(a.score??0)).slice(0,8);
    const mkt=RAW_DATA.market;
    const prompt=`米国株スイングトレードの専門アナリストとして分析してください。
市場環境: ${{mkt?.icon}} ${{mkt?.mode}} | VIX: ${{mkt?.vix}}
RS計算: percentrank(TICKER/SPY日次終値, period) TradingView準拠
スコア上位銘柄:
${{top.map(d=>`${{d.ticker}}: RS21=${{fmtRS(d.rs_p2)}} RS63=${{fmtRS(d.rs_p3)}} | EPS=${{fmt(d.eps)}} | Stage2=${{d.stage2}}/9 | VCS=${{d.vcs}}`).join("\\n")}}
日本語200字以内で: 1.最注目銘柄と理由 2.セクター傾向 3.今週の戦略`;
    try{{
      const res=await fetch("https://api.anthropic.com/v1/messages",{{
        method:"POST",headers:{{"Content-Type":"application/json"}},
        body:JSON.stringify({{model:"claude-sonnet-4-20250514",max_tokens:1000,
          messages:[{{role:"user",content:prompt}}]}})
      }});
      const j=await res.json();
  }},[rows]);

  const rsCols=[
    {{k:"rs_p1",label:"RS5",  show:showP1,set:setShowP1}},
    {{k:"rs_p2",label:"RS21★",show:showP2,set:setShowP2}},
    {{k:"rs_p3",label:"RS63", show:showP3,set:setShowP3}},
    {{k:"rs_p4",label:"RS126",show:showP4,set:setShowP4}},
    {{k:"rs_avg",label:"Avg", show:showAvg,set:setShowAvg}},
  ].filter(c=>c.show);

  const market=RAW_DATA.market;
  const topN=rows.filter(r=>(r.score??0)>=9).length;
  const midN=rows.filter(r=>(r.score??0)>=7&&(r.score??0)<9).length;
  const vcsN=rows.filter(r=>(r.vcs??100)<=40).length;

  const e=React.createElement;

  return e("div",{{style:{{background:"#060a10",minHeight:"100vh",fontFamily:"monospace",color:"#ddeeff"}}}},
    // TOP BAR
    e("div",{{style:{{position:"sticky",top:0,zIndex:100,background:"rgba(6,10,16,.97)",
      borderBottom:"1px solid #0e1c30",padding:"9px 14px",
      display:"flex",alignItems:"center",gap:10,flexWrap:"wrap"}}}},
      e("div",{{style:{{fontFamily:"sans-serif",fontSize:18,fontWeight:800}}}},
        "Stock",e("span",{{style:{{color:"#00e5ff"}}}},"Screen")),
      e("div",{{style:{{fontSize:9,color:"#2a4a6a",lineHeight:1.6}}}},
        e("div",{{style:{{color:"#00e5ff"}}}},RAW_DATA.updated_jst),
        e("div",null,`${{RAW_DATA.universe_size}}銘柄 → ${{RAW_DATA.total_found}}候補`)),
      e("input",{{value:search,onChange:ev=>setSearch(ev.target.value),
        placeholder:"Ticker / 銘柄名 / セクター",
        style:{{background:"#0a1320",border:"1px solid #0e1c30",borderRadius:8,
          padding:"5px 11px",color:"#ddeeff",fontFamily:"monospace",fontSize:12,width:168,outline:"none"}}}}),
      e("div",{{style:{{display:"flex",gap:3,alignItems:"center"}}}},
        e("span",{{style:{{fontSize:9,color:"#2a4a6a"}}}},"RS:"),
        ...[["5d",showP1,setShowP1],["21d★",showP2,setShowP2],["63d",showP3,setShowP3],["126d",showP4,setShowP4],["Avg",showAvg,setShowAvg]].map(
          ([l,v,s])=>e("button",{{key:l,onClick:()=>s(x=>!x),style:{{
            background:v?"rgba(0,229,255,.15)":"#0a1320",
            border:`1px solid ${{v?"rgba(0,229,255,.4)":"#0e1c30"}}`,
            borderRadius:6,padding:"3px 8px",color:v?"#00e5ff":"#2a4a6a",
            fontFamily:"monospace",fontSize:9,
          }}}},l)
        )
      ),
      e("button",{{onClick:()=>window.location.reload(),
        style:{{background:"#0a1320",border:"1px solid #0e1c30",borderRadius:8,
          padding:"5px 11px",color:"#4a6a88",fontSize:11}}}},
        "🔄 最新に更新"),
        marginLeft:"auto",
        border:"none",borderRadius:8,padding:"7px 14px",
        fontFamily:"sans-serif",fontWeight:800,fontSize:12,
    ),

    e("div",{{style:{{display:"flex",maxWidth:1700,margin:"0 auto"}}}},
      // SIDEBAR
      e("div",{{style:{{width:180,flexShrink:0,padding:"14px 11px",
        borderRight:"1px solid #0e1c30",display:"flex",flexDirection:"column",gap:14}}}},
        e("div",{{style:{{fontSize:9,color:"#1e3450",letterSpacing:".12em",textTransform:"uppercase"}}}},"フィルター"),
        ...[
          {{label:"RS P2 最低", val:minRS,   set:setMinRS,   min:0,  max:99, step:5,  d:v=>`${{v}}+`}},
          {{label:"EPS 最低%",  val:minEPS,  set:setMinEPS,  min:-50,max:200,step:10, d:v=>`${{v}}%+`}},
          {{label:"VCS 最高",   val:maxVCS,  set:setMaxVCS,  min:30, max:100,step:5,  d:v=>`〜${{v}}`}},
          {{label:"Stage2 最低",val:minStage,set:setMinStage,min:0,  max:9,  step:1,  d:v=>`${{v}}/9+`}},
        ].map(f=>e("div",{{key:f.label}},
          e("div",{{style:{{display:"flex",justifyContent:"space-between",marginBottom:4}}}},
            e("span",{{style:{{fontSize:10,color:"#4a6a88"}}}},f.label),
            e("span",{{style:{{fontSize:10,color:"#00e5ff",fontWeight:700}}}},f.d(f.val))
          ),
          e("input",{{type:"range",min:f.min,max:f.max,step:f.step,value:f.val,
            onChange:ev=>f.set(+ev.target.value)}})
        )),
        e("button",{{onClick:()=>{{setMinRS(0);setMinEPS(-999);setMaxVCS(100);setMinStage(0);setSearch("");}},
          style:{{background:"none",border:"1px solid #0e1c30",borderRadius:6,
            color:"#4a6a88",padding:"5px 0",fontSize:11}}}},
          "リセット"),
        market&&e("div",{{style:{{borderTop:"1px solid #0e1c30",paddingTop:12}}}},
          e("div",{{style:{{fontSize:9,color:"#1e3450",textTransform:"uppercase",marginBottom:8}}}},"マクロ"),
          e("div",{{style:{{fontSize:13,fontWeight:700,color:"#ddeeff",marginBottom:6}}}},`${{market.icon}} ${{market.mode}}`),
          e("div",{{style:{{fontSize:11,color:"#f0d060",marginBottom:8}}}},`VIX: ${{market.vix}}`),
          ...Object.entries(market.etfs||{{}}).map(([etf,d])=>
            e("div",{{key:etf,style:{{fontSize:10,color:d.above&&d.rising?"#00e676":"#ff5252",marginBottom:3}}}},
              `${{d.above&&d.rising?"🟢":"🔴"}} ${{etf}} $${{d.close}}`))
        ),
        e("div",{{style:{{borderTop:"1px solid #0e1c30",paddingTop:10,fontSize:9,color:"#2a4a6a",lineHeight:2}}}},
          e("div",{{style:{{color:"#4a6a88",marginBottom:4}}}},"スコア"),
          "EPS≥25% +2",e("br"),"売上≥15% +2",e("br"),"ROE≥12% +1",e("br"),
          "RS≥80 +2",e("br"),"VCS≤60 +2",e("br"),"VCS≤40 +1",e("br"),"Stage2≥8 +1"
        )
      ),

      // MAIN
      e("div",{{style:{{flex:1,padding:"12px 13px",minWidth:0}}}},
        // スタッツ
        e("div",{{style:{{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8,marginBottom:10}}}},
          ...[ {{label:"表示銘柄",val:rows.length,col:"#00e5ff"}},
               {{label:"◎ 最有力",val:topN,col:"#00ff88"}},
               {{label:"○ 有力",val:midN,col:"#7dff9e"}},
               {{label:"VCS≤40",val:vcsN,col:"#ff4444"}} ].map(s=>
            e("div",{{key:s.label,style:{{background:"#0a1320",border:"1px solid #0e1c30",borderRadius:10,padding:"9px 12px"}}}},
              e("div",{{style:{{fontSize:9,color:"#2a4a6a",textTransform:"uppercase",marginBottom:3}}}},s.label),
              e("div",{{style:{{fontSize:22,fontWeight:500,color:s.col,fontFamily:"sans-serif"}}}},s.val)
            )
          )
        ),

        selected&&e("div",{{style:{{background:"rgba(0,255,140,.04)",border:"1px solid rgba(0,255,140,.18)",
          borderRadius:12,padding:"12px 16px",marginBottom:10}}}},
          e("div",{{style:{{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:10}}}},
            e("div",{{style:{{display:"flex",alignItems:"center",gap:10}}}},
              e("span",{{style:{{fontFamily:"sans-serif",fontSize:22,fontWeight:800,color:"#00ff88"}}}},selected.ticker),
              e("span",{{style:{{color:"#4a6a88",fontSize:12}}}},selected.name),
              e(ScoreBadge,{{score:selected.score??0}})
            ),
            e("button",{{onClick:()=>setSelected(null),
              style:{{background:"none",border:"none",color:"#4a6a88",fontSize:16}}}},"✕")
          ),
          e("div",{{style:{{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:6}}}},
            ...[ {{l:"RS P1(5日)",v:fmtRS(selected.rs_p1),col:rsCol(selected.rs_p1??0)}},
                 {{l:"RS P2(21日)★",v:fmtRS(selected.rs_p2),col:rsCol(selected.rs_p2??0)}},
                 {{l:"RS P3(63日)",v:fmtRS(selected.rs_p3),col:rsCol(selected.rs_p3??0)}},
                 {{l:"RS P4(126日)",v:fmtRS(selected.rs_p4),col:rsCol(selected.rs_p4??0)}},
                 {{l:"RS Avg",v:fmtRS(selected.rs_avg),col:rsCol(selected.rs_avg??0)}},
                 {{l:"EPS成長",v:fmt(selected.eps),col:numCol(selected.eps??0)}},
                 {{l:"売上成長",v:fmt(selected.rev),col:numCol(selected.rev??0)}},
                 {{l:"ROE",v:`${{selected.roe??'—'}}%`,col:(selected.roe??0)>=12?"#00e676":"#ff8a80"}},
                 {{l:"Stage2",v:`${{selected.stage2}}/9`,col:(selected.stage2??0)>=8?"#00e5ff":"#f0d060"}},
                 {{l:"VCS",v:String(selected.vcs??'—'),col:(selected.vcs??100)<=40?"#ff4444":(selected.vcs??100)<=60?"#00e676":"#f0d060"}},
                 {{l:"52W高値比",v:fmt(selected.pct_high),col:numCol(selected.pct_high??0)}},
                 {{l:"ADR%",v:selected.adr?`${{selected.adr}}%`:"—",col:"#ddeeff"}},
                 {{l:"損切り",v:`$${{selected.stop}}`,col:"#ff5252"}},
                 {{l:"TP1(+10%)",v:`$${{selected.tp1}}`,col:"#00ff88"}},
                 {{l:"TP2(+20%)",v:`$${{selected.tp2}}`,col:"#00e5ff"}},
                 {{l:"時価総額",v:`$${{selected.market_cap_b}}B`,col:"#ddeeff"}},
            ].map(m=>e("div",{{key:m.l,style:{{background:"#0a1320",border:"1px solid #0e1c30",
              borderRadius:8,padding:"7px 10px",textAlign:"center"}}}},
              e("div",{{style:{{fontSize:9,color:"#2a4a6a",marginBottom:3}}}},m.l),
              e("div",{{style:{{fontSize:13,fontWeight:500,color:m.col}}}},m.v)
            ))
          )
        ),

        // TABLE
        e("div",{{style:{{background:"#0a1320",border:"1px solid #0e1c30",borderRadius:12,overflow:"hidden"}}}},
          e("div",{{style:{{overflowX:"auto"}}}},
            e("table",{{style:{{width:"100%",borderCollapse:"collapse",fontSize:11}}}},
              e("thead",null,
                e("tr",{{style:{{background:"#060a10",borderBottom:"1px solid #0e1c30"}}}},
                  ...[ {{k:"rs_p2",label:"RS★",align:"center"}},
                       {{k:"t",label:"Ticker",align:"left",noSort:true}},
                       {{k:"n",label:"銘柄名",align:"left",noSort:true}},
                       {{k:"s",label:"セクター",align:"left",noSort:true}} ].map(c=>
                    e("th",{{key:c.k,onClick:()=>!c.noSort&&handleSort(c.k),style:{{
                      padding:"8px 9px",textAlign:c.align||"right",
                      color:sortK===c.k?"#00e5ff":"#1e3a5a",
                      fontSize:9,fontWeight:700,textTransform:"uppercase",
                      letterSpacing:".08em",userSelect:"none",
                      cursor:c.noSort?"default":"pointer",
                      background:sortK===c.k?"rgba(0,229,255,.04)":"transparent",
                    }}}},c.label,!c.noSort&&sortK===c.k&&(sortD==="desc"?" ↓":" ↑"))
                  ),
                  ...rsCols.map(c=>e("th",{{key:c.k,onClick:()=>handleSort(c.k),style:{{
                    padding:"8px 9px",textAlign:"right",
                    color:sortK===c.k?"#00e5ff":"#1e3a5a",
                    fontSize:9,fontWeight:700,textTransform:"uppercase",
                    letterSpacing:".08em",userSelect:"none",cursor:"pointer",
                    background:sortK===c.k?"rgba(0,229,255,.06)":"transparent",
                    borderBottom:sortK===c.k?"2px solid #00e5ff":"none",
                  }}}},c.label,sortK===c.k&&(sortD==="desc"?" ↓":" ↑"))),
                  ...[ {{k:"eps",label:"EPS%"}},{{k:"rev",label:"売上%"}},{{k:"roe",label:"ROE%"}},
                       {{k:"stage2",label:"Stage2"}},{{k:"vcs",label:"VCS"}},
                       {{k:"pct_high",label:"52W"}},{{k:"score",label:"Score"}} ].map(c=>
                    e("th",{{key:c.k,onClick:()=>handleSort(c.k),style:{{
                      padding:"8px 9px",textAlign:"right",
                      color:sortK===c.k?"#00e5ff":"#1e3a5a",
                      fontSize:9,fontWeight:700,textTransform:"uppercase",
                      letterSpacing:".08em",userSelect:"none",cursor:"pointer",
                    }}}},c.label,sortK===c.k&&(sortD==="desc"?" ↓":" ↑"))
                  ),
                  e("th",{{style:{{padding:"8px 9px",fontSize:9,color:"#1e3a5a",textTransform:"uppercase"}}}},"評価")
                )
              ),
              e("tbody",null,
                rows.length===0
                  ? e("tr",null,e("td",{{colSpan:20,style:{{padding:"40px",textAlign:"center",color:"#2a4a6a"}}}},"条件に合う銘柄がありません"))
                  : rows.map(d=>e("tr",{{key:d.ticker,className:"row",
                      onClick:()=>setSelected(d),
                      style:{{background:selected?.ticker===d.ticker?"rgba(0,255,140,0.05)":"transparent"}}}},
                    e("td",{{style:{{...TD,textAlign:"center"}}}},
                      e("span",{{style:{{
                        display:"inline-block",
                        background:`${{rsCol(d.rs_p2??0)}}15`,color:rsCol(d.rs_p2??0),
                        border:`1px solid ${{rsCol(d.rs_p2??0)}}35`,
                        borderRadius:5,padding:"1px 7px",fontSize:12,fontWeight:700,
                      }}}},fmtRS(d.rs_p2))
                    ),
                    e("td",{{style:{{...TD,textAlign:"left"}}}},
                      e("span",{{style:{{fontFamily:"sans-serif",fontWeight:800,color:"#00e5ff",fontSize:13}}}},d.ticker)),
                    e("td",{{style:{{...TD,textAlign:"left",color:"#5a7a9a",fontSize:10,maxWidth:140,overflow:"hidden",textOverflow:"ellipsis"}}}},d.name),
                    e("td",{{style:{{...TD,textAlign:"left",color:"#3a5a7a",fontSize:10}}}},d.sector),
                    ...rsCols.map(c=>e(RSCell,{{key:c.k,val:d[c.k],isSort:sortK===c.k}})),
                    e("td",{{style:{{...TD,textAlign:"right",color:numCol(d.eps??0),fontWeight:(d.eps??0)>=25?700:400}}}},fmt(d.eps)),
                    e("td",{{style:{{...TD,textAlign:"right",color:numCol(d.rev??0),fontWeight:(d.rev??0)>=15?700:400}}}},fmt(d.rev)),
                    e("td",{{style:{{...TD,textAlign:"right",color:(d.roe??0)>=12?"#00e676":"#ff8a80"}}}},`${{d.roe??'—'}}%`),
                    e("td",{{style:{{...TD,textAlign:"right",color:(d.stage2??0)>=8?"#00e5ff":(d.stage2??0)>=7?"#f0d060":"#ff8a80"}}}},`${{d.stage2}}/9`),
                    e("td",{{style:{{...TD,textAlign:"right"}}}},
                      e("span",{{style:{{fontWeight:700,color:(d.vcs??100)<=40?"#ff4444":(d.vcs??100)<=60?"#00e676":(d.vcs??100)<=75?"#f0d060":"#607d8b"}}}},d.vcs??'—')),
                    e("td",{{style:{{...TD,textAlign:"right",color:numCol(d.pct_high??0)}}}},fmt(d.pct_high)),
                    e("td",{{style:{{...TD,textAlign:"right",fontFamily:"sans-serif",fontWeight:700,color:"#ddeeff"}}}},d.score??0),
                    e("td",{{style:{{...TD,textAlign:"center"}}}},e(ScoreBadge,{{score:d.score??0}}))
                  ))
              )
            )
          )
        ),
        e("div",{{style:{{marginTop:8,fontSize:9,color:"#1e3450",display:"flex",justifyContent:"space-between"}}}},
          e("span",null,"RS = percentrank(TICKER/SPY, period) | ★P2(21日)ソート | 行クリック→詳細"),
          e("span",null,`${{rows.length}}件 / ${{RAW_DATA.total_found}}候補 / ${{RAW_DATA.universe_size}}スキャン`)
        )
      )
    )
  );
}}

ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(App));
</script>
</body>
</html>"""
    path = DOCS / "index.html"
    path.write_text(html, encoding="utf-8")
    print(f"  → docs/index.html 出力完了")

# ============================================================
# エントリーポイント
# ============================================================
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="full", choices=["full","sp500","nasdaq","russell2000"])
    args = p.parse_args()

    print(f"\n{'='*60}")
    print(f"Stock Screener v4.0")
    print(f"RS = percentrank(TICKER/SPY, period) — TradingView準拠")
    print(f"{NOW.strftime('%Y-%m-%d %H:%M JST')} | mode={args.mode}")
    print(f"{'='*60}\n")

    print("📋 Step1: ユニバース構築...")
    universe = build_universe(args.mode)

    print(f"\n💹 Step2: 価格データ取得...")
    price_data = fetch_prices(MACRO_TICKERS, period="2y")
    price_data.update(fetch_prices(universe, period=CONFIG["price_period"]))

    spy_df = price_data.get("SPY")
    if spy_df is None:
        print("❌ SPYデータ取得失敗")
        return

    price_ok = [t for t in universe if t in price_data]
    print(f"\n📊 Step3: ファンダメンタル取得 ({len(price_ok)}銘柄)...")
    funda_data = fetch_fundamentals(price_ok)

    print(f"\n🌐 Step4: マクロ環境判定...")
    market = assess_market(price_data)
    print(f"  {market['icon']} {market['mode']} | VIX: {market['vix']}")

    print(f"\n🔍 Step5: スクリーニング...")
    candidates = run_screening(price_data, funda_data, spy_df)
    print(f"  候補: {len(candidates)}銘柄")

    print(f"\n💾 Step6: JSON + HTML出力...")
    json_data = save_json(candidates, market, len(universe))
    save_html(json_data)

    print(f"\n✅ 完了 | 候補{len(candidates)}銘柄 / ユニバース{len(universe)}銘柄")
    print(f"   🌐 アプリURL: https://choco00chip.github.io/stock-screener-choco/\n")
if __name__ == "__main__":
    main()
