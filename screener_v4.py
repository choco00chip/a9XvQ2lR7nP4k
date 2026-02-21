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
# ユニバース取得
# ============================================================
def fetch_sp500():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        resp = requests.get(url, headers=headers, timeout=30)
        tables = pd.read_html(resp.text)
        tickers = [t.replace(".", "-") for t in tables[0]["Symbol"].tolist()]
        print(f"  S&P500: {len(tickers)}銘柄")
        return tickers
    except Exception as e:
        print(f"  S&P500取得失敗: {e}")
        # フォールバック: 主要銘柄を直接指定
        return [
            "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AVGO","LLY","JPM",
            "V","MA","UNH","XOM","PG","HD","COST","MRK","ABBV","CVX","CRM","NFLX",
            "AMD","NOW","ISRG","GS","TMO","CAT","AMAT","MU","PANW","CRWD","UBER",
            "ANET","VST","CEG","APP","PLTR","AXON","EME","PWR","DECK","ONON","CAVA",
            "COIN","SPOT","DDOG","SNOW","TTD","MRVL","KLAC","LRCX","MPWR","FTNT",
        ]

def fetch_nasdaq100():
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        for t in tables:
            if "Ticker" in t.columns:
                tickers = t["Ticker"].dropna().tolist()
                print(f"  NASDAQ100: {len(tickers)}銘柄")
                return tickers
        return []
    except Exception as e:
        print(f"  NASDAQ100取得失敗: {e}")
        return []

def build_universe(mode="full"):
    tickers = set()
    if mode in ["full", "sp500"]:
        tickers.update(fetch_sp500())
    if mode in ["full", "nasdaq"]:
        tickers.update(fetch_nasdaq100())
    exclude = {"SPY","QQQ","IWM","DIA","RSP","GLD","SLV","TLT","HYG","VXX"}
    tickers -= exclude
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

# ============================================================
# エントリーポイント
# ============================================================
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="full", choices=["full","sp500","nasdaq"])
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

    print(f"\n💾 Step6: JSON出力...")
    save_json(candidates, market, len(universe))

    print(f"\n✅ 完了 | 候補{len(candidates)}銘柄 / ユニバース{len(universe)}銘柄\n")

if __name__ == "__main__":
    main()
