"""
Russell 2000 Ticker Updater
============================
月1回実行してrussell2000.txtを更新する。
複数ソースを順番に試し、最初に成功したものを使用。

Sources:
  1. stockanalysis.com  (Next.js SSR → JSON in HTML → 全銘柄1リクエスト)
  2. finviz.com         (HTML scraping → 100ページ × 20銘柄)
  3. 既存ファイル保持   (全ソース失敗時はファイルを変更しない)

使用方法:
  python update_russell2000.py
"""

import requests
import json
import re
import time
from pathlib import Path
from bs4 import BeautifulSoup

OUTPUT_FILE = Path("russell2000.txt")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def is_valid_ticker(t: str) -> bool:
    """有効な米国株ティッカーかどうか判定"""
    t = t.strip()
    if not t:
        return False
    # 1〜5文字、アルファベットのみ（ハイフン許容）
    clean = t.replace("-", "")
    return 1 <= len(clean) <= 5 and clean.isalpha() and clean.isupper()


# ============================================================
# Method 1: stockanalysis.com
# Next.js SSR → __NEXT_DATA__ に全銘柄JSONが埋め込まれている
# ============================================================
def fetch_from_stockanalysis() -> list[str]:
    print("  [1] stockanalysis.com を試行...")
    try:
        url = "https://stockanalysis.com/list/russell-2000/"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        # __NEXT_DATA__ からJSONを抽出
        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            resp.text,
            re.DOTALL,
        )
        if match:
            data = json.loads(match.group(1))
            # データ構造を再帰的に探索してtickerリストを見つける
            tickers = _extract_tickers_from_json(data)
            if len(tickers) > 500:
                print(f"  [1] ✅ stockanalysis: {len(tickers)}銘柄取得")
                return tickers

        # __NEXT_DATA__がなければ通常のHTMLテーブルを試す
        soup = BeautifulSoup(resp.text, "html.parser")
        tickers = []
        for a in soup.find_all("a", href=re.compile(r"^/stocks/")):
            symbol = a.text.strip().upper()
            if is_valid_ticker(symbol):
                tickers.append(symbol)

        tickers = list(dict.fromkeys(tickers))  # 重複除去・順序保持
        if len(tickers) > 500:
            print(f"  [1] ✅ stockanalysis (HTML): {len(tickers)}銘柄取得")
            return tickers

        print(f"  [1] ❌ stockanalysis: {len(tickers)}銘柄のみ（不十分）")
    except Exception as e:
        print(f"  [1] ❌ stockanalysis エラー: {e}")
    return []


def _extract_tickers_from_json(obj, depth=0) -> list[str]:
    """JSONオブジェクトを再帰的に探索してティッカーリストを抽出"""
    if depth > 10:
        return []
    tickers = []
    if isinstance(obj, dict):
        # "s" または "symbol" または "ticker" キーがある場合
        for key in ("s", "symbol", "ticker", "Symbol", "Ticker"):
            if key in obj:
                val = obj[key]
                if isinstance(val, str) and is_valid_ticker(val):
                    tickers.append(val.upper())
        for val in obj.values():
            tickers.extend(_extract_tickers_from_json(val, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            tickers.extend(_extract_tickers_from_json(item, depth + 1))
    return tickers


# ============================================================
# Method 2: finviz.com screener (ページネーション)
# JS不要、HTMLテーブル、1ページ20銘柄 × 最大110ページ
# ============================================================
def fetch_from_finviz() -> list[str]:
    print("  [2] finviz.com を試行（最大110ページ）...")
    tickers = []
    base_url = "https://finviz.com/screener.ashx?v=111&f=idx_russell2000&o=ticker&r={}"

    # まず1ページ目で総数を確認
    try:
        url = base_url.format(1)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 総銘柄数を確認
        total = 2000  # デフォルト
        for el in soup.find_all(["td", "div", "span"]):
            m = re.search(r"(\d[\d,]+)\s*stocks", el.text)
            if m:
                total = int(m.group(1).replace(",", ""))
                break

        # 1ページ目のティッカーを取得
        page_tickers = _parse_finviz_page(soup)
        tickers.extend(page_tickers)
        print(f"    p1: {len(page_tickers)}銘柄 | 推定総数: {total}")

        if not page_tickers:
            print("  [2] ❌ finviz: 1ページ目からティッカー取得失敗")
            return []

        # 残りのページを取得
        rows_per_page = len(page_tickers)  # 通常20
        max_page = min(((total - 1) // rows_per_page) + 1, 110)

        for page_num in range(2, max_page + 1):
            r_start = (page_num - 1) * rows_per_page + 1
            try:
                resp = requests.get(base_url.format(r_start), headers=HEADERS, timeout=20)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                page_tickers = _parse_finviz_page(soup)
                if not page_tickers:
                    print(f"    p{page_num}: 終端検出 → 停止")
                    break
                tickers.extend(page_tickers)
                if page_num % 10 == 0:
                    print(f"    p{page_num}: 累計{len(tickers)}銘柄...")
                time.sleep(0.4)  # レート制限対策
            except Exception as e:
                print(f"    p{page_num}: エラー {e} → スキップ")
                time.sleep(1)
                continue

    except Exception as e:
        print(f"  [2] ❌ finviz エラー: {e}")
        return []

    tickers = list(dict.fromkeys(tickers))  # 重複除去
    if len(tickers) > 500:
        print(f"  [2] ✅ finviz: {len(tickers)}銘柄取得")
        return tickers
    else:
        print(f"  [2] ❌ finviz: {len(tickers)}銘柄のみ（不十分）")
        return []


def _parse_finviz_page(soup) -> list[str]:
    """finvizの1ページからティッカーを抽出"""
    tickers = []
    # 方法A: screener-link-primary クラス
    for a in soup.find_all("a", {"class": "screener-link-primary"}):
        t = a.text.strip().upper()
        if is_valid_ticker(t):
            tickers.append(t)
    if tickers:
        return tickers
    # 方法B: /quote.ashx?t= リンク
    for a in soup.find_all("a", href=re.compile(r"quote\.ashx\?t=")):
        m = re.search(r"t=([A-Z]+)", a["href"])
        if m and is_valid_ticker(m.group(1)):
            tickers.append(m.group(1))
    return list(dict.fromkeys(tickers))


# ============================================================
# Method 3: 既存ファイル（フォールバック）
# ============================================================
def load_existing() -> list[str]:
    if OUTPUT_FILE.exists():
        tickers = [t.strip() for t in OUTPUT_FILE.read_text().splitlines()
                   if t.strip() and is_valid_ticker(t.strip())]
        if tickers:
            print(f"  [3] 既存ファイル: {len(tickers)}銘柄")
            return tickers
    return []


# ============================================================
# Main
# ============================================================
def main():
    print("=" * 50)
    print("Russell 2000 Ticker Updater")
    print("=" * 50)

    # ソース1: stockanalysis.com
    tickers = fetch_from_stockanalysis()

    # ソース2: finviz（ソース1失敗時）
    if len(tickers) < 500:
        tickers = fetch_from_finviz()

    # ソース3: 既存ファイル保持（全ソース失敗時）
    if len(tickers) < 500:
        existing = load_existing()
        if existing:
            print(f"  全ソース失敗 → 既存ファイルを保持（{len(existing)}銘柄）")
            return  # ファイル変更なし

    if not tickers:
        print("❌ 全ソース失敗かつ既存ファイルなし")
        return

    # 重複除去・ソート・フィルタリング
    exclude = {
        "SPY","QQQ","IWM","DIA","RSP","GLD","SLV","TLT","HYG","VXX",
        "IEMG","EFA","AGG","BND","VEA","VWO","IEFA","ITOT","IVV","VOO",
    }
    tickers = sorted(set(t for t in tickers if is_valid_ticker(t) and t not in exclude))

    # ファイル書き込み
    OUTPUT_FILE.write_text("\n".join(tickers) + "\n", encoding="utf-8")
    print(f"\n✅ {OUTPUT_FILE} に {len(tickers)}銘柄を保存しました")


if __name__ == "__main__":
    main()
