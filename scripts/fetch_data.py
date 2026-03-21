import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import xlrd

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NOW_UTC = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
TODAY = datetime.utcnow().strftime("%Y-%m-%d")

# ─────────────────────────────────────────────
# FOREX
# ─────────────────────────────────────────────
def fetch_forex():
    out = {"usd_inr": 0, "fetched_at": NOW_UTC}
    try:
        r = requests.get(
            "https://api.frankfurter.app/latest?from=USD&to=INR", timeout=10
        )
        data = r.json()
        out["usd_inr"] = round(data["rates"]["INR"], 2)
    except Exception as e:
        print(f"[FOREX ERROR] {e}")
    with open(f"{OUTPUT_DIR}/forex.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"[FOREX] USD/INR = {out['usd_inr']}")


# ─────────────────────────────────────────────
# MCX FUTURES (Bhavcopy)
# ─────────────────────────────────────────────
def fetch_mcx():
    empty = {
        "ltp": 0, "chg": 0, "volume": 0, "oi": 0,
        "oi_change": 0, "avg_price": 0, "close": 0,
        "unit": "--", "contracts": [], "bid_depth": [], "ask_depth": []
    }
    out = {
        "date": TODAY,
        "fetched_at": NOW_UTC,
        "gold": dict(empty),
        "silver": dict(empty)
    }

    # MCX Bhavcopy — try last 3 days to handle weekends/holidays
    for days_back in range(0, 4):
        dt = datetime.utcnow() - timedelta(days=days_back)
        # Skip Saturday(5) and Sunday(6)
        if dt.weekday() >= 5:
            continue
        date_str = dt.strftime("%d%b%Y").upper()  # e.g. 20MAR2026
        url = f"https://www.mcxindia.com/backpage.aspx/GetBhavCopy?strDate={date_str}"
        try:
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.mcxindia.com/"
            }
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200 or len(r.content) < 500:
                continue

            df = pd.read_excel(BytesIO(r.content), engine="xlrd")
            df.columns = [str(c).strip().upper() for c in df.columns]

            for metal, symbol in [("gold", "GOLD"), ("silver", "SILVER")]:
                rows = df[df.iloc[:, 0].astype(str).str.upper().str.startswith(symbol)]
                if rows.empty:
                    continue
                # Pick nearest expiry (first row)
                row = rows.iloc[0]
                cols = list(df.columns)

                def gcol(names):
                    for n in names:
                        for c in cols:
                            if n in c:
                                return row[c]
                    return 0

                close  = gcol(["CLOSE", "CLOSEPRICE"])
                prev   = gcol(["PREV", "PREVIOUS"])
                volume = gcol(["VOLUME", "TRADEDQTY"])
                oi     = gcol(["OI", "OPENINT"])

                try:
                    ltp = float(close)
                    chg = round(float(close) - float(prev), 2) if prev else 0
                except:
                    ltp, chg = 0, 0

                out[metal].update({
                    "ltp": ltp,
                    "chg": chg,
                    "close": ltp,
                    "volume": int(volume) if volume else 0,
                    "oi": int(oi) if oi else 0,
                    "unit": "INR/10g" if metal == "gold" else "INR/kg",
                    "fetched_date": dt.strftime("%Y-%m-%d")
                })

                # Top contracts
                contracts = []
                for _, r2 in rows.head(3).iterrows():
                    contracts.append({
                        "expiry": str(r2.get("EXPIRY", r2.get("EXPIRYDATE", "--"))),
                        "close": float(r2.get("CLOSE", r2.get("CLOSEPRICE", 0)) or 0)
                    })
                out[metal]["contracts"] = contracts

            out["date"] = dt.strftime("%Y-%m-%d")
            print(f"[MCX] Fetched for {dt.strftime('%Y-%m-%d')}")
            break

        except Exception as e:
            print(f"[MCX ERROR day -{days_back}] {e}")
            continue

    if out["gold"]["ltp"] == 0:
        out["fetched_at"] = NOW_UTC + " (fallback)"
        print("[MCX] All attempts failed — fallback zeros saved")

    with open(f"{OUTPUT_DIR}/mcx_futures.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# COMEX WAREHOUSE (CME XLS)
# ─────────────────────────────────────────────
def fetch_comex_warehouse():
    out = {
        "fetched_at": NOW_UTC,
        "report_date": "--",
        "activity_date": "--",
        "gold":   _empty_warehouse(),
        "silver": _empty_warehouse()
    }

    # CME publishes warehouse stocks as XLS
    # Try last 5 weekdays
    for days_back in range(0, 7):
        dt = datetime.utcnow() - timedelta(days=days_back)
        if dt.weekday() >= 5:
            continue
        date_str = dt.strftime("%Y%m%d")  # e.g. 20260320
        url = f"https://www.cmegroup.com/CmeWS/mvc/Warehouse/Download/{date_str}/G"

        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200 or len(r.content) < 500:
                # Try alternate URL pattern
                url2 = f"https://www.cmegroup.com/delivery_reports/GoldWarehouseStocksReport{date_str}.xls"
                r = requests.get(url2, headers=headers, timeout=15)
                if r.status_code != 200 or len(r.content) < 500:
                    continue

            wb = xlrd.open_workbook(file_contents=r.content)

            for metal, sheet_idx in [("gold", 0), ("silver", 1)]:
                try:
                    ws = wb.sheets()[sheet_idx]
                    rows_data = []
                    for i in range(ws.nrows):
                        rows_data.append([str(c.value).strip() for c in ws.row(i)])

                    # Find header row
                    header_row = 0
                    for idx, row in enumerate(rows_data):
                        if any("DEPOSITORY" in c.upper() or "VAULT" in c.upper() for c in row):
                            header_row = idx
                            break

                    headers_list = rows_data[header_row]
                    depositories = []
                    total_reg = total_elig = 0

                    for row in rows_data[header_row + 1:]:
                        if not row or not row[0] or "TOTAL" in row[0].upper():
                            continue
                        try:
                            name = row[0]
                            reg  = _safe_int(row, 1)
                            elig = _safe_int(row, 2)
                            depositories.append({
                                "name": name,
                                "registered": reg,
                                "eligible": elig,
                                "total": reg + elig
                            })
                            total_reg  += reg
                            total_elig += elig
                        except:
                            continue

                    out[metal] = {
                        "registered": total_reg,
                        "eligible": total_elig,
                        "combined": total_reg + total_elig,
                        "received": 0,
                        "withdrawn": 0,
                        "net_change": 0,
                        "depositories": depositories
                    }
                except Exception as e:
                    print(f"[COMEX WH sheet {sheet_idx} ERROR] {e}")

            out["report_date"] = dt.strftime("%Y-%m-%d")
            out["activity_date"] = dt.strftime("%Y-%m-%d")
            print(f"[COMEX WH] Fetched for {dt.strftime('%Y-%m-%d')}")
            break

        except Exception as e:
            print(f"[COMEX WH ERROR day -{days_back}] {e}")
            continue

    with open(f"{OUTPUT_DIR}/warehouse_comex.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# SGE WAREHOUSE
# ─────────────────────────────────────────────
def fetch_sge_warehouse():
    out = {
        "fetched_at": NOW_UTC,
        "report_date": "--",
        "activity_date": "--",
        "gold":   _empty_warehouse(),
        "silver": _empty_warehouse()
    }

    try:
        url = "https://www.sge.com.cn/sjzx/mrysjgb"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.sge.com.cn/"
        }
        r = requests.get(url, headers=headers, timeout=15)
        # SGE returns HTML table — parse with pandas
        tables = pd.read_html(r.text, flavor="lxml")
        if not tables:
            raise ValueError("No tables found")

        for metal, keyword in [("gold", "黄金"), ("silver", "白银")]:
            for tbl in tables:
                flat = tbl.to_string()
                if keyword in flat:
                    # Try to extract registered/eligible from first numeric columns
                    nums = tbl.select_dtypes(include="number")
                    if not nums.empty:
                        out[metal]["registered"] = int(nums.iloc[0, 0]) if nums.shape[1] > 0 else 0
                        out[metal]["eligible"]   = int(nums.iloc[0, 1]) if nums.shape[1] > 1 else 0
                        out[metal]["combined"]   = out[metal]["registered"] + out[metal]["eligible"]
                    break

        out["report_date"] = TODAY
        print("[SGE WH] Fetched")

    except Exception as e:
        print(f"[SGE WH ERROR] {e}")

    with open(f"{OUTPUT_DIR}/warehouse_sge.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# COMEX DELIVERY (already working — keep as is)
# ─────────────────────────────────────────────
def fetch_comex_delivery():
    # Your existing working function — DO NOT CHANGE
    pass


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _empty_warehouse():
    return {
        "registered": 0, "eligible": 0, "combined": 0,
        "received": 0, "withdrawn": 0, "net_change": 0,
        "depositories": []
    }

def _safe_int(row, idx):
    try:
        return int(float(str(row[idx]).replace(",", "")))
    except:
        return 0


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Starting fetch ===")
    fetch_forex()
    fetch_mcx()
    fetch_comex_warehouse()
    fetch_sge_warehouse()
    fetch_comex_delivery()  # Keep your existing one
    print("=== Done ===")
