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
        print(f"[FOREX] USD/INR = {out['usd_inr']}")
    except Exception as e:
        print(f"[FOREX ERROR] {e}")
    with open(f"{OUTPUT_DIR}/forex.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# MCX FUTURES
# ─────────────────────────────────────────────
def fetch_mcx():
    empty = {
        "ltp": 0, "chg": 0, "volume": 0, "oi": 0,
        "oi_change": 0, "avg_price": 0, "close": 0,
        "unit": "--", "contracts": [], "bid_depth": [], "ask_depth": []
    }
    out = {
        "date": TODAY,
        "fetched_at": NOW_UTC + " (fallback)",
        "gold": dict(empty),
        "silver": dict(empty)
    }

    for days_back in range(0, 5):
        dt = datetime.utcnow() - timedelta(days=days_back)
        if dt.weekday() >= 5:
            continue
        date_str = dt.strftime("%d%b%Y").upper()
        url = f"https://www.mcxindia.com/backpage.aspx/GetBhavCopy?strDate={date_str}"
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://www.mcxindia.com/"
            }
            r = requests.get(url, headers=headers, timeout=15)
            print(f"[MCX] day-{days_back} status={r.status_code} size={len(r.content)}")
            if r.status_code != 200 or len(r.content) < 500:
                continue

            df = pd.read_excel(BytesIO(r.content), engine="xlrd")
            df.columns = [str(c).strip().upper() for c in df.columns]
            print(f"[MCX] Columns: {list(df.columns)}")

            for metal, symbol in [("gold", "GOLD"), ("silver", "SILVER")]:
                rows = df[df.iloc[:, 0].astype(str).str.upper().str.startswith(symbol)]
                if rows.empty:
                    print(f"[MCX] No rows found for {symbol}")
                    continue
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
                volume = gcol(["VOLUME", "TRADEDQTY", "TRADED"])
                oi     = gcol(["OI", "OPENINT", "OPEN INT"])

                try:
                    ltp = float(close)
                    chg = round(float(close) - float(prev), 2) if prev else 0
                except:
                    ltp, chg = 0, 0

                contracts = []
                for _, r2 in rows.head(3).iterrows():
                    contracts.append({
                        "expiry": str(r2.iloc[1]) if len(r2) > 1 else "--",
                        "close": float(close) if close else 0
                    })

                out[metal].update({
                    "ltp": ltp,
                    "chg": chg,
                    "close": ltp,
                    "volume": int(volume) if volume else 0,
                    "oi": int(oi) if oi else 0,
                    "unit": "INR/10g" if metal == "gold" else "INR/kg",
                    "contracts": contracts
                })
                print(f"[MCX] {metal} ltp={ltp}")

            out["date"] = dt.strftime("%Y-%m-%d")
            out["fetched_at"] = NOW_UTC
            break

        except Exception as e:
            print(f"[MCX ERROR day-{days_back}] {e}")
            continue

    with open(f"{OUTPUT_DIR}/mcx_futures.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# COMEX WAREHOUSE
# ─────────────────────────────────────────────
def fetch_comex_warehouse():
    out = {
        "fetched_at": NOW_UTC,
        "report_date": "--",
        "activity_date": "--",
        "gold":   _empty_warehouse(),
        "silver": _empty_warehouse()
    }

    for days_back in range(0, 7):
        dt = datetime.utcnow() - timedelta(days=days_back)
        if dt.weekday() >= 5:
            continue
        date_str = dt.strftime("%Y%m%d")
        url = f"https://www.cmegroup.com/CmeWS/mvc/Warehouse/Download/{date_str}/G"
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, timeout=15)
            print(f"[COMEX WH] day-{days_back} status={r.status_code} size={len(r.content)}")
            if r.status_code != 200 or len(r.content) < 500:
                continue

            wb = xlrd.open_workbook(file_contents=r.content)
            print(f"[COMEX WH] Sheets: {wb.sheet_names()}")

            for metal, sheet_idx in [("gold", 0), ("silver", 1)]:
                try:
                    ws = wb.sheets()[sheet_idx]
                    depositories = []
                    total_reg = total_elig = 0

                    for i in range(ws.nrows):
                        row = [str(ws.cell(i, j).value).strip() for j in range(ws.ncols)]
                        if not row[0] or "TOTAL" in row[0].upper() or "DEPOSITORY" in row[0].upper():
                            continue
                        reg  = _safe_int(row, 1)
                        elig = _safe_int(row, 2)
                        if reg == 0 and elig == 0:
                            continue
                        depositories.append({
                            "name": row[0],
                            "registered": reg,
                            "eligible": elig,
                            "total": reg + elig
                        })
                        total_reg  += reg
                        total_elig += elig

                    out[metal] = {
                        "registered": total_reg,
                        "eligible": total_elig,
                        "combined": total_reg + total_elig,
                        "received": 0,
                        "withdrawn": 0,
                        "net_change": 0,
                        "depositories": depositories
                    }
                    print(f"[COMEX WH] {metal} registered={total_reg}")
                except Exception as e:
                    print(f"[COMEX WH sheet {sheet_idx} ERROR] {e}")

            out["report_date"] = dt.strftime("%Y-%m-%d")
            out["activity_date"] = dt.strftime("%Y-%m-%d")
            break

        except Exception as e:
            print(f"[COMEX WH ERROR day-{days_back}] {e}")
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
        print(f"[SGE WH] status={r.status_code} size={len(r.content)}")
        tables = pd.read_html(BytesIO(r.content), flavor="lxml")
        print(f"[SGE WH] Found {len(tables)} tables")

        for metal, keyword in [("gold", "黄金"), ("silver", "白银")]:
            for tbl in tables:
                flat = tbl.to_string()
                if keyword in flat:
                    nums = tbl.select_dtypes(include="number")
                    if not nums.empty:
                        out[metal]["registered"] = int(nums.iloc[0, 0]) if nums.shape[1] > 0 else 0
                        out[metal]["eligible"]   = int(nums.iloc[0, 1]) if nums.shape[1] > 1 else 0
                        out[metal]["combined"]   = out[metal]["registered"] + out[metal]["eligible"]
                    print(f"[SGE WH] {metal} found")
                    break

        out["report_date"] = TODAY
        out["activity_date"] = TODAY

    except Exception as e:
        print(f"[SGE WH ERROR] {e}")

    with open(f"{OUTPUT_DIR}/warehouse_sge.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# COMEX DELIVERY
# ─────────────────────────────────────────────
def fetch_comex_delivery():
    empty_metal = {
        "total_issued": 0, "total_stopped": 0, "mtd": 0, "firms": []
    }
    out = {
        "date": TODAY,
        "contract": "--",
        "settlement": "--",
        "delivery_date": "--",
        "fetched_at": NOW_UTC,
        "gold": dict(empty_metal),
        "silver": dict(empty_metal)
    }

    try:
        url = "https://www.cmegroup.com/CmeWS/mvc/Notices/deliveryIntentionsReport.pdf"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        print(f"[COMEX DEL] status={r.status_code} size={len(r.content)}")

        if r.status_code == 200 and len(r.content) > 500:
            import pdfplumber
            with pdfplumber.open(BytesIO(r.content)) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if not row:
                                continue
                            row_str = [str(c).strip() if c else "" for c in row]
                            for metal, key in [("GOLD", "gold"), ("SILVER", "silver")]:
                                if any(metal in c.upper() for c in row_str):
                                    try:
                                        nums = [c for c in row_str if c.replace(",","").isdigit()]
                                        if len(nums) >= 2:
                                            out[key]["total_issued"]  = int(nums[0].replace(",",""))
                                            out[key]["total_stopped"] = int(nums[1].replace(",",""))
                                    except:
                                        pass
            print(f"[COMEX DEL] gold issued={out['gold']['total_issued']}")

    except Exception as e:
        print(f"[COMEX DEL ERROR] {e}")

    with open(f"{OUTPUT_DIR}/comex_delivery.json", "w") as f:
        json.dump(out, f, indent=2)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Starting fetch ===")
    fetch_forex()
    fetch_mcx()
    fetch_comex_warehouse()
    fetch_sge_warehouse()
    fetch_comex_delivery()
    print("=== Done ===")
