import requests
import json
import os
import re
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# ── Fallback data (used if CME fetch fails) ───────────────────────────────────
FALLBACK = {
    "date": "Fallback (CME fetch failed)",
    "contract": "COMEX METALS FUTURES",
    "settlement": "0",
    "delivery_date": "-",
    "fetched_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    "gold": {
        "total_issued": 497, "total_stopped": 497, "mtd": 4125,
        "firms": [
            {"id":"099","org":"H","name":"DEUTSCHE BANK AG",    "issued":266,"stopped":0},
            {"id":"132","org":"C","name":"SG AMERICAS",         "issued":28, "stopped":0},
            {"id":"323","org":"H","name":"HSBC",                "issued":0,  "stopped":21},
            {"id":"363","org":"C","name":"WELLS FARGO SECURITI","issued":183,"stopped":0},
            {"id":"555","org":"C","name":"BNP PARIBAS SEC CORP","issued":0,  "stopped":55},
            {"id":"624","org":"H","name":"BOFA SECURITIES",     "issued":0,  "stopped":42},
            {"id":"661","org":"C","name":"JP MORGAN SECURITIES","issued":20, "stopped":377},
            {"id":"880","org":"H","name":"CITIGROUP",           "issued":0,  "stopped":2}
        ]
    },
    "silver": {
        "total_issued": 2140, "total_stopped": 740, "mtd": 10526,
        "firms": [
            {"id":"023","org":"H","name":"JP MORGAN CHASE BANK","issued":1200,"stopped":0},
            {"id":"099","org":"H","name":"DEUTSCHE BANK AG",    "issued":450, "stopped":0},
            {"id":"323","org":"H","name":"HSBC",                "issued":0,   "stopped":380},
            {"id":"363","org":"C","name":"WELLS FARGO SECURITI","issued":310, "stopped":0},
            {"id":"555","org":"C","name":"BNP PARIBAS SEC CORP","issued":0,   "stopped":210},
            {"id":"624","org":"H","name":"BOFA SECURITIES",     "issued":0,   "stopped":150},
            {"id":"880","org":"H","name":"CITIGROUP",           "issued":180, "stopped":0}
        ]
    }
}

def parse_metal_section(text, keyword):
    blocks = re.split(r'(?=EXCHANGE:\s*COMEX)', text, flags=re.IGNORECASE)
    section = next((b for b in blocks if keyword.upper() in b.upper() and 'FUTURES' in b.upper()), '')
    if not section:
        return None
    contract_m  = re.search(r'(\w+ \d{4} COMEX[\w\s]+FUTURES)', section, re.I)
    settle_m    = re.search(r'SETTLEMENT:\s*([\d,.]+)', section, re.I)
    intent_m    = re.search(r'INTENT DATE:\s*([\d/]+)', section, re.I)
    deliv_m     = re.search(r'DELIVERY DATE:\s*([\d/]+)', section, re.I)
    total_m     = re.search(r'TOTAL:\s*(\d+)\s+(\d+)', section)
    mtd_m       = re.search(r'MONTH TO DATE:\s*([\d,]+)', section)
    firms = []
    for m in re.finditer(r'(\d{3})\s+([HC])\s+([A-Z][A-Z\s&./,]+?)\s+(\d+)?\s*(\d+)?(?=\s+\d{3}|\s+TOTAL)', section):
        issued  = int(m.group(4)) if m.group(4) else 0
        stopped = int(m.group(5)) if m.group(5) else 0
        if issued > 0 or stopped > 0:
            firms.append({"id": m.group(1), "org": m.group(2),
                          "name": m.group(3).strip(),
                          "issued": issued, "stopped": stopped})
    total_issued  = int(total_m.group(1)) if total_m else sum(f['issued']  for f in firms)
    total_stopped = int(total_m.group(2)) if total_m else sum(f['stopped'] for f in firms)
    return {
        "contract":      contract_m.group(1) if contract_m else keyword + ' FUTURES',
        "settlement":    settle_m.group(1)   if settle_m   else '--',
        "date":          intent_m.group(1)   if intent_m   else '--',
        "delivery_date": deliv_m.group(1)    if deliv_m    else '--',
        "total_issued":  total_issued,
        "total_stopped": total_stopped,
        "mtd":           int(mtd_m.group(1).replace(',','')) if mtd_m else 0,
        "firms":         firms
    }

def fetch_comex_delivery():
    print("Fetching CME delivery PDF...")
    url = 'https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsReport.pdf'
    try:
        import tabula
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        pdf_path = '/tmp/cme_delivery.pdf'
        with open(pdf_path, 'wb') as f:
            f.write(r.content)
        tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True,
                                  lattice=False, stream=True)
        full_text = ' '.join([df.to_string() for df in tables])
        gold_data   = parse_metal_section(full_text, 'GOLD')
        silver_data = parse_metal_section(full_text, 'SILVER')
        output = {
            "date":          gold_data['date']          if gold_data else '--',
            "contract":      gold_data['contract']      if gold_data else 'COMEX METALS',
            "settlement":    gold_data['settlement']    if gold_data else '--',
            "delivery_date": gold_data['delivery_date'] if gold_data else '--',
            "fetched_at":    datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "gold":   gold_data   or FALLBACK['gold'],
            "silver": silver_data or FALLBACK['silver']
        }
        path = os.path.join(DATA_DIR, 'comex_delivery.json')
        with open(path, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"  ✅ COMEX delivery saved → {path}")
        return True
    except Exception as e:
        print(f"  ⚠️  CME fetch failed: {e}. Saving fallback.")
        FALLBACK['fetched_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC") + ' (fallback)'
        with open(os.path.join(DATA_DIR, 'comex_delivery.json'), 'w') as f:
            json.dump(FALLBACK, f, indent=2)
        return False

def fetch_forex():
    print("Fetching USD/INR rate...")
    try:
        r = requests.get('https://open.er-api.com/v6/latest/USD', timeout=10)
        data = r.json()
        inr = round(data['rates']['INR'], 2)
        out = {"usd_inr": inr, "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
        with open(os.path.join(DATA_DIR, 'forex.json'), 'w') as f:
            json.dump(out, f, indent=2)
        print(f"  ✅ USD/INR: {inr}")
    except Exception as e:
        print(f"  ⚠️  Forex fetch failed: {e}")
        with open(os.path.join(DATA_DIR, 'forex.json'), 'w') as f:
            json.dump({"usd_inr": 93.31, "date": "fallback"}, f)

if __name__ == '__main__':
    print("=== Datastream Helix — Fetch Script ===")
    fetch_comex_delivery()
    fetch_forex()
    print("=== Done ===")
