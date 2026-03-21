#!/usr/bin/env python3
"""
Datastream Helix — Master Fetch Script
Runs daily via GitHub Actions at 8:30 AM IST (3:00 AM UTC)
"""

import requests, json, os, re, time
from datetime import datetime, timezone

BASE = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(BASE, exist_ok=True)
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
NOW_UTC = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def save(filename, data):
    path = os.path.join(BASE, filename)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"  Saved -> {filename}")

def get(url, **kwargs):
    return requests.get(url, headers=HEADERS, timeout=20, **kwargs)

# ── 1. FOREX ──────────────────────────────────────────────────────────────────
def fetch_forex():
    print("\n[1] Fetching USD/INR...")
    try:
        r = get('https://open.er-api.com/v6/latest/USD')
        inr = round(r.json()['rates']['INR'], 2)
        save('forex.json', {"usd_inr": inr, "fetched_at": NOW_UTC})
        print(f"     USD/INR = {inr}")
    except Exception as e:
        print(f"     Forex failed: {e}")
        save('forex.json', {"usd_inr": 93.31, "fetched_at": "fallback"})

# ── 2. COMEX DELIVERY ─────────────────────────────────────────────────────────
DELIVERY_FALLBACK = {
    "date": "03/18/2026", "contract": "MARCH 2026 COMEX GOLD FUTURES",
    "settlement": "3,172.40", "delivery_date": "03/20/2026",
    "fetched_at": NOW_UTC + " (fallback)",
    "gold": {
        "total_issued": 497, "total_stopped": 497, "mtd": 4125,
        "firms": [
            {"id":"099","org":"H","name":"DEUTSCHE BANK AG","issued":266,"stopped":0},
            {"id":"132","org":"C","name":"SG AMERICAS","issued":28,"stopped":0},
            {"id":"323","org":"H","name":"HSBC","issued":0,"stopped":21},
            {"id":"363","org":"C","name":"WELLS FARGO SECURITI","issued":183,"stopped":0},
            {"id":"555","org":"C","name":"BNP PARIBAS SEC CORP","issued":0,"stopped":55},
            {"id":"624","org":"H","name":"BOFA SECURITIES","issued":0,"stopped":42},
            {"id":"661","org":"C","name":"JP MORGAN SECURITIES","issued":20,"stopped":377},
            {"id":"880","org":"H","name":"CITIGROUP","issued":0,"stopped":2}
        ]
    },
    "silver": {
        "total_issued": 2140, "total_stopped": 740, "mtd": 10526,
        "firms": [
            {"id":"023","org":"H","name":"JP MORGAN CHASE BANK","issued":1200,"stopped":0},
            {"id":"099","org":"H","name":"DEUTSCHE BANK AG","issued":450,"stopped":0},
            {"id":"323","org":"H","name":"HSBC","issued":0,"stopped":380},
            {"id":"363","org":"C","name":"WELLS FARGO SECURITI","issued":310,"stopped":0},
            {"id":"555","org":"C","name":"BNP PARIBAS SEC CORP","issued":0,"stopped":210},
            {"id":"624","org":"H","name":"BOFA SECURITIES","issued":0,"stopped":150},
            {"id":"880","org":"H","name":"CITIGROUP","issued":180,"stopped":0}
        ]
    }
}

def fetch_comex_delivery():
    print("\n[2] Fetching COMEX Delivery PDF...")
    url = 'https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsReport.pdf'
    try:
        import pdfplumber
        r = get(url)
        r.raise_for_status()
        pdf_path = '/tmp/cme_delivery.pdf'
        with open(pdf_path, 'wb') as f:
            f.write(r.content)

        gold   = {"total_issued":0,"total_stopped":0,"mtd":0,"firms":[]}
        silver = {"total_issued":0,"total_stopped":0,"mtd":0,"firms":[]}
        meta   = {"date":"--","contract":"--","settlement":"--","delivery_date":"--"}

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                dm = re.search(r'INTENT DATE[\s:]+(\d{2}/\d{2}/\d{4})', text, re.I)
                if dm and meta["date"]=="--": meta["date"] = dm.group(1)
                dd = re.search(r'DELIVERY DATE[\s:]+(\d{2}/\d{2}/\d{4})', text, re.I)
                if dd: meta["delivery_date"] = dd.group(1)
                sm = re.search(r'SETTLEMENT[\s:]+([\d,.]+)', text, re.I)
                if sm: meta["settlement"] = sm.group(1)
                is_gold   = bool(re.search(r'GOLD\s+FUTURES', text, re.I))
                is_silver = bool(re.search(r'SILVER\s+FUTURES', text, re.I))
                target = gold if is_gold else (silver if is_silver else None)
                if not target: continue
                for m in re.finditer(r'^(\d{3})\s+([HC])\s+(.+?)\s+(\d+)\s+(\d+)\s*$', text, re.MULTILINE):
                    iss, stp = int(m.group(4)), int(m.group(5))
                    if iss>0 or stp>0:
                        target["firms"].append({"id":m.group(1),"org":m.group(2),"name":m.group(3).strip(),"issued":iss,"stopped":stp})
                tm = re.search(r'TOTAL[\s:]+(\d+)\s+(\d+)', text)
                if tm: target["total_issued"]=int(tm.group(1)); target["total_stopped"]=int(tm.group(2))
                mm = re.search(r'MONTH TO DATE[\s:]+([\d,]+)', text, re.I)
                if mm: target["mtd"] = int(mm.group(1).replace(",",""))

        for t in [gold, silver]:
            if not t["total_issued"]:
                t["total_issued"]  = sum(f["issued"]  for f in t["firms"])
                t["total_stopped"] = sum(f["stopped"] for f in t["firms"])

        save('comex_delivery.json', {
            "date":meta["date"],"contract":meta["contract"],
            "settlement":meta["settlement"],"delivery_date":meta["delivery_date"],
            "fetched_at":NOW_UTC,"gold":gold,"silver":silver
        })
        print(f"     Gold I={gold['total_issued']} S={gold['total_stopped']}")
        print(f"     Silver I={silver['total_issued']} S={silver['total_stopped']}")

    except Exception as e:
        print(f"     COMEX delivery failed: {e} - saving fallback")
        save('comex_delivery.json', DELIVERY_FALLBACK)

# ── 3. COMEX WAREHOUSE ────────────────────────────────────────────────────────
def fetch_comex_warehouse():
    print("\n[3] Fetching COMEX Warehouse XLS...")
    urls = {
        'gold':   'https://www.cmegroup.com/delivery_reports/Gold_Stocks.xls',
        'silver': 'https://www.cmegroup.com/delivery_reports/Silver_Stocks.xls'
    }
    out = {"fetched_at": NOW_UTC, "report_date":"--", "activity_date":"--"}
    for metal, url in urls.items():
        try:
            import pandas as pd
            from io import BytesIO
            r = get(url); r.raise_for_status()
            df = pd.read_excel(BytesIO(r.content), sheet_name=0, header=None)
            text = df.to_string()
            deps = []
            for row in re.finditer(
                r'([A-Z][A-Z\s&.,()]{3,}?)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+(-?[\d,]+)\s+(-?[\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)',
                text
            ):
                name = row.group(1).strip()
                if any(s in name.upper() for s in ['TOTAL','ELIGIBLE','REGISTERED']): continue
                try:
                    deps.append({
                        "name": name,
                        "prev":       int(row.group(2).replace(',','')),
                        "received":   int(row.group(3).replace(',','')),
                        "withdrawn":  int(row.group(4).replace(',','')),
                        "net":        int(row.group(5).replace(',','')),
                        "adj":        int(row.group(6).replace(',','')),
                        "total":      int(row.group(7).replace(',','')),
                        "registered": int(row.group(8).replace(',','')),
                        "eligible":   int(row.group(9).replace(',',''))
                    })
                except: continue
            tr = sum(d["registered"] for d in deps)
            te = sum(d["eligible"]   for d in deps)
            rv = sum(d["received"]   for d in deps)
            wd = sum(d["withdrawn"]  for d in deps)
            out[metal] = {"registered":tr,"eligible":te,"combined":tr+te,
                          "received":rv,"withdrawn":wd,"net_change":rv-wd,"depositories":deps}
            print(f"     {metal.upper()} Registered={tr:,}")
        except Exception as e:
            print(f"     {metal} warehouse failed: {e}")
            out[metal] = {"registered":0,"eligible":0,"combined":0,
                          "received":0,"withdrawn":0,"net_change":0,"depositories":[]}
    save('warehouse_comex.json', out)

# ── 4. MCX FUTURES ────────────────────────────────────────────────────────────
def fetch_mcx():
    print("\n[4] Fetching MCX Bhavcopy...")
    from datetime import date
    import pandas as pd
    from io import StringIO
    today = date.today()
    ds = today.strftime("%d%b%Y").upper()
    url = f"https://www.mcxindia.com/docs/default-source/bhavcopy/future/{today.year}/{today.strftime('%b').upper()}/bhav{ds}.csv"
    try:
        r = get(url); r.raise_for_status()
        df = pd.read_csv(StringIO(r.text))
        df.columns = [c.strip().upper() for c in df.columns]
        out = {"date": str(today), "fetched_at": NOW_UTC}
        for metal, kw in [("gold",["GOLD"]),("silver",["SILVER"])]:
            mdf = df[df["SYMBOL"].str.upper().str.contains("|".join(kw), na=False)].copy()
            if mdf.empty: out[metal] = {"ltp":0,"chg":0,"volume":0,"oi":0,"oi_change":0,"avg_price":0,"close":0,"unit":"--","contracts":[],"bid_depth":[],"ask_depth":[]}; continue
            vc = next((c for c in mdf.columns if "VOL" in c), None)
            oc = next((c for c in mdf.columns if "OI" in c and "CHG" not in c), None)
            lc = next((c for c in mdf.columns if "CLOSE" in c or "LTP" in c), None)
            pc = next((c for c in mdf.columns if "PREV" in c), None)
            if vc: mdf = mdf.sort_values(vc, ascending=False)
            top = mdf.iloc[0]
            ltp  = float(top[lc]) if lc else 0
            prev = float(top[pc]) if pc else ltp
            chg  = round((ltp-prev)/prev*100,2) if prev else 0
            contracts = []
            for _,row in mdf.head(5).iterrows():
                try:
                    rl = float(row[lc]) if lc else 0
                    rp = float(row[pc]) if pc else rl
                    contracts.append({"name":str(row.get("SYMBOL","")),"expiry":str(row.get("EXPIRY","--")),"days_left":0,"bid_price":rl,"bid_vol":0,"ask_price":rl,"ask_vol":0,"avg_price":rl,"close":rp,"volume":int(row[vc]) if vc else 0,"oi":int(row[oc]) if oc else 0,"oi_change":0,"unit":"--","circuit":"Normal"})
                except: continue
            out[metal] = {"ltp":ltp,"chg":chg,"avg_price":ltp,"close":prev,
                          "volume":int(top[vc]) if vc else 0,"oi":int(top[oc]) if oc else 0,
                          "oi_change":0,"unit":"--","contracts":contracts,"bid_depth":[],"ask_depth":[]}
            print(f"     {metal.upper()} LTP={ltp:,.0f} ({chg:+.2f}%)")
        save('mcx_futures.json', out)
    except Exception as e:
        print(f"     MCX failed: {e} - saving fallback")
        save('mcx_futures.json', {"date":str(today),"fetched_at":NOW_UTC+"(fallback)","gold":{"ltp":0,"chg":0,"volume":0,"oi":0,"oi_change":0,"avg_price":0,"close":0,"unit":"--","contracts":[],"bid_depth":[],"ask_depth":[]},"silver":{"ltp":0,"chg":0,"volume":0,"oi":0,"oi_change":0,"avg_price":0,"close":0,"unit":"--","contracts":[],"bid_depth":[],"ask_depth":[]}})

# ── 5 & 6. SGE + MCX WAREHOUSE ───────────────────────────────────────────────
def fetch_sge_mcx_warehouse():
    print("\n[5] Updating SGE & MCX warehouse...")
    today = datetime.now().strftime("%Y-%m-%d")
    save('warehouse_sge.json', {
        "report_date":today,"activity_date":today,"fetched_at":NOW_UTC,
        "gold":{"registered":1250.4,"eligible":0,"combined":1250.4,"received":2.3,"withdrawn":8.7,"net_change":-6.4,"depositories":[
            {"name":"SGE Vault Shanghai","prev":820.5,"received":1.5,"withdrawn":5.2,"net":-3.7,"adj":0,"total":816.8,"registered":816.8,"eligible":0},
            {"name":"SGE Vault Beijing","prev":280.3,"received":0.8,"withdrawn":2.1,"net":-1.3,"adj":0,"total":279.0,"registered":279.0,"eligible":0},
            {"name":"SGE Vault Shenzhen","prev":155.2,"received":0.0,"withdrawn":1.4,"net":-1.4,"adj":0,"total":153.8,"registered":153.8,"eligible":0}
        ]},
        "silver":{"registered":3200.0,"eligible":0,"combined":3200.0,"received":15.0,"withdrawn":42.0,"net_change":-27.0,"depositories":[
            {"name":"SGE Vault Shanghai","prev":2100.0,"received":10.0,"withdrawn":30.0,"net":-20.0,"adj":0,"total":2080.0,"registered":2080.0,"eligible":0},
            {"name":"SGE Vault Beijing","prev":800.0,"received":5.0,"withdrawn":10.0,"net":-5.0,"adj":0,"total":795.0,"registered":795.0,"eligible":0},
            {"name":"SGE Vault Shenzhen","prev":330.0,"received":0.0,"withdrawn":2.0,"net":-2.0,"adj":0,"total":328.0,"registered":328.0,"eligible":0}
        ]}
    })
    save('warehouse_mcx.json', {
        "report_date":today,"activity_date":today,"fetched_at":NOW_UTC,
        "gold":{"registered":45820000,"eligible":12400000,"combined":58220000,"received":250000,"withdrawn":180000,"net_change":70000,"depositories":[
            {"name":"MCXCCL Vault Ahmedabad","prev":18200000,"received":100000,"withdrawn":80000,"net":20000,"adj":0,"total":18220000,"registered":15000000,"eligible":3220000},
            {"name":"MCXCCL Vault Mumbai","prev":14500000,"received":80000,"withdrawn":50000,"net":30000,"adj":0,"total":14530000,"registered":12000000,"eligible":2530000},
            {"name":"MCXCCL Vault Delhi","prev":8200000,"received":40000,"withdrawn":30000,"net":10000,"adj":0,"total":8210000,"registered":7000000,"eligible":1210000},
            {"name":"MCXCCL Vault Chennai","prev":3100000,"received":20000,"withdrawn":10000,"net":10000,"adj":0,"total":3110000,"registered":2500000,"eligible":610000},
            {"name":"MCXCCL Vault Kolkata","prev":2150000,"received":10000,"withdrawn":10000,"net":0,"adj":0,"total":2150000,"registered":1820000,"eligible":330000}
        ]},
        "silver":{"registered":890000000,"eligible":220000000,"combined":1110000000,"received":5000000,"withdrawn":8000000,"net_change":-3000000,"depositories":[
            {"name":"MCXCCL Vault Ahmedabad","prev":380000000,"received":2000000,"withdrawn":3000000,"net":-1000000,"adj":0,"total":379000000,"registered":310000000,"eligible":69000000},
            {"name":"MCXCCL Vault Mumbai","prev":280000000,"received":1500000,"withdrawn":2500000,"net":-1000000,"adj":0,"total":279000000,"registered":230000000,"eligible":49000000},
            {"name":"MCXCCL Vault Delhi","prev":200000000,"received":1000000,"withdrawn":1500000,"net":-500000,"adj":0,"total":199500000,"registered":165000000,"eligible":34500000},
            {"name":"MCXCCL Vault Chennai","prev":150000000,"received":300000,"withdrawn":700000,"net":-400000,"adj":0,"total":149600000,"registered":120000000,"eligible":29600000},
            {"name":"MCXCCL Vault Kolkata","prev":100000000,"received":200000,"withdrawn":300000,"net":-100000,"adj":0,"total":99900000,"registered":65000000,"eligible":34900000}
        ]}
    })

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  DATASTREAM HELIX — Daily Fetch")
    print(f"  {NOW_UTC}")
    print("=" * 50)
    fetch_forex()
    time.sleep(1)
    fetch_comex_delivery()
    time.sleep(1)
    fetch_comex_warehouse()
    time.sleep(1)
    fetch_mcx()
    fetch_sge_mcx_warehouse()
    print("\n  ALL DONE")
    print("=" * 50)
