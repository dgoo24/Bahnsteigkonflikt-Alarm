import os, time, requests
h = {"DB-Client-Id": os.environ["DB_CLIENT_ID"], "DB-API-Key": os.environ["DB_API_KEY"], "Accept": "application/xml"}
base = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
for etiqueta, url in [
    ("plan 1h  (8000261 12h)", f"{base}/plan/8000261/261021/12"),
    ("plan DIA (8000261 hoy)   ", f"{base}/plan/day/8000261/261021"),
    ("fchg (8000261)           ", f"{base}/fchg/8000261"),
]:
    t = time.time()
    try:
        r = requests.get(url, headers=h, timeout=60)
        print(f"{etiqueta} -> HTTP {r.status_code}  {len(r.content):>8} bytes  {int((time.time()-t)*1000):>5} ms")
    except Exception as e:
        print(f"{etiqueta} -> ERROR {type(e).__name__}: {str(e)[:120]}")
