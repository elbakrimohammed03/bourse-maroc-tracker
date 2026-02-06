import requests
import pandas as pd
import re
import time
import os
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client

# Configuration Supabase
SUPABASE_URL = "https://nbgpxasdgucltfcygqua.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5iZ3B4YXNkZ3VjbHRmY3lncXVhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzMzc1NDAsImV4cCI6MjA4NTkxMzU0MH0.EpLaGobOZxa_VI-_cOBXoDBiB7J-5QaC9vNV4lyNNKc"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {"User-Agent": "Mozilla/5.0"}
base_url = "https://www.casablancabourse.com"

def clean_val(val):
    try:
        if val is None or str(val).strip() in ['', '-', 'N/A']:
            return None
        res = re.sub(r'[^\d.-]', '', str(val).replace(',', '.'))
        f_val = float(res)
        if np.isnan(f_val) or np.isinf(f_val):
            return None
        return f_val
    except:
        return None

# Etape 1 : Collecte des cours
print("Collecte des cours en cours...")
try:
    resp = requests.get(base_url, headers=headers, timeout=15)
    soup = BeautifulSoup(resp.text, 'html.parser')
except Exception as e:
    print(f"Erreur lors de l'accès au site : {e}")
    exit(1)

all_tickers = {}
for a in soup.find_all('a', href=True):
    if '/action/capitalisation' in a['href']:
        ticker = a['href'].split('/')[1]
        name = a.get_text(strip=True).upper()
        all_tickers[name] = ticker

rows_base = []
for row in soup.find_all('tr'):
    cols = row.find_all('td')
    if len(cols) == 9:
        rows_base.append({
            'entreprise': cols[1].get_text(strip=True).upper(),
            'volume_actions': clean_val(cols[3].get_text(strip=True)),
            'prix_mad': clean_val(cols[6].get_text(strip=True)),
            'variation_pct': clean_val(cols[7].get_text(strip=True))
        })

# Etape 2 : Extraction des fondamentaux
print(f"Extraction des détails pour {len(all_tickers)} entreprises...")
all_details = []
for name, ticker in all_tickers.items():
    try:
        r = requests.get(f"{base_url}/{ticker}/action/capitalisation", headers=headers, timeout=15)
        text = BeautifulSoup(r.text, 'html.parser').get_text()
        data = {'entreprise': name, 'ticker': ticker}
        
        sec = re.search(r'(\w[\w\s]+?)\s*\n\s*Secteur', text)
        data['secteur'] = sec.group(1).replace('actions','').strip() if sec else None

        pe_m = re.search(r'P/E Ratio\s*([\d.,]+)', text)
        data['pe_actuel'] = clean_val(pe_m.group(1)) if pe_m else None
        
        div_m = re.search(r'Rendement Dividende\s*([\d.,]+)%', text)
        data['div_yield_pct'] = clean_val(div_m.group(1)) if div_m else None

        all_details.append(data)
        time.sleep(0.1)
    except:
        continue

# Etape 3 : Fusion et Nettoyage Strict pour compatibilité JSON
df_base = pd.DataFrame(rows_base)
df_details = pd.DataFrame(all_details)

if not df_base.empty:
    df_base['entreprise'] = df_base['entreprise'].str.strip()
    if not df_details.empty:
        df_details['entreprise'] = df_details['entreprise'].str.strip()
        df_final = df_base.merge(df_details, on='entreprise', how='left')
    else:
        df_final = df_base
        for col in ['ticker', 'secteur', 'pe_actuel', 'div_yield_pct']:
            df_final[col] = None

    df_final['date_collecte'] = datetime.now().strftime("%Y-%m-%d")

    # Conversion manuelle pour assurer la conformité JSON (Remplacement des NaN/Inf)
    raw_records = df_final.to_dict(orient='records')
    final_records = []
    
    for record in raw_records:
        clean_record = {}
        for k, v in record.items():
            if isinstance(v, float):
                if np.isnan(v) or np.isinf(v):
                    clean_record[k] = None
                else:
                    clean_record[k] = v
            elif pd.isna(v):
                clean_record[k] = None
            else:
                clean_record[k] = v
        final_records.append(clean_record)

    print(f"Tentative d'insertion de {len(final_records)} lignes...")
    try:
        if final_records:
            supabase.table("bourse_details").insert(final_records).execute()
            print("Données insérées avec succès.")
        else:
            print("Aucune donnée disponible pour l'insertion.")
    except Exception as e:
        print(f"Erreur lors de l'insertion : {e}")
else:
    print("Echec de la collecte des données de base.")
