import requests
import pandas as pd
import re
import time
import os
import numpy as np  # Import nécessaire pour gérer les NaN
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client

# ============================================
# CONFIGURATION SUPABASE
# ============================================
SUPABASE_URL = "https://nbgpxasdgucltfcygqua.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5iZ3B4YXNkZ3VjbHRmY3lncXVhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzMzc1NDAsImV4cCI6MjA4NTkxMzU0MH0.EpLaGobOZxa_VI-_cOBXoDBiB7J-5QaC9vNV4lyNNKc"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {"User-Agent": "Mozilla/5.0"}
base_url = "https://www.casablancabourse.com"

def clean_val(val):
    try:
        if val is None or str(val).strip() in ['', '-', 'N/A']: return None
        res = re.sub(r'[^\d.-]', '', str(val).replace(',', '.'))
        f_val = float(res)
        # Sécurité anti-Infini et anti-NaN
        if np.isnan(f_val) or np.isinf(f_val): return None
        return f_val
    except:
        return None

# --- ÉTAPE 1 : RÉCUPÉRATION DES COURS ---
print("Collecte des cours en cours...")
resp = requests.get(base_url, headers=headers, timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')

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

# --- ÉTAPE 2 : SCRAPING PROFOND ---
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
        time.sleep(0.2)
    except: continue

# --- ÉTAPE 3 : FUSION ET NETTOYAGE JSON ---
df_base = pd.DataFrame(rows_base)
df_details = pd.DataFrame(all_details)

if not df_base.empty and not df_details.empty:
    df_final = df_base.merge(df_details, on='entreprise', how='inner')
    df_final['date_collecte'] = datetime.now().strftime("%Y-%m-%d")
    
    # Remplacement des NaN/Inf par None pour la conformité JSON
    df_final = df_final.replace([np.inf, -np.inf], np.nan)
    records = df_final.where(pd.notnull(df_final), None).to_dict(orient='records')
    
    print(f"Tentative d'insertion de {len(records)} lignes...")
    try:
        supabase.table("bourse_details").insert(records).execute()
        print("✅ Données insérées avec succès !")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion : {e}")
else:
    print("⚠️ Aucune donnée fusionnée.")
