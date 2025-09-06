import re
import time
import json
import pandas as pd
import streamlit as st
import requests
from io import BytesIO, StringIO
from collections import Counter
from difflib import SequenceMatcher
from functools import lru_cache

# ---- Page & layout ----
st.set_page_config(
    page_title="Abdel_SPCA_Nettoyage d'adresses + Nominatim",
    page_icon="🧹",
    layout="wide",
    menu_items={"Get Help": None, "Report a bug": None, "About": None},
)

# ---- CSS ----
st.markdown("""
<style>
.main .block-container {max-width: 1280px; padding-top: 1.3rem; padding-bottom: 2.6rem;}
h1 span.app-title {display:inline-block; font-weight: 800; letter-spacing:.2px;}
p.sub {margin-top:-6px; color:#6b7280;}
div[data-testid="stFileUploader"] > section {border:1px dashed #d1d5db; border-radius:14px; padding:18px 16px;}
.stButton>button, .stDownloadButton>button {border-radius:12px; padding:.6rem 1rem; font-weight:600;}
.badge {display:inline-block;background:#eef2ff;color:#4338ca;border:1px solid #c7d2fe;
        padding:2px 8px;border-radius:999px;font-size:12px;margin-right:6px;}
.diff {font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono","Courier New", monospace;
       font-size: 0.9rem; line-height:1.4; }
.ins {background: #dcfce7; text-decoration:none;}
.del {background: #fee2e2; text-decoration:line-through;}
.eq  {background: transparent;}
footer, #MainMenu {visibility:hidden;}
</style>
""", unsafe_allow_html=True)

# ---- Header ----
st.markdown('<h1>🧹 <span class="app-title">Nettoyage d’adresses</span></h1>', unsafe_allow_html=True)
st.markdown('<p class="sub">Pipeline local robuste + normalisation en ligne (Nominatim/OSM). Export, comparaison et stats inclus.</p>', unsafe_allow_html=True)

# ==================================
#  LECTURE ROBUSTE CSV / EXCEL
# ==================================
def read_any(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file)

    uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file)
        if df.shape[1] == 1:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, sep=';', engine='python')
        return df
    except Exception:
        pass

    uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        if df.shape[1] == 1:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig', sep=';', engine='python')
        return df
    except Exception:
        pass

    uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file, encoding='latin-1')
        if df.shape[1] == 1:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='latin-1', sep=';', engine='python')
        return df
    except Exception:
        pass

    uploaded_file.seek(0)
    data = uploaded_file.read()
    if not data:
        raise pd.errors.EmptyDataError("Fichier vide.")
    text = data.decode('utf-8', errors='ignore')
    sep = ';' if text.count(';') > text.count(',') else ','
    return pd.read_csv(StringIO(text), sep=sep, engine='python')

# ==================================
#  PIPELINE LOCAL RENFORCÉ
# ==================================
WORDS_TO_REMOVE = ["Canada","QC","Québec","Montréal","Qc","Quebec","Montreal"]
POSTAL_CODE_RE = r'\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b'

NOMS_FEMININS = ["Anne","Catherine","Claire","Élisabeth","Geneviève","Hélène","Jacqueline","Jeanne",
                 "Julie","Lucie","Marguerite","Marie","Marthe","Thérèse","Adèle","Angèle","Ariane",
                 "Audrey","Béatrice","Caroline","Christine","Colette","Diane","Émilie","Florence",
                 "Gabrielle","Isabelle","Joséphine","Louise","Madeleine","Mathilde","Pauline",
                 "Rosalie","Simone","Suzanne","Valérie"]

VOIE_MAPPING_FULL = {
    "Av": "Avenue", "Ave": "Avenue", "Ave.": "Avenue", "Av.": "Avenue", "Avé": "Avenue",
    "Blvd": "Boulevard", "BVD": "Boulevard", "Bve": "Boulevard", "Boul": "Boulevard", "Bl": "Boulevard",
    "Ch": "Chemin", "Cte": "Côte", "Prom": "Promenade", "Terr": "Terrasse", "Pl": "Place", "Rg": "Rang",
    "Cr": "Crois", "Crois": "Croissant", "Cres": "Croissant", "Cres.": "Croissant",
    "Rt": "Route", "Rd": "Route", "Rd.": "Route",
    "V": "Voie",
    "St": "Street", "St.": "Street",
    "Dr": "Drive", "Dr.": "Drive",
    "Ln": "Lane", "Ln.": "Lane",
    "Hwy": "Highway", "Hwy.": "Highway",
    "Ct": "Court", "Ct.": "Court",
    "Pl.": "Place",
}

DIRECTION_MAPPING = {
    r'\bEst\b':'E', r'\bOuest\b':'O', r'\bNord\b':'N', r'\bSud\b':'S',
    r'\bEast\b':'E', r'\bWest\b':'W', r'\bNorth\b':'N', r'\bSouth\b':'S'
}

ACCENT_CORRECTIONS = {
    "Ecole":"École","Erables":"Érables","Montreal":"Montréal","Trois Rivieres":"Trois-Rivières"
}

COMPOUND_CORRECTIONS = {
    r'\bCote St Luc\b': 'Côte-Saint-Luc',
    r'\bCote Saint Luc\b': 'Côte-Saint-Luc',
    r'\bSt Charles Sur Richeli(e|eu)?\b': 'Saint-Charles-sur-Richelieu',
    r'\bSt[- ]Laurent\b': 'Saint-Laurent',
    r'\bSte[- ]Foy\b': 'Sainte-Foy',
    r"\bL Ile\b": "L’Île",
}

UNIT_TERMS = [
    "App","Apt","Appt","Appartement","Unit","Unité","Logement","Suite","Apartment",
    "app","apt","appt","unit","suite","no","n0","#"
]

KEEP_UPPER = {"N","S","E","O","NE","NO","SE","SO","W","NW","SW",
              "QC","ON","BC","AB","SK","MB","NB","NS","NL","PE","YT","NT","NU"}

STREET_TYPES_RE = r'(Rue|Avenue|Boulevard|Chemin|Place|Terrasse|Voie|Allée|Promenade|Côte|Rang|Route|Croissant|Crois|Street|Road|Drive|Lane|Court|Highway|Way|Trail|Esplanade)'

def clean_text(text):
    if pd.isna(text): return None
    text = re.sub(r'[.,;:/#&@"*|]', ' ', str(text))
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_address(address):
    if pd.isna(address): return address
    for w in WORDS_TO_REMOVE:
        address = re.sub(r'\b' + re.escape(w) + r'\b','',address, flags=re.IGNORECASE).strip()
    address = re.sub(POSTAL_CODE_RE,'',address).strip()
    address = re.sub(r'\s+',' ',address).strip()
    return address

def remove_inline_unit_terms(address):
    if pd.isna(address): return address
    pattern = r'\b(?:' + '|'.join(map(re.escape, UNIT_TERMS)) + r')\.?\b'
    address = re.sub(pattern,'',address, flags=re.IGNORECASE)
    address = re.sub(r'\s+',' ',address).strip()
    return address

def capitalize_letter_after_number(address):
    if pd.isna(address): return address
    return re.sub(r'(\d+)([a-z])\b', lambda m: f"{m.group(1)}{m.group(2).upper()}", address)

def replace_cardinal_directions(address):
    if pd.isna(address): return address
    for pat, rep in DIRECTION_MAPPING.items():
        address = re.sub(pat, rep, address)
    address = re.sub(r'\b([NSEOW])\.\b', r'\1', address)
    return address

def replace_st_with_saint_or_sainte(address):
    if pd.isna(address): return address
    m = re.search(r'\bSt-([A-Za-zÉéÈèÀàÙù]+)', address)
    if m:
        following = m.group(1)
        return re.sub(r'\bSt-', "Sainte-" if following in NOMS_FEMININS else "Saint-", address)
    return address

def expand_abbreviations(address):
    if pd.isna(address): return address
    s = address
    for abbr, full in VOIE_MAPPING_FULL.items():
        s = re.sub(r'\b' + re.escape(abbr) + r'\b', full, s, flags=re.IGNORECASE)
    s = re.sub(r'\bCote St Luc Route\b', 'Chemin Cote St Luc', s, flags=re.IGNORECASE)
    return s

def correct_accents(address):
    if pd.isna(address): return address
    s = address
    for typo, corr in ACCENT_CORRECTIONS.items():
        s = re.sub(r'\b' + re.escape(typo) + r'\b', corr, s)
    return s

def correct_compounds(address):
    if pd.isna(address): return address
    s = address
    for pat, corr in COMPOUND_CORRECTIONS.items():
        s = re.sub(pat, corr, s, flags=re.IGNORECASE)
    return s

def normalize_hyphens_apostrophes(address):
    if pd.isna(address): return address
    address = re.sub(r'\s*-\s*', '-', address)
    address = re.sub(r"'", "’", address)
    return address

def standardize_ordinal_suffix(address):
    if pd.isna(address): return address
    address = re.sub(r'\b1([èeé]re|er|re)\b', '1RE', address, flags=re.IGNORECASE)
    for n in range(2, 10):
        address = re.sub(rf'\b{n}([ìi]eme|ieme|ième|[èeé]me|e)\b', f'{n}E', address, flags=re.IGNORECASE)
    address = re.sub(r'\b([1-9][0-9])([èeé]me|e)\b', lambda m: f"{m.group(1)}E", address, flags=re.IGNORECASE)
    return address

def move_trailing_apt_to_front(address):
    if pd.isna(address): return address
    m = re.search(r'(.+?)\s+(\d+)$', address)
    if m:
        street_part, apt_number = m.group(1), m.group(2)
        first_word = street_part.split()[0]
        if first_word.isdigit():
            return f"{apt_number}-{first_word} {street_part[len(first_word):].strip()}"
    return address

def remove_final_duplicate_number(address):
    if pd.isna(address): return address
    m = re.match(r'^(\d+)[\-\s](\d+)\s+(.*?)(?:\s+(\d+))$', address)
    if m:
        first_num, second_num, street, last_num = m.group(1), m.group(2), m.group(3).strip(), m.group(4)
        if first_num == last_num:
            return f"{first_num}-{second_num} {street}"
    return address

def remove_unit_terms_tail(address):
    if pd.isna(address): return address
    tail_pat = r'\b(?:' + '|'.join(map(re.escape, UNIT_TERMS)) + r')\.?\s*\d*\s*$'
    return re.sub(tail_pat, '', address, flags=re.IGNORECASE).strip()

def ensure_street_type_if_missing(address):
    if pd.isna(address): return address
    has_type = re.search(r'\b' + STREET_TYPES_RE + r'\b', address, flags=re.IGNORECASE)
    if has_type:
        return address
    m = re.match(r'^\s*(\d+[A-Za-z]?)\s+([A-Za-zÀ-ÖØ-öø-ÿ\-’]+(?:\s+[A-Za-zÀ-ÖØ-öø-ÿ\-’]+)*)$', address)
    if m:
        civic, name = m.group(1), m.group(2)
        if not re.search(r'\b(P\.?O\.?\s*Box|BP|Case)\b', name, flags=re.IGNORECASE):
            return f"{civic} Rue {name}"
    return address

def remove_duplicate_words_numbers(address):
    if pd.isna(address): return address
    words = address.split()
    seen, out = set(), []
    for w in words:
        lw = w.lower()
        if lw not in seen:
            out.append(w); seen.add(lw)
    return " ".join(out)

def title_preserve_tokens(address):
    if pd.isna(address): return address
    t = address.title()
    t = re.sub(r'\b(\d+R?E)\b', lambda m: m.group(1).upper(), t)
    def fix_token(m):
        tok = m.group(0); up = tok.upper()
        return up if up in KEEP_UPPER else tok
    t = re.sub(r'\b([A-Za-z]{1,3})\b', fix_token, t)
    return t

def clean_pipeline(address):
    if pd.isna(address): return address
    address = clean_text(address)
    address = clean_address(address)
    address = remove_inline_unit_terms(address)
    address = capitalize_letter_after_number(address)
    address = replace_cardinal_directions(address)
    address = replace_st_with_saint_or_sainte(address)
    address = expand_abbreviations(address)
    address = correct_accents(address)
    address = correct_compounds(address)
    address = normalize_hyphens_apostrophes(address)
    address = standardize_ordinal_suffix(address)
    address = move_trailing_apt_to_front(address)
    address = remove_final_duplicate_number(address)
    address = remove_unit_terms_tail(address)
    address = ensure_street_type_if_missing(address)
    address = remove_duplicate_words_numbers(address)
    address = title_preserve_tokens(address)
    return address

# ==================================
#  DÉTECTION AUTO DE COLONNE
# ==================================
def normalize_colname(c: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(c).strip().lower())

PREFERRED_KEYS = ["rue","adresse","address","street","street1","street_1","addr","address1","ligne1","line1"]

def find_address_column(df: pd.DataFrame) -> str:
    norm_map = {c: normalize_colname(c) for c in df.columns}
    for key in PREFERRED_KEYS:
        keyn = normalize_colname(key)
        for col, norm in norm_map.items():
            if norm == keyn:
                return col
    key_frags = ["rue","adress","address","street","addr"]
    candidates = [col for col, norm in norm_map.items() if any(k in norm for k in key_frags)]
    if candidates:
        return max(candidates, key=lambda c: df[c].notna().sum())
    raise ValueError("Colonne d'adresse introuvable. Colonnes : " + ", ".join(map(str, df.columns)))

# ==================================
#  NOMINATIM (OSM) — Normalisation
# ==================================
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Respect du rate-limit : 1 req/s
if "last_call_ts" not in st.session_state:
    st.session_state.last_call_ts = 0.0

def _respect_rate_limit(min_interval=1.0):
    now = time.time()
    elapsed = now - st.session_state.last_call_ts
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    st.session_state.last_call_ts = time.time()

@lru_cache(maxsize=5000)
def nominatim_lookup(q: str, user_agent: str):
    """
    Retour brut JSON de Nominatim (cache par @lru_cache).
    """
    _respect_rate_limit(1.0)
    headers = {"User-Agent": user_agent or "address-normalizer-example/1.0 (contact: you@example.com)"}
    params = {
        "q": q,
        "format": "jsonv2",
        "countrycodes": "ca",
        "addressdetails": 1,
        "limit": 1,
    }
    r = requests.get(NOMINATIM_URL, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    return None

def build_canadian_string_from_osm(addr: dict) -> str:
    """
    Construit une chaîne 'canadian-like' à partir de address OSM:
      'house_number street road, city, province, postcode'
    """
    if not addr:
        return ""
    hn = addr.get("house_number", "")
    road = addr.get("road", "") or addr.get("pedestrian", "") or addr.get("footway", "")
    # directionnel si dispo
    predir = addr.get("road_direction", "")  # rarement présent
    city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality") or ""
    province = (addr.get("state_code") or "").upper() or addr.get("state") or ""
    postcode = addr.get("postcode", "")

    left = " ".join([x for x in [hn, predir, road] if x]).strip()
    right = ", ".join([x for x in [city, province, postcode] if x]).strip(", ")
    return f"{left}{(', ' + right) if right else ''}".strip()

def normalize_with_nominatim(address: str, user_agent: str) -> str | None:
    """
    1) Requête Nominatim avec l'adresse (FR/EN).
    2) Si résultat, formate en chaîne canadienne simple.
    3) Retourne None si pas de résultat.
    """
    if not address or not address.strip():
        return None
    try:
        res = nominatim_lookup(address, user_agent)
        if not res or "address" not in res:
            return None
        candidate = build_canadian_string_from_osm(res["address"])
        return candidate if candidate else (res.get("display_name") or None)
    except Exception:
        return None

# ==================================
#  OUTILS COMPARAISON
# ==================================
def diff_html(a: str, b: str) -> str:
    a = "" if pd.isna(a) else str(a)
    b = "" if pd.isna(b) else str(b)
    sm = SequenceMatcher(a=a, b=b)
    out = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == 'equal':
            out.append(f'<span class="eq">{b[j1:j2]}</span>')
        elif tag == 'insert':
            out.append(f'<span class="ins">{b[j1:j2]}</span>')
        elif tag == 'delete':
            out.append(f'<span class="del">{a[i1:i2]}</span>')
        elif tag == 'replace':
            out.append(f'<span class="del">{a[i1:i2]}</span><span class="ins">{b[j1:j2]}</span>')
    return '<div class="diff">' + "".join(out).replace(" ", "&nbsp;") + '</div>'

# ==================================
#  UI — SIDEBAR (Nominatim)
# ==================================
st.sidebar.header("🔗 Normalisation en ligne (OSM/Nominatim)")
use_nominatim = st.sidebar.checkbox("Activer la normalisation en ligne", value=True)
user_agent = st.sidebar.text_input(
    "User-Agent (requis par Nominatim — mets un email de contact)",
    value="nettoyage-adresses-app/1.0 (contact: you@example.com)"
)
st.sidebar.caption("⚠️ Respecte le quota : ~1 requête/seconde. Le traitement peut être plus lent avec l’API en ligne.")

# ==================================
#  UI PRINCIPALE
# ==================================
st.caption("Formats supportés : CSV / XLSX • Limite ~200 MB par fichier")
uploaded = st.file_uploader("Importer un fichier", type=["csv","xlsx"], label_visibility="collapsed")

with st.expander("📎 Conseils", expanded=False):
    st.markdown("""
    - Le fichier doit contenir **au moins une colonne d’adresse** (`Rue`, `Address`, `Adresse`…).
    - La sortie ajoute **`Rue_corrigee`** (pipeline local) et **`Rue_normalisee`** (si Nominatim activé).
    - Aucune autre colonne n’est supprimée (ex. `donorbox receipt`, `constituant id`).
    - OSM **≈** normalisation “meilleure effort” (gratuite) — pas 100% CPC/AddressComplete.
    """)

if not uploaded:
    st.info("👆 Déposez votre fichier pour commencer.")
    st.stop()

# Lecture
try:
    df = read_any(uploaded)
except Exception as e:
    st.error(f"Impossible de lire le fichier : {e}")
    st.stop()

cols = list(df.columns)
st.markdown("Colonnes détectées : " + " ".join([f'<span class="badge">{c}</span>' for c in cols]), unsafe_allow_html=True)

# Détection auto + override
try:
    auto_col = find_address_column(df)
except Exception:
    auto_col = cols[0]
col_rue = st.selectbox("Colonne à nettoyer :", options=cols, index=cols.index(auto_col) if auto_col in cols else 0)

# --- Tabs ---
tab_clean, tab_compare, tab_stats = st.tabs(["✨ Nettoyage", "🪄 Comparaison", "📊 Stats"])

with tab_clean:
    st.write("Aperçu initial :")
    st.dataframe(df.head(), use_container_width=True)

    if st.button("Lancer le nettoyage", type="primary"):
        with st.spinner("Nettoyage (local) en cours…"):
            df["Rue_corrigee"] = df[col_rue].apply(clean_pipeline)

        if use_nominatim:
            with st.spinner("Normalisation en ligne (OSM/Nominatim)…"):
                # On normalise à partir de la version corrigée locale (plus propre pour le matching)
                df["Rue_normalisee"] = df["Rue_corrigee"].astype(str).apply(
                    lambda s: normalize_with_nominatim(s, user_agent)
                )
                # fallback: si None, on tente l’adresse d’origine
                mask_none = df["Rue_normalisee"].isna() | (df["Rue_normalisee"] == "")
                if mask_none.any():
                    df.loc[mask_none, "Rue_normalisee"] = df.loc[mask_none, col_rue].astype(str).apply(
                        lambda s: normalize_with_nominatim(s, user_agent)
                    )

        # Stats simples
        diff_local = (df[col_rue].fillna("").astype(str).str.strip()
                      != df["Rue_corrigee"].fillna("").astype(str).str.strip()).sum()
        msg = f"Terminé ✅  |  Lignes: {len(df):,}  •  Modifiées (local): {diff_local:,}"
        if use_nominatim and "Rue_normalisee" in df:
            filled = df["Rue_normalisee"].fillna("").astype(str).str.strip() != ""
            msg += f"  •  Normalisées (OSM): {filled.sum():,}"
        st.success(msg)

        # Aperçu
        show_cols = [col_rue, "Rue_corrigee"]
        if use_nominatim and "Rue_normalisee" in df.columns:
            show_cols.append("Rue_normalisee")
        st.write("Aperçu :")
        st.dataframe(df[show_cols].head(30), use_container_width=True)

        # Exports
        c1, c2 = st.columns(2)
        with c1:
            csv_bytes = df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button("⬇️ Télécharger CSV", data=csv_bytes,
                               file_name="adresses_nettoyees.csv", mime="text/csv")
        with c2:
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Adresses")
            st.download_button("⬇️ Télécharger Excel", data=buf.getvalue(),
                               file_name="adresses_nettoyees.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with tab_compare:
    st.markdown("Compare **avant / après** (local) et, si activé, **OSM**. Surlignage : <span class='ins'>ajouts</span>, <span class='del'>suppressions</span>", unsafe_allow_html=True)

    if "Rue_corrigee" not in df.columns:
        st.warning("⚠️ Lance d’abord le nettoyage dans l’onglet **Nettoyage**.")
    else:
        only_changed = st.checkbox("Afficher uniquement les lignes modifiées (local)", value=True)
        limit = st.slider("Nombre de lignes à afficher", 10, 500, 100, 10)

        view = df.copy()
        changed_mask = (view[col_rue].fillna("").astype(str).str.strip()
                        != view["Rue_corrigee"].fillna("").astype(str).str.strip())
        if only_changed:
            view = view[changed_mask]

        sample = view.head(limit)

        rows = []
        has_osm = "Rue_normalisee" in df.columns
        for _, r in sample.iterrows():
            a = str(r[col_rue])
            b = str(r["Rue_corrigee"])
            local_html = diff_html(a, b)
            osm_html = diff_html(b, str(r["Rue_normalisee"])) if has_osm else ""
            rows.append(f"""
            <tr>
              <td>{a}</td>
              <td>{b}</td>
              <td>{local_html}</td>
              <td>{(str(r["Rue_normalisee"]) if has_osm else '')}</td>
              <td>{(osm_html if has_osm else '')}</td>
            </tr>""")

        header_osm = "<th>Rue_normalisee</th><th>Diff. Corrigée ➜ OSM</th>" if has_osm else "<th></th><th></th>"
        html_table = f"""
        <table style="width:100%; border-collapse:collapse;">
          <thead>
            <tr style="text-align:left; border-bottom:1px solid #e5e7eb;">
              <th style="padding:6px 4px;">{col_rue}</th>
              <th style="padding:6px 4px;">Rue_corrigee</th>
              <th style="padding:6px 4px;">Diff. Original ➜ Corrigée</th>
              {header_osm}
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
        """
        st.markdown(html_table, unsafe_allow_html=True)

with tab_stats:
    st.markdown("**Stats** : modifications locales, et (si activé) taux de normalisation OSM.")

    if "Rue_corrigee" not in df.columns:
        st.warning("⚠️ Lance d’abord le nettoyage.")
    else:
        mod_count = (df[col_rue].fillna("").astype(str).str.strip()
                     != df["Rue_corrigee"].fillna("").astype(str).str.strip()).sum()
        pct_mod = 100.0 * mod_count / len(df) if len(df) else 0.0
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total lignes", f"{len(df):,}")
        with m2:
            st.metric("Modifiées (local)", f"{mod_count:,}", delta=f"{pct_mod:.1f}%")
        with m3:
            if "Rue_normalisee" in df.columns:
                ok = df["Rue_normalisee"].fillna("").astype(str).str.strip() != ""
                st.metric("Normalisées (OSM)", f"{ok.sum():,}")
            else:
                st.metric("Normalisées (OSM)", "—")
