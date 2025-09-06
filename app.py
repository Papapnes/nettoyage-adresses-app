import re
import pandas as pd
import streamlit as st
from io import BytesIO

# ---- Page & layout ----
st.set_page_config(
    page_title="Abdel_SPCA_Nettoyage d'adresses",
    page_icon="🧹",
    layout="centered",
    menu_items={"Get Help": None, "Report a bug": None, "About": None},
)

# ---- CSS minimal (sans dépendances) ----
st.markdown("""
<style>
.main .block-container {max-width: 980px; padding-top: 2rem; padding-bottom: 4rem;}
h1 span.app-title {display:inline-block; font-weight: 800; letter-spacing:.2px;}
p.sub {margin-top:-6px; color:#6b7280;}
div[data-testid="stFileUploader"] > section {border:1px dashed #d1d5db; border-radius:14px; padding:18px 16px;}
.stButton>button, .stDownloadButton>button {border-radius:12px; padding:.7rem 1.2rem; font-weight:600;}
.dataframe tbody td, .dataframe th {font-size:0.92rem;}
.badge {display:inline-block;background:#eef2ff;color:#4338ca;border:1px solid #c7d2fe;
        padding:2px 8px;border-radius:999px;font-size:12px;margin-right:6px;}
footer, #MainMenu {visibility:hidden;}
</style>
""", unsafe_allow_html=True)

# ---- En-tête ----
st.markdown('<h1>🧹 <span class="app-title">Nettoyage d’adresses</span></h1>', unsafe_allow_html=True)
st.markdown('<p class="sub">Importez votre fichier CSV/XLSX, corrigez les adresses en 1 clic, puis téléchargez les résultats.</p>', unsafe_allow_html=True)

# ---------- Dictionnaires / paramètres (pipeline renforcé) ----------
WORDS_TO_REMOVE = ["Canada","QC","Québec","Montréal","Qc","Quebec","Montreal"]
POSTAL_CODE_RE = r'\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b'

NOMS_FEMININS = ["Anne","Catherine","Claire","Élisabeth","Geneviève","Hélène","Jacqueline","Jeanne",
                 "Julie","Lucie","Marguerite","Marie","Marthe","Thérèse","Adèle","Angèle","Ariane",
                 "Audrey","Béatrice","Caroline","Christine","Colette","Diane","Émilie","Florence",
                 "Gabrielle","Isabelle","Joséphine","Louise","Madeleine","Mathilde","Pauline",
                 "Rosalie","Simone","Suzanne","Valérie"]

VOIE_MAPPING_FULL = {
    # français
    "Av": "Avenue", "Ave": "Avenue", "Ave.": "Avenue", "Av.": "Avenue", "Avé": "Avenue",
    "Blvd": "Boulevard", "BVD": "Boulevard", "Bve": "Boulevard", "Boul": "Boulevard", "Bl": "Boulevard",
    "Ch": "Chemin", "Cte": "Côte", "Prom": "Promenade", "Terr": "Terrasse", "Pl": "Place", "Rg": "Rang",
    "Cr": "Crois", "Crois": "Croissant", "Cres": "Croissant", "Cres.": "Croissant",
    "Rt": "Route", "Rd": "Route", "Rd.": "Route",
    "V": "Voie",
    # anglais génériques
    "St": "Street", "St.": "Street",    # 'St-' avec tiret géré ailleurs (Saint/Sainte)
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

# ---------- Fonctions du pipeline ----------
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
    # enlever N./E./O./S./W.
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
    # cas spécifique
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
    address = re.sub(r'\s*-\s*', '-', address)  # normaliser espaces autour du tiret
    address = re.sub(r"'", "’", address)        # apostrophe française
    return address

def standardize_ordinal_suffix(address):
    if pd.isna(address): return address
    # 1er/1re -> 1RE
    address = re.sub(r'\b1([èeé]re|er|re)\b', '1RE', address, flags=re.IGNORECASE)
    # 2e..9e
    for n in range(2, 10):
        address = re.sub(rf'\b{n}([ìi]eme|ieme|ième|[èeé]me|e)\b', f'{n}E', address, flags=re.IGNORECASE)
    # 10e+
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
    """
    Si l'adresse commence par n° civique + nom simple sans type de voie,
    insère 'Rue' (ex: 211 Myconos -> 211 Rue Myconos)
    """
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
    # préserver ordinaux (1RE/2E/…)
    t = re.sub(r'\b(\d+R?E)\b', lambda m: m.group(1).upper(), t)
    # préserver points cardinaux/provinces
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

# ---------- Détection auto de la colonne d’adresse ----------
def normalize_colname(c: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(c).strip().lower())

PREFERRED_KEYS = ["rue","adresse","address","street","street1","street_1","addr","address1","ligne1","line1"]

def find_address_column(df: pd.DataFrame) -> str:
    norm_map = {c: normalize_colname(c) for c in df.columns}
    # 1) correspondance EXACTE par priorité
    for key in PREFERRED_KEYS:
        keyn = normalize_colname(key)
        for col, norm in norm_map.items():
            if norm == keyn:
                return col
    # 2) correspondance PARTIELLE
    key_frags = ["rue","adress","address","street","addr"]
    candidates = [col for col, norm in norm_map.items() if any(k in norm for k in key_frags)]
    if candidates:
        return max(candidates, key=lambda c: df[c].notna().sum())
    # 3) échec
    raise ValueError("Colonne d'adresse introuvable. Colonnes disponibles : " + ", ".join(map(str, df.columns)))

# ---------- UI ----------
st.caption("Formats supportés : CSV / XLSX • Limite ~200 MB par fichier")

with st.container():
    uploaded = st.file_uploader("Importer un fichier", type=["csv","xlsx"], label_visibility="collapsed")

with st.expander("📎 Comment préparer mon fichier ?", expanded=False):
    st.markdown("""
    - Le fichier doit contenir **au moins une colonne d’adresse** (ex. `Rue`, `Address`, `Adresse`).
    - Une nouvelle colonne **`Rue_corrigee`** sera ajoutée avec le résultat.
    - Aucune autre colonne n’est supprimée (ex. `donorbox receipt`, `constituant id`).
    """)

if uploaded:
    # lecture robuste CSV (essaie "," puis ";")
    if uploaded.name.lower().endswith(".csv"):
        try:
            df = pd.read_csv(uploaded)
            if df.shape[1] == 1:  # mauvais séparateur probable
                df = pd.read_csv(uploaded, sep=';')
        except Exception:
            df = pd.read_csv(uploaded, sep=';')
    else:
        df = pd.read_excel(uploaded)

    # aperçu + badges colonnes
    st.write("Aperçu :")
    st.dataframe(df.head(), use_container_width=True)
    cols = list(df.columns)
    st.markdown("Colonnes détectées : " + " ".join([f'<span class="badge">{c}</span>' for c in cols]), unsafe_allow_html=True)

    # sélection intelligente (auto + override manuel)
    try:
        auto_col = find_address_column(df)
    except Exception:
        auto_col = cols[0]
    col_rue = st.selectbox("Colonne à nettoyer :", options=cols, index=cols.index(auto_col) if auto_col in cols else 0)

    # action
    if st.button("✨ Corriger"):
        with st.spinner("Nettoyage en cours…"):
            df["Rue_corrigee"] = df[col_rue].apply(clean_pipeline)

        # stats simples
        diff_count = (df[col_rue].fillna("").astype(str).str.strip()
                      != df["Rue_corrigee"].fillna("").astype(str).str.strip()).sum()
        st.success(f"Terminé ✅  |  Lignes: {len(df):,}  •  Modifiées: {diff_count:,}")

        st.write("Aperçu des corrections :")
        st.dataframe(df[[col_rue, "Rue_corrigee"]].head(30), use_container_width=True)

        # téléchargements (2 boutons)
        csv_bytes = df.to_csv(index=False, encoding="utf-8-sig")
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Adresses")

        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Télécharger CSV corrigé",
                               data=csv_bytes, file_name="adresses_corrigees.csv", mime="text/csv")
        with c2:
            st.download_button("⬇️ Télécharger Excel corrigé",
                               data=buffer.getvalue(),
                               file_name="adresses_corrigees.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("👆 Déposez votre fichier pour commencer (ou cliquez sur **Browse files**).")
