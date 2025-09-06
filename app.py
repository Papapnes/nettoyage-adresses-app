import re
import pandas as pd
import streamlit as st
from io import BytesIO, StringIO
from collections import Counter
from difflib import SequenceMatcher

# ---- Page & layout ----
st.set_page_config(
    page_title="Abdel_appy_Clean_SPCA",
    page_icon="🧹",
    layout="centered",
    menu_items={"Get Help": None, "Report a bug": None, "About": None},
)


# ---- CSS minimal ----
st.markdown("""
<style>
.main .block-container {max-width: 1280px; padding-top: 1.5rem; padding-bottom: 3rem;}
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

# ---- En-tête ----
st.markdown("""
<h1>
🧹 <span class="app-title">Abdel_Data_Analyste_</span>
<span style="font-size:1.2em; color:#ae0f27; font-weight:900;">SPCA</span>
<span class="app-title"></span>
</h1>
""", unsafe_allow_html=True)
st.markdown('<p class="sub">Importez votre fichier CSV/XLSX, corrigez les adresses en 1 clic, puis téléchargez les résultats.</p>', unsafe_allow_html=True)

# ============================
#  LECTURE ROBUSTE DE FICHIERS
# ============================
def read_any(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    # Excel
    if name.endswith((".xlsx", ".xls")):
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file)

    # CSV — 1) UTF-8, ',' puis ';'
    uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file)
        if df.shape[1] == 1:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, sep=';', engine='python')
        return df
    except Exception:
        pass

    # 2) UTF-8-SIG
    uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        if df.shape[1] == 1:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig', sep=';', engine='python')
        return df
    except Exception:
        pass

    # 3) latin-1
    uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file, encoding='latin-1')
        if df.shape[1] == 1:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, encoding='latin-1', sep=';', engine='python')
        return df
    except Exception:
        pass

    # 4) Fallback : sép. dominant
    uploaded_file.seek(0)
    data = uploaded_file.read()
    if not data:
        raise pd.errors.EmptyDataError("Fichier vide.")
    text = data.decode('utf-8', errors='ignore')
    sep = ';' if text.count(';') > text.count(',') else ','
    return pd.read_csv(StringIO(text), sep=sep, engine='python')

# ======================
#  PIPELINE RENFORCÉ
# ======================
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

# -- Étapes du pipeline (fonctions pures) --
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

# --- Pipeline simple (prod) ---
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

# --- Pipeline avec stats (diagnostic par règle) ---
RULES = [
    ("01_clean_text", clean_text),
    ("02_clean_geo_postal", clean_address),
    ("03_remove_unit_terms", remove_inline_unit_terms),
    ("04_cap_after_number", capitalize_letter_after_number),
    ("05_cardinals", replace_cardinal_directions),
    ("06_Stdash_to_Saint", replace_st_with_saint_or_sainte),
    ("07_expand_abbrev", expand_abbreviations),
    ("08_fix_accents", correct_accents),
    ("09_fix_compounds", correct_compounds),
    ("10_norm_hyphen_apos", normalize_hyphens_apostrophes),
    ("11_ordinals", standardize_ordinal_suffix),
    ("12_move_trailing_number", move_trailing_apt_to_front),
    ("13_drop_final_dupnum", remove_final_duplicate_number),
    ("14_remove_unit_tail", remove_unit_terms_tail),
    ("15_insert_default_Rue", ensure_street_type_if_missing),
    ("16_dedupe_tokens", remove_duplicate_words_numbers),
    ("17_title_preserve", title_preserve_tokens),
]

def run_pipeline_with_stats(s: str):
    """
    Retourne (final_string, set(des_noms_de_regles_appliquees))
    """
    applied = []
    cur = s
    for name, fn in RULES:
        before = cur
        cur = fn(cur)
        if before != cur:
            applied.append(name)
    return cur, applied

# ============================
#  DÉTECTION AUTO DE COLONNE
# ============================
def normalize_colname(c: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(c).strip().lower())

PREFERRED_KEYS = ["rue","adresse","address","street","street1","street_1","addr","address1","ligne1","line1"]

def find_address_column(df: pd.DataFrame) -> str:
    norm_map = {c: normalize_colname(c) for c in df.columns}
    # EXACT
    for key in PREFERRED_KEYS:
        keyn = normalize_colname(key)
        for col, norm in norm_map.items():
            if norm == keyn:
                return col
    # PARTIAL
    key_frags = ["rue","adress","address","street","addr"]
    candidates = [col for col, norm in norm_map.items() if any(k in norm for k in key_frags)]
    if candidates:
        return max(candidates, key=lambda c: df[c].notna().sum())
    raise ValueError("Colonne d'adresse introuvable. Colonnes : " + ", ".join(map(str, df.columns)))

# ==================
#  OUTILS COMPARAISON
# ==================
def diff_html(a: str, b: str) -> str:
    """
    Surlignage caractère-par-caractère (SequenceMatcher).
    rouge = supprimé, vert = ajouté, normal = inchangé
    """
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

# ==================
#  UI PRINCIPALE
# ==================
st.caption("Formats supportés : CSV / XLSX • Limite ~200 MB par fichier")

uploaded = st.file_uploader("Importer un fichier", type=["csv","xlsx"], label_visibility="collapsed")

with st.expander("📎 Conseils", expanded=False):
    st.markdown("""
    - Le fichier doit contenir **au moins une colonne d’adresse** (ex. `Rue`, `Address`, `Adresse`).
    - La sortie ajoute une colonne **`Rue_corrigee`**.
    - Aucune autre colonne n’est supprimée (ex. `donorbox receipt`, `constituant id`).
    """)

if not uploaded:
    st.info("👆 Déposez votre fichier pour commencer (ou cliquez sur **Browse files**).")
    st.stop()

# --- Lecture robuste ---
try:
    df = read_any(uploaded)
except Exception as e:
    st.error(f"Impossible de lire le fichier : {e}")
    st.stop()

cols = list(df.columns)
st.markdown("Colonnes détectées : " + " ".join([f'<span class="badge">{c}</span>' for c in cols]), unsafe_allow_html=True)

# Détection auto + override utilisateur
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
        with st.spinner("Nettoyage en cours…"):
            df["Rue_corrigee"] = df[col_rue].apply(clean_pipeline)
        diff_count = (df[col_rue].fillna("").astype(str).str.strip()
                      != df["Rue_corrigee"].fillna("").astype(str).str.strip()).sum()
        st.success(f"Terminé ✅  |  Lignes: {len(df):,}  •  Modifiées: {diff_count:,}")
        st.write("Aperçu des corrections :")
        st.dataframe(df[[col_rue, "Rue_corrigee"]].head(30), use_container_width=True)

        # exports
        c1, c2 = st.columns(2)
        with c1:
            csv_bytes = df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button("⬇️ Télécharger CSV corrigé", data=csv_bytes,
                               file_name="adresses_corrigees.csv", mime="text/csv")
        with c2:
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Adresses")
            st.download_button("⬇️ Télécharger Excel corrigé", data=buf.getvalue(),
                               file_name="adresses_corrigees.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with tab_compare:
    st.markdown("Compare **avant / après** avec surlignage : <span class='ins'>ajouts</span>, <span class='del'>suppressions</span>", unsafe_allow_html=True)

    # S'assurer que Rue_corrigee existe
    if "Rue_corrigee" not in df.columns:
        st.warning("⚠️ Lance d’abord le nettoyage dans l’onglet **Nettoyage**.")
    else:
        only_changed = st.checkbox("Afficher uniquement les lignes modifiées", value=True)
        search = st.text_input("Filtrer (contient)", "")
        limit = st.slider("Nombre de lignes à afficher", min_value=10, max_value=500, value=100, step=10)

        view = df.copy()
        changed_mask = (view[col_rue].fillna("").astype(str).str.strip()
                        != view["Rue_corrigee"].fillna("").astype(str).str.strip())
        if only_changed:
            view = view[changed_mask]

        if search.strip():
            mask = view[col_rue].fillna("").astype(str).str.contains(search, case=False) | \
                   view["Rue_corrigee"].fillna("").astype(str).str.contains(search, case=False)
            view = view[mask]

        st.write(f"Résultats : {len(view):,} lignes")
        sample = view.head(limit)

        # construire un tableau HTML des diffs
        rows = []
        for _, r in sample.iterrows():
            a, b = str(r[col_rue]), str(r["Rue_corrigee"])
            html = diff_html(a, b)
            rows.append(f"""
                <tr>
                  <td>{a}</td>
                  <td>{b}</td>
                  <td>{html}</td>
                </tr>
            """)
        html_table = f"""
        <table style="width:100%; border-collapse:collapse;">
          <thead>
            <tr style="text-align:left; border-bottom:1px solid #e5e7eb;">
              <th style="padding:6px 4px;">{col_rue}</th>
              <th style="padding:6px 4px;">Rue_corrigee</th>
              <th style="padding:6px 4px;">Différences</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
        """
        st.markdown(html_table, unsafe_allow_html=True)

with tab_stats:
    st.markdown("Comptage **par règle du pipeline** (diagnostic exhaustif).")
    if "Rue_corrigee" not in df.columns:
        st.warning("⚠️ Lance d’abord le nettoyage dans l’onglet **Nettoyage**.")
    else:
        # Exécuter le pipeline avec stats sur TOUTES les lignes (peut prendre un peu de temps selon la taille)
        with st.spinner("Analyse des règles appliquées…"):
            applied_list = []
            finals = []
            for s in df[col_rue].astype(str).fillna(""):
                final, applied = run_pipeline_with_stats(s)
                finals.append(final)
                applied_list.append(applied)

        # Agréger les stats
        c = Counter()
        for L in applied_list:
            c.update(L)
        stats_df = pd.DataFrame(
            {"regle": list(c.keys()), "comptage": list(c.values())}
        ).sort_values("comptage", ascending=False)

        mod_count = (df[col_rue].fillna("").astype(str).str.strip()
                     != pd.Series(finals).fillna("").astype(str).str.strip()).sum()
        pct = 100.0 * mod_count / len(df) if len(df) else 0.0

        # Affichage
        m1, m2 = st.columns(2)
        with m1:
            st.metric("Lignes modifiées", f"{mod_count:,}", delta=f"{pct:.1f}%")
        with m2:
            st.metric("Total lignes", f"{len(df):,}")

        st.write("**Top règles appliquées :**")
        st.dataframe(stats_df, use_container_width=True, height=360)

        # Petit bar chart
        try:
            st.bar_chart(stats_df.set_index("regle")["comptage"])
        except Exception:
            pass

        # Rapport exportable : binaire par règle
        st.write("**Rapport diagnostics (binaire par ligne et par règle)**")
        diag_df = df.copy()
        # colonnes binaires par règle
        for name, _ in RULES:
            diag_df[name] = [int(name in applied) for applied in applied_list]
        diag_df["Rue_corrigee_stats"] = finals

        bufx = BytesIO()
        with pd.ExcelWriter(bufx, engine="xlsxwriter") as writer:
            diag_df.to_excel(writer, index=False, sheet_name="Diagnostics")
            stats_df.to_excel(writer, index=False, sheet_name="Stats")
        st.download_button(
            "⬇️ Télécharger rapport diagnostics (Excel)",
            data=bufx.getvalue(),
            file_name="rapport_diagnostics_adresses.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
