import re
import pandas as pd
import streamlit as st
from io import BytesIO

# ---------- Dictionnaires / paramètres ----------
words_to_remove = ["Canada","QC","Québec","Montréal","Qc","Quebec","Montreal"]
postal_code_pattern = r'\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b'
noms_feminins = ["Anne","Catherine","Claire","Élisabeth","Geneviève","Hélène","Jacqueline","Jeanne",
                 "Julie","Lucie","Marguerite","Marie","Marthe","Thérèse","Adèle","Angèle","Ariane",
                 "Audrey","Béatrice","Caroline","Christine","Colette","Diane","Émilie","Florence",
                 "Gabrielle","Isabelle","Joséphine","Louise","Madeleine","Mathilde","Pauline",
                 "Rosalie","Simone","Suzanne","Valérie"]
voie_mapping_full = {
    "St":"Saint","St.":"Saint","Ste":"Sainte","Ste.":"Sainte","Av":"Avenue","Ave":"Avenue","Ave.":"Avenue","Avé":"Avenue",
    "Rd":"Route","Rd.":"Route","Rt":"Route","Blvd":"Boulevard","BVD":"Boulevard","Bve":"Boulevard",
    "Boul":"Boulevard","Bl":"Boulevard","Ch":"Chemin","V":"Voie","Pl":"Place","Rg":"Rang",
    "Al":"Allée","Terr":"Terrasse","Cte":"Côte","Prom":"Promenade","Cr":"Crois"
}
direction_mapping = {
    r'\bEst\b':'E', r'\bOuest\b':'O', r'\bNord\b':'N', r'\bSud\b':'S',
    r'\bEast\b':'E', r'\bWest\b':'W', r'\bNorth\b':'N', r'\bSouth\b':'S'
}
accent_corrections = {"Ecole":"École","Erables":"Érables","Montreal":"Montréal","Trois Rivieres":"Trois-Rivières"}
terms_to_remove = ["App","Apt","Appartement","Unit","Unité","Logement","Suite","apt","n0","no","Appt","app","Apartment","ap","Ap"]

# ---------- Fonctions ----------
def clean_text(text):
    if pd.isna(text): return None
    text = re.sub(r'[.,;:/#&@"*|]', ' ', str(text))
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_address(address):
    if pd.isna(address): return address
    for w in words_to_remove:
        address = re.sub(r'\b' + re.escape(w) + r'\b','',address, flags=re.IGNORECASE).strip()
    address = re.sub(postal_code_pattern,'',address).strip()
    address = re.sub(r'\s+',' ',address).strip()
    return address

def clean_address_specific_suffix_attached(address):
    if pd.isna(address): return address
    pattern = r'(?:' + '|'.join(map(re.escape, terms_to_remove)) + r')(?=\d*[A-Za-z]?$)'
    return re.sub(pattern,'',address).strip()

def capitalize_letter_after_number(address):
    if pd.isna(address): return address
    return re.sub(r'(\d+)([a-z])\b', lambda m: f"{m.group(1)}{m.group(2).upper()}", address)

def replace_cardinal_directions(address):
    if pd.isna(address): return address
    s = address
    for pat, rep in direction_mapping.items():
        s = re.sub(pat, rep, s)
    return s

def replace_st_with_saint_or_sainte(address):
    if pd.isna(address): return address
    m = re.search(r'\bSt-([A-Za-zÉéÈèÀàÙù]+)', address)
    if m:
        following = m.group(1)
        return re.sub(r'\bSt-', "Sainte-" if following in noms_feminins else "Saint-", address)
    return address

def expand_abbreviations(address):
    if pd.isna(address): return address
    s = address
    for abbr, full in voie_mapping_full.items():
        s = re.sub(r'\b' + re.escape(abbr) + r'\b', full, s, flags=re.IGNORECASE)
    return s

def correct_accents(address):
    if pd.isna(address): return address
    s = address
    for typo, corr in accent_corrections.items():
        s = re.sub(r'\b' + re.escape(typo) + r'\b', corr, s)
    return s

def format_address_part(address):
    if pd.isna(address): return address
    m = re.search(r'(.+?)\s+(\d+)$', address)
    if m:
        street_part, apt_number = m.group(1), m.group(2)
        first_word = street_part.split()[0]
        if first_word.isdigit():
            return f"{apt_number}-{first_word} {street_part[len(first_word):].strip()}"
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

def clean_pipeline(address):
    if pd.isna(address): return address
    address = clean_text(address)
    address = clean_address(address)
    address = clean_address_specific_suffix_attached(address)
    address = capitalize_letter_after_number(address)
    address = replace_cardinal_directions(address)
    address = replace_st_with_saint_or_sainte(address)
    address = expand_abbreviations(address)
    address = correct_accents(address)
    address = format_address_part(address)
    address = remove_duplicate_words_numbers(address)  # dédoublonnage final
    return address.title() if pd.notna(address) else address

# ---------- UI ----------
st.set_page_config(page_title="Nettoyage d'adresses", page_icon="🧹", layout="centered")
st.title("🧹 Nettoyage d'adresses (CSV / Excel)")
uploaded = st.file_uploader("Importer un fichier", type=["csv","xlsx"])
rue_candidates = ["Rue","Adresse","Address","Street","street1","street_1","rue"]

if uploaded:
    if uploaded.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded)
    else:
        df = pd.read_excel(uploaded)
    st.write("Aperçu :", df.head())

    default_col = next((c for c in df.columns if c in rue_candidates or c.lower() in [x.lower() for x in rue_candidates]), None)
    col_rue = st.selectbox("Colonne à nettoyer :", options=list(df.columns), index=list(df.columns).index(default_col) if default_col in df.columns else 0)

    if st.button("Corriger"):
        df[f"{col_rue}_corrigee"] = df[col_rue].apply(clean_pipeline)
        st.success("Nettoyage terminé ✅")
        st.write(df[[col_rue, f"{col_rue}_corrigee"]].head(50))

        csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("⬇️ Télécharger CSV corrigé", data=csv_bytes, file_name="adresses_corrigees.csv", mime="text/csv")

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Adresses")
        st.download_button("⬇️ Télécharger Excel corrigé", data=buffer.getvalue(), file_name="adresses_corrigees.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
