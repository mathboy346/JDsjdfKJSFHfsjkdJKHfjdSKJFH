import re
import unicodedata

NORTH_STATES = {
    "Delhi", "NCR", "Uttar Pradesh", "Haryana", "Punjab", "Rajasthan",
    "Madhya Pradesh", "Chhattisgarh", "Bihar", "Jharkhand", "Uttarakhand",
    "Himachal Pradesh", "Jammu And Kashmir", "Chandigarh", "Jammu & Kashmir",
    "Ladakh",
}
SOUTH_STATES = {
    "Tamil Nadu", "Karnataka", "Kerala", "Telangana", "Andhra Pradesh",
    "Puducherry", "Lakshadweep",
}
WEST_STATES  = {
    "Maharashtra", "Gujarat", "Goa", "Daman And Diu",
    "Dadra And Nagar Haveli And Daman And Diu",
}
EAST_STATES  = {
    "West Bengal", "Odisha", "Assam", "Meghalaya", "Nagaland",
    "Manipur", "Tripura", "Mizoram", "Arunachal Pradesh", "Sikkim",
    "Andaman And Nicobar",
}
SOUTH_LANGUAGES = {"Tamil", "Telugu", "Malayalam", "Kannada", "Tulu"}


def state_to_region(state: str) -> str:
    s = state.strip().title()
    if s in NORTH_STATES: return "North"
    if s in SOUTH_STATES: return "South"
    if s in WEST_STATES:  return "West"
    if s in EAST_STATES:  return "East"
    return "Other"


def parse_variant_key(key: str) -> dict:
    """'War 2 [2D | Hindi]' → {name, format, language}"""
    m = re.match(r"^(.*)\[(.+?)\|\s*(.+?)\]$", key.strip())
    if m:
        return {
            "name":     m.group(1).strip(),
            "format":   m.group(2).strip(),
            "language": m.group(3).strip(),
        }
    return {"name": key.strip(), "format": "", "language": ""}


def normalize_for_matching(name: str) -> str:
    """Canonical key for cross-source matching."""
    name = re.sub(r'\s*\[.*?\]', '', name)
    name = re.sub(r'\b(19|20)\d{2}\b', '', name)
    name = re.sub(r'\s+Box Office.*$', '', name, flags=re.IGNORECASE)
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = re.sub(r'[^a-z0-9]', ' ', name.lower())
    return re.sub(r'\s+', ' ', name).strip()


def slugify(text: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return text.strip("-")


def _is_english_language(language: str) -> bool:
    """True when the variant language label denotes English (incl. misparsed keys)."""
    norm = language.strip().lower()
    if not norm:
        return False
    if norm == "english":
        return True
    if norm.endswith(" english") or norm.endswith("| english"):
        return True
    return norm.startswith("english")


def classify_language(language: str) -> str:
    """Returns 'Hindi', 'South', 'English', or 'Other' for tab filtering."""
    lang = language.strip()
    if lang == "Hindi":
        return "Hindi"
    if _is_english_language(lang):
        return "English"
    if lang in SOUTH_LANGUAGES:
        return "South"
    return "Other"
