import os
import json
import requests
from urllib.parse import urlparse, urlunparse

AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
VIEW_NAME = os.environ.get("AIRTABLE_VIEW_NAME", "")
SESSIONS_TABLE_NAME = os.environ["AIRTABLE_SESSIONS_TABLE_NAME"]
SESSIONS_API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{SESSIONS_TABLE_NAME}"

API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"

HEADERS = {"Authorization": f"Bearer {AIRTABLE_PAT}"}
UTM_QUERY = "utm_source=jobat&utm_medium=affiliate"


def add_jobat_utm(url: str) -> str:
    """Append Jobat UTM query if there is no query yet; if query exists, keep it (no double UTM)."""
    if not url:
        return ""
    parts = urlparse(url)
    if parts.query:
        return url
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, UTM_QUERY, parts.fragment))


def cdata(html: str) -> str:
    if html is None:
        html = ""
    return f"<![CDATA[{html}]]>"


def to_int(value, default=0):
    """Safely coerce to int. Accepts numbers or numeric strings."""
    if value is None or value == "":
        return default
    try:
        # Airtable sometimes returns "5.0" as string; handle that too.
        return int(float(value))
    except Exception:
        return default


def skills_to_comma_string(fields: dict) -> str:
    """
    Output must be a single string, comma-separated.
    Prefer Airtable formula field 'skills_export' if you have it,
    but normalize ';' to ',' if needed.
    """
    s = fields.get("skills_export", "")
    if isinstance(s, str) and s.strip():
        txt = s.strip()
        # normalize legacy semicolon export to comma
        txt = txt.replace("; ", ", ").replace(";", ", ")
        return txt.strip()

    raw = fields.get("skills", "")
    if isinstance(raw, list):
        return ", ".join([str(x).strip() for x in raw if str(x).strip()])

    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return ""
        # if it looks semicolon-separated, convert
        if ";" in txt and "," not in txt:
            parts = [p.strip() for p in txt.split(";") if p.strip()]
            return ", ".join(parts)
        return txt

    return ""

def price_to_2dec(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and not value.strip():
        return ""
    try:
        num = float(value)
        return f"{num:.2f}"
    except Exception:
        # als Airtable al "1900.00" als tekst geeft, hou het zo (maar trim)
        txt = str(value).strip()
        return txt if txt else ""

def duration_length_format(value) -> str:
    if value is None:
        return ""
    txt = str(value).strip()
    if not txt:
        return ""
    try:
        num = float(txt)
        if num.is_integer():
            return str(int(num))              # "2.0" -> "2"
        return f"{num:.1f}"                   # "2.5" -> "2.5", "0.75" -> "0.8"
    except Exception:
        return txt

def airtable_fetch_all(api_url: str) -> list[dict]:
    records = []
    offset = None
    while True:
        params = {}
        if VIEW_NAME and api_url == API_URL:
            params["view"] = VIEW_NAME
        if offset:
            params["offset"] = offset

        r = requests.get(api_url, headers=HEADERS, params=params, timeout=60)
        if not r.ok:
            print("Airtable status:", r.status_code)
            print("Airtable response:", r.text[:800])
            r.raise_for_status()

        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records

def norm_str(x) -> str:
    return "" if x is None else str(x).strip()

def norm_date(x) -> str:
    # Airtable date komt meestal als 'YYYY-MM-DD' of ISO; we houden gewoon string
    return norm_str(x)

def build_location_block(sf: dict) -> dict:
    block = {
        "date_start": norm_date(sf.get("date_start")),
        "date_end": norm_date(sf.get("date_end")),
        "hours": norm_str(sf.get("hours")),
        "location_name": norm_str(sf.get("location_name")),
        "location_address": norm_str(sf.get("location_address")),
        "location_zip": norm_str(sf.get("location_zip")),
    }
    # Optioneel velden (alleen toevoegen als ze bestaan en niet leeg zijn)
    city = norm_str(sf.get("location_city"))
    if city:
        block["location_city"] = city

    maxp = norm_str(sf.get("maximum_participants"))
    if maxp:
        block["maximum_participants"] = maxp

    reg = norm_str(sf.get("registration_deadline"))
    if reg:
        block["registration_deadline"] = reg

    return block

def main():
    course_records = airtable_fetch_all(API_URL)
session_records = airtable_fetch_all(SESSIONS_API_URL)

# Group sessions by internal_id
sessions_by_id = {}
for sr in session_records:
    sf = sr.get("fields", {})
    iid = norm_str(sf.get("internal_id"))
    if not iid:
        continue
    sessions_by_id.setdefault(iid, []).append(build_location_block(sf))

# Sort sessions per opleiding (eerst date_start, dan location_name)
def sort_key(b):
    return (b.get("date_start",""), b.get("location_name",""), b.get("hours",""))

for iid in sessions_by_id:
    sessions_by_id[iid] = sorted(sessions_by_id[iid], key=sort_key)
    records = airtable_fetch_all()

    output = []
    for rec in records:
        f = rec.get("fields", {})

        obj = {
            "internal_id": f.get("internal_id", ""),
            "title": f.get("title", ""),
            "language": f.get("language", ""),
            "price": price_to_2dec(f.get("price", "")),
            "certificate_name": f.get("certificate_name", ""),
            "course_image": f.get("course_image", ""),
            "email": f.get("email", ""),
            "job_title": f.get("job_title", ""),  # ✅ nu ingevuld in Airtable
            "skills": skills_to_comma_string(f),
            "audience": f.get("audience", ""),
            "domain_category": f.get("domain_category", ""),
            "domain_subcategory": f.get("domain_subcategory", ""),
            "webaddress": add_jobat_utm(f.get("webaddress", "")),
            "degree_type": to_int(f.get("degree_type", "")),  # ✅ numeriek
            "duration_length": duration_length_format(f.get("duration_length", "")),
            "duration_type": f.get("duration_type", ""),
            "provider": "Karel de Grote Hogeschool",
            "course_type": to_int(f.get("course_type", "")),
            "description": cdata(f.get("description_html", "")),
            "description_program": cdata(f.get("description_program_html", "")),
            "description_extrainfo": cdata(f.get("description_extrainfo_html", "")),
            # Deze velden waren bij jou eerder missing: neem ze expliciet mee
            "job_function_category": to_int(f.get("job_function_category", ""), default=0),
            "esco_category_code": to_int(f.get("esco_category_code", ""), default=0),
            "nacebel_sector": f.get("nacebel_sector", ""),
            "required_knowledge": f.get("required_knowledge", ""),
            "government_subsidy": (
    ", ".join([str(x).strip() for x in f.get("government_subsidy", []) if str(x).strip()])
    if isinstance(f.get("government_subsidy", ""), list)
    else (
        f.get("government_subsidy", "").replace("; ", ", ").replace(";", ", ")
        if isinstance(f.get("government_subsidy", ""), str)
        else ""
    )
),
            # Sessions later
            "location_and_date": sessions_by_id.get(f.get("internal_id",""), [])
        }

        output.append(obj)

    with open("data/jobat.json", "w", encoding="utf-8") as fp:
        json.dump(output, fp, ensure_ascii=False, indent=2)

    print(f"Exported {len(output)} records to data/jobat.json")


if __name__ == "__main__":
    main()
