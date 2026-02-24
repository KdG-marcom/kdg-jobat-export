import os
import json
import requests
from urllib.parse import urlparse, urlunparse

AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
VIEW_NAME = os.environ.get("AIRTABLE_VIEW_NAME", "")

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


def skills_to_semicolon_string(fields: dict) -> str:
    """
    Preferred: use Airtable formula field 'skills_export' = ARRAYJOIN({skills}, '; ')
    Fallbacks:
      - if skills is list -> join with '; '
      - if skills is comma string -> convert to '; '
    """
    s = fields.get("skills_export", "")
    if isinstance(s, str) and s.strip():
        return s.strip()

    raw = fields.get("skills", "")
    if isinstance(raw, list):
        return "; ".join([str(x).strip() for x in raw if str(x).strip()])

    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return ""
        # if it looks comma-separated, convert commas to semicolons
        if "," in txt and ";" not in txt:
            parts = [p.strip() for p in txt.split(",") if p.strip()]
            return "; ".join(parts)
        return txt

    return ""


def airtable_fetch_all() -> list[dict]:
    records = []
    offset = None
    while True:
        params = {}
        if VIEW_NAME:
            params["view"] = VIEW_NAME
        if offset:
            params["offset"] = offset

        r = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)
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


def main():
    records = airtable_fetch_all()

    output = []
    for rec in records:
        f = rec.get("fields", {})

        obj = {
            "internal_id": f.get("internal_id", ""),
            "title": f.get("title", ""),
            "language": f.get("language", ""),
            "price": f.get("price", ""),
            "certificate_name": f.get("certificate_name", ""),
            "course_image": f.get("course_image", ""),
            "email": f.get("email", ""),
            "job_title": f.get("job_title", ""),  # ✅ nu ingevuld in Airtable
            "skills": skills_to_semicolon_string(f),  # ✅ ';' divider
            "audience": f.get("audience", ""),
            "domain_category": f.get("domain_category", ""),
            "domain_subcategory": f.get("domain_subcategory", ""),
            "webaddress": add_jobat_utm(f.get("webaddress", "")),
            "degree_type": to_int(f.get("degree_type", "")),  # ✅ numeriek
            "duration_length": to_int(f.get("duration_length", "")),
            "duration_type": f.get("duration_type", ""),
            "provider": "Karel de Grote Hogeschool",
            "course_type": to_int(f.get("course_type", "")),
            "description": cdata(f.get("description_html", "")),
            "description_program": cdata(f.get("description_program_html", "")),
            "description_extrainfo": cdata(f.get("description_extrainfo_html", "")),
            # Deze velden waren bij jou eerder missing: neem ze expliciet mee
            "job_function_category": f.get("job_function_category", ""),
            "esco_category_code": f.get("esco_category_code", ""),
            "nacebel_sector": f.get("nacebel_sector", ""),
            "required_knowledge": f.get("required_knowledge", ""),
            "government_subsidy": (
                "; ".join(f.get("government_subsidy", []))
                if isinstance(f.get("government_subsidy", ""), list)
                else f.get("government_subsidy", "")
            ),
            # Sessions later
            "location_and_date": []
        }

        output.append(obj)

    with open("data/jobat.json", "w", encoding="utf-8") as fp:
        json.dump(output, fp, ensure_ascii=False, indent=2)

    print(f"Exported {len(output)} records to data/jobat.json")


if __name__ == "__main__":
    main()
