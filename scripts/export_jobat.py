import os
import json
import requests
from urllib.parse import urlparse, urlunparse

AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
VIEW_NAME = os.environ.get("AIRTABLE_VIEW_NAME", "")

API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}"
}

UTM_QUERY = "utm_source=jobat&utm_medium=affiliate"

def add_jobat_utm(url: str) -> str:
    """Append Jobat UTM query if there is no query yet; if query exists, keep it (no double UTM)."""
    if not url:
        return ""
    parts = urlparse(url)
    if parts.query:
        # If you want to ALWAYS override, change this logic. For now: keep existing query.
        return url
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, UTM_QUERY, parts.fragment))

def cdata(html: str) -> str:
    if html is None:
        html = ""
    return f"<![CDATA[{html}]]>"

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
            "job_title": f.get("job_title", ""),
            "skills": f.get("skills", ""),
            "audience": f.get("audience", ""),
            "domain_category": f.get("domain_category", ""),
            "domain_subcategory": f.get("domain_subcategory", ""),
            "webaddress": add_jobat_utm(f.get("webaddress", "")),
            "degree_type": f.get("degree_type", ""),
            "duration_length": f.get("duration_length", ""),
            "duration_type": f.get("duration_type", ""),
            "provider": "Karel de Grote Hogeschool",
            "course_type": f.get("course_type", ""),
            "description": cdata(f.get("description_html", "")),
            "description_program": cdata(f.get("description_program_html", "")),
            "description_extrainfo": cdata(f.get("description_extrainfo_html", "")),
            # location_and_date komt in de volgende stap (Sessions), voorlopig leeg zodat schema stabiel blijft
            "location_and_date": []
        }

        output.append(obj)

    with open("data/jobat.json", "w", encoding="utf-8") as fp:
        json.dump(output, fp, ensure_ascii=False, indent=2)

    print(f"Exported {len(output)} records to data/jobat.json")

if __name__ == "__main__":
    main()
