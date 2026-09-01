import os
import json
import requests
from urllib.parse import urlparse, urlunparse

AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
HEADERS = {"Authorization": f"Bearer {AIRTABLE_PAT}"}
UTM_QUERY = "utm_source=jobat&utm_medium=affiliate"

# --- Primaire bron (bestaand, V2) ---
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
VIEW_NAME = os.environ.get("AIRTABLE_VIEW_NAME", "")
SESSIONS_TABLE_NAME = os.environ["AIRTABLE_SESSIONS_TABLE_NAME"]
SESSIONS_COURSE_LINK_FIELD = os.environ["AIRTABLE_SESSIONS_COURSE_LINK_FIELD"]


def add_jobat_utm(url: str) -> str:
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
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def skills_to_comma_string(fields: dict) -> str:
    s = fields.get("skills_export", "")
    if isinstance(s, str) and s.strip():
        return s.strip().replace("; ", ", ").replace(";", ", ")
    raw = fields.get("skills", "")
    if isinstance(raw, list):
        return ", ".join([str(x).strip() for x in raw if str(x).strip()])
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return ""
        if ";" in txt and "," not in txt:
            return ", ".join([p.strip() for p in txt.split(";") if p.strip()])
        return txt
    return ""


def price_to_2dec(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and not value.strip():
        return ""
    try:
        return f"{float(value):.2f}"
    except Exception:
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
            return str(int(num))
        return f"{num:.1f}"
    except Exception:
        return txt


def airtable_fetch_all(api_url: str, view_name: str = "") -> list:
    records, offset = [], None
    while True:
        params = {}
        if view_name:
            params["view"] = view_name
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


def build_location_block(sf: dict) -> dict:
    block = {
        "date_start": norm_str(sf.get("date_start")),
        "date_end": norm_str(sf.get("date_end")),
        "hours": norm_str(sf.get("hours")),
        "location_name": norm_str(sf.get("location_name")),
        "location_address": norm_str(sf.get("location_address")),
        "location_zip": norm_str(sf.get("location_zip")),
    }
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


def build_output(base_id, table_name, sessions_table_name, view_name, sessions_link_field):
    """Bouwt de Jobat-records voor EEN bron (base). Dezelfde mapping voor V2 en navormingen."""
    api_url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    sessions_api_url = f"https://api.airtable.com/v0/{base_id}/{sessions_table_name}"

    course_records = airtable_fetch_all(api_url, view_name)

    course_id_to_internal = {}
    for cr in course_records:
        iid = cr.get("fields", {}).get("internal_id", "")
        if iid:
            course_id_to_internal[cr["id"]] = iid

    session_records = airtable_fetch_all(sessions_api_url, "")
    sessions_by_internal = {}
    for sr in session_records:
        sf = sr.get("fields", {})
        linked = sf.get(sessions_link_field, [])
        if not isinstance(linked, list):
            linked = [linked] if linked else []
        block = build_location_block(sf)
        for course_rec_id in linked:
            iid = course_id_to_internal.get(course_rec_id)
            if not iid:
                continue
            sessions_by_internal.setdefault(iid, []).append(block)

    def sort_key(b):
        return (b.get("date_start", ""), b.get("location_name", ""), b.get("hours", ""))

    for iid in sessions_by_internal:
        sessions_by_internal[iid] = sorted(sessions_by_internal[iid], key=sort_key)

    output = []
    for rec in course_records:
        f = rec.get("fields", {})
        internal_id = f.get("internal_id", "")
        gs = f.get("government_subsidy", "")
        obj = {
            "internal_id": internal_id,
            "title": f.get("title", ""),
            "language": f.get("language", ""),
            "price": price_to_2dec(f.get("price", "")),
            "certificate_name": f.get("certificate_name", ""),
            "course_image": f.get("course_image", ""),
            "email": f.get("email", ""),
            "job_title": f.get("job_title", ""),
            "skills": skills_to_comma_string(f),
            "audience": f.get("audience", ""),
            "domain_category": f.get("domain_category", ""),
            "domain_subcategory": f.get("domain_subcategory", ""),
            "webaddress": add_jobat_utm(f.get("webaddress", "")),
            "degree_type": to_int(f.get("degree_type", "")),
            "duration_length": duration_length_format(f.get("duration_length", "")),
            "duration_type": f.get("duration_type", ""),
            "provider": "Karel de Grote Hogeschool",
            "course_type": to_int(f.get("course_type", "")),
            "description": cdata(f.get("description_html", "")),
            "description_program": cdata(f.get("description_program_html", "")),
            "description_extrainfo": cdata(f.get("description_extrainfo_html", "")),
            "job_function_category": to_int(f.get("job_function_category", ""), default=0),
            "esco_category_code": to_int(f.get("esco_category_code", ""), default=0),
            "nacebel_sector": f.get("nacebel_sector", ""),
            "required_knowledge": f.get("required_knowledge", ""),
            "government_subsidy": (
                ", ".join([str(x).strip() for x in gs if str(x).strip()])
                if isinstance(gs, list)
                else (gs.replace("; ", ", ").replace(";", ", ") if isinstance(gs, str) else "")
            ),
            "location_and_date": sessions_by_internal.get(internal_id, []),
        }
        output.append(obj)
    return output


def main():
    # 1) Primaire bron (V2)
    output = build_output(BASE_ID, TABLE_NAME, SESSIONS_TABLE_NAME, VIEW_NAME, SESSIONS_COURSE_LINK_FIELD)

    # 2) Optionele tweede bron: navormingen-base
    nav_base = os.environ.get("NAV_BASE_ID", "").strip()
    if nav_base:
        output += build_output(
            nav_base,
            os.environ["NAV_TABLE_NAME"],
            os.environ["NAV_SESSIONS_TABLE_NAME"],
            os.environ.get("NAV_VIEW_NAME_JOBAT", ""),
            os.environ.get("NAV_SESSIONS_COURSE_LINK_FIELD", "Course"),
        )

    # 3) Dedupe op internal_id (primaire bron wint bij botsing)
    seen, deduped = set(), []
    for o in output:
        iid = o.get("internal_id", "")
        if iid and iid in seen:
            continue
        if iid:
            seen.add(iid)
        deduped.append(o)

    os.makedirs("data", exist_ok=True)
    with open("data/jobat.json", "w", encoding="utf-8") as fp:
        json.dump(deduped, fp, ensure_ascii=False, indent=2)
    print(f"Exported {len(deduped)} records to data/jobat.json")


if __name__ == "__main__":
    main()
