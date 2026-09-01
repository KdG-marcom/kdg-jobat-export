import os
import json
import requests
import re
from urllib.parse import urlparse, urlunparse
from datetime import datetime

AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
HEADERS = {"Authorization": f"Bearer {AIRTABLE_PAT}"}
UTM_QUERY = "utm_source=smartlions&utm_medium=affiliate"

# --- Primaire bron (bestaand, V2) ---
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
COURSES_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
SESSIONS_TABLE_NAME = os.environ["AIRTABLE_SESSIONS_TABLE_NAME"]
SESSIONS_COURSE_LINK_FIELD = os.environ["AIRTABLE_SESSIONS_COURSE_LINK_FIELD"]
COURSES_VIEW = os.environ.get("AIRTABLE_VIEW_NAME_SMARTLIONS", "")
SESSIONS_VIEW = os.environ.get("AIRTABLE_SESSIONS_VIEW_NAME_SMARTLIONS", "")


def norm_str(x) -> str:
    return "" if x is None else str(x).strip()


def to_int(value, default=0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def to_float_or_none(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return float(value)
    except Exception:
        return None


def add_utm(url: str) -> str:
    url = norm_str(url)
    if not url or not UTM_QUERY:
        return url
    parts = urlparse(url)
    if parts.query:
        return url
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, UTM_QUERY, parts.fragment))


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


def government_subsidy_to_comma(fields: dict) -> str:
    raw = fields.get("government_subsidy", "")
    if isinstance(raw, list):
        return ", ".join([str(x).strip() for x in raw if str(x).strip()])
    if isinstance(raw, str):
        return raw.strip().replace("; ", ", ").replace(";", ", ")
    return ""


def audience_to_str(value) -> str:
    if isinstance(value, list):
        return ", ".join([str(x).strip() for x in value if str(x).strip()])
    return norm_str(value)


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
            print("Airtable response:", r.text[:1200])
            r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def ymd_to_dmy(date_str: str) -> str:
    s = norm_str(date_str)
    if not s:
        return ""
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return s


def extract_times(hours_str: str):
    s = norm_str(hours_str)
    times = re.findall(r"\b([01]\d|2[0-3]):[0-5]\d\b", s)
    if len(times) >= 2:
        return times[0], times[1]
    return "", ""


def build_location_and_date_block(sf: dict) -> dict:
    return {
        "date_start": norm_str(sf.get("date_start")),
        "date_end": norm_str(sf.get("date_end")),
        "hours": norm_str(sf.get("hours")),
        "location_name": norm_str(sf.get("location_name")),
        "location_address": norm_str(sf.get("location_address")),
        "location_zip": norm_str(sf.get("location_zip")),
        "location_city": norm_str(sf.get("location_city")),
    }


def make_session_id(internal_id: str, kind: str, date_str: str) -> str:
    return f"{norm_str(internal_id)}-{kind}-{norm_str(date_str)[:10]}".replace(" ", "-")


def build_smartlions_sessions_from_session_record(internal_id: str, sf: dict) -> list:
    start_date = norm_str(sf.get("date_start"))
    end_date = norm_str(sf.get("date_end"))
    hours = norm_str(sf.get("hours"))
    locationName = norm_str(sf.get("location_name"))
    address = norm_str(sf.get("location_address"))
    zipCode = norm_str(sf.get("location_zip"))
    city = norm_str(sf.get("location_city"))
    startTime, endTime = extract_times(hours)
    out = []
    if start_date:
        out.append({
            "date": ymd_to_dmy(start_date), "sessionDescription": "Startdatum",
            "sessionId": make_session_id(internal_id, "start", start_date),
            "locationName": locationName, "address": address, "zipCode": zipCode, "city": city,
            "startTime": startTime, "endTime": endTime,
        })
    if end_date:
        out.append({
            "date": ymd_to_dmy(end_date), "sessionDescription": "Einddatum",
            "sessionId": make_session_id(internal_id, "end", end_date),
            "locationName": locationName, "address": address, "zipCode": zipCode, "city": city,
            "startTime": startTime, "endTime": endTime,
        })
    return out


def build_output(base_id, courses_table, sessions_table, courses_view, sessions_view, link_field):
    """Bouwt de Smartlions-records voor EEN bron (base). Zelfde mapping voor V2 en navormingen."""
    courses_api_url = f"https://api.airtable.com/v0/{base_id}/{courses_table}"
    sessions_api_url = f"https://api.airtable.com/v0/{base_id}/{sessions_table}"

    course_records = airtable_fetch_all(courses_api_url, courses_view)
    course_id_to_internal = {}
    for cr in course_records:
        iid = norm_str(cr.get("fields", {}).get("internal_id"))
        if iid:
            course_id_to_internal[cr["id"]] = iid

    session_records = airtable_fetch_all(sessions_api_url, sessions_view)
    lad_by_internal, sessions_by_internal = {}, {}
    for sr in session_records:
        sf = sr.get("fields", {})
        linked = sf.get(link_field, [])
        if not isinstance(linked, list):
            linked = [linked] if linked else []
        lad_block = build_location_and_date_block(sf)
        for course_rec_id in linked:
            iid = course_id_to_internal.get(course_rec_id)
            if not iid:
                continue
            if any(v for v in lad_block.values()):
                lad_by_internal.setdefault(iid, []).append(lad_block)
            for sess in build_smartlions_sessions_from_session_record(iid, sf):
                sessions_by_internal.setdefault(iid, []).append(sess)

    for iid in lad_by_internal:
        lad_by_internal[iid] = sorted(
            lad_by_internal[iid],
            key=lambda b: (b.get("date_start", ""), b.get("location_name", ""), b.get("hours", "")),
        )
    for iid in sessions_by_internal:
        seen, unique = set(), []
        for s in sorted(sessions_by_internal[iid], key=lambda x: (x.get("date", ""), x.get("sessionDescription", ""))):
            sid = s.get("sessionId", "")
            if sid and sid in seen:
                continue
            if sid:
                seen.add(sid)
            unique.append(s)
        sessions_by_internal[iid] = unique

    output = []
    for cr in course_records:
        f = cr.get("fields", {})
        internal_id = norm_str(f.get("internal_id"))
        output.append({
            "internal_id": internal_id,
            "title": norm_str(f.get("title")),
            "language": norm_str(f.get("language")).upper(),
            "webaddress": add_utm(norm_str(f.get("webaddress"))),
            "provider": "Karel de Grote Hogeschool",
            "domain_category": norm_str(f.get("domain_category")),
            "domain_subcategory": norm_str(f.get("domain_subcategory")),
            "duration_length": f.get("duration_length", None),
            "duration_type": norm_str(f.get("duration_type")),
            "course_type": to_int(f.get("course_type"), 0),
            "degree_type": to_int(f.get("degree_type"), 0),
            "job_title": norm_str(f.get("job_title")),
            "job_function_category": to_int(f.get("job_function_category"), 0),
            "esco_category_code": norm_str(f.get("esco_category_code")),
            "nacebel_sector": norm_str(f.get("nacebel_sector")),
            "price": to_float_or_none(f.get("price")),
            "government_subsidy": government_subsidy_to_comma(f),
            "skills": skills_to_comma_string(f),
            "audience": audience_to_str(f.get("audience")),
            "required_knowledge": norm_str(f.get("required_knowledge")),
            "certificate_name": norm_str(f.get("certificate_name")),
            "email": norm_str(f.get("email")),
            "course_image": norm_str(f.get("course_image")),
            "description": norm_str(f.get("description_html")),
            "description_program": norm_str(f.get("description_program_html")),
            "description_extrainfo": norm_str(f.get("description_extrainfo_html")),
            "location_and_date": lad_by_internal.get(internal_id, []),
            "sessions": sessions_by_internal.get(internal_id, []),
        })
    return output


def main():
    # 1) Primaire bron (V2)
    output = build_output(BASE_ID, COURSES_TABLE_NAME, SESSIONS_TABLE_NAME, COURSES_VIEW, SESSIONS_VIEW, SESSIONS_COURSE_LINK_FIELD)

    # 2) Optionele tweede bron: navormingen-base
    nav_base = os.environ.get("NAV_BASE_ID", "").strip()
    if nav_base:
        output += build_output(
            nav_base,
            os.environ["NAV_TABLE_NAME"],
            os.environ["NAV_SESSIONS_TABLE_NAME"],
            os.environ.get("NAV_VIEW_NAME_SMARTLIONS", ""),
            "",  # geen aparte sessions-view voor navormingen
            os.environ.get("NAV_SESSIONS_COURSE_LINK_FIELD", "Course"),
        )

    # 3) Dedupe op internal_id (primaire bron wint), dan sorteren
    seen, deduped = set(), []
    for o in output:
        iid = o.get("internal_id", "")
        if iid and iid in seen:
            continue
        if iid:
            seen.add(iid)
        deduped.append(o)
    deduped = sorted(deduped, key=lambda x: x.get("internal_id", ""))

    os.makedirs("data", exist_ok=True)
    with open("data/smartlions.json", "w", encoding="utf-8") as fp:
        json.dump(deduped, fp, ensure_ascii=False, indent=2)
    print(f"Exported {len(deduped)} records to data/smartlions.json")


if __name__ == "__main__":
    main()
