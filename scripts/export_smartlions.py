# scripts/export_smartlions.py
# Airtable (Courses + Sessions) -> data/smartlions.json
#
# Uses:
# - Courses table: AIRTABLE_TABLE_NAME (same as Jobat)
# - Courses view:  AIRTABLE_VIEW_NAME_SMARTLIONS (e.g. Published_Smartlions)
# - Sessions table: AIRTABLE_SESSIONS_TABLE_NAME
# - Sessions link field to Courses: AIRTABLE_SESSIONS_COURSE_LINK_FIELD (e.g. "Course")
#
# Notes:
# - location_and_date is built from Sessions fields (date_start/date_end/hours/location_*)
# - sessions array is OPTIONAL: this script will populate it ONLY if the expected
#   Smartlions session fields exist in Sessions (date/sessionDescription/sessionId/...)
#   Otherwise, sessions will be [] for all courses (safe default).
#
# Make sure requirements.txt includes: requests

import os
import json
import requests
from urllib.parse import urlparse, urlunparse

# --- ENV ---
AIRTABLE_PAT = os.environ["AIRTABLE_PAT"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]

COURSES_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
SESSIONS_TABLE_NAME = os.environ["AIRTABLE_SESSIONS_TABLE_NAME"]
SESSIONS_COURSE_LINK_FIELD = os.environ["AIRTABLE_SESSIONS_COURSE_LINK_FIELD"]

# Smartlions-specific view
COURSES_VIEW = os.environ.get("AIRTABLE_VIEW_NAME_SMARTLIONS", "")  # e.g. Published_Smartlions

# Optional: separate Sessions view (leave empty if you want all sessions)
SESSIONS_VIEW = os.environ.get("AIRTABLE_SESSIONS_VIEW_NAME_SMARTLIONS", "")

COURSES_API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{COURSES_TABLE_NAME}"
SESSIONS_API_URL = f"https://api.airtable.com/v0/{BASE_ID}/{SESSIONS_TABLE_NAME}"

HEADERS = {"Authorization": f"Bearer {AIRTABLE_PAT}"}

# If you want NO UTM for Smartlions, set this to "".
UTM_QUERY = "utm_source=smartlions&utm_medium=affiliate"


# --- HELPERS ---
def norm_str(x) -> str:
    return "" if x is None else str(x).strip()


def to_int(value, default=0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def add_utm(url: str) -> str:
    """Add UTM only if URL has no existing query string."""
    if not url or not UTM_QUERY:
        return url or ""
    parts = urlparse(url)
    if parts.query:
        return url
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, UTM_QUERY, parts.fragment))


def cdata(html) -> str:
    html = "" if html is None else str(html)
    return f"<![CDATA[{html}]]>"


def skills_to_comma_string(fields: dict) -> str:
    """
    Output must be a single string with commas.
    Prefer Airtable formula field 'skills_export' (ARRAYJOIN({skills}, ", ")).
    """
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


def airtable_fetch_all(api_url: str, view_name: str = "") -> list[dict]:
    records = []
    offset = None
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


# --- SESSION BUILDERS ---
def build_location_and_date_block(sf: dict) -> dict:
    """
    Matches your Jobat 'location_and_date' structure.
    Expected Sessions fields:
      - date_start, date_end, hours
      - location_name, location_address, location_zip
      - (optional) location_city, maximum_participants, registration_deadline
    """
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


def build_smartlions_session_block(sf: dict) -> dict:
    """
    Smartlions 'sessions' block (as seen in DEF-Smartlions.json).
    This is OPTIONAL and will only be meaningful if your Sessions table contains these exact fields.
    If your Sessions table doesn't contain them, the returned block will be empty and ignored.
    """
    block = {
        "date": norm_str(sf.get("date")),
        "sessionDescription": norm_str(sf.get("sessionDescription")),
        "sessionId": norm_str(sf.get("sessionId")),
        "locationName": norm_str(sf.get("locationName")),
        "address": norm_str(sf.get("address")),
        "zipCode": norm_str(sf.get("zipCode")),
        "city": norm_str(sf.get("city")),
        "startTime": norm_str(sf.get("startTime")),
        "endTime": norm_str(sf.get("endTime")),
    }
    return block


def any_nonempty(d: dict) -> bool:
    return any(v for v in d.values())


def main():
    # 1) Fetch Courses (Smartlions view)
    course_records = airtable_fetch_all(COURSES_API_URL, COURSES_VIEW)

    # Map Airtable course record id -> internal_id
    course_id_to_internal = {}
    for cr in course_records:
        f = cr.get("fields", {})
        iid = norm_str(f.get("internal_id"))
        if iid:
            course_id_to_internal[cr["id"]] = iid

    # 2) Fetch Sessions (optional view)
    session_records = airtable_fetch_all(SESSIONS_API_URL, SESSIONS_VIEW)

    # Group by internal_id
    lad_by_internal: dict[str, list[dict]] = {}
    sessions_by_internal: dict[str, list[dict]] = {}

    for sr in session_records:
        sf = sr.get("fields", {})

        linked = sf.get(SESSIONS_COURSE_LINK_FIELD, [])
        if not isinstance(linked, list):
            linked = [linked] if linked else []

        lad_block = build_location_and_date_block(sf)
        sess_block = build_smartlions_session_block(sf)

        for course_rec_id in linked:
            iid = course_id_to_internal.get(course_rec_id)
            if not iid:
                continue

            if any_nonempty(lad_block):
                lad_by_internal.setdefault(iid, []).append(lad_block)

            # Only add if your Sessions has these Smartlions-specific fields filled
            if any_nonempty(sess_block):
                sessions_by_internal.setdefault(iid, []).append(sess_block)

    # Sort deterministically
    def lad_sort(b):
        return (b.get("date_start", ""), b.get("location_name", ""), b.get("hours", ""))

    def sess_sort(b):
        return (b.get("date", ""), b.get("sessionDescription", ""), b.get("startTime", ""))

    for iid in lad_by_internal:
        lad_by_internal[iid] = sorted(lad_by_internal[iid], key=lad_sort)

    for iid in sessions_by_internal:
        sessions_by_internal[iid] = sorted(sessions_by_internal[iid], key=sess_sort)

    # 3) Build Smartlions JSON
    output = []
    for cr in course_records:
        f = cr.get("fields", {})
        internal_id = norm_str(f.get("internal_id"))

        obj = {
            "internal_id": internal_id,
            "title": norm_str(f.get("title")),
            "language": norm_str(f.get("language")),
            "webaddress": add_utm(norm_str(f.get("webaddress"))),
            "provider": "Karel de Grote Hogeschool",

            "course_type": to_int(f.get("course_type"), 0),
            "degree_type": to_int(f.get("degree_type"), 0),

            "job_title": norm_str(f.get("job_title")),
            "job_function_category": to_int(f.get("job_function_category"), 0),
            "esco_category_code": to_int(f.get("esco_category_code"), 0),
            "nacebel_sector": norm_str(f.get("nacebel_sector")),

            # Keep your current policy for Smartlions:
            # - if Airtable stores price already formatted, we keep as string
            # - if empty -> empty string
            "price": norm_str(f.get("price")),
            "government_subsidy": government_subsidy_to_comma(f),

            "skills": skills_to_comma_string(f),
            "audience": norm_str(f.get("audience")),
            "required_knowledge": norm_str(f.get("required_knowledge")),

            "certificate_name": norm_str(f.get("certificate_name")),
            "email": norm_str(f.get("email")),
            "course_image": norm_str(f.get("course_image")),

            "description": cdata(f.get("description_html")),
            "description_program": cdata(f.get("description_program_html")),
            "description_extrainfo": cdata(f.get("description_extrainfo_html")),

            "location_and_date": lad_by_internal.get(internal_id, []),
            "sessions": sessions_by_internal.get(internal_id, []),
        }

        output.append(obj)

    # Sort output by internal_id for stable diffs
    output = sorted(output, key=lambda x: x.get("internal_id", ""))

    os.makedirs("data", exist_ok=True)
    with open("data/smartlions.json", "w", encoding="utf-8") as fp:
        json.dump(output, fp, ensure_ascii=False, indent=2)

    print(f"Exported {len(output)} records to data/smartlions.json")


if __name__ == "__main__":
    main()
