import csv
import os
import time
from datetime import datetime
from urllib.parse import urljoin

import boto3
import requests
from bs4 import BeautifulSoup

ALL_STATE_CODES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
]

DEFAULT_STATE_CODES = ["CA", "AZ", "UT", "CO", "NM"]


def parse_state_codes():
    raw = os.getenv("NPS_STATE_CODES", "").strip()
    if raw:
        if raw.upper() == "ALL":
            return ALL_STATE_CODES
        return [value.strip().upper() for value in raw.split(",") if value.strip()]
    return DEFAULT_STATE_CODES


def parse_int(value, fallback):
    try:
        parsed = int(value)
        return parsed
    except (TypeError, ValueError):
        return fallback


def normalize_text(value):
    if not value:
        return ""
    return " ".join(str(value).split())


def getenv_with_fallback(primary_key, fallback_key, default=None):
    value = os.getenv(primary_key)
    if not value:
        value = os.getenv(fallback_key)
    if not value:
        return default
    return value


def parse_state_name(soup):
    title = soup.title.string if soup.title else ""
    return normalize_text(title.split("(")[0])


def extract_park_code(href):
    if not href:
        return ""
    cleaned = href.strip().strip("/")
    if not cleaned:
        return ""
    return cleaned.split("/")[0].lower()


def extract_description(container):
    if not container:
        return ""
    for paragraph in container.find_all("p"):
        classes = paragraph.get("class", [])
        if "list_left__kicker" in classes or "list_left__subtitle" in classes:
            continue
        text = normalize_text(paragraph.get_text(" ", strip=True))
        if text:
            return text
    return ""


def parse_parks_from_state(html, state_code, base_url):
    soup = BeautifulSoup(html, "html.parser")
    state_name = parse_state_name(soup)
    list_root = soup.select_one("ul#list_parks")
    if not list_root:
        return [], state_name

    parks = []
    for item in list_root.find_all("li", recursive=False):
        anchor = item.select_one("h3 a")
        if not anchor:
            continue
        park_name = normalize_text(anchor.get_text(" ", strip=True))
        href = anchor.get("href", "")
        park_code = extract_park_code(href)
        if not park_code or not park_name:
            continue

        designation = ""
        designation_el = item.select_one(".list_left__kicker")
        if designation_el:
            designation = normalize_text(designation_el.get_text(" ", strip=True))

        location = ""
        location_el = item.select_one(".list_left__subtitle")
        if location_el:
            location = normalize_text(location_el.get_text(" ", strip=True))

        description = ""
        left = item.select_one(".list_left")
        if left:
            description = extract_description(left)

        park_url = urljoin(base_url, f"/{park_code}/")

        parks.append(
            {
                "ParkCode": park_code.upper(),
                "ParkName": park_name,
                "StateCode": state_code.upper(),
                "StateName": state_name,
                "Location": location,
                "Designation": designation,
                "Description": description,
                "ParkUrl": park_url,
            }
        )

    return parks, state_name


def main():
    bucket = getenv_with_fallback("SUPABASE_S3_BUCKET", "S3_BUCKET")
    endpoint = getenv_with_fallback("SUPABASE_S3_ENDPOINT", "S3_ENDPOINT")
    access_key = getenv_with_fallback("SUPABASE_S3_ACCESS_KEY", "S3_ACCESS_KEY")
    secret_key = getenv_with_fallback("SUPABASE_S3_SECRET_KEY", "S3_SECRET_KEY")
    region = getenv_with_fallback("SUPABASE_S3_REGION", "S3_REGION", "us-east-1")
    prefix = getenv_with_fallback("SUPABASE_S3_PREFIX", "S3_PREFIX", "inbound/")

    if not all([bucket, endpoint, access_key, secret_key]):
        raise SystemExit(
            "Missing S3 environment variables. "
            "Set SUPABASE_S3_* or S3_* values."
        )

    base_url = os.getenv("NPS_BASE_URL", "https://www.nps.gov")
    user_agent = os.getenv("NPS_USER_AGENT", "tp3-crawler/1.0")
    delay_seconds = float(os.getenv("NPS_REQUEST_DELAY_SECONDS", "0.5"))
    park_limit = parse_int(os.getenv("NPS_PARK_LIMIT", "80"), 80)
    state_codes = parse_state_codes()
    crawl_date = datetime.utcnow().date().isoformat()

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    rows = []
    seen_codes = set()

    for state_code in state_codes:
        state_code = state_code.upper()
        url = f"{base_url}/state/{state_code.lower()}/index.htm"
        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"Failed to fetch {url}: {exc}")
            continue

        parks, _state_name = parse_parks_from_state(response.text, state_code, base_url)
        for park in parks:
            park_code_key = park["ParkCode"].upper()
            if park_code_key in seen_codes:
                continue
            seen_codes.add(park_code_key)
            park["CrawlDate"] = crawl_date
            rows.append(park)
            if park_limit > 0 and len(rows) >= park_limit:
                break

        if park_limit > 0 and len(rows) >= park_limit:
            break

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    filename = f"nps_parks_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    local_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)

    fieldnames = [
        "ParkCode",
        "ParkName",
        "StateCode",
        "StateName",
        "Location",
        "Designation",
        "Description",
        "ParkUrl",
        "CrawlDate",
    ]

    with open(local_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    key = f"{prefix}{filename}"
    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded {local_path} to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
