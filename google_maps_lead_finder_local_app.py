import csv
import io
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
import streamlit as st
from bs4 import BeautifulSoup

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
HUNTER_DOMAIN_SEARCH_URL = "https://api.hunter.io/v2/domain-search"
DEFAULT_TIMEOUT = 30
EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)
COMMON_CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/support",
    "/customer-service",
]
BAD_EMAIL_PREFIXES = {"example", "test", "sample", "yourname", "name"}


@dataclass
class Lead:
    business_name: str
    rating: Optional[float]
    reviews: Optional[int]
    category_hint: str
    search_query: str
    formatted_address: str
    website: str
    phone: str
    international_phone: str
    email: str
    email_source: str
    google_maps_url: str
    place_id: str
    business_status: str
    types: str


class GooglePlacesClient:
    def __init__(self, api_key: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 lead-finder/1.0"})

    def text_search(self, query: str, page_token: Optional[str] = None) -> Dict:
        params = {"key": self.api_key}
        if page_token:
            params["pagetoken"] = page_token
        else:
            params["query"] = query

        response = self.session.get(TEXT_SEARCH_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        status = payload.get("status")

        if status not in {"OK", "ZERO_RESULTS"}:
            error_message = payload.get("error_message", "")
            raise RuntimeError(
                f"Google Places Text Search failed. "
                f"Query='{query}' | Status='{status}' | Error='{error_message}'"
            )

        return payload

    def place_details(self, place_id: str) -> Dict:
        params = {
            "key": self.api_key,
            "place_id": place_id,
            "fields": ",".join(
                [
                    "name",
                    "place_id",
                    "business_status",
                    "formatted_address",
                    "formatted_phone_number",
                    "international_phone_number",
                    "website",
                    "rating",
                    "user_ratings_total",
                    "types",
                    "url",
                ]
            ),
        }

        response = self.session.get(DETAILS_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        status = payload.get("status")

        if status != "OK":
            error_message = payload.get("error_message", "")
            raise RuntimeError(
                f"Google Place Details failed. "
                f"Place ID='{place_id}' | Status='{status}' | Error='{error_message}'"
            )

        return payload["result"]


class HunterClient:
    def __init__(self, api_key: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

    def domain_search(self, domain: str) -> List[str]:
        params = {"domain": domain, "api_key": self.api_key, "limit": 10}
        response = self.session.get(HUNTER_DOMAIN_SEARCH_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        emails = payload.get("data", {}).get("emails", [])

        found: List[str] = []
        for item in emails:
            value = (item.get("value") or "").strip()
            if value:
                found.append(value)

        return found


def normalize_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def is_likely_real_email(email: str, domain: str = "") -> bool:
    email = email.strip().lower()
    if not email or "@" not in email:
        return False

    prefix = email.split("@", 1)[0]
    if prefix in BAD_EMAIL_PREFIXES:
        return False

    return True


def score_email(email: str, domain: str) -> Tuple[int, str]:
    lower = email.lower()
    score = 0

    if domain and lower.endswith("@" + domain):
        score += 5

    for good_prefix in ["owner", "info", "office", "sales", "contact", "support", "admin"]:
        if lower.startswith(good_prefix + "@"):
            score += 3
            return score, lower

    return score, lower


def extract_emails_from_text(text: str, domain: str = "") -> List[str]:
    matches = EMAIL_RE.findall(text or "")
    cleaned = []
    seen = set()

    for email in matches:
        email = email.strip().strip(".,;:()[]{}<>")
        if not is_likely_real_email(email, domain=domain):
            continue
        if email.lower() in seen:
            continue
        seen.add(email.lower())
        cleaned.append(email)

    return cleaned


def fetch_page(session: requests.Session, url: str, timeout: int) -> str:
    resp = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 lead-finder/1.0"})
    resp.raise_for_status()
    return resp.text


def find_emails_from_website(session: requests.Session, website: str, timeout: int) -> Tuple[str, str]:
    if not website:
        return "", ""

    domain = normalize_domain(website)
    urls_to_try = [website]

    for path in COMMON_CONTACT_PATHS:
        urls_to_try.append(urljoin(website, path))

    found: List[str] = []

    for url in urls_to_try:
        try:
            html = fetch_page(session, url, timeout)
            found.extend(extract_emails_from_text(html, domain=domain))

            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.lower().startswith("mailto:"):
                    candidate = href.split(":", 1)[1].split("?")[0].strip()
                    if is_likely_real_email(candidate, domain=domain):
                        found.append(candidate)
        except Exception:
            continue

    deduped = []
    seen = set()
    for email in found:
        key = email.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(email)

    if not deduped:
        return "", ""

    best = sorted(deduped, key=lambda e: score_email(e, domain), reverse=True)[0]
    return best, "website"


def iter_place_ids(client: GooglePlacesClient, query: str, max_pages: int) -> Iterable[Dict]:
    page_token: Optional[str] = None
    pages_seen = 0

    while pages_seen < max_pages:
        payload = client.text_search(query=query, page_token=page_token)
        results = payload.get("results", [])

        for item in results:
            yield item

        page_token = payload.get("next_page_token")
        pages_seen += 1

        if not page_token:
            break

        time.sleep(2.5)


def lead_from_details(details: Dict, search_query: str, category_hint: str, email: str, email_source: str) -> Lead:
    return Lead(
        business_name=details.get("name", ""),
        rating=details.get("rating"),
        reviews=details.get("user_ratings_total"),
        category_hint=category_hint,
        search_query=search_query,
        formatted_address=details.get("formatted_address", ""),
        website=details.get("website", ""),
        phone=details.get("formatted_phone_number", ""),
        international_phone=details.get("international_phone_number", ""),
        email=email,
        email_source=email_source,
        google_maps_url=details.get("url", ""),
        place_id=details.get("place_id", ""),
        business_status=details.get("business_status", ""),
        types=", ".join(details.get("types", [])),
    )


def passes_filters(
    lead: Lead,
    min_rating: float,
    max_rating: float,
    min_reviews: int,
    require_website: bool,
    only_operational: bool,
    require_email: bool,
) -> bool:
    if lead.rating is None or lead.reviews is None:
        return False
    if lead.rating < min_rating or lead.rating > max_rating:
        return False
    if lead.reviews < min_reviews:
        return False
    if require_website and not lead.website:
        return False
    if only_operational and lead.business_status and lead.business_status != "OPERATIONAL":
        return False
    if require_email and not lead.email:
        return False

    return True


def dedupe_leads(leads: List[Lead]) -> List[Lead]:
    seen: Set[str] = set()
    deduped: List[Lead] = []

    for lead in leads:
        key = lead.place_id or f"{lead.business_name}|{lead.formatted_address}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(lead)

    return deduped


def leads_to_csv_bytes(leads: List[Lead]) -> bytes:
    output = io.StringIO()
    fieldnames = list(Lead.__dataclass_fields__.keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for lead in leads:
        writer.writerow(asdict(lead))

    return output.getvalue().encode("utf-8")


def run_search(
    google_api_key: str,
    hunter_api_key: str,
    queries: List[str],
    min_rating: float,
    max_rating: float,
    min_reviews: int,
    max_pages: int,
    require_email: bool,
    allow_no_website: bool,
    include_non_operational: bool,
) -> List[Lead]:
    client = GooglePlacesClient(api_key=google_api_key)
    hunter = HunterClient(hunter_api_key) if hunter_api_key else None
    leads: List[Lead] = []

    for query in queries:
        for item in iter_place_ids(client, query=query, max_pages=max_pages):
            place_id = item.get("place_id")
            if not place_id:
                continue

            try:
                details = client.place_details(place_id)
                website = details.get("website", "")
                email = ""
                email_source = ""

                if website:
                    email, email_source = find_emails_from_website(client.session, website, client.timeout)

                if not email and hunter and website:
                    domain = normalize_domain(website)
                    if domain:
                        hunter_emails = hunter.domain_search(domain)
                        if hunter_emails:
                            email = hunter_emails[0]
                            email_source = "hunter"

                lead = lead_from_details(
                    details=details,
                    search_query=query,
                    category_hint=query,
                    email=email,
                    email_source=email_source,
                )

                if passes_filters(
                    lead=lead,
                    min_rating=min_rating,
                    max_rating=max_rating,
                    min_reviews=min_reviews,
                    require_website=not allow_no_website,
                    only_operational=not include_non_operational,
                    require_email=require_email,
                ):
                    leads.append(lead)

            except Exception:
                continue

    leads = dedupe_leads(leads)
    leads.sort(
        key=lambda x: (
            x.rating if x.rating is not None else 99,
            -(x.reviews or 0),
            x.business_name.lower(),
        )
    )
    return leads


st.set_page_config(page_title="Google Maps Lead Finder", page_icon="📍", layout="wide")
st.title("📍 Google Maps Lead Finder")
st.caption("Find low-rated businesses with 50+ reviews and export them with email when available.")

with st.sidebar:
    st.header("API keys")
    google_api_key = st.text_input(
        "Google Maps API key",
        value=os.getenv("GOOGLE_MAPS_API_KEY", ""),
        type="password",
    )
    hunter_api_key = st.text_input(
        "Hunter API key",
        value=os.getenv("HUNTER_API_KEY", ""),
        type="password",
        help="Optional, but improves email coverage.",
    )

    st.header("Filters")
    min_rating = st.number_input("Min rating", min_value=0.0, max_value=5.0, value=2.0, step=0.1)
    max_rating = st.number_input("Max rating", min_value=0.0, max_value=5.0, value=3.9, step=0.1)
    min_reviews = st.number_input("Min reviews", min_value=0, value=50, step=1)
    max_pages = st.number_input("Max pages per query", min_value=1, max_value=3, value=3, step=1)
    require_email = st.checkbox("Require email", value=True)
    allow_no_website = st.checkbox("Allow no website", value=False)
    include_non_operational = st.checkbox("Include non-operational", value=False)

queries_text = st.text_area(
    "Search queries",
    value="roofing company in New Jersey\nplumber in Jersey City NJ",
    height=160,
    help="One query per line.",
)

col1, col2 = st.columns([1, 2])

with col1:
    run_button = st.button("Run search", type="primary", use_container_width=True)

with col2:
    st.info("Use focused searches like 'roofing company in New Jersey' or 'hvac contractor in Miami FL'.")

if run_button:
    if not google_api_key:
        st.error("Add your Google Maps API key in the sidebar first.")
    elif min_rating > max_rating:
        st.error("Min rating cannot be higher than max rating.")
    else:
        queries = [q.strip() for q in queries_text.splitlines() if q.strip()]
        if not queries:
            st.error("Add at least one search query.")
        else:
            try:
                with st.spinner("Running search and collecting leads..."):
                    leads = run_search(
                        google_api_key=google_api_key,
                        hunter_api_key=hunter_api_key,
                        queries=queries,
                        min_rating=min_rating,
                        max_rating=max_rating,
                        min_reviews=min_reviews,
                        max_pages=max_pages,
                        require_email=require_email,
                        allow_no_website=allow_no_website,
                        include_non_operational=include_non_operational,
                    )

                st.success(f"Found {len(leads)} leads.")

                if leads:
                    rows = [asdict(lead) for lead in leads]
                    st.dataframe(rows, use_container_width=True)
                    st.download_button(
                        label="Download CSV",
                        data=leads_to_csv_bytes(leads),
                        file_name="google_maps_leads.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                else:
                    st.warning("No leads matched your filters. Try broader searches or remove the email requirement.")

            except Exception as e:
                st.error(str(e))
                st.stop()

with st.expander("How to run this locally"):
    st.code(
        "pip install streamlit requests beautifulsoup4\n"
        "streamlit run google_maps_lead_finder_local_app.py",
        language="bash",
    )
    st.write("You can also set GOOGLE_MAPS_API_KEY and HUNTER_API_KEY as environment variables before launching.")
