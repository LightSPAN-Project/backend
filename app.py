"""
NUS LightUP Admin Dashboard (Streamlit + Firestore)

Key features:
- Fixed allowlist loaded from repo file (absolute path default: /app/allowlist.csv)
- Password gate for public URL deployments
- Robust submittedAt string parsing (JS Date string variants) using dateutil, with fallbacks
- Sections:
  1) Goals (latest per user)
  2) History update responses (all; small)
  3) Last place table (sync cursor)
  4) Responses (can grow; supports per-user limit)
  5) Questionnaire (all; small; optional sibling explode)
- CSV export via download button, plus optional local save (disabled on Cloud Run because ephemeral)
"""

import os
import json
import hmac
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional
import json
import pandas as pd

import pandas as pd
import streamlit as st

import firebase_admin
from firebase_admin import credentials, firestore

# Robust datetime parsing
from dateutil import parser as dateparser  # python-dateutil

# ------------------- Constants -------------------
SGT = ZoneInfo("Asia/Singapore")
RUNNING_ON_CLOUD_RUN = bool(os.getenv("K_SERVICE"))

# Allowlist absolute path inside container by default
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ALLOWLIST_PATH = os.getenv(
    "ALLOWLIST_PATH",
    os.path.join(BASE_DIR, "app", "allowlist.csv"),
)


EXPORT_DIR = os.getenv("EXPORT_DIR", "exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

DEFAULT_CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "120"))

# ------------------- Password gate -------------------
def check_password_gate() -> None:
    """
    Minimal password gate suitable for a public Cloud Run URL.
    Set DASHBOARD_PASSWORD in Cloud Run environment variables.

    If DASHBOARD_PASSWORD is unset/empty, the gate is disabled (useful for local dev).
    """
    pw = os.getenv("DASHBOARD_PASSWORD", "")
    if not pw:
        return

    if "password_ok" not in st.session_state:
        st.session_state.password_ok = False

    if st.session_state.password_ok:
        return

    st.sidebar.header("Access")
    st.sidebar.text_input("Password", type="password", key="pw_input")
    if st.sidebar.button("Enter", use_container_width=True):
        st.session_state.password_ok = hmac.compare_digest(st.session_state.pw_input, pw)

    if not st.session_state.password_ok:
        st.error("Password required.")
        st.stop()


# ---------- Firebase init ----------
def init_firebase():
    """
    Simple + reliable:
    - Cloud Run: uses ADC (service account attached to Cloud Run)
    - Local: uses FIREBASE_KEY_PATH JSON
    - If a previous Firebase app exists but is unusable, delete & re-init.
    """

    def _local_key_path() -> str:
        key_path = os.getenv("FIREBASE_KEY_PATH")
        if not key_path:
            raise RuntimeError(
                "FIREBASE_KEY_PATH is not set (local dev).\n"
                "Set it to your service account JSON absolute path, e.g.\n"
                'export FIREBASE_KEY_PATH="/Users/denzdelvillar/keys/nus-firestore.json"'
            )
        return key_path

    # 1) If something already initialized firebase, try using it.
    if firebase_admin._apps:
        try:
            return firestore.client()
        except Exception:
            # Existing app is broken (often because it was initialized with ADC but ADC not available).
            # Delete and re-init deterministically.
            for app in list(firebase_admin._apps.values()):
                try:
                    firebase_admin.delete_app(app)
                except Exception:
                    pass

    # 2) Deterministic init
    if os.getenv("K_SERVICE"):
        # Cloud Run
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"projectId": os.getenv("GOOGLE_CLOUD_PROJECT")})
        return firestore.client()

    # Local dev (no ADC): always use service account JSON
    key_path = _local_key_path()
    firebase_admin.initialize_app(credentials.Certificate(key_path))
    return firestore.client()

db = init_firebase()

# ------------------- Helpers -------------------
def now_stamp() -> str:
    return datetime.now(tz=SGT).strftime("%Y%m%d_%H%M%S")


def save_and_download(df: pd.DataFrame, section_name: str) -> None:
    ts = now_stamp()
    fname = f"{section_name}_{ts}.csv".replace(" ", "_").lower()

    # Download (reliable everywhere)
    st.download_button(
        label=f"Export CSV: {fname}",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=fname,
        mime="text/csv",
        use_container_width=True,
    )

    # Optional server-side save (NOT persistent on Cloud Run)
    if not RUNNING_ON_CLOUD_RUN:
        fpath = os.path.join(EXPORT_DIR, fname)
        df.to_csv(fpath, index=False)
        st.caption(f"Also saved locally to: {fpath}")
    else:
        st.caption("Note: Cloud Run filesystem is ephemeral; only the download is persistent.")


def to_dt_firestore_any(x: Any) -> Optional[datetime]:
    # Firestore Timestamp / DatetimeWithNanoseconds
    if hasattr(x, "to_datetime"):
        try:
            x = x.to_datetime()
        except Exception:
            pass
    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=ZoneInfo("UTC")).astimezone(SGT)
        return x.astimezone(SGT)
    return None


_JS_TZ_RE = re.compile(r"\bGMT([+-]\d{4})\b")

def parse_submitted_at_any(s: Any) -> Optional[datetime]:
    """
    Robust interpreter for strings like:
    'Wed Feb 19 2025 16:06:17 GMT+0800 (Singapore Standard Time)'

    Strategy:
    - If already datetime => normalize to SGT
    - If Firestore timestamp => normalize
    - If string:
      - strip trailing parenthetical timezone name
      - normalize 'GMT+0800' => '+0800' (dateutil parses offsets well)
      - try dateutil.parse with fuzzy=True
      - fallback: isoformat parse
    """
    if s is None:
        return None

    if isinstance(s, datetime):
        return s.astimezone(SGT) if s.tzinfo else s.replace(tzinfo=ZoneInfo("UTC")).astimezone(SGT)

    # Firestore timestamp object
    dt_fs = to_dt_firestore_any(s)
    if dt_fs is not None:
        return dt_fs

    if not isinstance(s, str):
        return None

    txt = s.strip()
    if not txt:
        return None

    # Remove "(Singapore Standard Time)" or similar
    txt = txt.split(" (")[0].strip()

    # Convert "GMT+0800" -> "+0800" (more parser-friendly)
    txt = _JS_TZ_RE.sub(r"\1", txt)

    # Some strings might include commas or extra tokens; dateutil can handle fuzzy parsing.
    try:
        dt = dateparser.parse(txt, fuzzy=True)
        if dt is None:
            return None
        if dt.tzinfo is None:
            # If no tz info, assume SGT because original strings are typically local device time
            dt = dt.replace(tzinfo=SGT)
        return dt.astimezone(SGT)
    except Exception:
        pass

    # Fallback: ISO
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return dt.astimezone(SGT) if dt.tzinfo else dt.replace(tzinfo=SGT)
    except Exception:
        return None


def safe_get(d: Dict, path: str, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


@st.cache_data(ttl=3600, show_spinner=False)
def load_allowlist() -> pd.DataFrame:
    if not os.path.exists(ALLOWLIST_PATH):
        raise FileNotFoundError(
            f"Allowlist file not found at '{ALLOWLIST_PATH}'. "
            "Make sure allowlist.csv is included in the image and ALLOWLIST_PATH is correct."
        )
    df = pd.read_csv(ALLOWLIST_PATH, dtype=str)
    if "user_id" not in df.columns or "lightup_id" not in df.columns:
        raise ValueError("Allowlist must have columns: user_id, lightup_id")

    df = df[["user_id", "lightup_id"]].dropna().drop_duplicates()
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df["lightup_id"] = df["lightup_id"].astype(str).str.strip()

    # Optional sanity checks (recommended)
    # Ensure each lightup_id maps to exactly one user_id (or relax if needed)
    # dup = df["lightup_id"].duplicated(keep=False)
    # if dup.any():
    #     raise ValueError("Allowlist has duplicate lightup_id values; expected 1:1 mapping.")

    return df


def enforce_allowlist(df: pd.DataFrame, df_map: pd.DataFrame) -> pd.DataFrame:
    """
    Extra safety: only show rows that match allowlist.
    If both ids exist in df, enforce pair match.
    If only one exists, enforce membership and add the other via join if possible.
    """
    if df.empty:
        return df

    if "user_id" in df.columns and "lightup_id" in df.columns:
        return df.merge(df_map, on=["user_id", "lightup_id"], how="inner")

    if "user_id" in df.columns:
        return df.merge(df_map, on=["user_id"], how="inner")

    if "lightup_id" in df.columns:
        return df.merge(df_map, on=["lightup_id"], how="inner")

    # If neither id exists, refuse to show
    return df.iloc[0:0]


def ms_or_s_to_sgt(ts: Any) -> Optional[datetime]:
    if not isinstance(ts, (int, float)):
        return None
    # heuristics: ms vs sec
    if ts > 1e12:  # ms
        return datetime.fromtimestamp(ts / 1000.0, tz=SGT)
    if ts > 1e9:  # sec
        return datetime.fromtimestamp(ts, tz=SGT)
    return None


# ------------------- Fetchers -------------------
@st.cache_data(ttl=DEFAULT_CACHE_TTL, show_spinner=False)
def fetch_goals_latest(user_ids: List[str]) -> pd.DataFrame:
    rows = []
    for uid in user_ids:
        col = db.collection("actigraphy_data").document(str(uid)).collection("goal")
        docs = list(col.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(1).stream())
        if not docs:
            continue
        d = docs[0].to_dict() or {}
        ts_raw = d.get("timestamp")
        ts_dt = ms_or_s_to_sgt(ts_raw)
        rows.append(
            {
                "user_id": str(uid),
                "goal": d.get("goal"),
                "timestamp_raw": ts_raw,
                "timestamp_sgt": ts_dt.isoformat() if ts_dt else None,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["_sort"] = pd.to_datetime(df["timestamp_sgt"], errors="coerce")
        df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
    return df


@st.cache_data(ttl=DEFAULT_CACHE_TTL, show_spinner=False)
def fetch_history_all(lightup_ids: List[str]) -> pd.DataFrame:
    """
    History is not expected to grow big, so we fetch all for allowlisted users.
    """
    rows = []
    for pid in lightup_ids:
        col = db.collection("history_update").document(str(pid)).collection("responses")
        for doc in col.stream():
            d = doc.to_dict() or {}
            dt = parse_submitted_at_any(d.get("submittedAt"))

            rows.append(
                {
                    "lightup_id": str(pid),
                    "submittedAt_raw": d.get("submittedAt"),
                    "submittedAt_sgt": dt.isoformat() if dt else None,

                    # Required fields
                    "adverseEvents": safe_get(d, "response.adverseEvents"),
                    "issuesWithWearable": safe_get(d, "response.issuesWithWearable"),

                    # Optional description fields (missing before)
                    "adverseDescription": safe_get(d, "response.adverseDescription"),
                    "issueDescription": safe_get(d, "response.issueDescription"),
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["_sort"] = pd.to_datetime(df["submittedAt_sgt"], errors="coerce")
        df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
    return df


@st.cache_data(ttl=DEFAULT_CACHE_TTL, show_spinner=False)
def fetch_last_place(user_ids: List[str]) -> pd.DataFrame:
    rows = []
    for uid in user_ids:
        doc = db.collection("last_place_table").document(str(uid)).get()
        if not doc.exists:
            continue
        d = doc.to_dict() or {}

        last_data_time = d.get("last_data_time")
        ldt = parse_last_data_time_assume_sgt(last_data_time)

        updated_at = to_dt_firestore_any(d.get("updated_at"))

        rows.append(
            {
                "user_id": str(uid),
                "last_data_time_sgt": ldt.isoformat() if ldt else None,
                "fetched_at_sgt": updated_at.isoformat() if updated_at else None,
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["_sort"] = pd.to_datetime(df["last_data_time_sgt"], errors="coerce")
        df = df.sort_values("_sort", ascending=True).drop(columns=["_sort"])
    return df


def parse_message_delivered_at_any(x: Any) -> Optional[datetime]:
    """
    Parses messageDeliveredAt strings like:
      "Sun, 04 Jan 2026 10:00"
    If tz is missing, assume SGT.
    """
    if x is None:
        return None

    if isinstance(x, datetime):
        return x.astimezone(SGT) if x.tzinfo else x.replace(tzinfo=SGT)

    # Firestore timestamp object support (if it ever becomes one)
    dt_fs = to_dt_firestore_any(x)
    if dt_fs is not None:
        return dt_fs

    if not isinstance(x, str):
        return None

    txt = x.strip()
    if not txt:
        return None

    try:
        dt = dateparser.parse(txt, fuzzy=True)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SGT)
        return dt.astimezone(SGT)
    except Exception:
        return None


@st.cache_data(ttl=DEFAULT_CACHE_TTL, show_spinner=False)
def fetch_responses_per_user(lightup_ids: List[str], per_user_limit: int) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for pid in lightup_ids:
        col = db.collection("responses").document(str(pid)).collection("responses")

        docs = []
        used_fast_path = False
        try:
            docs = list(
                col.order_by("submittedAtTs", direction=firestore.Query.DESCENDING)
                   .limit(per_user_limit)
                   .stream()
            )

            used_fast_path = True
        except Exception:
            docs = list(col.stream())

        if not docs:
            docs = list(col.stream())
            used_fast_path = False

        tmp: List[Dict[str, Any]] = []

        for doc in docs:
            d = doc.to_dict() or {}

            submitted_raw = d.get("submittedAt") or safe_get(d, "response.submittedAt")
            submitted_dt = parse_submitted_at_any(submitted_raw) or to_dt_firestore_any(d.get("submittedAtTs"))

            delivered_raw = d.get("messageDeliveredAt")
            delivered_dt = parse_message_delivered_at_any(delivered_raw)

            resp = d.get("response")
            if resp is None:
                resp = {}
            if not isinstance(resp, (dict, list)):
                # unexpected type, keep as string
                resp_json = json.dumps({"value": str(resp)}, ensure_ascii=False)
            else:
                resp_json = json.dumps(resp, ensure_ascii=False)

            # UI: empty response => acknowledged
            is_empty = (resp == {} or resp == [] or resp_json in ("{}", "[]"))
            responses_display = "" if is_empty else resp_json

            tmp.append(
                {
                    "lightup_id": str(pid),
                    "doc_id": doc.id,
                    "messageId": d.get("messageId"),
                    "messageDeliveredAt_raw": delivered_raw,
                    "messageDeliveredAt_sgt": delivered_dt.isoformat() if delivered_dt else None,
                    "submittedAt_raw": submitted_raw,
                    "submittedAt_sgt": submitted_dt.isoformat() if submitted_dt else None,
                    "responses_display": responses_display,     # <-- for UI
                    "response_json": resp_json,                # <-- for CSV/audit
                }
            )

        # If we had to stream all docs, enforce per_user_limit by sorting in python
        if not used_fast_path and tmp:
            tdf = pd.DataFrame(tmp)
            tdf["_sort"] = pd.to_datetime(tdf["submittedAt_sgt"], errors="coerce")
            tdf = tdf.sort_values("_sort", ascending=False).drop(columns=["_sort"]).head(per_user_limit)
            rows.extend(tdf.to_dict(orient="records"))
        else:
            rows.extend(tmp)

    df = pd.DataFrame(rows)

    # Sort for UI: within user, most recent submittedAt first (fallback to deliveredAt)
    if not df.empty:
        df["_sort_submit"] = pd.to_datetime(df["submittedAt_sgt"], errors="coerce")
        df["_sort_deliv"] = pd.to_datetime(df["messageDeliveredAt_sgt"], errors="coerce")
        df["_sort"] = df["_sort_submit"].fillna(df["_sort_deliv"])
        df = df.sort_values(["lightup_id", "_sort"], ascending=[True, False]).drop(
            columns=["_sort_submit", "_sort_deliv", "_sort"]
        )

    return df




def parse_last_data_time_assume_sgt(raw: Any) -> Optional[datetime]:
    """
    Project rule:
    - last_data_time is already in SGT (even if it ends with 'Z')
    - If raw has a timezone offset like +08:00, respect it
    - If raw is naive, assume SGT
    """
    if raw is None:
        return None

    if isinstance(raw, datetime):
        return raw.astimezone(SGT) if raw.tzinfo else raw.replace(tzinfo=SGT)

    if not isinstance(raw, str):
        return None

    txt = raw.strip()
    if not txt:
        return None

    # If it ends with 'Z', DO NOT treat as UTC. Strip it and assume SGT.
    if txt.endswith("Z"):
        txt = txt[:-1]  # remove trailing Z
        try:
            dt = datetime.fromisoformat(txt)  # naive now
            return dt.replace(tzinfo=SGT)
        except Exception:
            return None

    # Otherwise try normal ISO parse (may include +08:00)
    try:
        dt = datetime.fromisoformat(txt)
        return dt.astimezone(SGT) if dt.tzinfo else dt.replace(tzinfo=SGT)
    except Exception:
        return None

def format_sgt_display(x: Any) -> Optional[str]:
    """
    Formats a datetime / ISO string into:
    dd mmm yyyy HH:MM:SS (SGT)
    """
    if x is None:
        return None
    try:
        dt = pd.to_datetime(x)
        if dt.tzinfo is None:
            dt = dt.tz_localize("Asia/Singapore")
        else:
            dt = dt.tz_convert("Asia/Singapore")
        return dt.strftime("%d %b %Y %H:%M:%S")
    except Exception:
        return None


def reorder_columns(df: pd.DataFrame, first: List[str]) -> pd.DataFrame:
    existing_first = [c for c in first if c in df.columns]
    rest = [c for c in df.columns if c not in existing_first]
    return df[existing_first + rest]

def add_hours_since_last_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column `hours_since_last_data` based on last_data_time_sgt.
    """
    if "last_data_time_sgt" not in df.columns:
        return df

    now = datetime.now(tz=SGT)

    def compute_hours(x):
        try:
            dt = pd.to_datetime(x)
            return round((now - dt).total_seconds() / 3600, 2)
        except Exception:
            return None

    df = df.copy()
    df["hours_since_last_data"] = df["last_data_time_sgt"].apply(compute_hours)
    return df

def snapshot_timestamp_sgt() -> str:
    """
    Human-readable snapshot time for captions, in SGT.
    Example: 07 Jan 2026 13:16
    """
    return datetime.now(tz=SGT).strftime("%d %b %Y %H:%M")


# -----------------------
# Flatten helpers
# -----------------------
def flatten_dict(d: dict, prefix: str = "", sep: str = ".") -> dict:
    out = {}
    for k, v in (d or {}).items():
        key = f"{prefix}{sep}{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(flatten_dict(v, prefix=key, sep=sep))
        else:
            out[key] = v
    return out


def siblings_to_columns(siblings: list, max_siblings: int = 5) -> dict:
    out = {"siblings_count": len(siblings) if isinstance(siblings, list) else 0}
    if not isinstance(siblings, list):
        return out

    for i, sib in enumerate(siblings[:max_siblings]):
        if not isinstance(sib, dict):
            continue
        for k, v in sib.items():
            out[f"siblings_{i}_{k}"] = v
    return out


MAX_SIBLINGS = 5  # adjust if you want


def questionnaire_row_for_csv(base: dict) -> dict:
    """
    Takes a base row that includes:
      - indoorActivities (dict or None)
      - outdoorActivities (dict or None)
      - siblings (list or None)
    Returns a fully flattened row suitable for CSV export.
    """
    row = dict(base)

    indoor = row.pop("indoorActivities", None)
    outdoor = row.pop("outdoorActivities", None)
    siblings = row.pop("siblings", None)

    # Flatten indoor activities
    if isinstance(indoor, dict):
        row.update(flatten_dict(indoor, prefix="indoorActivities"))
    else:
        row["indoorActivities"] = indoor  # fallback (should be rare)

    # Flatten outdoor activities
    if isinstance(outdoor, dict):
        row.update(flatten_dict(outdoor, prefix="outdoorActivities"))
    else:
        row["outdoorActivities"] = outdoor

    # Flatten siblings into columns + keep raw JSON
    if isinstance(siblings, list):
        row.update(siblings_to_columns(siblings, max_siblings=MAX_SIBLINGS))
        row["siblings_json"] = json.dumps(siblings, ensure_ascii=False)
    else:
        row["siblings_count"] = 0
        row["siblings_json"] = None

    return row


# -----------------------
# Main fetcher (UPDATED)
# -----------------------
@st.cache_data(ttl=DEFAULT_CACHE_TTL, show_spinner=False)
def fetch_questionnaires(lightup_ids: List[str], explode_siblings: bool = False) -> pd.DataFrame:
    """
    explode_siblings=False:
      - one row per questionnaire response
      - indoor/outdoor flattened into columns
      - siblings indexed into columns (siblings_0_age, siblings_1_sex, ...)
      - includes siblings_json for raw audit

    explode_siblings=True:
      - one row per sibling (keeps base fields + sibling_index + sibling_* fields)
      - ALSO flattens indoor/outdoor into columns for each sibling row
    """
    rows: List[Dict[str, Any]] = []

    for pid in lightup_ids:
        col = db.collection("questionnaire").document(str(pid)).collection("responses")

        for doc in col.stream():
            d = doc.to_dict() or {}

            # submittedAt is usually top-level; fallback to response.submittedAt if ever needed
            submitted_raw = d.get("submittedAt") or safe_get(d, "response.submittedAt")
            dt = parse_submitted_at_any(submitted_raw)

            # ✅ answers live inside "response"
            r = d.get("response") or {}
            if not isinstance(r, dict):
                r = {}

            indoor = r.get("indoorActivities")
            outdoor = r.get("outdoorActivities")
            siblings = r.get("siblings", [])

            base = {
                "lightup_id": str(pid),
                "doc_id": doc.id,
                "submittedAt_raw": submitted_raw,
                "submittedAt_sgt": dt.isoformat() if dt else None,

                # requested fields (read from r)
                "housingType": r.get("housingType"),
                "motherEducation": r.get("motherEducation"),
                "fatherEducation": r.get("fatherEducation"),

                "motherMyopia": r.get("motherMyopia"),
                "motherMyopiaDegree": r.get("motherMyopiaDegree"),
                "motherAge": r.get("motherAge"),

                "fatherMyopia": r.get("fatherMyopia"),
                "fatherMyopiaDegree": r.get("fatherMyopiaDegree"),
                "fatherAge": r.get("fatherAge"),

                "childMyopia": r.get("childMyopia"),
                "childMyopiaGroup": r.get("childMyopiaGroup"),
                "childAge": r.get("childAge"),
                "childLeftEyeDegree": r.get("childLeftEyeDegree"),
                "childRightEyeDegree": r.get("childRightEyeDegree"),

                "booksPerWeek": r.get("booksPerWeek"),

                # include raw nested structures so we can flatten consistently
                "indoorActivities": indoor,
                "outdoorActivities": outdoor,
                "siblings": siblings,
            }

            if explode_siblings and isinstance(siblings, list) and siblings:
                # one row per sibling, but still keep flattened indoor/outdoor per row
                for i, sib in enumerate(siblings):
                    sib_row = dict(base)
                    sib_row["sibling_index"] = i

                    if isinstance(sib, dict):
                        sib_row["sibling_age"] = sib.get("age")
                        sib_row["sibling_sex"] = sib.get("sex")
                        sib_row["sibling_myopia"] = sib.get("myopia")
                        sib_row["sibling_myopiaUnknown"] = sib.get("myopiaUnknown")
                    else:
                        sib_row["sibling_age"] = None
                        sib_row["sibling_sex"] = None
                        sib_row["sibling_myopia"] = None
                        sib_row["sibling_myopiaUnknown"] = None

                    # Flatten into a single row
                    rows.append(questionnaire_row_for_csv(sib_row))
            else:
                # one row per response (recommended default)
                rows.append(questionnaire_row_for_csv(base))

    df = pd.DataFrame(rows)
    if not df.empty:
        df["_sort"] = pd.to_datetime(df["submittedAt_sgt"], errors="coerce")
        df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
    return df


# ------------------- Streamlit App -------------------
st.set_page_config(page_title="NUS LightUP Dashboard", layout="wide")
check_password_gate()

st.title("NUS LightUP Admin Dashboard")
st.subheader(f"Captured at {snapshot_timestamp_sgt()} (SGT)")

# Load fixed allowlist
try:
    df_map = load_allowlist()
except Exception as e:
    st.error(str(e))
    st.stop()

with st.sidebar:
    st.header("Participants")
    st.caption(f"{len(df_map)} participants")

    scope = st.radio("View scope", ["All Users", "Select subset"], horizontal=True)

    if scope == "Select subset":
        selected = st.multiselect(
            "Select participants (lightup_id)",
            options=df_map["lightup_id"].tolist(),
            default=df_map["lightup_id"].tolist()[:10],
        )
        df_use = df_map[df_map["lightup_id"].isin(selected)].copy()
    else:
        df_use = df_map.copy()

    user_ids = df_use["user_id"].astype(str).tolist()
    lightup_ids = df_use["lightup_id"].astype(str).tolist()

    st.divider()
    st.subheader("Options")
    explode_siblings = st.checkbox("Questionnaire: explode siblings into rows", value=False)

    # responses/ grows: control costs
    per_user_limit = st.slider("Responses: max rows per user", min_value=10, max_value=500, value=100, step=10)

    if st.button("Refresh (clear cache)", use_container_width=True):
        st.cache_data.clear()

tabs = st.tabs(
    [
        "Latest Synced Data",
        "Questionnaire Responses",
        "Goal Responses",
        "History Update Responses",
        "Notification Responses",
    ]
)


with tabs[0]:
    st.subheader("Latest Synced Data")
    st.subheader(f"As of {snapshot_timestamp_sgt()} (SGT)")

    st.caption(
        "Shows the most recent data successfully retrieved by LightUP from Condor Cloud. "
        "This reflects the latest data state visible to the user at the time of capture."
    )

    df = fetch_last_place(user_ids)
    df = enforce_allowlist(df, df_use)

    # Add freshness metric
    df = add_hours_since_last_data(df)

    # Add user-friendly timestamps
    df["last_data_time"] = df["last_data_time_sgt"].apply(format_sgt_display)
    df["fetched_at"] = df["fetched_at_sgt"].apply(format_sgt_display)

    # Column order
    df = reorder_columns(
        df,
        [
            "lightup_id",
            "user_id",
            "last_data_time",
            "fetched_at",
            "hours_since_last_data",
        ],
    )

    # Hide raw technical columns
    df_display = df.drop(
        columns=[c for c in df.columns if c.endswith("_sgt") or c.endswith("_raw")],
        errors="ignore",
    )

    # Format hours nicely
    df_display["Hours since last data"] = df_display["hours_since_last_data"].apply(
        lambda x: f"{x:.2f} hrs" if pd.notnull(x) else None
    )
    df_display = df_display.drop(columns=["hours_since_last_data"])

    # Rename to Title Case
    df_display = df_display.rename(
        columns={
            "lightup_id": "LightUP ID",
            "user_id": "User ID",
            "last_data_time": "Last data time",
            "fetched_at": "Fetched at",
        }
    )

    st.dataframe(df_display, use_container_width=True)
    #save_and_download(df, "latest_synced_data")

with tabs[1]:
    st.subheader("Questionnaire Responses")

    df = fetch_questionnaires(lightup_ids, explode_siblings=explode_siblings)
    df = enforce_allowlist(df, df_use)

    # UI-friendly view
    df_display = df.copy()

    # Hide raw / noisy columns in UI
    df_display = df_display.drop(
        columns=["siblings_json", "submittedAt_raw", "doc_id"],
        errors="ignore",
    )

    # Beautify timestamp for UI only
    if "submittedAt_sgt" in df_display.columns:
        df_display["submittedAt_sgt"] = df_display["submittedAt_sgt"].apply(
            format_sgt_display
        )

    # Force ID columns first
    df_display = reorder_columns(
        df_display,
        ["lightup_id", "user_id"],
    )

    # Prettify non-response headers only
    HEADER_RENAME = {
        "lightup_id": "LightUP ID",
        "user_id": "User ID",
        "submittedAt_sgt": "Submitted at (SGT)",
        "siblings_count": "Siblings count",
    }

    df_display = df_display.rename(
        columns={k: v for k, v in HEADER_RENAME.items() if k in df_display.columns}
    )

    st.dataframe(df_display, use_container_width=True)

    # CSV keeps EVERYTHING (raw timestamps, doc_id, siblings_json, etc.)
    #save_and_download(df, "questionnaire_responses")


with tabs[2]:
    st.subheader("Goal Responses")

    df = fetch_goals_latest(user_ids)
    df = enforce_allowlist(df, df_use)

    # Sort by most recent (timestamp_sgt is already ISO in SGT)
    if not df.empty and "timestamp_sgt" in df.columns:
        df["_sort"] = pd.to_datetime(df["timestamp_sgt"], errors="coerce")
        df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])

    # UI-friendly view
    df_display = df.copy()

    # Prettify Submitted At
    if "timestamp_sgt" in df_display.columns:
        df_display["Submitted At"] = df_display["timestamp_sgt"].apply(format_sgt_display)

    # Hide raw timestamps in UI
    df_display = df_display.drop(columns=["timestamp_raw", "timestamp_sgt"], errors="ignore")

    # Put IDs first
    df_display = reorder_columns(df_display, ["lightup_id", "user_id"])

    # Ensure Goal is last column
    if "goal" in df_display.columns:
        cols = [c for c in df_display.columns if c != "goal"] + ["goal"]
        df_display = df_display[cols]

    # Prettify headers
    df_display = df_display.rename(
        columns={
            "lightup_id": "LightUP ID",
            "user_id": "User ID",
            "goal": "Goal",
        }
    )

    st.dataframe(df_display, use_container_width=True)

    # CSV keeps raw fields (timestamp_raw, timestamp_sgt)
    # save_and_download(df, "goals_latest")


with tabs[3]:
    st.subheader("History Update Responses")

    df = fetch_history_all(lightup_ids)
    df = enforce_allowlist(df, df_use)

    # UI-friendly view
    df_display = df.copy()

    # Prettify submission time
    if "submittedAt_sgt" in df_display.columns:
        df_display["Submitted At"] = df_display["submittedAt_sgt"].apply(format_sgt_display)

    # Hide raw/noisy columns in UI
    df_display = df_display.drop(columns=["submittedAt_raw", "submittedAt_sgt"], errors="ignore")

    # IDs first
    df_display = reorder_columns(df_display, ["lightup_id", "user_id"])

    # Prettify non-response headers only
    df_display = df_display.rename(
        columns={
            "lightup_id": "LightUP ID",
            "user_id": "User ID",
            "adverseEvents": "Adverse events",
            "adverseDescription": "Adverse description",
            "issuesWithWearable": "Issues with wearable",
            "issueDescription": "Issue description",
        }
    )

    st.dataframe(df_display, use_container_width=True)

    # CSV keeps everything (including submittedAt_raw + submittedAt_sgt)
    #save_and_download(df, "history_latest")


with tabs[4]:
    st.subheader("Notification Responses")

    df = fetch_responses_per_user(lightup_ids, per_user_limit=per_user_limit)

    if df.empty:
        st.info("No responses found for the selected participants.")
        # Helpful debug: show which IDs you tried
        st.caption(f"Queried lightup_ids: {', '.join(lightup_ids)}")
        st.stop()

    # Normalize IDs before allowlist enforcement
    df = df.copy()
    df["lightup_id"] = df["lightup_id"].astype(str).str.strip()
    df_use["lightup_id"] = df_use["lightup_id"].astype(str).str.strip()

    df = enforce_allowlist(df, df_use)

    if df.empty:
        st.warning("Rows were fetched, but all were removed by allowlist enforcement.")
        st.caption("Debug tip: check that allowlist lightup_id exactly matches Firestore doc id.")
        st.stop()

    # prettify timestamps for UI
    df["message_sent_at"] = df["messageDeliveredAt_sgt"].apply(format_sgt_display)
    df["response_submitted_at"] = df["submittedAt_sgt"].apply(format_sgt_display)

    # Ensure per-user ordering = most recent response first
    df["_sort_submit"] = pd.to_datetime(df["submittedAt_sgt"], errors="coerce")
    df["_sort_deliv"] = pd.to_datetime(df["messageDeliveredAt_sgt"], errors="coerce")
    df["_sort"] = df["_sort_submit"].fillna(df["_sort_deliv"])
    df = df.sort_values(["lightup_id", "_sort"], ascending=[True, False]).drop(
        columns=["_sort_submit", "_sort_deliv", "_sort"]
    )

    # one table per user with >= 1 response
    for (pid, uid), g in df.groupby(["lightup_id", "user_id"], sort=True):
        g = g.copy()

        # last response time = newest submittedAt (fallback to deliveredAt)
        last_dt = None
        if g["submittedAt_sgt"].notna().any():
            last_dt = g["submittedAt_sgt"].dropna().iloc[0]
        elif g["messageDeliveredAt_sgt"].notna().any():
            last_dt = g["messageDeliveredAt_sgt"].dropna().iloc[0]

        last_label = format_sgt_display(last_dt) if last_dt else "—"

        st.subheader(f"{pid} - {uid}")
        st.caption(f"Last response: {last_label}")

        g_display = g[["message_sent_at", "response_submitted_at", "responses_display"]].rename(
            columns={
                "message_sent_at": "Message sent at",
                "response_submitted_at": "Response submitted at",
                "responses_display": "Responses",
            }
        )

        st.dataframe(g_display, use_container_width=True)

    # CSV download: keep raw df (includes response_json, raw timestamps, doc_id)
    #save_and_download(df, "notification_responses")


