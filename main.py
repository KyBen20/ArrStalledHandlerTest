import os
import sqlite3
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import time
import logging

# Load environment variables
load_dotenv()

# Configuration from .env
RADARR_URL = os.getenv("RADARR_URL").split(",") if os.getenv("RADARR_URL") else None
RADARR_API_KEY = os.getenv("RADARR_API_KEY").split(",") if os.getenv("RADARR_API_KEY") else None
SONARR_URL = os.getenv("SONARR_URL").split(",") if os.getenv("SONARR_URL") else None
SONARR_API_KEY = os.getenv("SONARR_API_KEY").split(",") if os.getenv("SONARR_API_KEY") else None
LIDARR_URL = os.getenv("LIDARR_URL").split(",") if os.getenv("LIDARR_URL") else None
LIDARR_API_KEY = os.getenv("LIDARR_API_KEY").split(",") if os.getenv("LIDARR_API_KEY") else None
READARR_URL = os.getenv("READARR_URL").split(",") if os.getenv("READARR_URL") else None
READARR_API_KEY = os.getenv("READARR_API_KEY").split(",") if os.getenv("READARR_API_KEY") else None

STALLED_TIMEOUT = int(os.getenv("STALLED_TIMEOUT", 900))
STALLED_ACTION = os.getenv("STALLED_ACTION", "BLOCKLIST_AND_SEARCH").upper()
VERBOSE = os.getenv("VERBOSE", "false").lower() == "true"
RUN_INTERVAL = int(os.getenv("RUN_INTERVAL", 300))
COUNT_DOWNLOADING_METADATA_AS_STALLED = os.getenv("COUNT_DOWNLOADING_METADATA_AS_STALLED", "false").lower() == "true"

DB_FILE = "stalled_downloads.db"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if VERBOSE else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def initialize_database():
    """Initialize the SQLite database only once."""
    if STALLED_TIMEOUT == 0:
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stalled_downloads (
            download_id TEXT,
            first_detected TIMESTAMP NOT NULL,
            arr_service TEXT NOT NULL,
            PRIMARY KEY (download_id, arr_service)
        )
    """)
    conn.commit()
    conn.close()

def get_stalled_downloads_from_db(arr_service):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT download_id, first_detected FROM stalled_downloads WHERE arr_service = ?", (arr_service,))
    rows = cursor.fetchall()
    conn.close()
    return {str(row[0]): datetime.fromisoformat(row[1]) for row in rows}

def add_stalled_download_to_db(download_id, first_detected, arr_service):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO stalled_downloads (download_id, first_detected, arr_service)
        VALUES (?, ?, ?)
    """, (str(download_id), first_detected.isoformat(), arr_service))
    added = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return added

def remove_stalled_download_from_db(download_id, arr_service):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stalled_downloads WHERE download_id = ? AND arr_service = ?", (download_id, arr_service))
    conn.commit()
    conn.close()

def query_api(url, headers, params=None):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"API Request Error ({url}): {e}")
        return None

def post_api(url, headers, data=None):
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logging.info(f"Command sent successfully to {url}")
    except requests.RequestException as e:
        logging.error(f"API POST Error: {e}")

def delete_api(url, headers, params=None):
    try:
        response = requests.delete(url, headers=headers, params=params)
        if response.status_code == 404:
            logging.warning(f"Item already deleted (404) on {url}")
            return False # Indicate failure/already gone
        response.raise_for_status()
        logging.debug(f"Successfully deleted item on {url}")
        return True # Indicate success
    except requests.RequestException as e:
        logging.error(f"API DELETE Error: {e}")
        return False

def query_api_paginated(base_url, headers, params=None, page_size=50):
    all_records = []
    page = 1
    total_records = None
    
    while True:
        paginated_params = params.copy() if params else {}
        paginated_params.update({"page": page, "pageSize": page_size})
        
        response = query_api(base_url, headers, paginated_params)
        if not response or not isinstance(response, dict) or "records" not in response:
            break
            
        records = response.get("records", [])
        total_records = response.get("totalRecords", total_records)
        
        if not records:
            break
            
        all_records.extend(records)
        if total_records and len(all_records) >= total_records:
            break
        page += 1
        
    return all_records

def perform_action(base_url, headers, download_id, movie_id, service_name, api_version, episode_ids=None, series_id=None):
    # Action Logic
    action_url = f"{base_url}/api/{api_version}/queue/{download_id}"
    params = {"blocklist": "true", "skipRedownload": "false"}
    
    # 1. DELETE + BLOCKLIST
    logging.info(f"Removing and Blocklisting download {download_id} in {service_name}...")
    success = delete_api(action_url, headers, params)
    
    if not success and STALLED_ACTION != "REMOVE":
        logging.warning("Delete failed or item missing. Skipping search trigger as safety measure.")
        return

    # 2. TRIGGER SEARCH (Only if action is BLOCKLIST_AND_SEARCH)
    if STALLED_ACTION == "BLOCKLIST_AND_SEARCH":
        command_url = f"{base_url}/api/{api_version}/command"
        
        if service_name.startswith("Sonarr"):
            # Logic Improved for Sonarr
            if episode_ids:
                logging.info(f"Triggering EpisodeSearch for IDs: {episode_ids}")
                post_api(command_url, headers, {"name": "EpisodeSearch", "episodeIds": episode_ids})
            elif series_id:
                 # Fallback for Season Packs: Search the whole Series (or Season if we had seasonNumber)
                logging.info(f"No Episode IDs found (likely Season Pack). Triggering SeriesSearch for Series ID: {series_id}")
                post_api(command_url, headers, {"name": "SeriesSearch", "seriesId": series_id})
            else:
                logging.warning(f"Could not trigger search: No Episode or Series ID found for download {download_id}.")
                
        elif service_name.startswith("Radarr") and movie_id:
            logging.info(f"Triggering MoviesSearch for Movie ID: {movie_id}")
            post_api(command_url, headers, {"name": "MoviesSearch", "movieIds": [movie_id]})
            
        else:
            logging.warning(f"Skipping search: No valid IDs identified for {service_name}.")

def check_queue_and_act(base_url, api_key, service_name, api_version, metadata_check=False):
    """Unified function to check queue for stalled or stuck metadata items."""
    
    # Determine what we are looking for
    status_filter = "queued" if metadata_check else "warning"
    check_type = "Downloading Metadata" if metadata_check else "Stalled"
    
    # IMPORTANT: Always includeEpisode=true for Sonarr to get IDs
    params = {
        "protocol": "torrent",
        "status": status_filter,
        "includeEpisode": "true" if service_name.startswith("Sonarr") else "false"
    }

    headers = {"X-Api-Key": api_key}
    queue_url = f"{base_url}/api/{api_version}/queue"
    
    logging.debug(f"Checking {service_name} queue for {check_type} items...")
    records = query_api_paginated(queue_url, headers, params)
    
    if not records:
        return

    db_stalled = get_stalled_downloads_from_db(service_name)
    
    for item in records:
        # Check condition (Metadata vs Stalled)
        is_target = False
        error_msg = item.get("errorMessage", "").lower()
        
        if metadata_check:
            # Check for metadata stuck
            if "downloading metadata" in error_msg:
                is_target = True
        else:
            # Check for generic stall - More permissive check than original script
            # Accept "warning" status items that have stalled messages or 0 time left
            if "stalled" in error_msg or "connection" in error_msg or item.get("status") == "warning":
                is_target = True

        if not is_target:
            continue

        # Extract IDs - IMPROVED LOGIC
        download_id = str(item["id"])
        movie_id = item.get("movieId")
        
        # Sonarr ID extraction (Handle Packs)
        episode_ids = None
        series_id = item.get("seriesId") # Always grab seriesId as fallback
        if "episodeId" in item and item["episodeId"]:
            episode_ids = [item["episodeId"]]
        elif "episodeIds" in item and item["episodeIds"]:
            episode_ids = item["episodeIds"]
            
        # DB Logic
        if download_id in db_stalled:
            first_detected = db_stalled[download_id]
            elapsed = (datetime.now(timezone.utc) - first_detected).total_seconds()
            
            if elapsed > STALLED_TIMEOUT:
                logging.info(f"TIMEOUT REACHED for {download_id} in {service_name} ({elapsed:.0f}s). ACTING NOW.")
                perform_action(base_url, headers, download_id, movie_id, service_name, api_version, episode_ids, series_id)
                remove_stalled_download_from_db(download_id, service_name)
            else:
                 # Just log periodically, not every loop to reduce noise
                if elapsed % 60 < 5: 
                    logging.info(f"Item {download_id} waiting... {elapsed:.0f}/{STALLED_TIMEOUT}s")
        else:
            logging.info(f"New {check_type} detected: {download_id}. Timer started.")
            add_stalled_download_to_db(download_id, datetime.now(timezone.utc), service_name)

if __name__ == "__main__":
    logging.info("Starting ArrStalledHandler Optimized...")
    initialize_database() # Init once at startup
    
    try:
        while True:
            # Process Radarr
            if RADARR_URL:
                for i, url in enumerate(RADARR_URL):
                    check_queue_and_act(url, RADARR_API_KEY[i], f"Radarr{i}", "v3", metadata_check=False)
                    if COUNT_DOWNLOADING_METADATA_AS_STALLED:
                        check_queue_and_act(url, RADARR_API_KEY[i], f"Radarr{i}", "v3", metadata_check=True)

            # Process Sonarr
            if SONARR_URL:
                for i, url in enumerate(SONARR_URL):
                    check_queue_and_act(url, SONARR_API_KEY[i], f"Sonarr{i}", "v3", metadata_check=False)
                    if COUNT_DOWNLOADING_METADATA_AS_STALLED:
                        check_queue_and_act(url, SONARR_API_KEY[i], f"Sonarr{i}", "v3", metadata_check=True)

            # (Lidarr/Readarr removed for brevity as you focused on Sonarr/Radarr, but logic is same)

            logging.debug(f"Sleeping {RUN_INTERVAL}s...")
            time.sleep(RUN_INTERVAL)
            
    except KeyboardInterrupt:
        logging.info("Stopping...")
    except Exception as e:
        logging.exception(f"Critical Error: {e}")
