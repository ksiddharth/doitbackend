import os
import json
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings("ignore")

from firebase_functions import firestore_fn, https_fn, options
from firebase_admin import initialize_app, firestore
import google.cloud.firestore
from google.cloud import tasks_v2
from google.cloud import storage
import google.generativeai as genai

# Initialize Firebase Admin
initialize_app()

# Configuration
PROJECT_ID = os.environ.get("GCLOUD_PROJECT") or "ephemeral-access-backend"
LOCATION = "us-central1"
QUEUE_NAME = "analysis-queue"
STORAGE_BUCKET = f"{PROJECT_ID}.firebasestorage.app"
BATCH_SIZE = 15  # Max screenshots per Gemini call


SYSTEM_PROMPT = """You are an activity analyzer for a productivity app called DoIt. You receive sequential phone screen captures with their UI element data (from Android's accessibility service), along with the user's profile.

Your job: analyze what the user was ACTUALLY doing on their phone, classify each activity as "aligned" (with their goals) or "drifting" (away from their goals), and compute an updated score.

## Input data format
Each screen capture includes:
- A screenshot image
- UI element text extracted via accessibility service: TXT (visible text/label), ID (android resource ID), CLS (widget class)
- These tell you the app being used and what content is on screen

## User profile
The user provides:
- Their goals/interests (e.g., "writing", "swimming", "astronaut")
- Target split: what percentage of time they WANT to spend aligned vs drifting
- Current score: their running aligned vs drifting percentages before this session

## Classification rules
- **Aligned**: Content that directly supports the user's stated goals/interests — educational content related to their goals, research, reading articles/books in their interest areas, note-taking, productivity apps, learning apps, coding, writing, or any activity that moves them toward their goals
- **Drifting**: Content unrelated to goals — social media scrolling (Instagram, TikTok, Reddit, Twitter), entertainment YouTube (memes, shorts, gaming), games, casual browsing not related to goals
- **Neutral (classify as drifting)**: Launcher/home screen, app stores, system screens, "no internet" errors, loading screens, ads. These are not goal-aligned time.
- Use the screenshot + UI text together. A YouTube video about physics for a science-interested user = aligned. YouTube shorts about cricket = drifting. Reddit r/learnprogramming for a coder = aligned. Reddit r/memes = drifting.
- News browsing is "drifting" unless it directly relates to the user's stated goals/interests. General news (politics, weather, local events) is not aligned.
- If a screen is ambiguous, use the content visible in the screenshot and text to make your best judgment.

## Required output
Return ONLY valid JSON (no markdown, no backticks, no explanation outside the JSON):
{
  "activities": [
    {
      "capture": "001",
      "app": "com.google.android.youtube",
      "app_name": "YouTube",
      "category": "drifting",
      "description": "Watching cricket highlights shorts",
      "zone_out_match": "meme_compilations"
    }
  ],
  "transitions": [
    {
      "at_capture": "006",
      "from": "aligned",
      "to": "drifting",
      "trigger": "Switched from Khan Academy to Instagram Reels"
    }
  ],
  "streaks": {
    "longest_aligned": 5,
    "longest_drifting": 8,
    "ended_on": "drifting"
  },
  "session_summary": {
    "total_captures": 10,
    "aligned_captures": 3,
    "drifting_captures": 7,
    "aligned_pct": 30,
    "drifting_pct": 70
  },
  "updated_score": {
    "aligned_pct": 45,
    "drifting_pct": 55
  },
  "feedback": "You spent most of the session on YouTube shorts and Instagram instead of working toward your goals. To hit your 60% aligned target, try starting with goal-aligned activities before opening YouTube."
}

Rules:
- "transitions": CRITICAL — you MUST walk through the activities array in order and record EVERY point where the category changes from the previous capture. If capture 005 is "drifting" and capture 006 is "aligned", that is a transition. If capture 012 is "aligned" and capture 013 is "drifting", that is another transition. Do NOT skip transitions. The number of transitions should equal the number of times the category flips when reading activities in sequence. Include what app/content caused each switch.
- "streaks": longest consecutive run of aligned and drifting captures. "ended_on" is the category of the LAST capture.
- "session_summary": capture counts and percentages only. The app handles all time/duration math.
- "updated_score": blend the user's current_score with this session (weight session at 30%, historical at 70%). If no current_score is provided, just use this session's numbers.
- "feedback": be specific about what apps and content you saw. Reference capture numbers if helpful. Do NOT reference timestamps or durations — the app handles time display.
- "zone_out_match": if the user has provided content_zone_outs flags, check each capture against them. If a capture's content matches one of the flags, include "zone_out_match" with the flag name (e.g., "rage_bait", "celebrity_gossip"). Only include this field when there is a match — omit it entirely when there is no match. Match based on the actual content visible in the screenshot, not just the app name.
"""

MERGE_PROMPT = """You are given multiple activity analysis reports (as JSON) from different batches of screen captures from the same phone session. Merge them into a single JSON report.

Rules:
- Combine all "activities" arrays into one, in capture order. Preserve all per-capture fields exactly as they appear (including "zone_out_match" if present).
- Rebuild "transitions" by walking the merged activities array in order and recording every category flip (between "aligned" and "drifting"). Also check the boundary between batches (last capture of batch N vs first capture of batch N+1).
- Recalculate "streaks" (longest_aligned, longest_drifting) from the merged activities
- Recalculate session_summary (aligned_captures, drifting_captures, aligned_pct, drifting_pct) across all batches
- Recompute updated_score (aligned_pct, drifting_pct) based on the merged totals
- Write one unified "feedback" message (no timestamps or durations — the app handles time)
- Return ONLY valid JSON (no markdown, no backticks)

Here are the batch reports:

"""


BOOKMARK_PROMPT = """You are a content extraction assistant for a productivity app called DoIt. You receive a single phone screenshot with optional UI element data (from Android's accessibility service).

Your job: identify what content the user is viewing and extract structured metadata so the app can generate a bookmark link.

## Input data format
- A screenshot image of the user's phone screen
- Optional: UI element text extracted via accessibility service (TXT = visible text/label, ID = android resource ID, CLS = widget class)

## What to extract
Analyze the screenshot and accessibility data to identify the content being viewed. Focus on:
1. **Platform**: Which app/website is being used (youtube, instagram, x/twitter, reddit, web, other)
2. **Title**: The exact title of the content as shown on screen. For X/Twitter posts, use the first ~100 characters of the tweet text.
3. **Channel/Author**: The display name of the creator or channel
4. **Handle**: The @username/handle (e.g., "@elonmusk"). Look in the accessibility text for @mentions next to the author name. CRITICAL for X/Twitter — this is how we construct the link.
5. **Video ID**: For YouTube, look for the video ID in accessibility resource IDs (e.g., in ID fields containing video identifiers), URL bars, or share dialogs
6. **URL**: Any full URL visible in the screenshot or accessibility text
7. **Description**: Brief description of the content being viewed
8. **Content type**: video, short, live, playlist, post, story, article, other

## Required output
Return ONLY valid JSON (no markdown, no backticks, no explanation outside the JSON):
{
  "platform": "youtube",
  "title": "Exact video title as shown on screen",
  "channel": "Channel or creator name",
  "handle": "@username or null",
  "video_id": "dQw4w9WgXcQ or null if not found",
  "url": "full URL if visible, or null",
  "description": "Brief description of the content",
  "content_type": "video"
}

Rules:
- For "platform", use lowercase: "youtube", "instagram", "x", "reddit", "web", "other"
- For "title", copy the title EXACTLY as displayed — do not paraphrase or shorten
- For "channel", extract the display name of the channel/creator if visible
- For "handle", extract the @username. On X/Twitter, this appears as "@username" in the accessibility text right after the display name. Do NOT confuse @mentions within tweet text with the post author's handle — the author's handle appears at the top of the post near their display name and "Verified account".
- For "video_id", look carefully in accessibility IDs, URL bars, and any text that contains YouTube video identifiers (11-character alphanumeric strings)
- For "url", only include if you can see a complete URL — do not guess or construct URLs
- For "content_type", classify as: video, short, live, playlist, post, story, article, other
- If a field cannot be determined, set it to null
- Always return valid JSON with all fields present
- When multiple posts are visible on a feed, identify the MOST PROMINENT one — the post that takes up the most screen space or is most centered on the screen. That is the one the user is bookmarking.
"""


def youtube_search(query, api_key, max_results=3):
    """Search YouTube Data API v3 for videos matching a query.

    Args:
        query: Search query string (typically "title channel_name")
        api_key: YouTube Data API key
        max_results: Number of results to fetch (default 3)

    Returns:
        List of dicts with keys: video_id, title, channel
        Empty list on failure.
    """
    import urllib.request
    import urllib.parse
    import urllib.error

    params = urllib.parse.urlencode({
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_results,
        "key": api_key,
    })
    url = f"https://www.googleapis.com/youtube/v3/search?{params}"

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        print(f"[youtube_search] API error: {e}")
        return []

    results = []
    for item in data.get("items", []):
        video_id = item.get("id", {}).get("videoId")
        snippet = item.get("snippet", {})
        if video_id:
            results.append({
                "video_id": video_id,
                "title": snippet.get("title", ""),
                "channel": snippet.get("channelTitle", ""),
            })
    return results


def _title_similarity(a, b):
    """Simple word-overlap similarity between two strings (0.0 to 1.0)."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    return len(intersection) / max(len(words_a), len(words_b))


def resolve_bookmark_url(gemini_result, youtube_api_key):
    """Resolve a bookmark URL using a tiered strategy.

    Args:
        gemini_result: Parsed JSON dict from Gemini's BOOKMARK_PROMPT response
        youtube_api_key: YouTube Data API key

    Returns:
        Tuple of (url, confidence, resolution_method, extras)
        extras is a dict with optional platform-specific fields (search_url, hashtags, etc.)
    """
    import urllib.parse
    import re

    platform = (gemini_result.get("platform") or "").lower()
    title = gemini_result.get("title")
    channel = gemini_result.get("channel")
    handle = gemini_result.get("handle")
    video_id = gemini_result.get("video_id")
    url = gemini_result.get("url")

    # Tier 0: Gemini found a full URL on any platform
    if url and url.startswith("http"):
        return url, "high", "direct_url", {}

    # ── YouTube ──
    if platform == "youtube":
        if url and ("youtube.com/" in url or "youtu.be/" in url):
            return url, "high", "direct_url", {}

        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}", "high", "direct_id", {}

        if title and youtube_api_key:
            query = f"{title} {channel}" if channel else title
            confidence = "high" if channel else "medium"

            results = youtube_search(query, youtube_api_key)
            if results:
                best = max(results, key=lambda r: _title_similarity(r["title"], title))
                similarity = _title_similarity(best["title"], title)
                if similarity < 0.3:
                    confidence = "low"
                return (
                    f"https://www.youtube.com/watch?v={best['video_id']}",
                    confidence,
                    "api_search",
                    {},
                )

    # ── X / Twitter ──
    if platform == "x":
        # Clean handle: remove @ prefix if present
        clean_handle = handle.lstrip("@") if handle else None

        if clean_handle and title:
            # Build X search: from:handle + first few words of post
            words = title.split()[:8]
            search_text = " ".join(words)
            query = f"from:{clean_handle} {search_text}"
            search_url = f"https://x.com/search?q={urllib.parse.quote(query)}&f=top"
            return search_url, "medium", "constructed_search", {}

        if clean_handle:
            return f"https://x.com/{clean_handle}", "low", "constructed_profile", {}

    # ── Instagram ──
    if platform == "instagram":
        # Instagram uses usernames, not @handles — Gemini puts it in channel
        username = (handle.lstrip("@") if handle else None) or channel
        if username:
            # Clean username: strip whitespace, possessives from accessibility text
            username = username.strip().split("'")[0].split(" ")[0]

            caption_text = title or gemini_result.get("description") or ""
            profile_url = f"https://www.instagram.com/{username}/"

            if caption_text:
                words = caption_text.split()[:6]
                search_query = f"{username} {' '.join(words)}"
                search_url = f"https://www.instagram.com/explore/search/keyword/?q={urllib.parse.quote(search_query)}"
                return profile_url, "medium", "constructed_profile", {
                    "search_url": search_url,
                }

            return profile_url, "low", "constructed_profile", {}

    # Nothing usable
    return None, "low", "failed", {}


@firestore_fn.on_document_created(document="jobs/{job_id}", region=LOCATION)
def dispatch_analysis(event: firestore_fn.Event[firestore_fn.DocumentSnapshot]) -> None:
    """
    Triggered when a new job is created in Firestore.
    Enqueues a task to the Cloud Tasks queue to process the job.
    """
    tasks_client = tasks_v2.CloudTasksClient()

    job_id = event.params["job_id"]
    snapshot = event.data
    if not snapshot:
        print(f"[dispatch] No data for job {job_id}")
        return

    job_data = snapshot.to_dict()
    gcs_path = job_data.get("gcs_path")

    if not gcs_path:
        print(f"[dispatch] Missing 'gcs_path' in job {job_id}")
        return

    payload = {"job_id": job_id, "gcs_path": gcs_path}
    parent = tasks_client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
    url = f"https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/process_worker"

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {
                "service_account_email": f"{PROJECT_ID}@appspot.gserviceaccount.com"
            }
        }
    }

    response = tasks_client.create_task(request={"parent": parent, "task": task})
    print(f"[dispatch] Created task for job {job_id}")
    snapshot.reference.update({"status": "queued"})
    print(f"[dispatch] Job {job_id} status -> queued")


@https_fn.on_request(
    region=LOCATION,
    timeout_sec=540,
    memory=options.MemoryOption.GB_2,
    secrets=["GEMINI_API_KEY"]
)
def process_worker(req: https_fn.Request) -> https_fn.Response:
    """
    Worker function processed by Cloud Tasks.
    Analyzes phone screen captures and scores aligned vs drifting time.
    """
    job_start = time.time()
    firestore_client = firestore.client()
    storage_client = storage.Client()

    try:
        data = req.get_json()
        job_id = data.get("job_id")
        gcs_path = data.get("gcs_path")

        if not job_id or not gcs_path:
            print(f"[worker] Bad request: job_id={job_id}, gcs_path={gcs_path}")
            return https_fn.Response("Missing job_id or gcs_path", status=400)

        print(f"[worker] === START job {job_id} ===")
        print(f"[worker] GCS path: {gcs_path}")

        # 1. Read job document for user goals
        job_doc = firestore_client.collection("jobs").document(job_id).get()
        job_data = job_doc.to_dict() if job_doc.exists else {}

        user_goals = job_data.get("user_goals", {})
        user_id = job_data.get("user_id")

        # If no goals in job doc, try loading from profiles collection
        if not user_goals and user_id:
            print(f"[worker] Loading profile for user {user_id}")
            profile_doc = firestore_client.collection("profiles").document(user_id).get()
            if profile_doc.exists:
                user_goals = profile_doc.to_dict()
                print(f"[worker] Loaded profile: {json.dumps(user_goals, default=str)}")
            else:
                print(f"[worker] No profile found for user {user_id}")

        # Build user context string for the prompt
        if user_goals:
            user_context = f"\n## User profile data\n{json.dumps(user_goals, default=str, indent=2)}"
            print(f"[worker] User goals: {json.dumps(user_goals, default=str)}")

            # Parse content_zone_outs if present
            raw_zone_outs = user_goals.get("content_zone_outs")
            if raw_zone_outs:
                try:
                    zone_outs = json.loads(raw_zone_outs) if isinstance(raw_zone_outs, str) else raw_zone_outs
                    if zone_outs:
                        zone_out_list = ", ".join(zone_outs)
                        user_context += f"\n\n## Content zone-outs\nThe user has flagged these content patterns as zone-outs they want to catch: {zone_out_list}. If a capture's content matches one of these patterns, include \"zone_out_match\" with the matching flag name in that capture's activity entry."
                        print(f"[worker] Zone-outs: {zone_outs}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"[worker] WARNING: Failed to parse content_zone_outs: {e}")
        else:
            user_context = "\n## User profile data\nNo user profile provided. Classify activities using general goal-aligned vs drifting criteria."
            print(f"[worker] No user goals provided, using defaults")

        # 2. Setup Gemini
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print(f"[worker] GEMINI_API_KEY not found")
            return https_fn.Response("GEMINI_API_KEY missing", status=500)

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        print(f"[worker] Gemini ready (gemini-2.0-flash)")

        # 3. List files from GCS
        t0 = time.time()
        bucket = storage_client.bucket(STORAGE_BUCKET)
        blobs = list(bucket.list_blobs(prefix=gcs_path))
        blobs.sort(key=lambda x: x.name)
        print(f"[worker] Listed {len(blobs)} blobs from gs://{STORAGE_BUCKET}/{gcs_path} ({time.time()-t0:.1f}s)")

        # Categorize blobs
        image_blobs = {}
        meta_blobs = {}
        session_log_blob = None
        temp_files = []

        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            fname = os.path.basename(blob.name)
            if fname == "session.log":
                session_log_blob = blob
            elif fname.endswith("_meta.txt"):
                key = fname.replace("_meta.txt", "")
                meta_blobs[key] = blob
            elif blob.content_type and blob.content_type.startswith("image/"):
                key = os.path.splitext(fname)[0]
                image_blobs[key] = blob
            else:
                print(f"[worker] Skipping: {fname} (type={blob.content_type})")

        sorted_keys = sorted(image_blobs.keys())
        unpaired = [k for k in sorted_keys if k not in meta_blobs]
        print(f"[worker] Found: {len(sorted_keys)} images, {len(meta_blobs)} meta, session_log={'yes' if session_log_blob else 'no'}")
        if unpaired:
            print(f"[worker] WARNING: {len(unpaired)} images without meta: {unpaired[:5]}")

        if not sorted_keys:
            print(f"[worker] ERROR: No images found, nothing to analyze")
            firestore_client.collection("jobs").document(job_id).update({
                "status": "failed",
                "error": "No screenshots found in GCS path"
            })
            return https_fn.Response("No screenshots found", status=400)

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Download session log
                session_log_text = ""
                if session_log_blob:
                    log_path = os.path.join(temp_dir, "session.log")
                    session_log_blob.download_to_filename(log_path)
                    with open(log_path, "r") as f:
                        session_log_text = f.read()
                    print(f"[worker] Session log: {len(session_log_text)} chars")

                # Split into batches
                batches = []
                for i in range(0, len(sorted_keys), BATCH_SIZE):
                    batches.append(sorted_keys[i:i + BATCH_SIZE])

                total_batches = len(batches)
                print(f"[worker] Processing {len(sorted_keys)} screenshots in {total_batches} batch(es) (batch_size={BATCH_SIZE})")

                batch_results = []

                for batch_idx, batch_keys in enumerate(batches):
                    batch_num = batch_idx + 1
                    print(f"[worker] --- Batch {batch_num}/{total_batches}: {len(batch_keys)} screenshots ({batch_keys[0]}..{batch_keys[-1]}) ---")

                    t1 = time.time()
                    inputs = [user_context]
                    batch_temp_files = []

                    for i, key in enumerate(batch_keys):
                        img_blob = image_blobs[key]
                        local_img = os.path.join(temp_dir, os.path.basename(img_blob.name))
                        img_blob.download_to_filename(local_img)
                        file_size = os.path.getsize(local_img)

                        uploaded_file = genai.upload_file(local_img)
                        batch_temp_files.append(uploaded_file)
                        temp_files.append(uploaded_file)

                        global_idx = batch_idx * BATCH_SIZE + i + 1
                        inputs.append(f"\n--- Screen capture {global_idx} ({key}) ---")
                        inputs.append(uploaded_file)
                        print(f"[worker]   {key}: uploaded ({file_size/1024:.0f}KB)")

                        if key in meta_blobs:
                            meta_path = os.path.join(temp_dir, f"{key}_meta.txt")
                            meta_blobs[key].download_to_filename(meta_path)
                            with open(meta_path, "r") as f:
                                meta_text = f.read()
                            inputs.append(f"UI elements on this screen:\n{meta_text}")
                            print(f"[worker]   {key}: meta paired ({len(meta_text)} chars)")

                    if batch_idx == 0 and session_log_text:
                        inputs.append(f"\n--- Session activity log ---\n{session_log_text}")

                    upload_time = time.time() - t1
                    print(f"[worker]   Upload done ({upload_time:.1f}s)")

                    # Call Gemini
                    t2 = time.time()
                    print(f"[worker]   Calling Gemini...")
                    response = model.generate_content([SYSTEM_PROMPT] + inputs)
                    inference_time = time.time() - t2
                    result_text = response.text
                    batch_results.append(result_text)
                    print(f"[worker]   Gemini responded ({inference_time:.1f}s, {len(result_text)} chars)")

                    # Cleanup batch files immediately
                    for f in batch_temp_files:
                        try:
                            f.delete()
                        except Exception as e:
                            print(f"[worker]   GenAI cleanup failed: {f.name}: {e}")
                    temp_files = [f for f in temp_files if f not in batch_temp_files]
                    print(f"[worker]   Batch {batch_num} complete")

                # Merge if multiple batches
                if total_batches == 1:
                    final_result = batch_results[0]
                    print(f"[worker] Single batch, no merge needed")
                else:
                    print(f"[worker] Merging {total_batches} batch results...")
                    t3 = time.time()
                    merge_input = MERGE_PROMPT
                    for i, r in enumerate(batch_results):
                        merge_input += f"\n=== BATCH {i+1} ===\n{r}\n"
                    merge_response = model.generate_content(merge_input)
                    final_result = merge_response.text
                    print(f"[worker] Merge done ({time.time()-t3:.1f}s)")

                # Try to parse JSON from response, store both raw and parsed
                result_data = {"raw": final_result}
                try:
                    # Strip markdown code fences if present
                    cleaned = final_result.strip()
                    if cleaned.startswith("```"):
                        cleaned = cleaned.split("\n", 1)[1]
                    if cleaned.endswith("```"):
                        cleaned = cleaned.rsplit("```", 1)[0]
                    parsed = json.loads(cleaned.strip())
                    result_data = parsed
                    print(f"[worker] Response parsed as valid JSON")
                except json.JSONDecodeError:
                    print(f"[worker] WARNING: Response is not valid JSON, storing as raw text")

                # Write result
                firestore_client.collection("jobs").document(job_id).update({
                    "status": "complete",
                    "result": result_data,
                    "completed_at": firestore.SERVER_TIMESTAMP
                })
                total_time = time.time() - job_start
                print(f"[worker] === DONE job {job_id} in {total_time:.1f}s ===")

            except Exception as e:
                print(f"[worker] ERROR: {type(e).__name__}: {e}")
                firestore_client.collection("jobs").document(job_id).update({
                    "status": "failed",
                    "error": str(e)
                })
                return https_fn.Response(f"Processing failed: {e}", status=500)

            finally:
                if temp_files:
                    print(f"[worker] Cleaning up {len(temp_files)} remaining GenAI files")
                    for f in temp_files:
                        try:
                            f.delete()
                        except Exception as e:
                            print(f"[worker] GenAI cleanup failed: {f.name}: {e}")

        # GCS cleanup
        print(f"[worker] Deleting {len(blobs)} source files from GCS...")
        try:
            bucket.delete_blobs(blobs)
            print(f"[worker] Source files deleted")
        except Exception as del_err:
            print(f"[worker] CRITICAL: GCS delete failed: {del_err}")

        return https_fn.Response("OK", status=200)

    except Exception as e:
        print(f"[worker] FATAL: {type(e).__name__}: {e}")
        return https_fn.Response(str(e), status=500)


# ── Bookmark Pipeline ──────────────────────────────────────────────────────────


@firestore_fn.on_document_created(document="bookmark_jobs/{job_id}", region=LOCATION)
def dispatch_bookmark(event: firestore_fn.Event[firestore_fn.DocumentSnapshot]) -> None:
    """
    Triggered when a new bookmark job is created in Firestore.
    Enqueues a task to the Cloud Tasks queue to process the bookmark.
    """
    tasks_client = tasks_v2.CloudTasksClient()

    job_id = event.params["job_id"]
    snapshot = event.data
    if not snapshot:
        print(f"[dispatch_bookmark] No data for job {job_id}")
        return

    job_data = snapshot.to_dict()
    gcs_path = job_data.get("gcs_path")

    if not gcs_path:
        print(f"[dispatch_bookmark] Missing 'gcs_path' in job {job_id}")
        return

    payload = {"job_id": job_id, "gcs_path": gcs_path}
    parent = tasks_client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
    url = f"https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/process_bookmark"

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {
                "service_account_email": f"{PROJECT_ID}@appspot.gserviceaccount.com"
            }
        }
    }

    response = tasks_client.create_task(request={"parent": parent, "task": task})
    print(f"[dispatch_bookmark] Created task for job {job_id}")
    snapshot.reference.update({"status": "queued"})
    print(f"[dispatch_bookmark] Job {job_id} status -> queued")


@https_fn.on_request(
    region=LOCATION,
    timeout_sec=120,
    memory=options.MemoryOption.MB_512,
    secrets=["GEMINI_API_KEY", "YOUTUBE_API_KEY"]
)
def process_bookmark(req: https_fn.Request) -> https_fn.Response:
    """
    Worker function for bookmark processing. Invoked by Cloud Tasks.
    Downloads a single screenshot + optional meta, extracts content info via Gemini,
    resolves the URL (YouTube only for now), writes result to Firestore, cleans up.
    """
    job_start = time.time()
    firestore_client = firestore.client()
    storage_client = storage.Client()

    try:
        data = req.get_json()
        job_id = data.get("job_id")
        gcs_path = data.get("gcs_path")

        if not job_id or not gcs_path:
            print(f"[bookmark] Bad request: job_id={job_id}, gcs_path={gcs_path}")
            return https_fn.Response("Missing job_id or gcs_path", status=400)

        print(f"[bookmark] === START job {job_id} ===")
        print(f"[bookmark] GCS path: {gcs_path}")

        # 1. Setup Gemini
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print(f"[bookmark] GEMINI_API_KEY not found")
            return https_fn.Response("GEMINI_API_KEY missing", status=500)

        youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
        if not youtube_api_key:
            print(f"[bookmark] WARNING: YOUTUBE_API_KEY not found, API search disabled")

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')

        # 2. List files from GCS
        bucket = storage_client.bucket(STORAGE_BUCKET)
        blobs = list(bucket.list_blobs(prefix=gcs_path))
        blobs.sort(key=lambda x: x.name)
        print(f"[bookmark] Listed {len(blobs)} blobs from gs://{STORAGE_BUCKET}/{gcs_path}")

        # Find the screenshot and optional meta file
        image_blob = None
        meta_blob = None

        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            fname = os.path.basename(blob.name)
            if fname.endswith("_meta.txt"):
                meta_blob = blob
            elif blob.content_type and blob.content_type.startswith("image/"):
                image_blob = blob

        if not image_blob:
            print(f"[bookmark] ERROR: No screenshot found")
            firestore_client.collection("bookmark_jobs").document(job_id).update({
                "status": "failed",
                "error": "No screenshot found in GCS path"
            })
            return https_fn.Response("No screenshot found", status=400)

        print(f"[bookmark] Screenshot: {os.path.basename(image_blob.name)}, meta: {'yes' if meta_blob else 'no'}")

        with tempfile.TemporaryDirectory() as temp_dir:
            uploaded_file = None
            try:
                # 3. Download and upload screenshot to Gemini
                local_img = os.path.join(temp_dir, os.path.basename(image_blob.name))
                image_blob.download_to_filename(local_img)
                uploaded_file = genai.upload_file(local_img)
                print(f"[bookmark] Screenshot uploaded to Gemini ({os.path.getsize(local_img)/1024:.0f}KB)")

                # Build prompt inputs
                inputs = []
                inputs.append(uploaded_file)

                if meta_blob:
                    meta_path = os.path.join(temp_dir, os.path.basename(meta_blob.name))
                    meta_blob.download_to_filename(meta_path)
                    with open(meta_path, "r") as f:
                        meta_text = f.read()
                    inputs.append(f"\nUI elements on this screen:\n{meta_text}")
                    print(f"[bookmark] Meta file: {len(meta_text)} chars")

                # 4. Call Gemini
                t1 = time.time()
                print(f"[bookmark] Calling Gemini...")
                response = model.generate_content([BOOKMARK_PROMPT] + inputs)
                result_text = response.text
                print(f"[bookmark] Gemini responded ({time.time()-t1:.1f}s, {len(result_text)} chars)")

                # 5. Parse Gemini response
                cleaned = result_text.strip()
                if cleaned.startswith("```"):
                    cleaned = cleaned.split("\n", 1)[1]
                if cleaned.endswith("```"):
                    cleaned = cleaned.rsplit("```", 1)[0]

                try:
                    gemini_result = json.loads(cleaned.strip())
                    print(f"[bookmark] Gemini result parsed: platform={gemini_result.get('platform')}, title={gemini_result.get('title')}")
                except json.JSONDecodeError:
                    print(f"[bookmark] ERROR: Gemini response is not valid JSON")
                    firestore_client.collection("bookmark_jobs").document(job_id).update({
                        "status": "failed",
                        "error": "Failed to parse Gemini response",
                        "raw_response": result_text,
                    })
                    return https_fn.Response("Gemini response parse error", status=500)

                # 6. Resolve URL
                resolved_url, confidence, method, extras = resolve_bookmark_url(
                    gemini_result, youtube_api_key
                )
                print(f"[bookmark] URL resolution: method={method}, confidence={confidence}, url={resolved_url}")

                # 7. Build result
                result = {
                    "platform": gemini_result.get("platform"),
                    "title": gemini_result.get("title"),
                    "channel": gemini_result.get("channel"),
                    "handle": gemini_result.get("handle"),
                    "url": resolved_url,
                    "description": gemini_result.get("description"),
                    "content_type": gemini_result.get("content_type"),
                    "confidence": confidence,
                    "resolution_method": method,
                }
                # Merge platform-specific extras (search_url, hashtag_urls, etc.)
                result.update(extras)

                status = "complete" if resolved_url else "failed"

                firestore_client.collection("bookmark_jobs").document(job_id).update({
                    "status": status,
                    "result": result,
                    "completed_at": firestore.SERVER_TIMESTAMP,
                })
                total_time = time.time() - job_start
                print(f"[bookmark] === DONE job {job_id} in {total_time:.1f}s (status={status}) ===")

            except Exception as e:
                print(f"[bookmark] ERROR: {type(e).__name__}: {e}")
                firestore_client.collection("bookmark_jobs").document(job_id).update({
                    "status": "failed",
                    "error": str(e),
                })
                return https_fn.Response(f"Processing failed: {e}", status=500)

            finally:
                if uploaded_file:
                    try:
                        uploaded_file.delete()
                    except Exception as e:
                        print(f"[bookmark] GenAI cleanup failed: {e}")

        # GCS cleanup
        print(f"[bookmark] Deleting {len(blobs)} source files from GCS...")
        try:
            bucket.delete_blobs(blobs)
            print(f"[bookmark] Source files deleted")
        except Exception as del_err:
            print(f"[bookmark] CRITICAL: GCS delete failed: {del_err}")

        return https_fn.Response("OK", status=200)

    except Exception as e:
        print(f"[bookmark] FATAL: {type(e).__name__}: {e}")
        return https_fn.Response(str(e), status=500)


# ── Weekly Review Pipeline ────────────────────────────────────────────────────


REVIEW_PROMPT = """You are analyzing a user's weekly phone usage data for a productivity app called DoIt. This is NOT a study app — it tracks whether the user's phone time is aligned with their personal goals or drifting away from them.

You will receive:
1. The user's goals (interests, targets, previously detected zone-out patterns)
2. Daily usage summaries for the past week
3. Zone-out events detected during the week

## Tasks

1. **Weekly Summary**: Summarize the week — total active time, overall aligned/drifting split, and whether the trend is improving, declining, or stable compared to their target.

2. **Zone-Out Profile Update**: Compare this week's zone-out events against the user's previous zone-out lists.
   - A pattern is **resolved** ONLY if it appears in the previous list but has zero occurrences this week.
   - A pattern is **emerging** ONLY if it does NOT appear in the previous list but was observed this week.
   - A pattern is **persistent** ONLY if it appears in the previous list AND was also observed this week.
   - `content_zone_outs` in the output = all current content patterns (persistent + emerging, excluding resolved).
   - `behavior_zone_outs` in the output = all current behavior patterns (persistent + emerging, excluding resolved).
   - When adding a pattern to emerging, assign it to `content_zone_outs` or `behavior_zone_outs` based on the `type` field in the zone_out_events input.

3. **Observations**: Write 2-4 specific observations about patterns you notice (correlations, time-of-day trends, app-specific habits).

4. **Feedback**: Write a short personalized feedback paragraph. Frame everything in terms of goal alignment, not studying.

## Required output
Return ONLY valid JSON (no markdown, no backticks, no explanation outside the JSON):
{
  "weekly_summary": {
    "total_active_minutes": 412.0,
    "days_active": 6,
    "aligned_pct": 44,
    "drifting_pct": 56,
    "trend": "declining",
    "trend_detail": "Goal-aligned time dropped from 55% early in the week to 35% by Friday. Active 6 of 7 days."
  },
  "zone_out_profile": {
    "content_zone_outs": ["celebrity_gossip", "rage_bait"],
    "behavior_zone_outs": ["past_midnight", "same_app_60min"],
    "emerging": ["rage_bait"],
    "persistent": ["celebrity_gossip", "past_midnight"],
    "resolved": ["morning_scrolling"]
  },
  "observations": [
    "Instagram usage doubled from Monday to Friday, mostly celebrity content after 11pm",
    "Your YouTube goal-aligned sessions are consistent — 30min average, always before 8pm"
  ],
  "feedback": "You're holding strong on YouTube for your interests but Instagram is pulling you off track late at night."
}

Rules:
- "trend" must be one of: "improving", "declining", "stable"
- "observations" must have 2-4 items
- "total_active_minutes" should be the sum of all daily total_minutes
- "days_active" should match the number of days with data
- "aligned_pct" and "drifting_pct" should be time-weighted across the entire week, not averaged per day
- All zone-out patterns should be lowercase_snake_case
- Never use the word "study" — frame everything as goal-aligned vs drifting
"""


def validate_zone_out_profile(profile, input_goals, zone_out_events):
    """Validate and correct zone-out profile from Gemini using conservative strip-and-rebuild.

    Trust the raw data (input lists + zone_out_events) over Gemini's categorization.
    Corrections:
    - persistent but not in input list → emerging
    - persistent but not observed this week → resolved
    - resolved but not in either input list → drop
    - emerging but already in input list → persistent (if observed) or resolved (if not)
    - input patterns Gemini forgot → persistent (if observed) or resolved (if not)
    Rebuild content/behavior lists from corrected persistent + emerging.
    """
    input_content = set(input_goals.get("content_zone_outs") or [])
    input_behavior = set(input_goals.get("behavior_zone_outs") or [])
    input_all = input_content | input_behavior

    # Patterns observed this week, keyed by type
    observed_content = set()
    observed_behavior = set()
    for event in zone_out_events:
        pattern = event.get("pattern")
        etype = event.get("type")
        if pattern and etype == "content":
            observed_content.add(pattern)
        elif pattern and etype == "behavior":
            observed_behavior.add(pattern)
    observed_all = observed_content | observed_behavior

    # Gemini's raw output
    raw_persistent = list(profile.get("persistent") or [])
    raw_emerging = list(profile.get("emerging") or [])
    raw_resolved = list(profile.get("resolved") or [])
    gemini_content = set(profile.get("content_zone_outs") or [])
    gemini_behavior = set(profile.get("behavior_zone_outs") or [])

    corrected_persistent = set()
    corrected_emerging = set()
    corrected_resolved = set()

    # Validate persistent: must be in input AND observed this week
    for p in raw_persistent:
        if p not in input_all:
            corrected_emerging.add(p)
        elif p not in observed_all:
            corrected_resolved.add(p)
        else:
            corrected_persistent.add(p)

    # Validate emerging: must NOT be in input lists
    for p in raw_emerging:
        if p in input_all:
            if p in observed_all:
                corrected_persistent.add(p)
            else:
                corrected_resolved.add(p)
        else:
            corrected_emerging.add(p)

    # Validate resolved: must have been in input lists
    for p in raw_resolved:
        if p not in input_all:
            pass  # Drop: wasn't in profile, can't be "resolved"
        elif p in observed_all:
            corrected_persistent.add(p)  # Still observed → persistent
        else:
            corrected_resolved.add(p)

    # Catch input patterns Gemini forgot to mention
    for p in input_all:
        if p not in corrected_persistent and p not in corrected_resolved:
            if p in observed_all:
                corrected_persistent.add(p)
            else:
                corrected_resolved.add(p)

    # Determine type for each active pattern (persistent + emerging)
    # Priority: input type > observed type > Gemini's placement > default content
    corrected_content = set()
    corrected_behavior = set()

    for p in corrected_persistent | corrected_emerging:
        if p in input_content:
            corrected_content.add(p)
        elif p in input_behavior:
            corrected_behavior.add(p)
        elif p in observed_content:
            corrected_content.add(p)
        elif p in observed_behavior:
            corrected_behavior.add(p)
        elif p in gemini_content:
            corrected_content.add(p)
        elif p in gemini_behavior:
            corrected_behavior.add(p)
        else:
            corrected_content.add(p)

    return {
        "content_zone_outs": sorted(corrected_content),
        "behavior_zone_outs": sorted(corrected_behavior),
        "emerging": sorted(corrected_emerging),
        "persistent": sorted(corrected_persistent),
        "resolved": sorted(corrected_resolved),
    }


@firestore_fn.on_document_created(document="review_jobs/{job_id}", region=LOCATION)
def dispatch_review(event: firestore_fn.Event[firestore_fn.DocumentSnapshot]) -> None:
    """
    Triggered when a new review job is created in Firestore.
    Enqueues a Cloud Task to process_review endpoint.
    """
    tasks_client = tasks_v2.CloudTasksClient()

    job_id = event.params["job_id"]
    snapshot = event.data
    if not snapshot:
        print(f"[dispatch_review] No data for job {job_id}")
        return

    job_data = snapshot.to_dict()
    review_data = job_data.get("review_data")

    if not review_data:
        print(f"[dispatch_review] Missing 'review_data' in job {job_id}")
        return

    payload = {"job_id": job_id}
    parent = tasks_client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
    url = f"https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/process_review"

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {
                "service_account_email": f"{PROJECT_ID}@appspot.gserviceaccount.com"
            }
        }
    }

    response = tasks_client.create_task(request={"parent": parent, "task": task})
    print(f"[dispatch_review] Created task for job {job_id}")
    snapshot.reference.update({"status": "queued"})
    print(f"[dispatch_review] Job {job_id} status -> queued")


@https_fn.on_request(
    region=LOCATION,
    timeout_sec=120,
    memory=options.MemoryOption.MB_512,
    secrets=["GEMINI_API_KEY"]
)
def process_review(req: https_fn.Request) -> https_fn.Response:
    """
    Worker for weekly review processing. Invoked by Cloud Tasks.
    Reads review_data from Firestore (no GCS), calls Gemini (text-only),
    validates zone-out profile, writes result.
    """
    job_start = time.time()
    firestore_client = firestore.client()

    job_id = None
    try:
        data = req.get_json()
        job_id = data.get("job_id")

        if not job_id:
            print(f"[review] Bad request: missing job_id")
            return https_fn.Response("Missing job_id", status=400)

        print(f"[review] === START job {job_id} ===")

        # 1. Read review_data from Firestore
        job_ref = firestore_client.collection("review_jobs").document(job_id)
        job_doc = job_ref.get()
        if not job_doc.exists:
            print(f"[review] Job {job_id} not found")
            return https_fn.Response(f"Job {job_id} not found", status=404)

        job_data = job_doc.to_dict()
        review_data = job_data.get("review_data")
        if not review_data:
            print(f"[review] Missing review_data in job {job_id}")
            job_ref.update({"status": "failed", "error": "Missing review_data"})
            return https_fn.Response("Missing review_data", status=400)

        # 2. Setup Gemini
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print(f"[review] GEMINI_API_KEY not found")
            return https_fn.Response("GEMINI_API_KEY missing", status=500)

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        print(f"[review] Gemini ready (gemini-2.0-flash)")

        # 3. Extract input fields and build prompt
        user_goals = review_data.get("user_goals", {})
        daily_summaries = review_data.get("daily_summaries", [])
        zone_out_events = review_data.get("zone_out_events", [])
        review_period = review_data.get("review_period", {})

        prompt_input = f"""
## Review Period
{json.dumps(review_period, indent=2)}

## User Goals
{json.dumps(user_goals, indent=2)}

## Daily Usage Summaries
{json.dumps(daily_summaries, indent=2)}

## Zone-Out Events This Week
{json.dumps(zone_out_events, indent=2)}
"""

        print(f"[review] Prompt: {len(prompt_input)} chars, {len(daily_summaries)} days, {len(zone_out_events)} zone-out events")

        # 4. Call Gemini (text-only, single call)
        t1 = time.time()
        print(f"[review] Calling Gemini...")
        response = model.generate_content(REVIEW_PROMPT + prompt_input)
        result_text = response.text
        inference_time = time.time() - t1
        print(f"[review] Gemini responded ({inference_time:.1f}s, {len(result_text)} chars)")

        # 5. Parse response
        cleaned = result_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
        if cleaned.endswith("```"):
            cleaned = cleaned.rsplit("```", 1)[0]

        try:
            result = json.loads(cleaned.strip())
            print(f"[review] Response parsed as valid JSON")
        except json.JSONDecodeError:
            print(f"[review] ERROR: Response is not valid JSON")
            job_ref.update({
                "status": "failed",
                "error": "Failed to parse Gemini response",
                "raw_response": result_text,
            })
            return https_fn.Response("Gemini response parse error", status=500)

        # 6. Validate and correct zone-out profile
        raw_profile = result.get("zone_out_profile", {})
        corrected_profile = validate_zone_out_profile(
            raw_profile, user_goals, zone_out_events
        )
        if corrected_profile != {
            "content_zone_outs": sorted(raw_profile.get("content_zone_outs") or []),
            "behavior_zone_outs": sorted(raw_profile.get("behavior_zone_outs") or []),
            "emerging": sorted(raw_profile.get("emerging") or []),
            "persistent": sorted(raw_profile.get("persistent") or []),
            "resolved": sorted(raw_profile.get("resolved") or []),
        }:
            print(f"[review] Zone-out profile corrected")
            print(f"[review]   Raw:       {json.dumps(raw_profile)}")
            print(f"[review]   Corrected: {json.dumps(corrected_profile)}")
        result["zone_out_profile"] = corrected_profile

        # 7. Validate other fields
        summary = result.get("weekly_summary", {})
        if summary.get("trend") not in ("improving", "declining", "stable"):
            print(f"[review] Invalid trend '{summary.get('trend')}', defaulting to 'stable'")
            summary["trend"] = "stable"
        if "days_active" not in summary:
            summary["days_active"] = review_period.get("days_active", len(daily_summaries))
        result["weekly_summary"] = summary

        observations = result.get("observations", [])
        if len(observations) > 4:
            result["observations"] = observations[:4]

        # 8. Write result
        job_ref.update({
            "status": "complete",
            "result": result,
            "completed_at": firestore.SERVER_TIMESTAMP,
        })
        total_time = time.time() - job_start
        print(f"[review] === DONE job {job_id} in {total_time:.1f}s ===")
        return https_fn.Response("OK", status=200)

    except Exception as e:
        print(f"[review] FATAL: {type(e).__name__}: {e}")
        if job_id:
            try:
                firestore_client.collection("review_jobs").document(job_id).update({
                    "status": "failed",
                    "error": str(e),
                })
            except Exception:
                pass
        return https_fn.Response(str(e), status=500)
