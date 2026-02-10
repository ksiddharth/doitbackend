# DoIt Backend

Firebase Cloud Functions backend for **DoIt** — a goal-alignment tracking app. Users set personal goals and target time splits. The app captures phone screenshots + accessibility trees, sends them to this backend, which uses Gemini to classify each screen as "aligned" (with goals) or "drifting" (away from goals) and returns scores and feedback.

The backend is **stateless** — it classifies *what* the user was doing. The Android app owns all temporal data (timestamps, durations, time blocks).

## Architecture

![Architecture Diagram](docs/architecture.excalidraw)

Single `main.py` with six Firebase Cloud Functions across three pipelines:

### Activity Tracking Pipeline
| Function | Type | Description |
|----------|------|-------------|
| `dispatch_analysis` | Firestore `onCreate` trigger on `jobs/{id}` | Enqueues Cloud Task to worker |
| `process_worker` | HTTPS worker (540s, 2GB) | Downloads screenshots from GCS, pairs with accessibility meta, classifies via Gemini vision, writes result |

### Bookmark Pipeline
| Function | Type | Description |
|----------|------|-------------|
| `dispatch_bookmark` | Firestore `onCreate` trigger on `bookmark_jobs/{id}` | Enqueues Cloud Task to worker |
| `process_bookmark` | HTTPS worker (120s, 512MB) | Extracts content metadata via Gemini, resolves URL (YouTube Data API, constructed search) |

### Weekly Review Pipeline
| Function | Type | Description |
|----------|------|-------------|
| `dispatch_review` | Firestore `onCreate` trigger on `review_jobs/{id}` | Enqueues Cloud Task to worker |
| `process_review` | HTTPS worker (120s, 512MB) | Text-only Gemini call on aggregated weekly data, validates zone-out profile, returns trend analysis |

### Data Flow

```
Activity:  App → GCS (screenshots) + Firestore (job doc) → Cloud Task → Gemini (vision) → Firestore (result) → App
Bookmark:  App → GCS (screenshot) + Firestore (job doc) → Cloud Task → Gemini + YouTube API → Firestore (result) → App
Review:    App → Firestore (job doc with inline JSON) → Cloud Task → Gemini (text-only) → Firestore (result) → App
```

All source files (screenshots, accessibility data) are **permanently deleted** after processing. GCS bucket has a 1-day lifecycle policy as a safety net.

## Tech Stack

- **Firebase Cloud Functions** (2nd Gen, Python 3.11)
- **Gemini 2.0 Flash** — activity classification (vision + text), bookmark extraction, weekly review analysis
- **Google Cloud Firestore** — job queue and result storage
- **Google Cloud Storage** — ephemeral screenshot uploads
- **Google Cloud Tasks** — async job dispatch with retry
- **YouTube Data API v3** — bookmark URL resolution
- **Firebase Authentication** — client auth for security rules

## Setup

### Prerequisites
- Firebase CLI
- Google Cloud SDK
- Python 3.11+

### Install Dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Initial GCP Setup
```bash
./setup.sh
```

### Configure Secrets
```bash
firebase functions:secrets:set GEMINI_API_KEY
firebase functions:secrets:set YOUTUBE_API_KEY
```

## Deploy

```bash
# All functions
firebase deploy --only functions

# Firestore rules
firebase deploy --only firestore:rules

# Storage rules
firebase deploy --only storage
```

## Logs

```bash
gcloud functions logs read process_worker --region=us-central1 --limit=50
gcloud functions logs read dispatch_analysis --region=us-central1 --limit=50
gcloud functions logs read process_bookmark --region=us-central1 --limit=50
gcloud functions logs read dispatch_bookmark --region=us-central1 --limit=50
gcloud functions logs read process_review --region=us-central1 --limit=50
gcloud functions logs read dispatch_review --region=us-central1 --limit=50
```

## Infrastructure

| Resource | Value |
|----------|-------|
| GCP Project | `ephemeral-access-backend` |
| Region | `us-central1` |
| Cloud Tasks Queue | `analysis-queue` |
| GCS Bucket | `ephemeral-access-backend.firebasestorage.app` |
| Model | `gemini-2.0-flash` |
| Secrets | `GEMINI_API_KEY`, `YOUTUBE_API_KEY` |

## Response Schemas

### Activity Classification (`jobs/{id}`)

Categories: `"aligned"` (goal-aligned) or `"drifting"` (away from goals).

```json
{
  "activities": [{"capture": "001", "app": "...", "app_name": "...", "category": "aligned|drifting", "description": "..."}],
  "transitions": [{"at_capture": "006", "from": "aligned", "to": "drifting", "trigger": "..."}],
  "streaks": {"longest_aligned": 5, "longest_drifting": 8, "ended_on": "drifting"},
  "session_summary": {"total_captures": 10, "aligned_captures": 3, "drifting_captures": 7, "aligned_pct": 30, "drifting_pct": 70},
  "updated_score": {"aligned_pct": 45, "drifting_pct": 55},
  "feedback": "..."
}
```

### Bookmark (`bookmark_jobs/{id}`)

```json
{
  "status": "complete",
  "result": {
    "platform": "youtube", "title": "...", "channel": "...",
    "url": "https://...", "description": "...", "content_type": "video",
    "confidence": "high|medium|low", "resolution_method": "direct_url|direct_id|api_search|constructed_search|failed"
  }
}
```

### Weekly Review (`review_jobs/{id}`)

```json
{
  "weekly_summary": {"total_active_minutes": 412.0, "days_active": 6, "aligned_pct": 44, "drifting_pct": 56, "trend": "improving|declining|stable", "trend_detail": "..."},
  "zone_out_profile": {"content_zone_outs": ["..."], "behavior_zone_outs": ["..."], "emerging": ["..."], "persistent": ["..."], "resolved": ["..."]},
  "observations": ["...", "..."],
  "feedback": "..."
}
```

Zone-out profile is server-validated: resolved items must have been in the previous profile, emerging items must be new, persistent items must appear in both.

## Privacy

- Screenshots and accessibility data are deleted immediately after Gemini processing
- GCS bucket has a 1-day lifecycle policy as a safety net
- Storage rules deny all client reads
- No user data is retained on the backend after job completion
