# GCP Function Implementation Plan

## Overview

Implement a unified `gcp()` native function supporting Google Cloud Platform services, starting with GCS (Cloud Storage).

## API Design

```python
# Single unified function with "service.action" pattern
gcp("gcs.upload", bucket="my-bucket", object="path/to/file.txt", data="content")
gcp("gcs.download", bucket="my-bucket", object="path/to/file.txt")
gcp("gcs.list", bucket="my-bucket", prefix="folder/")
gcp("gcs.delete", bucket="my-bucket", object="path/to/file.txt")
gcp("gcs.exists", bucket="my-bucket", object="path/to/file.txt")

# Authentication: optional explicit, fallback to ADC
gcp("gcs.upload", ..., credentials="/path/to/service-account.json")
gcp("gcs.upload", ..., credentials_json='{"type": "service_account", ...}')
# If no credentials kwarg → use Application Default Credentials
```

## GCS Operations (Initial Scope)

| Action | Description | Required Kwargs | Optional Kwargs |
|--------|-------------|-----------------|-----------------|
| `gcs.upload` | Upload content to object | `bucket`, `object`, `data` | `content_type` |
| `gcs.upload_file` | Upload local file | `bucket`, `object`, `file` | `content_type` |
| `gcs.download` | Download object content | `bucket`, `object` | - |
| `gcs.download_file` | Download to local file | `bucket`, `object`, `file` | - |
| `gcs.list` | List objects in bucket | `bucket` | `prefix`, `delimiter`, `max_results` |
| `gcs.delete` | Delete object | `bucket`, `object` | - |
| `gcs.exists` | Check if object exists | `bucket`, `object` | - |
| `gcs.copy` | Copy object | `src_bucket`, `src_object`, `dest_bucket`, `dest_object` | - |
| `gcs.get_metadata` | Get object metadata | `bucket`, `object` | - |

## Authentication Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      gcp() called                               │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │ credentials kwarg provided? │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
            ┌───────────┐               ┌───────────────┐
            │    Yes    │               │      No       │
            └─────┬─────┘               └───────┬───────┘
                  │                             │
                  ▼                             ▼
     ┌────────────────────────┐    ┌───────────────────────────┐
     │ Parse service account  │    │ Use ADC:                  │
     │ JSON (file or inline)  │    │ 1. GOOGLE_APPLICATION_    │
     └────────────────────────┘    │    CREDENTIALS env var    │
                  │                │ 2. gcloud auth default    │
                  │                │ 3. Metadata server (GCE)  │
                  │                └───────────────────────────┘
                  │                             │
                  └──────────────┬──────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │ Generate OAuth2 token   │
                    │ (JWT → access token)    │
                    └─────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │ Call GCS JSON API       │
                    │ with Bearer token       │
                    └─────────────────────────┘
```

## Implementation Structure

```
crates/blueprint_eval/src/natives/
├── gcp/
│   ├── mod.rs          # Main gcp() function, action routing
│   ├── auth.rs         # OAuth2 token generation, ADC support
│   └── gcs.rs          # GCS operations
└── mod.rs              # Add: mod gcp; gcp::register(evaluator);
```

## Dependencies

Add to `crates/blueprint_eval/Cargo.toml`:
```toml
jsonwebtoken = "9"      # JWT signing for service account auth
base64 = "0.22"         # Encoding for JWT
```

Note: `reqwest` and `serde_json` already available.

## Implementation Steps

### Step 1: Create gcp module structure
- Create `natives/gcp/mod.rs` with main `gcp()` function
- Parse first arg as "service.action" string
- Route to appropriate service handler

### Step 2: Implement authentication (auth.rs)
- `get_access_token()` - main entry point
- `from_service_account_json()` - parse SA credentials
- `from_adc()` - Application Default Credentials lookup
- `generate_jwt()` - create signed JWT for token exchange
- `exchange_jwt_for_token()` - call Google OAuth2 endpoint

### Step 3: Implement GCS operations (gcs.rs)
- Use GCS JSON API v1: `https://storage.googleapis.com/storage/v1/b/{bucket}/o`
- Each operation returns a dict with result + metadata

### Step 4: Register and test
- Add to `natives/mod.rs`
- Create example script
- Test with real GCS bucket

## Return Value Format

All GCP operations return a consistent dict:

```python
{
    "success": True,
    "data": <operation-specific result>,
    "metadata": {
        "bucket": "my-bucket",
        "object": "path/to/file.txt",
        "size": 1234,
        "content_type": "text/plain",
        ...
    }
}

# On error:
{
    "success": False,
    "error": "Object not found",
    "code": 404
}
```

## GCS API Endpoints

| Operation | HTTP Method | Endpoint |
|-----------|-------------|----------|
| upload | POST | `https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={object}` |
| download | GET | `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object}?alt=media` |
| list | GET | `https://storage.googleapis.com/storage/v1/b/{bucket}/o` |
| delete | DELETE | `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object}` |
| exists/metadata | GET | `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object}` |
| copy | POST | `https://storage.googleapis.com/storage/v1/b/{srcBucket}/o/{srcObject}/copyTo/b/{destBucket}/o/{destObject}` |

## Future Expansion (Not in initial scope)

```python
# BigQuery
gcp("bq.query", query="SELECT * FROM dataset.table", project="my-project")
gcp("bq.insert", table="dataset.table", rows=[...])

# IAM
gcp("iam.get_policy", resource="projects/my-project")
gcp("iam.test_permissions", resource="...", permissions=["storage.buckets.get"])

# Pub/Sub
gcp("pubsub.publish", topic="my-topic", message="hello")
gcp("pubsub.pull", subscription="my-sub")
```
