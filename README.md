# ISA-PHM Backend

FastAPI backend for converting ISA-PHM Wizard payloads into ISA-JSON.

## What Improved

- Config-driven runtime (`CONVERTER_PYTHON`, `CONVERTER_TIMEOUT_SECONDS`, `MAX_UPLOAD_MB`, `CORS_ALLOW_ORIGINS`, `STRICT_SCHEMA`)
- Structured error responses with request correlation (`request_id`)
- Liveness/readiness endpoints (`/healthz`, `/readyz`)
- Semantic payload validation before conversion
- Importable converter entrypoint (`app.converter.create_isa_data`)
- Backend test suite + CI smoke conversion

## Project Structure

```text
ISA-PHM-Backend/
├── app/
│   ├── main.py                     # FastAPI app and API endpoints
│   ├── web-to-isa-phm.py           # CLI wrapper around converter entrypoint
│   └── converter/                  # Conversion modules (normalization/mapping/graph)
├── schema/
│   ├── IsaPhmInfo.schema.json      # Compatibility schema (default)
│   └── IsaPhmInfo.strict.schema.json # Stricter v2 schema (feature-flagged)
├── tools/
│   └── verify-isa-json.py          # Validate generated ISA-JSON with isatools
├── tests/
│   ├── test_api_unit.py
│   ├── test_integration_conversion.py
│   └── fixtures/minimal_payload.json
├── .github/workflows/backend-ci.yml
├── requirements.txt
├── requirements-dev.txt
├── environment.yml
└── Dockerfile
```

## Runtime Configuration

| Variable | Default | Description |
|---|---|---|
| `CONVERTER_PYTHON` | current Python interpreter | Python executable used to run `app/web-to-isa-phm.py` |
| `CONVERTER_TIMEOUT_SECONDS` | `120` | Converter subprocess timeout |
| `MAX_UPLOAD_MB` | `50` | Max upload size for `/convert` |
| `CORS_ALLOW_ORIGINS` | `https://nathanhouwaart.github.io,http://localhost:5173` | Comma-separated origin list |
| `STRICT_SCHEMA` | `false` | If `true`, validates against `IsaPhmInfo.strict.schema.json` |

## Run Locally

```bash
python -m pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

Conda option:

```bash
conda env create -f environment.yml
conda activate isa-phm-backend
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

## Docker

```bash
docker build -t isa-phm-backend .
docker run -p 8080:8080 isa-phm-backend
```

## API

### `GET /`
Returns a simple status message.

### `GET /healthz`
Liveness endpoint.

### `GET /readyz`
Readiness endpoint (schema + converter readiness details). Returns `503` when not ready.

### `POST /convert`
Accepts `multipart/form-data` with field `file` containing a `.json` payload.

Success response:
- `200` with ISA-JSON body (`application/json`)

Error response shape:

```json
{
  "error": {
    "code": "semantic_validation_failed",
    "message": "Payload semantic validation failed",
    "details": [],
    "request_id": "..."
  }
}
```

## Validation Flow

1. File extension and content type checks
2. Upload size guard (`MAX_UPLOAD_MB`)
3. JSON parse validation
4. JSON schema validation (compat or strict schema)
5. Semantic validation (runs/protocol selections/reference integrity)
6. Converter subprocess execution
7. Converter output JSON parse check

## Tests

```bash
python -m pip install -r requirements-dev.txt
pytest -q
```

## Tooling

Validate generated ISA-JSON:

```bash
python tools/verify-isa-json.py <path-to-isa-json>
```
