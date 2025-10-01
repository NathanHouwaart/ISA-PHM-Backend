from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import tempfile
import os
import uvicorn

app = FastAPI()

# CORS setup
origins = [
    "https://nathanhouwaart.github.io",                    # Base domain without slash
    "https://nathanhouwaart.github.io/",                   # Base domain with slash  
    "https://nathanhouwaart.github.io/ISA-PHM-Wizard",     # Your app without slash
    "https://nathanhouwaart.github.io/ISA-PHM-Wizard/",    # Your app with slash
    "http://localhost:5173"                                # For development
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)

@app.post("/convert")
async def convert_json(file: UploadFile = File(...)):
    if not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are allowed")

    if file.content_type != "application/json":
        raise HTTPException(status_code=400, detail="Invalid file type")

    try:
        # Create secure temp input file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as input_f:
            input_path = input_f.name
            input_f.write(await file.read())

        # Create secure temp output file
        output_f = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        output_path = output_f.name
        output_f.close()

        # Run your external script
        subprocess.run(
            ["python3.9", "web-to-isa-phm.py", input_path, output_path],
            check=True
        )

        # Read the raw JSON output and return it as-is to preserve key order
        with open(output_path, "r") as out_f:
            raw_json = out_f.read()

        # Return the JSON text unchanged so the script's original ordering is preserved
        return PlainTextResponse(content=raw_json, media_type="application/json")

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Conversion process failed: {str(e)}")

    finally:
        # Always clean up
        if os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)

# Optional: generic fallback error handler
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return PlainTextResponse("Internal server error", status_code=500)


if __name__ == "__main__":
    print("Starting Webserver")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080)),
        proxy_headers=True
    )