from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import subprocess
import tempfile
import os

app = FastAPI()

# CORS setup
origins = [
    "https://nathanhouwaart.github.io/",  # ✅ Your production domain
    "http://localhost:5173"       # For development
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

        # Read output
        with open(output_path, "r") as out_f:
            converted_data = out_f.read()

        return JSONResponse(content=converted_data)

    except subprocess.CalledProcessError:
        raise HTTPException(status_code=500, detail="Conversion process failed")

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