from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import os
import uuid
import aiofiles
from pathlib import Path

app = FastAPI()

# Config
UPLOAD_DIR = "uploads"
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
Path(UPLOAD_DIR).mkdir(exist_ok=True)

@app.post("/api/v1/uploads")
async def upload_video(
    file: UploadFile = File(...),
    metadata: str = Form(...),
    chunk_number: int = Form(0),
    total_chunks: int = Form(1),
    upload_id: str = Form(None)
):
    """Handle chunked video uploads"""
    try:
        # Generate upload ID if not provided
        upload_id = upload_id or str(uuid.uuid4())
        session_dir = os.path.join(UPLOAD_DIR, upload_id)
        os.makedirs(session_dir, exist_ok=True)

        # Save chunk
        chunk_path = os.path.join(session_dir, f"chunk_{chunk_number:04d}")
        async with aiofiles.open(chunk_path, "wb") as f:
            while content := await file.read(CHUNK_SIZE):
                await f.write(content)

        # Check if all chunks arrived
        all_chunks = all(
            os.path.exists(os.path.join(session_dir, f"chunk_{i:04d}"))
            for i in range(total_chunks)
        )

        if all_chunks:
            # Reassemble file
            output_path = os.path.join(UPLOAD_DIR, f"{upload_id}.mp4")
            async with aiofiles.open(output_path, "wb") as outfile:
                for i in range(total_chunks):
                    chunk_path = os.path.join(session_dir, f"chunk_{i:04d}")
                    async with aiofiles.open(chunk_path, "rb") as infile:
                        await outfile.write(await infile.read())
            
            # Cleanup
            for i in range(total_chunks):
                os.remove(os.path.join(session_dir, f"chunk_{i:04d}"))
            os.rmdir(session_dir)

            return {"status": "complete", "path": output_path}
        
        return {"status": "partial", "received": chunk_number + 1, "total": total_chunks}

    except Exception as e:
        raise HTTPException(500, str(e))