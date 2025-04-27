from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import os
import uuid
import aiofiles
from pathlib import Path
import redis.asyncio as redis  # Using async Redis client

# Connect to Redis using your Redis URL
redis_client = redis.from_url("rediss://default:AXOzAAIjcDEwYTNmNTYyNjQzMTI0ZTUyOWViYTY0YWQ0MDk3NzcwYnAxMA@touched-wallaby-29619.upstash.io:6379", decode_responses=False)

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

        # Save chunk into Redis
        chunk_key = f"{upload_id}:{chunk_number:04d}"
        content = await file.read()
        await redis_client.set(chunk_key, content)

        # Check if all chunks arrived
        all_chunks = all(
            [await redis_client.exists(f"{upload_id}:{i:04d}") for i in range(total_chunks)]
        )

        if all_chunks:
            # Reassemble file
            output_path = os.path.join(UPLOAD_DIR, f"{upload_id}.mp4")
            async with aiofiles.open(output_path, "wb") as outfile:
                for i in range(total_chunks):
                    chunk_key = f"{upload_id}:{i:04d}"
                    chunk_data = await redis_client.get(chunk_key)
                    await outfile.write(chunk_data)
                    await redis_client.delete(chunk_key)  # Clean up Redis

            return {"status": "complete", "path": output_path}
        
        return {"status": "partial", "received": chunk_number + 1, "total": total_chunks}

    except Exception as e:
        raise HTTPException(500, str(e))

