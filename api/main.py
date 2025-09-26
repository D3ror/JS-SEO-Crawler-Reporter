# api/main.py
from fastapi import FastAPI, BackgroundTasks
from api.crawl import CrawlerService

app = FastAPI()
crawler: CrawlerService = None

@app.on_event("startup")
async def startup():
    global crawler
    crawler = CrawlerService(concurrency=3)   # reuse browser
    await crawler.start()

@app.on_event("shutdown")
async def shutdown():
    await crawler.stop()

@app.post("/crawl")
async def crawl_url(payload: dict):
    # payload: {"url": "...", "options": {...}}
    job = await crawler.enqueue(payload["url"], payload.get("options", {}))
    return {"job_id": job}

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    return await crawler.get_status(job_id)

@app.get("/results/{job_id}")
async def get_results(job_id: str):
    return await crawler.get_results(job_id)

@app.get("/cwv")
async def get_cwv(url: str, strategy: str = "mobile"):
    return await crawler.get_cwv(url, strategy=strategy)
