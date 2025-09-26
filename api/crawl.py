# api/crawl.py (abridged)
import asyncio, json, base64, hashlib
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.async_api import async_playwright

# simple URL normalization
def normalize_url(url: str) -> str:
    p = urlparse(url)
    scheme = p.scheme or "https"
    host = p.hostname.lower()
    path = p.path or "/"
    qs = [(k,v) for k,v in parse_qsl(p.query) if not k.startswith("utm_") and k not in ("fbclid","gclid")]
    qs_sorted = sorted(qs)
    query = urlencode(qs_sorted)
    return urlunparse((scheme, host, path, "", query, ""))

class CrawlerService:
    def __init__(self, concurrency=3):
        self.concurrency = concurrency
        self._queue = asyncio.Queue()
        self._tasks = {}
        self._browser = None
        self._worker_tasks = []
        self._client = httpx.AsyncClient(timeout=15)

    async def start(self):
        pw = await async_playwright().start()
        self._browser = await pw.chromium.launch(headless=True)
        for _ in range(self.concurrency):
            t = asyncio.create_task(self._worker())
            self._worker_tasks.append(t)

    async def stop(self):
        for t in self._worker_tasks:
            t.cancel()
        if self._browser:
            await self._browser.close()
        await self._client.aclose()

    async def enqueue(self, url, options):
        job_id = hashlib.sha1(url.encode()).hexdigest()[:10]
        await self._queue.put((job_id, url, options))
        self._tasks[job_id] = {"status":"queued"}
        return job_id

    async def _worker(self):
        while True:
            job_id, url, options = await self._queue.get()
            self._tasks[job_id]["status"] = "running"
            try:
                result = await self._crawl_single(url, options)
                self._tasks[job_id]["status"] = "done"
                self._tasks[job_id]["result"] = result
            except Exception as e:
                self._tasks[job_id]["status"] = "error"
                self._tasks[job_id]["error"] = str(e)
            finally:
                self._queue.task_done()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    async def _fetch_raw(self, url):
        r = await self._client.get(url)
        return r.status_code, r.text, r.headers

    async def _render(self, url, wait_until="networkidle", timeout=20000):
        ctx = await self._browser.new_context(viewport={"width":1366, "height":768})
        page = await ctx.new_page()
        await page.goto(url, wait_until=wait_until, timeout=timeout)
        content = await page.content()
        screenshot = await page.screenshot(full_page=True)
        await page.close()
        await ctx.close()
        return content, screenshot

    async def _crawl_single(self, url, options):
        norm = normalize_url(url)
        # robots check — naive: fetch robots.txt
        # (Implement robots parsing; skip here)
        status, raw_html, headers = await self._fetch_raw(norm)
        rendered_html, screenshot = await self._render(norm)
        diff_score = compute_diff_score(raw_html, rendered_html)
        jsonld = extract_jsonld(rendered_html)
        schema_validations = validate_jsonld(jsonld)
        psi = await self.get_cwv(norm)   # calls PSI
        # build result
        return {
            "url": norm,
            "status": status,
            "headers": dict(headers),
            "diff_score": diff_score,
            "jsonld": jsonld,
            "schema_validations": schema_validations,
            "psi": psi,
            "screenshot": base64.b64encode(screenshot).decode("ascii")
        }
