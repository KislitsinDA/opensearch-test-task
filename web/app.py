import os
from typing import List, Optional
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from opensearchpy import OpenSearch, RequestsHttpConnection
import time

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
INDEX_NAME = os.getenv("INDEX_NAME", "docs")
# Allowed content types for the radio filter
CONTENT_TYPES = [x.strip() for x in os.getenv("CONTENT_TYPES", "article,news,blog,report").split(",") if x.strip()]

client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_auth=None,
    use_ssl=False,
    verify_certs=False,
    connection_class=RequestsHttpConnection,
)

templates = Jinja2Templates(directory="templates")
app = FastAPI(title="OpenSearch Test Task")

def wait_for_opensearch(timeout_sec: int = 60):
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            health = client.cluster.health()
            if health and health.get("status"):
                return True
        except Exception:
            time.sleep(2)
    raise RuntimeError("OpenSearch not available")

def ensure_index():
    # Create index with mapping: title (text), content (text), content_type (keyword)
    if not client.indices.exists(INDEX_NAME):
        body = {
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            },
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "content_type": {"type": "keyword"}  # exact match for filter
                }
            }
        }
        client.indices.create(index=INDEX_NAME, body=body)

def seed_docs():
    # Only index seed docs if the index is empty
    count = client.count(index=INDEX_NAME)["count"]
    if count > 0:
        return
    seed = [
        {"title": "title1", "content": "content1", "content_type": "article"},
        {"title": "title2", "content": "content2", "content_type": "news"},
        {"title": "title3", "content": "content3", "content_type": "blog"},
        {"title": "title4", "content": "content4", "content_type": "report"},
        {"title": "title5", "content": "content5", "content_type": "article"},
    ]
    for i, doc in enumerate(seed):
        client.index(index=INDEX_NAME, id=i+1, body=doc, refresh=False)
    client.indices.refresh(index=INDEX_NAME)

@app.on_event("startup")
def startup():
    wait_for_opensearch()
    ensure_index()
    seed_docs()

class SearchResponseItem(BaseModel):
    title: str
    snippet: str

def do_search(q: str, content_type: Optional[str]) -> List[SearchResponseItem]:
    must = []
    if q:
        must.append({
            "multi_match": {
                "query": q,
                "fields": ["title", "content"]
            }
        })
    if content_type:
        if content_type not in CONTENT_TYPES:
            # Silently return empty if type invalid
            return []
        must.append({"term": {"content_type": content_type}})

    query = {"bool": {"must": must}} if must else {"match_all": {}}

    res = client.search(index=INDEX_NAME, body={
        "query": query,
        "_source": ["title", "content"],
        "size": 25
    })

    items: List[SearchResponseItem] = []
    for hit in res["hits"]["hits"]:
        src = hit["_source"]
        content = src.get("content", "") or ""
        snippet = content[:50]
        items.append(SearchResponseItem(title=src.get("title", ""), snippet=snippet))
    return items

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, q: str = "", content_type: Optional[str] = None):
    results = do_search(q, content_type) if (q or content_type) else []
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "q": q,
            "content_types": CONTENT_TYPES,
            "selected_type": content_type or "",
            "results": results
        },
    )

@app.get("/api/search")
async def api_search(q: str = Query("", description="keyword"), content_type: Optional[str] = Query(None)):
    items = do_search(q, content_type)
    # Return list of dicts with title and snippet
    return JSONResponse([item.model_dump() for item in items])
