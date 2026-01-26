from fastapi import FastAPI, HTTPException, Query
import sqlite3
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from pathlib import Path
import glob

# Load environment variables
load_dotenv()

db_directory = os.getenv("DB_PATH", "")

if not db_directory:
    raise ValueError("Please set DB_PATH environment variable.")

app = FastAPI(title="Scrapers API", version="1.0.0")


def get_db_files() -> Dict[str, str]:
    """Get all database files in the directory"""
    db_files = {}
    for db_file in glob.glob(str(Path(db_directory) / "*.db")):
        scraper_name = Path(db_file).stem
        db_files[scraper_name] = db_file
    return db_files


def get_db_connection(scraper_name: str):
    """Create a database connection for a specific scraper"""
    db_files = get_db_files()
    if scraper_name not in db_files:
        raise HTTPException(status_code=404, detail=f"Database for scraper '{scraper_name}' not found")
    return sqlite3.connect(db_files[scraper_name])


def get_table_data(scraper_name: str, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get all data from a table with optional limit and offset for pagination"""
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    try:
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        # Get data
        query = f"SELECT * FROM {table_name} ORDER BY created_at DESC"
        params = []
        # If limit is -1, return all results without pagination
        if hasattr(get_table_data, 'limit') and get_table_data.limit == -1:
            # Don't add LIMIT clause
            pass
        elif hasattr(get_table_data, 'offset') and hasattr(get_table_data, 'limit'):
            query += " LIMIT ? OFFSET ?"
            params.extend([get_table_data.limit, get_table_data.offset])
        elif hasattr(get_table_data, 'limit') and get_table_data.limit is not None:
            query += " LIMIT ?"
            params.append(get_table_data.limit)
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


@app.get("/")
def root():
    """Root endpoint with dynamic scraper and table listing"""
    db_files = get_db_files()
    
    endpoints = {
        "/scrapers": "List all available scrapers"
    }
    
    for scraper_name in db_files.keys():
        endpoints[f"/{scraper_name}"] = f"Get all tables from {scraper_name} scraper"
        endpoints[f"/{scraper_name}/{{table_name}}"] = f"Get all items from a specific table in {scraper_name}"
        endpoints[f"/{scraper_name}/{{table_name}}/{{id}}"] = f"Get specific item by id from a table in {scraper_name}"
    
    return {
        "message": "Scrapers API",
        "scrapers": list(db_files.keys()),
        "endpoints": endpoints
    }


@app.get("/scrapers")
def list_scrapers():
    """List all available scrapers"""
    db_files = get_db_files()
    scrapers_info = {}
    
    for scraper_name, db_file in db_files.items():
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            scrapers_info[scraper_name] = {
                "database": db_file,
                "tables": tables
            }
        finally:
            conn.close()
    
    return {"scrapers": scrapers_info}



@app.get("/{scraper_name}")
def get_scraper_tables(scraper_name: str):
    """Get all tables from a specific scraper"""
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        return {
            "scraper": scraper_name,
            "tables": tables
        }
    finally:
        conn.close()


@app.get("/{scraper_name}/{table_name}")
def get_table_items(
    scraper_name: str,
    table_name: str,
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    page_size: int = Query(20, ge=-1, description="Number of items per page. Use -1 to get all items.")
):
    """Get paginated items from any table in a specific scraper. Use page_size=-1 to get all items."""
    # If page_size is -1, get all results
    if page_size == -1:
        get_table_data.offset = 0
        get_table_data.limit = -1
        data = get_table_data(scraper_name, table_name, page_size)
    else:
        # Validate page_size is within limits when not -1
        if page_size > 100:
            raise HTTPException(status_code=400, detail="page_size cannot exceed 100 (use -1 for all items)")
        offset = (page - 1) * page_size
        # Attach pagination info to function for use in get_table_data
        get_table_data.offset = offset
        get_table_data.limit = page_size
        data = get_table_data(scraper_name, table_name, page_size)
    
    # Optionally, get total count for pagination info
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()[0]
    finally:
        conn.close()
    return {
        "scraper": scraper_name,
        "table": table_name,
        "page": page if page_size != -1 else None,
        "page_size": page_size,
        "total": total,
        "count": len(data),
        "data": data
    }


@app.get("/{scraper_name}/{table_name}/{item_id}")
def get_table_item(scraper_name: str, table_name: str, item_id: str):
    """Get a specific item by ID from any table in a specific scraper"""
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in scraper '{scraper_name}'")
        
        cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (item_id,))
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Item {item_id} not found in {table_name}")
        
        return dict(zip(columns, row))
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
