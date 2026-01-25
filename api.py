from fastapi import FastAPI, HTTPException, Query
import sqlite3
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from pathlib import Path

# Load environment variables
load_dotenv()

db_path = os.getenv("DB_PATH", "")

if not db_path:
    raise ValueError("Please set DB_PATH environment variable.")

app = FastAPI(title="Scrapers API", version="1.0.0")


def get_db_connection():
    """Create a database connection"""
    if not Path(db_path).exists():
        raise HTTPException(status_code=404, detail="Database not found")
    return sqlite3.connect(db_path)


def get_table_data(table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get all data from a table with optional limit and offset for pagination"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        # Get data
        query = f"SELECT * FROM {table_name} ORDER BY created_at DESC"
        params = []
        if hasattr(get_table_data, 'offset') and hasattr(get_table_data, 'limit'):
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
    """Root endpoint with dynamic table listing"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        endpoints = {
            "/tables": "List all available tables"
        }
        
        for table in tables:
            endpoints[f"/{table}"] = f"Get all {table}"
            endpoints[f"/{table}/{{id}}"] = f"Get specific {table} by id"
        
        return {
            "message": "Scrapers API",
            "endpoints": endpoints
        }
    finally:
        conn.close()


@app.get("/tables")
def list_tables():
    """List all available tables in the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        return {"tables": tables}
    finally:
        conn.close()



@app.get("/{table_name}")
def get_table_items(
    table_name: str,
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    pageSize: int = Query(20, ge=1, le=100, description="Number of items per page")
):
    """Get paginated items from any table"""
    offset = (page - 1) * pageSize
    # Attach pagination info to function for use in get_table_data
    get_table_data.offset = offset
    get_table_data.limit = pageSize
    data = get_table_data(table_name, pageSize)
    # Optionally, get total count for pagination info
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()[0]
    finally:
        conn.close()
    return {
        "table": table_name,
        "page": page,
        "pageSize": pageSize,
        "total": total,
        "count": len(data),
        "data": data
    }


@app.get("/{table_name}/{item_id}")
def get_table_item(table_name: str, item_id: str):
    """Get a specific item by ID from any table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
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
