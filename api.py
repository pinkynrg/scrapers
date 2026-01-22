from fastapi import FastAPI, HTTPException
import sqlite3
from typing import List, Dict, Any, Optional
from pathlib import Path

app = FastAPI(title="Scrapers API", version="1.0.0")

DB_PATH = "/Users/francescomeli/Projects/scrapers/data/scrapers.db"


def get_db_connection():
    """Create a database connection"""
    if not Path(DB_PATH).exists():
        raise HTTPException(status_code=404, detail="Database not found")
    return sqlite3.connect(DB_PATH)


def get_table_data(table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get all data from a table"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        # Get data
        query = f"SELECT * FROM {table_name} ORDER BY created_at DESC"
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
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
def get_table_items(table_name: str, limit: Optional[int] = None):
    """Get all items from any table"""
    data = get_table_data(table_name, limit)
    return {
        "table": table_name,
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
    uvicorn.run(app, host="127.0.0.1", port=8000)
