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


def get_table_columns(scraper_name: str, table_name: str) -> List[str]:
    """Get all column names for a table"""
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        return columns
    finally:
        conn.close()


def get_table_data(scraper_name: str, table_name: str, limit: Optional[int] = None, offset: int = 0, 
                   search: Optional[str] = None, search_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Get all data from a table with optional limit, offset, and search functionality"""
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    try:
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        # Get all columns for validation
        all_columns = get_table_columns(scraper_name, table_name)
        
        # Build query
        params = []
        
        # Add search conditions if provided
        if search:
            if search_columns:
                # Validate that requested columns exist
                invalid_columns = [col for col in search_columns if col not in all_columns]
                if invalid_columns:
                    raise HTTPException(status_code=400, detail=f"Invalid columns: {', '.join(invalid_columns)}")
                columns_to_search = search_columns
                # If user specified columns, they're all high priority
                user_specified = True
            else:
                # Search in all columns
                columns_to_search = all_columns
                user_specified = False
            
            # Build relevance score expression based on column priority
            relevance_cases = []
            num_columns = len(columns_to_search)
            for index, col in enumerate(columns_to_search):
                if user_specified:
                    # Earlier columns in the list get higher weight
                    # First column: 10 * num_columns, second: 10 * (num_columns - 1), etc.
                    base_weight = 10 * (num_columns - index)
                else:
                    # All columns get equal priority when searching all
                    base_weight = 1
                
                # Give extra weight if search term is at the start of the field (2x bonus)
                # Regular match gets base_weight, start match gets base_weight * 2
                relevance_cases.append(
                    f"CASE WHEN {col} LIKE ? THEN {base_weight * 2} "
                    f"WHEN {col} LIKE ? THEN {base_weight} ELSE 0 END"
                )
            
            relevance_score = " + ".join(relevance_cases)
            
            # SELECT with relevance score
            query = f"SELECT *, ({relevance_score}) as relevance_score FROM {table_name}"
            
            # Build WHERE clause with OR conditions for each column
            search_conditions = [f"{col} LIKE ?" for col in columns_to_search]
            query += " WHERE " + " OR ".join(search_conditions)
            search_term = f"%{search}%"
            search_term_start = f"{search}%"
            # Parameters for relevance score calculation (2 params per column: start match, contains match)
            for _ in columns_to_search:
                params.append(search_term_start)  # For "starts with" check
                params.append(search_term)        # For "contains" check
            # Parameters for WHERE clause
            params.extend([search_term] * len(columns_to_search))
            
            # Order by relevance first, then by created_at if available
            if "created_at" in all_columns:
                query += " ORDER BY relevance_score DESC, created_at DESC"
            else:
                query += " ORDER BY relevance_score DESC"
        else:
            # No search, just select all
            query = f"SELECT * FROM {table_name}"
            
            # Add ORDER BY if created_at column exists
            if "created_at" in all_columns:
                query += " ORDER BY created_at DESC"
        
        # Add pagination
        if limit == -1:
            # Don't add LIMIT clause
            pass
        elif limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        
        # Remove relevance_score from results if it was added
        if search and 'relevance_score' in columns:
            columns.remove('relevance_score')
            return [dict(zip(columns, row[:-1])) for row in rows]
        
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
    page_size: int = Query(20, ge=-1, description="Number of items per page. Use -1 to get all items."),
    search: Optional[str] = Query(None, description="Search term to filter results"),
    search_columns: Optional[str] = Query(None, description="Comma-separated column names to search in (searches all columns if not provided)")
):
    """Get paginated items from any table in a specific scraper. Use page_size=-1 to get all items. Optionally filter with search."""
    # Parse search_columns if provided
    search_columns_list = [col.strip() for col in search_columns.split(",")] if search_columns else None
    
    # If page_size is -1, get all results
    if page_size == -1:
        data = get_table_data(scraper_name, table_name, limit=-1, offset=0, search=search, search_columns=search_columns_list)
    else:
        # Validate page_size is within limits when not -1
        if page_size > 100:
            raise HTTPException(status_code=400, detail="page_size cannot exceed 100 (use -1 for all items)")
        offset = (page - 1) * page_size
        data = get_table_data(scraper_name, table_name, limit=page_size, offset=offset, search=search, search_columns=search_columns_list)
    
    # Get total count for pagination info (with search filter if applicable)
    conn = get_db_connection(scraper_name)
    cursor = conn.cursor()
    try:
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        count_params = []
        
        # Apply same search filter to count query
        if search:
            all_columns = get_table_columns(scraper_name, table_name)
            columns_to_search = search_columns_list if search_columns_list else all_columns
            search_conditions = [f"{col} LIKE ?" for col in columns_to_search]
            count_query += " WHERE " + " OR ".join(search_conditions)
            search_term = f"%{search}%"
            count_params.extend([search_term] * len(columns_to_search))
        
        cursor.execute(count_query, count_params)
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
        "search": search,
        "search_columns": search_columns_list,
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
