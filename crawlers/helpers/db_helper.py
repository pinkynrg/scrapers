import sqlite3
from pathlib import Path
from typing import List, Dict, Any


class DatabaseHelper:
    """Helper class for saving scraped data to SQLite database"""
    
    def __init__(self, db_path: str, schema: Dict[str, Any]):
        self.db_path = db_path
        self.table_name = schema.get("name", "scraped_data").replace(" ", "_").lower()
        self.fields = schema.get("fields", []) + schema.get("baseFields", [])
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def create_table_from_schema(self):
        """Create a table based on the extraction schema"""
        table_name = self.table_name
        
        # Build column definitions from schema fields
        columns = []
        for field in self.fields:
            field_name = field["name"]
            # Use id field as primary key if it exists
            if field_name == "id":
                columns.append(f"{field_name} TEXT PRIMARY KEY")
            else:
                columns.append(f"{field_name} TEXT")
        
        # Add metadata columns
        columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        columns_str = ", ".join(columns)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        )
        """
        
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        return table_name
    
    def save_data(self, data: List[Dict[str, Any]]):
        """Save scraped data to the database"""
        if not data:
            print("No data to save")
            return 0
        
        field_names = [field["name"] for field in self.fields]
        
        # Prepare INSERT OR REPLACE statement
        placeholders = ", ".join(["?" for _ in field_names])
        columns_str = ", ".join(field_names)
        
        insert_sql = f"""
        INSERT OR REPLACE INTO {self.table_name} ({columns_str}, updated_at)
        VALUES ({placeholders}, CURRENT_TIMESTAMP)
        """
        
        inserted_count = 0
        for item in data:
            # Extract values in the correct order
            values = [item.get(field_name) for field_name in field_names]
            
            # Skip if id is None or empty
            if "id" in field_names and not values[field_names.index("id")]:
                continue
            
            try:
                self.cursor.execute(insert_sql, values)
                inserted_count += 1
            except sqlite3.Error as e:
                print(f"Error inserting data: {e}")
                print(f"Data: {item}")
        
        self.conn.commit()
        return inserted_count
    
    def get_all_data(self) -> List[Dict[str, Any]]:
        """Retrieve all data from the table"""
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        columns = [description[0] for description in self.cursor.description]
        rows = self.cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    
    def close(self):
        """Close the database connection"""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
