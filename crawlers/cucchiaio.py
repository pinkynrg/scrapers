import os
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from helpers.db_helper import DatabaseHelper

# Load environment variables
load_dotenv()

db_path = os.getenv("DB_PATH", "")
scrape_interval = os.getenv("SCRAPE_INTERVAL", "3600")

if not db_path:
    raise ValueError("Please set DB_PATH environment variable.")

SITEMAP_URL = "https://www.cucchiaio.it/Sitemap-content-RICETTE.xml"

async def extract_recipes():
    """Extract recipe URLs from cucchiaio.it sitemap"""
    
    schema = {
        "name": "recipes",
        "primary_key": "url",  # url is the unique identifier
        "fields": [
            {
                "name": "url",
                "type": "text"
            },
            {
                "name": "name",
                "type": "text"
            },
            {
                "name": "source",
                "type": "text"
            }
        ],
        "baseFields": []
    }
    
    print(f"Fetching sitemap from {SITEMAP_URL}")
    
    async with aiohttp.ClientSession() as session:
        async with session.get(SITEMAP_URL) as response:
            if response.status != 200:
                print(f"✗ Failed to fetch sitemap: HTTP {response.status}")
                return
            
            xml_content = await response.text()
    
    print("Parsing sitemap XML...")
    
    # Parse XML
    root = ET.fromstring(xml_content)
    
    # Define namespace for sitemap
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    # Extract all URLs
    recipes = []
    skipped_count = 0
    
    for url_elem in root.findall('ns:url', namespace):
        loc = url_elem.find('ns:loc', namespace)
        if loc is not None and loc.text is not None:
            # Clean the URL by stripping whitespace
            url = loc.text.strip()
            
            # Only keep actual recipe URLs (with /ricetta/ not /ricette/)
            if '/ricetta/' not in url:
                skipped_count += 1
                continue
            
            # Extract recipe name from URL
            # Example: https://www.cucchiaio.it/ricetta/ricetta-cotto-crudo-tonno/
            # Should extract: ricetta cotto crudo tonno (with spaces)
            path_parts = url.rstrip('/').split('/')
            name = path_parts[-1] if path_parts else ""
            
            # Replace hyphens with spaces for cleaner names
            name = name.replace('-', ' ')
            
            # Skip if name is empty
            if not name:
                skipped_count += 1
                continue
            
            recipes.append({
                "url": url,
                "name": name,
                "source": "cucchiaio.it"
            })
    
    print(f"Found {len(recipes)} recipes (skipped {skipped_count} non-recipe URLs)")
    
    # Save to database
    with DatabaseHelper(db_path, "cucchiaio", schema) as db:
        db.create_table_from_schema()
        
        # Remove all existing cucchiaio recipes before inserting new ones
        deleted_count = db.delete_by_field("source", "cucchiaio.it")
        print(f"Deleted {deleted_count} existing recipes")
        
        inserted_count = db.save_data(recipes)
        print(f"Saved {inserted_count} recipes to database at {db.db_path}")
        
        # Print stats
        all_data = db.get_all_data()
        print(f"Total recipes in database: {len(all_data)}")

async def main():
    """Main function to run the scraper continuously"""
    print("Starting Cucchiaio recipe scraper...")
    
    while True:
        try:
            await extract_recipes()
            print(f"\nWaiting {scrape_interval} seconds before next scrape...")
            await asyncio.sleep(int(scrape_interval))
        except KeyboardInterrupt:
            print("\n\nStopping scraper...")
            break
        except Exception as e:
            print(f"Error during scraping: {e}")
            print(f"Retrying in {scrape_interval} seconds...")
            await asyncio.sleep(int(scrape_interval))

if __name__ == "__main__":
    asyncio.run(main())
