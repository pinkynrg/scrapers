import os
import json
import asyncio
from dotenv import load_dotenv
from crawl4ai import BFSDeepCrawlStrategy, BrowserConfig, CrawlerRunConfig
from crawl4ai import JsonCssExtractionStrategy
from crawl4ai.deep_crawling.filters import FilterChain, DomainFilter, URLPatternFilter
from helpers.db_helper import DatabaseHelper
from helpers.crawler_wrapper import CrawlerWrapper

# Load environment variables
load_dotenv()

local = os.getenv("LOCAL", "")
db_path = os.getenv("DB_PATH", "")
initial_url = os.getenv("BLOG_URL", "")

print(f"LOCAL: {local}, DB_PATH: {db_path}, BLOG_URL: {initial_url}")

if not local or not db_path or not initial_url:
    raise ValueError("Please set required environment variables.")

async def extract_blog_posts():
    schema = {
        "name": "blog_posts",
        "baseSelector": ".post",
        "baseFields": [
            {
                "name": "id",
                "type": "attribute",
                "attribute": "id",
            }
        ],
        "fields": [
            {
                "name": "title",
                "selector": "h2",
                "type": "text"
            },
            {
                "name": "content",
                "selector": "div.entry",
                "type": "text"
            }
        ]
    }

    extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
    
    deep_crawl_strategy = BFSDeepCrawlStrategy(
        max_depth=50,
        max_pages=100,
        include_external=False,
        filter_chain=FilterChain([
            DomainFilter(allowed_domains=["blog.francescomeli.com"]),
            URLPatternFilter(
                patterns=[
                    r"^https://blog\.francescomeli\.com/page/\d+/?$",
                    r"^https://blog\.francescomeli\.com/?$"
                ],
                use_glob=False
            )
        ])
    )
    
    browser_config = BrowserConfig(
        headless=(local != "true"),
        viewport_width=1920
    )
    
    crawler_config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        deep_crawl_strategy=deep_crawl_strategy
    )

    crawler_wrapper = CrawlerWrapper(
        browser_config=browser_config,
        local=local == "true",
    )
    
    results = await crawler_wrapper.crawl(initial_url, crawler_config)

    for result in results: 
        if not result.success:
            print(f"✗ Failed to crawl: {result.error_message}")
            return
        
        if result.extracted_content:
            with DatabaseHelper(db_path, schema) as db:
                data = json.loads(result.extracted_content)
                db.create_table_from_schema()
                inserted_count = db.save_data(data)
                print(f"Saved {inserted_count} items to database at {db_path}")
                
                # Print sample of saved data
                all_data = db.get_all_data()
                print(f"Total items in database: {len(all_data)}")
        else:
            print("No content extracted")

asyncio.run(extract_blog_posts())