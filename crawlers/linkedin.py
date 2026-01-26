import json
import asyncio
import os
import time
import random
from dotenv import load_dotenv
from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai import JsonCssExtractionStrategy
from helpers.crawler_wrapper import CrawlerWrapper
from helpers.db_helper import DatabaseHelper

# Load environment variables
load_dotenv()

local = os.getenv("LOCAL", "")
db_path = os.getenv("DB_PATH", "")
state_directory = os.getenv("STATE_DIRECTORY", "")
initial_url = os.getenv("LINKEDIN_URL", "")
scrape_interval = os.getenv("SCRAPE_INTERVAL", "3600")

if not local or not initial_url or not db_path or not state_directory:
    raise ValueError("Please set required environment variables.")

async def extract_linkedin_jobs():
    schema = {
        "name": "linkedin_jobs",
        "baseSelector": "body",
        "fields": [
            {
                "name": "id",
                "selector": ".jobs-search-results-list__list-item--active",
                "type": "attribute",
                "attribute": "data-job-id"
            },
            {
                "name": "title",
                "selector": ".job-details-jobs-unified-top-card__job-title",
                "type": "text"
            },
            {
                "name": "company",
                "selector": ".job-details-jobs-unified-top-card__company-name",
                "type": "text"
            },
            {
                "name": "body",
                "selector": ".jobs-description-content__text--stretch",
                "type": "text"
            },
        ]
    }
    
    extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
    
    js_click_next_job = """
      // Find the currently active job card
      const currentJob = document.querySelector('.jobs-search-results-list__list-item--active');
      const list = document.querySelector('.scaffold-layout__list header + div');
      
      // Find the next sibling job card
      if (currentJob) {
        console.log("currentJob found");
        const nextJob = currentJob.parentElement.parentElement.nextElementSibling;

        // if next job in the list exists, click it
        if (nextJob) {
          console.log("nextJob found");
          nextJob.firstElementChild.firstElementChild.click()
          list.scrollTop = nextJob.offsetTop;
        } else {
          console.log("nextJob not found, changing page");
          const el = document.querySelector('.jobs-search-pagination__button--next');
          el.click();
        }
      } else {
        console.log("No currentJob found");
      }
    """

    state_file = os.path.join(state_directory, "linkedin-pinkynrg.json")
    
    with open(state_file, "r") as f:
        storage_state_dict = json.load(f)
    
    browser_config = BrowserConfig(
        headless=(local != "true"),
        viewport_width=1920,
        viewport_height=1080,
        storage_state=storage_state_dict,
    )

    # Initialize the crawler wrapper once
    crawler_wrapper = CrawlerWrapper(
        browser_config=browser_config,
        local=local == "true",
    )

    for index in range(10):
        
        crawler_config = CrawlerRunConfig(
            js_only=True if index > 0 else False,
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS,
            js_code=js_click_next_job if index > 0 else "",
            session_id="linkedin-jobs-session",
            wait_for="js:() => !document.querySelector('.artdeco-loader__bars')",
            delay_before_return_html=random.uniform(1, 3),
        )

        results = await crawler_wrapper.crawl(initial_url, crawler_config)

        for result in results:
            if not result.success:
                print(f"✗ Failed to crawl: {result.error_message}")
                return
            
            if result.extracted_content:
                data = json.loads(result.extracted_content)
                with DatabaseHelper(db_path, "linkedin", schema) as db:
                    db.create_table_from_schema()
                    inserted_count = db.save_data(data)
                    all_data = db.get_all_data()
                    print(f"Saved {inserted_count} items to database at {db.db_path}")
                    print(f"Total items in database: {len(all_data)}")

while True:
    print("Starting LinkedIn scraper...")
    asyncio.run(extract_linkedin_jobs())
    print(f"LinkedIn scraper completed. Sleeping for {scrape_interval} seconds...")
    time.sleep(int(scrape_interval))