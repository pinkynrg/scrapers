import json
import asyncio
import re
from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai import JsonCssExtractionStrategy
from helpers.crawler_wrapper import CrawlerWrapper

local = True
initial_url = "https://www.linkedin.com/jobs/search/?keywords=python%20developer&location=Italy"
session_id = "linkedin-jobs-session"
final_data = []

async def extract_linkedin_jobs():
    schema = {
        "name": "linkedin jobs",
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
      
      // Find the next sibling job card
      if (currentJob) {
        const nextJob = currentJob.parentElement.parentElement.nextElementSibling;

        // if next job in the list exists, click it
        if (nextJob) {
          nextJob.firstElementChild.firstElementChild.click()
          list.scrollTop = nextJob.offsetTop;
        } else {
          // otherwise, change page
          const el = document.querySelector('.jobs-search-pagination__button--next');
          el.click();
        }
      }
    """

    with open("/Users/francescomeli/Projects/scrapers/state/linkedin-pinkynrg.json", "r") as f:
        storage_state_dict = json.load(f)
    
    browser_config = BrowserConfig(
        headless=not local,
        viewport_width=1920,
        storage_state=storage_state_dict,
    )

    # Initialize the crawler wrapper once
    crawler_wrapper = CrawlerWrapper(
        browser_config=browser_config,
        local=local,
    )
    
    for index in range(3):
        
        crawler_config = CrawlerRunConfig(
            js_only=True if index > 0 else False,
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS,
            js_code=js_click_next_job if index > 0 else "",
            session_id=session_id,
            delay_before_return_html=4,
        )

        results = await crawler_wrapper.crawl(initial_url, crawler_config)
        

        for result in results:
            if not result.success:
                print(f"✗ Failed to crawl: {result.error_message}")
                return
            
            if result.extracted_content:
                data = json.loads(result.extracted_content)
                final_data.extend(data)
                print(data)
            else:
                print("No content extracted")
    
    print(final_data)
      
asyncio.run(extract_linkedin_jobs())