import json
import asyncio
from crawl4ai import BFSDeepCrawlStrategy, BrowserConfig, CrawlerRunConfig
from crawl4ai import JsonCssExtractionStrategy
from crawl4ai.deep_crawling.filters import FilterChain, DomainFilter, URLPatternFilter
from helpers.crawler_wrapper import CrawlerWrapper

local = True
initial_url = "https://blog.francescomeli.com"
session_id = "blog-posts-session"
final_data = []

async def extract_blog_posts():
    schema = {
        "name": "blog posts",
        "baseSelector": ".post",
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
        headless=not local,
        viewport_width=1920
    )
    
    crawler_config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        deep_crawl_strategy=deep_crawl_strategy
    )

    crawler_wrapper = CrawlerWrapper(
        browser_config=browser_config,
        local=local,
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

asyncio.run(extract_blog_posts())