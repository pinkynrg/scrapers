from typing import List, cast, Optional
from crawl4ai import AsyncWebCrawler, BrowserConfig, Crawl4aiDockerClient, CrawlResult, CrawlerRunConfig


class CrawlerWrapper:
    """Unified interface for local and remote crawling"""
    
    def __init__(self, browser_config: BrowserConfig, local: bool = True, base_url: str = "https://crawl.francescomeli.com"):
        self.local = local
        self.base_url = base_url
        self.browser_config = browser_config
        self.crawler: Optional[AsyncWebCrawler] = None
        self.client: Optional[Crawl4aiDockerClient] = None
        
        if local:
            self.crawler = AsyncWebCrawler(config=browser_config)
        else:
            self.client = Crawl4aiDockerClient(base_url=base_url)
    
    async def crawl(self, url: str, crawler_config: CrawlerRunConfig) -> List[CrawlResult]:
        if self.local:
            assert self.crawler is not None
            result = await self.crawler.arun(
                url=url,
                config=crawler_config,
            )
            return cast(List[CrawlResult], result if isinstance(result, list) else [result])  
        else:
            assert self.client is not None
            result = await self.client.crawl(
                urls=[url],
                browser_config=self.browser_config,
                crawler_config=crawler_config,
            )
            return cast(List[CrawlResult], result if isinstance(result, list) else [result])
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
