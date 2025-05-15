from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from vacancyscraper.spiders import douspider, robotaspider, workspider  # Replace with your spider names

# Initialize the Scrapy process
configure_logging()
process = CrawlerProcess(get_project_settings())

# Add all spiders to the process
process.crawl(workspider)
process.crawl(robotaspider)
process.crawl(douspider)

# Start the crawling process
process.start()