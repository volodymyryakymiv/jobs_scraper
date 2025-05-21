import scrapy
import json
from datetime import date, timedelta
from vacancyscraper.items import RobotaItem
from w3lib.html import remove_tags

class RobotaSpider(scrapy.Spider):
    name = "robotaspider"  # Unique name for the spider
    allowed_domains = ["www.api.robota.ua", "api.robota.ua"]
    start_urls = ['https://api.robota.ua/vacancy/search?rubricIds=1&sortBy=Date']  # List of URLs to start scraping
    page_num = 0
    city_dict = {}

    def start_requests(self):
        # Fetch the cities dictionary first
        city_url = 'https://api.robota.ua/dictionary/city'

        yield scrapy.Request(city_url, callback=self.parse_city_dictionary)

    def parse_city_dictionary(self, response):
        # Parse and store the cities dictionary
        data = json.loads(response.text)

        self.city_dict = {line['id']: line['en'] for line in data}

        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):

        if response.status == 404:
            self.logger.error(f"Page not found: {response.url}")
            return

        # Extract data or follow links
        data = json.loads(response.text)
        if not len(data.get('documents')):
            return
        
        for item in data.get('documents', []):
            print(f"Parsing page {self.page_num} from {response.url}")
            # Check if the publication date is today
            item_date = item.get('date')[:10]
            if item_date == date.today().strftime("%Y-%m-%d"):
                job_url = f"https://api.robota.ua/vacancy?id={item.get('id')}"
                yield scrapy.Request(job_url, callback=self.get_job_details)

        self.page_num += 1
        yield scrapy.Request(f'{self.start_urls[0]}&page={self.page_num}', callback=self.parse)
    
    def get_job_details(self, response):
        # Extract job details from the job page
        data = json.loads(response.text)

        item = RobotaItem()



        
        # item['id'] = data.get('id')
        item['title'] = data.get('name')
        item['company'] = data.get('companyName')

        item['link'] = f"https://robota.ua/company1511203/vacancy{data.get('id')}"

        if data.get('salaryFrom'):
            item['salary'] = {
                "from": int(data.get('salaryFrom')),
                "to": int(data.get('salaryTo')),
                "currency": "UAH"
            }
        elif data.get('salary'):
            item['salary'] = {
                "amount": int(data.get('salary')),
                "currency": "UAH"
            }
        else:
            item['salary'] = None
        
        item['languages'] = data.get('languages')
        item['city'] = self.city_dict.get(data.get('cityId'), 'Unknown')
        item['publication_date'] = data.get('date')[:10]
        item['description'] = data.get('description')

        yield item

