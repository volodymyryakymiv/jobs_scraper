import scrapy
from vacancyscraper.items import DjinniItem
import json

class DjinniSpider(scrapy.Spider):
    name = "djinnispider"
    allowed_domains = ["djinni.co"]
    start_urls = ['https://djinni.co/api/jobs/']
    used_ids = set()

    def start_requests(self):
        url = 'https://djinni.co/jobs/rss'
        used_ids = self.used_ids
        try:
            with open("djinni_ids.txt", "r") as file:
                for line in file:
                    used_ids.add(line.strip())
        except FileNotFoundError:
            pass
        self.used_ids = used_ids

        yield scrapy.Request(url, callback=self.parse_job_ids)
    
    def parse_job_ids(self, response):
        items = response.css("item")
        used_ids = self.used_ids
        
        with open("djinni_ids.txt", "a") as file:
            for item in items:
                id = item.css("link::text").get().split("/")[-2].split("-")[0]
                pub_date = item.css("pubDate::text").get()
                unique_id = id + " " + pub_date
                if unique_id in used_ids:
                    continue
                yield scrapy.Request(f"{self.start_urls[0]}{id}/", callback=self.parse)
                used_ids.add(id + " " + pub_date)
                file.write(unique_id + "\n")
        self.used_ids = used_ids
        

    def parse(self, response):
        data = json.loads(response.text)
        item = DjinniItem()
        item["title"] = data.get("title")
        item["company_name"] = data.get("company_name")
        item["link"] = "https://djinni.co/jobs/" + str(data.get("id"))
        item["location"] = data.get("location").split(", ") if data.get("location") else None
        item["experience"] = data.get("experience")
        item["category"] = data.get("category").get("id")
        item["languages"] = {"English": self.transform_languages(data.get("english").get("id"))}
        item["employment_type"] = "part-time" if data.get("is_parttime") else "full-time"
        item["salary"] = {"from": data.get("public_salary_min"), "to": data.get("public_salary_max"), "currency": "USD"} if data.get("public_salary_min") or data.get("public_salary_max") else None
        item["publication_date"] = data.get("published")[:10]
        item["description"] = data.get("long_description")

        yield item

    def transform_languages(self, language):
        if language:
            if language == "pre":
                return "pre-intermediate"
            elif language == "upper":
                return "upper-intermediate"
            else:
                return language
        return None