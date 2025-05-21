import scrapy
from vacancyscraper.items import DouItem
from html import unescape
from datetime import datetime



class DOUSpider(scrapy.Spider):
    name = "douspider"
    allowed_domains = ["www.dou.ua", "dou.ua"]
    start_urls = ['https://jobs.dou.ua/vacancies/feeds/']  # List of URLs to start scraping
    used_ids = set()


    def start_requests(self):   
        used_ids = self.used_ids
        try:
            with open("dou_ids.txt", "r") as file:
                for line in file:
                    used_ids.add(line.strip())
        except FileNotFoundError:
            pass
        self.used_ids = used_ids
        yield scrapy.Request(self.start_urls[0], callback=self.parse)


    def parse(self, response):
        items = response.css("item")
        dou = DouItem()
        used_ids = self.used_ids
        with open("dou_ids.txt", "a") as file:
            for item in items:
                id = item.css("link::text").get().split("/")[-2]
                pub_date = item.css("pubDate::text").get()
                unique_id = id + " " + pub_date
                if unique_id in used_ids:
                    continue
                used_ids.add(unique_id)
                file.write(unique_id + "\n")
                title_data = self.get_data_from_title(item.css("title::text").get())
                for key, value in title_data.items():
                    dou[key] = value
                
                dou["link"] = item.css("link::text").get()
                # dou["description"] = remove_tags(item.css("description::text").get())
                dou["description"] = item.css("description::text").get()

                dou["publication_date"] = self.get_pub_date(item.css("pubDate::text").get())
                yield dou
        self.used_ids = used_ids




    def get_pub_date(self, pub_date: str) -> str:
        date_str = pub_date[5:16]
        date_object = datetime.strptime(date_str, "%d %B %Y")

        return date_object.strftime("%Y-%m-%d")
    
    def get_data_from_title(self, title: str) -> dict:
        # Extract the job title and company name from the title string

        title = unescape(title)

        data_dict = {
            "title": None,
            "company_name": None,
            "location": [],
            "salary": None
        }
        title = title.split(" в ")
        data_dict["title"] = title[0]
        
        title_parts = title[1].split(",")
        
        for index, part in enumerate(title_parts):
            part = part.strip()
            if index == 0:
                data_dict["company_name"] = part.strip()
                continue
            elif index == 1 and "Inc" in part:
                data_dict["company_name"] += ", Inc"
                continue
            elif "віддалено" in part.lower():
                data_dict["location"].append("Remote")
                continue
            elif "за кордоном" in part.lower():
                data_dict["location"].append("Abroad")
                continue
            elif "$" in part:
                salary_dict = dict()
                if "до" in part.lower():
                    print(part)
                    salary_dict["from"] = None
                    salary_dict["to"] = int(part.split("до")[1][2:])
                elif "від" in part.lower():
                    salary_dict["from"] = int(part.split("від")[1][2:])
                    salary_dict["to"] = None
                elif "–" in part:
                    s = part.split("–")
                    salary_dict["from"] = int(s[0][1:].strip())
                    salary_dict["to"] = int(s[1].strip())
                else:
                    salary_dict["salary"] = part[1:].strip()
                salary_dict["currency"] = "USD"
                data_dict["salary"] = salary_dict
            else:
                data_dict["location"].append(part.strip())

        return data_dict
    

        