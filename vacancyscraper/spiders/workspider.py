import scrapy
from vacancyscraper.items import WorkItem
from datetime import datetime, date, timedelta


class WorkSpider(scrapy.Spider):
    name = "workspider"
    allowed_domains = ["www.work.ua"]
    start_urls = ["https://www.work.ua/en/jobs-industry-it/"]
    base_link = "https://www.work.ua"

    def parse(self, response):
        job_cards = response.css("div#pjax-jobs-list div.card.card-hover.card-visited.wordwrap.job-link")

        for job_card in job_cards:
            publication_date = job_card.css("div.card.card-hover.card-visited.wordwrap.job-link div.mb-lg h2 a::attr(title)").get()

            publication_date = self._get_publication_date(publication_date)
            if publication_date == date.today().strftime("%Y-%m-%d"):
                self.publication_date = publication_date
            else:
                continue
            relative_url = f'{self.base_link}{job_card.css("div.card.card-hover.card-visited.wordwrap.job-link div.mb-lg h2 a::attr(href)").get()}'
            yield response.follow(relative_url, callback=self.parse_job_page)
        
        next_page = response.css("div#pjax-job-list nav li.add-left-default a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
    

    def _get_publication_date(self, date_str: str) -> str:
        if date_str:
            publication_date = date_str.split(" ")[-3:]
            publication_date = datetime.strptime(" ".join(publication_date), "%B %d, %Y")
            
            return publication_date.strftime("%Y-%m-%d")
        else:
            return None
    

    def parse_job_page(self, response):

        job_item = WorkItem()

        info = response.css("div.wordwrap")
        job_item["title"] = info.css("h1#h1-name::text").get()
        details = info.css("ul.list-unstyled.sm\\:mt-2xl.mt-lg.mb-0 li")
        
        for detail in details:
            field = detail.css("span.glyphicon::attr(title)").get()
            if field == "Salary":
                job_item["salary"] = detail.css('span.strong-500::text').get()
            
            elif field == "Work address":
                text = detail.css('li.text-indent::text').getall()
                job_item["location"] = ' '.join([t.strip() for t in text if t.strip()])

            elif field == "Company Information":
                job_item["company"] = detail.css("li a span::text").get()
            
            elif field == "Conditions and requirements":
                text = detail.css('li.text-indent::text').getall()
                job_item["conditions"] = ' '.join([t.strip() for t in text if t.strip()])

            elif field == "Language proficiencies":
                text = detail.css('li.text-indent::text').getall()
                job_item["languages"] = ' '.join([t.strip() for t in text if t.strip()])
        
        skills = info.css("div.mt-2xl.flex.flex-wrap ul li")
        job_item["skills"] = [skill.css("li span::text").get().lower() for skill in skills]
        job_item["skills"] = job_item["skills"] if len(job_item["skills"]) else None

        job_description = response.css("div#job-description::text, div#job-description *::text").getall()

        job_item["description"] = " ".join([text.strip() for text in job_description if text.strip()])

        job_item["link"] = response.url

        job_item["publication_date"] = self.publication_date

        yield job_item
