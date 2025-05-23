# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WorkItem(scrapy.Item):
    title = scrapy.Field()
    company = scrapy.Field()
    salary = scrapy.Field()
    location = scrapy.Field()
    languages = scrapy.Field()
    conditions = scrapy.Field()
    experience = scrapy.Field()
    education = scrapy.Field()
    employment_type = scrapy.Field()
    skills = scrapy.Field()
    link = scrapy.Field()
    publication_date = scrapy.Field()
    description = scrapy.Field()
# title, company, salary, location, languages, experience, education, employment_type, skills, link, publication_date

class RobotaItem(scrapy.Item):
    # id = scrapy.Field()
    link = scrapy.Field()
    title = scrapy.Field()
    company = scrapy.Field()
    salary = scrapy.Field()
    languages = scrapy.Field()
    city = scrapy.Field()
    publication_date = scrapy.Field()
    description = scrapy.Field()


class DouItem(scrapy.Item):
    title = scrapy.Field()
    company_name = scrapy.Field()
    location = scrapy.Field()
    salary = scrapy.Field()
    link = scrapy.Field()
    publication_date = scrapy.Field()
    description = scrapy.Field()

class DjinniItem(scrapy.Item):
    title = scrapy.Field()
    company_name = scrapy.Field()
    location = scrapy.Field()
    languages = scrapy.Field()
    employment_type = scrapy.Field()
    salary = scrapy.Field()
    experience = scrapy.Field()
    link = scrapy.Field()
    publication_date = scrapy.Field()
    description = scrapy.Field()

