# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from datetime import date
from html import unescape


class WorkScraperPipeline:
    def process_item(self, item, spider):
        if spider.name == "workspider":
            adapter = ItemAdapter(item)

            ## Salary --> convert to int or dict
            salary = adapter.get("salary")
            if salary:
                symbols_to_replace = ["\u202f", "\u2009"]
                for symbol in symbols_to_replace:
                    salary = salary.replace(symbol, "")
                
                salary = salary.split(" ")
                currency = salary[1]

                salary = salary[0].split("–")
                
                if len(salary) > 1:
                    salary_dict = {
                        "from": int(salary[0]),
                        "to": int(salary[1]),
                        "currency": currency
                    }
                else:
                    salary_dict = {
                        "amount": int(salary[0]),
                        "currency": currency
                    }
                adapter["salary"] = salary_dict
            else:
                adapter["salary"] = None
            
            ## Location --> convert to dict
            location = adapter.get("location")
            if location:
                location = location.split(", ")
                adapter["location"] = location[0]
            else:
                adapter["location"] = None

            ## Conditions --> extract data from conditions
            conditions = adapter.get("conditions")
            if conditions:
                conditions = conditions.lower().split(".")
            experience = education = employment_type = None
            if conditions:
                for condition in conditions:
                    condition = condition.strip()
                    if "work experience" in condition:
                        experience = int(condition.split("work experience more than ")[1].split(" ")[0].strip())
                    elif "education" in condition:
                        education = condition.split("education")[0].strip()
                    elif "time" in condition:
                        if "," in condition:
                            employment_type = [c.strip().lower() for c in condition.split(",") if c.strip()]
                        else:
                            employment_type = condition.lower()
                del adapter["conditions"]
            adapter['experience'] = experience
            adapter['education'] = education
            adapter['employment_type'] = employment_type

            ## Languages --> convert to dict
            languages = adapter.get("languages")
            if languages:
                languages = languages.lower().split(", ")
                languages_dict = {
                    lang.split(" — ")[0]: lang.split(" — ")[1] for lang in languages if lang.strip()
                }
                adapter["languages"] = languages_dict
            else:
                adapter["languages"] = None

            return item
        else:
            return item
