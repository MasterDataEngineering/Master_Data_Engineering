from abc import abstractmethod

from linkedin_custom_search import LinkedinSearchInterface
from processor import Processor


class ScraperConfigInterface:

    @property
    @abstractmethod
    def locations_to_scrape(self) -> list[str]:
        pass

    @property
    @abstractmethod
    def job_titles_to_scrape(self) -> list[str]:
        pass

    @property
    def scraping_plan(self):
        return [
            {
                "location_name": location_name,
                "job_title": job_title
            }
            for location_name in self.locations_to_scrape for job_title in self.job_titles_to_scrape
        ]


class ScraperConfig(ScraperConfigInterface):

    @property
    def locations_to_scrape(self) -> list[str]:
        return [
            "United Kingdom",
            "Netherlands",
            "Italy",
            "Germany",
            "France",
            "Spain",
            "Belgium",
            "Portugal",
            "Switzerland"
        ]

    @property
    def job_titles_to_scrape(self) -> list[str]:
        return [
            "Data Engineer",
            "Data Scientist",
            "Frontend Engineer",
            "Backend Engineer",
            "Frontend Developer",
            "Backend Developer",
            "Full-stack engineer",
            "Full-stack developer",
            "Machine Learning Engineer",
            "ML Engineer",
            "Android Developer",
            "Spark Engineer",
            "Apache Spark Engineer",
            "Analytics Engineer",
            "Research Engineer",
            "Snowflake Engineer",
            "Databricks Engineer"
        ]




class Scraper:

    def __init__(self, linkedin_interface: LinkedinSearchInterface, processor: Processor, config: ScraperConfigInterface):
        self.linkedin_interface = linkedin_interface
        self.processor = processor
        self.scraper_config = config

    def calculate_next_start(self, current_pagination: dict) -> int:
        return current_pagination["start"] + self.calculate_correct_pagination_offset(current_pagination, current_pagination["start"])

    def calculate_correct_pagination_offset(self, current_pagination: dict, next_start: int) -> int:
        total = current_pagination["total"]
        count = current_pagination["count"]

        if next_start + count > total:
            return self.calculate_final_offset(total, next_start)

        return count

    def calculate_final_offset(self, total: int, next_start: int) -> int:
        return total - next_start

    def generate_pagination_plan(self, initial_pagination: dict) -> list[dict]:
        pagination_plan = [initial_pagination]

        loops = int(initial_pagination["total"] / initial_pagination["count"])

        for i in range(loops):
            current_pagination = pagination_plan[i]
            next_start = self.calculate_next_start(current_pagination)
            pagination_plan.append({
                "start": next_start,
                "count": self.calculate_correct_pagination_offset(current_pagination, next_start),
                "total": initial_pagination["total"]
            })

        return pagination_plan

    def _fetch_with_logging(self, search_keyword:str, pagination_start: int, pagination_count: int, location_name: str):
        print("Fetching", pagination_start, "to", pagination_start+pagination_count)
        return self.linkedin_interface.job_search(search_keyword, pagination_start, pagination_count, location_name)

    def fetch_all_jobs_for_keyword(self, search_keyword: str, location_name: str):
        print("Searching:", search_keyword, "in", location_name)

        next_start = 0
        default_offset = 49
        first_response = self._fetch_with_logging(search_keyword, next_start, default_offset, location_name)
        initial_pagination = self.processor.extract_paging_info(first_response)

        if initial_pagination["total"] >= default_offset:
            pagination_plan = self.generate_pagination_plan(initial_pagination)
            print("Pagination Plan:", pagination_plan)

            already_fetched = pagination_plan.pop(0)

            return [
                first_response,
                *[
                    self._fetch_with_logging(search_keyword, pagination["start"], pagination["count"], location_name)
                    for pagination in pagination_plan
                ]
            ]

        return [first_response]

    def fetch_and_format_jobs(self, search_keyword: str, location_name: str) -> list[dict]:
        jobs = self.fetch_all_jobs_for_keyword(search_keyword, location_name)
        return self.processor.process_jobs_for_ingestion(jobs)

    def fetch_jobs_meta_by_scraping_plan(self) -> list:
        jobs = [
            self.fetch_all_jobs_for_keyword(search_keyword=conf["job_title"], location_name=conf["location_name"])
            for conf in self.scraper_config.scraping_plan
        ]
        flattened = self.processor.flatten(jobs)
        return self.processor.process_jobs_for_ingestion(flattened)

    def _fetch_job_descr_with_logging(self, job_id: int):
        print(f"Fetching Job Description for job posting {job_id}")
        return self.linkedin_interface.fetch_job_description(job_id)

    def _fetch_job_descriptions(self, jobs_meta: list):
        return [
            self._fetch_job_descr_with_logging(meta["job_id"])
            for meta in jobs_meta
        ]

    def fetch_job_descriptions_by_scraping_plan(self):
        jobs_meta = self.fetch_jobs_meta_by_scraping_plan()
        return self._fetch_job_descriptions(jobs_meta)


