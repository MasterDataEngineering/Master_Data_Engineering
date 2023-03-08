from pprint import pprint

from linkedin_custom_search import LinkedinSearchInterface
from processor import Processor
from scraper import Scraper, ScraperConfigInterface
from unittest.mock import MagicMock, call


class MockLinkedinSearch(LinkedinSearchInterface):

    def job_search(
            self,
            keywords: str,
            pagination_start: int,
            records_to_fetch: int,
            location_name: str,
            additional_params: dict = None
    ) -> dict:
        return {
            "data": {
                "paging": {
                    "count": records_to_fetch,
                    "start": pagination_start,
                    "total": 305,
                    "links": []
                }
            }
        }

    def fetch_job_description(self, job_id: int) -> dict:
        pass


class MockLinkedinSearchWithMagicMock(LinkedinSearchInterface):

    mock = MagicMock()

    def job_search(
            self,
            keywords: str,
            pagination_start: int,
            records_to_fetch: int,
            location_name: str,
            additional_params: dict = None
    ) -> dict:
        self.mock(keywords, pagination_start, records_to_fetch, location_name, additional_params)
        return {
            "included": [
                {
                    "$type": "com.linkedin.voyager.jobs.JobPosting",
                    "entityUrn": "urn:li:fs_normalized_jobPosting:3345043639"
                }
            ],
            "data": {
                "paging": {
                    "count": records_to_fetch,
                    "start": pagination_start,
                    "total": 52,
                    "links": []
                }
            }
        }

    def fetch_job_description(self, job_id: int) -> dict:
        pass


class MockScaperConfig(ScraperConfigInterface):

    @property
    def locations_to_scrape(self) -> list[str]:
        return [
            "United Kingdom",
            "Netherlands",
        ]

    @property
    def job_titles_to_scrape(self) -> list[str]:
        return [
            "Data Engineer",
            "Data Scientist",
        ]


class TestScraper:
    processor = Processor()
    scraper = Scraper(MockLinkedinSearch(), processor, MockScaperConfig())

    def get_expected_paging_info(self):
        return {
            "count": 49,
            "start": 0,
            "total": 305
        }

    def test_generate_pagination_plan(self):
        paging_info = self.scraper.generate_pagination_plan({
            "count": 49,
            "start": 0,
            "total": 305,
        })

        assert paging_info == [
            {'count': 49, 'start': 0, 'total': 305},
            {'count': 49, 'start': 49, 'total': 305},
            {'count': 49, 'start': 98, 'total': 305},
            {'count': 49, 'start': 147, 'total': 305},
            {'count': 49, 'start': 196, 'total': 305},
            {'count': 49, 'start': 245, 'total': 305},
            {'count': 11, 'start': 294, 'total': 305}
        ]

    def test_fetch_all_jobs_for_keyword(self):
        assert self.scraper.fetch_all_jobs_for_keyword("just testing the pagination stuff", "dummy location") == [
            {'data': {'paging': {'count': 49, 'start': 0, 'total': 305}}},
            {'data': {'paging': {'count': 49, 'links': [], 'start': 49, 'total': 305}}},
            {'data': {'paging': {'count': 49, 'links': [], 'start': 98, 'total': 305}}},
            {'data': {'paging': {'count': 49, 'links': [], 'start': 147, 'total': 305}}},
            {'data': {'paging': {'count': 49, 'links': [], 'start': 196, 'total': 305}}},
            {'data': {'paging': {'count': 49, 'links': [], 'start': 245, 'total': 305}}},
            {'data': {'paging': {'count': 11, 'links': [], 'start': 294, 'total': 305}}}
        ]

    def test_scrape_by_pagination_plan(self):
        processor = Processor()
        scraper = Scraper(MockLinkedinSearchWithMagicMock(), processor, MockScaperConfig())
        scraper.fetch_jobs_meta_by_scraping_plan()

        assert scraper.linkedin_interface.mock.mock_calls == [
            call('Data Engineer', 0, 49, 'United Kingdom', None),
            call('Data Engineer', 49, 3, 'United Kingdom', None),
            call('Data Scientist', 0, 49, 'United Kingdom', None),
            call('Data Scientist', 49, 3, 'United Kingdom', None),
            call('Data Engineer', 0, 49, 'Netherlands', None),
            call('Data Engineer', 49, 3, 'Netherlands', None),
            call('Data Scientist', 0, 49, 'Netherlands', None),
            call('Data Scientist', 49, 3, 'Netherlands', None)
        ]
