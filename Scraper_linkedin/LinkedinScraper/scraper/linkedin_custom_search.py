import random
from time import sleep
from urllib.parse import urlencode

from abc import ABC, abstractmethod


class LinkedinSearchInterface(ABC):

    @abstractmethod
    def job_search(
            self,
            keywords: str,
            pagination_start: int,
            records_to_fetch: int,
            location_name: str,
            additional_params: dict = None
    ) -> dict:
        pass

    @abstractmethod
    def fetch_job_description(self, job_id: int) -> dict:
        pass


class LinkedinCustomSearch(LinkedinSearchInterface):

    def __init__(self, api):
        self._api = api

    def job_search(
            self,
            keywords: str,
            pagination_start: int,
            records_to_fetch: int,
            location_name: str,
            additional_params: dict = None
    ) -> dict:
        return self._search_jobs(
            keywords=keywords,
            pagination_start=pagination_start,
            records_to_fetch=records_to_fetch,
            location_name=location_name
        )

    def _search_jobs(
            self,
            keywords=None,
            companies=None,
            experience=None,
            job_type=None,
            job_title=None,
            industries=None,
            location_name=None,
            remote=False,
            listed_at=1 * 60 * 60,
            distance=None,
            records_to_fetch=49,
            pagination_start=0
    ):
        params = {}
        if keywords:
            params["keywords"] = keywords

        filters = ["resultType->JOBS"]
        if companies:
            filters.append(f'company->{"|".join(companies)}')
        if experience:
            filters.append(f'experience->{"|".join(experience)}')
        if job_type:
            filters.append(f'jobType->{"|".join(job_type)}')
        if job_title:
            filters.append(f'title->{"|".join(job_title)}')
        if industries:
            filters.append(f'industry->{"|".join(industries)}')
        if location_name:
            filters.append(f"locationFallback->{location_name}")
        if remote:
            filters.append(f"workRemoteAllowed->{remote}")
        if distance:
            filters.append(f"distance->{distance}")
        filters.append(f"timePostedRange->r{listed_at}")

        default_params = {
            "decorationId": "com.linkedin.voyager.deco.jserp.WebJobSearchHitLite-14",
            "count": records_to_fetch,
            "filters": f"List({','.join(filters)})",
            "origin": "JOB_SEARCH_RESULTS_PAGE",
            "q": "jserpFilters",
            "start": pagination_start,
            "queryContext": "List(primaryHitType->JOBS,spellCorrectionEnabled->true)",
        }
        default_params.update(params)

        res = self._api._fetch(
            f"/search/hits?{urlencode(default_params, safe='(),')}",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
            evade=lambda: sleep(random.uniform(0.2, 0.8))
        )

        return res.json()

    def fetch_job_description(self, job_id: int) -> dict:
        res = self._api._fetch(
            f"/entities/jobs/{job_id}",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
            evade=lambda: sleep(random.uniform(0.2, 0.8))
        )
        return res.json()