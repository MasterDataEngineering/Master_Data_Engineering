import copy


class Processor:

    def extract_raw_jobs_metadata_list(self, jobs_meta: dict) -> list[dict]:
        return [
            meta
            for meta in jobs_meta["included"]
            if meta["$type"] == 'com.linkedin.voyager.jobs.JobPosting'
        ]

    def simplify_job_meta_object(self, job_meta_object: dict) -> dict:
        cp = copy.deepcopy(job_meta_object)

        if "logo" in cp:
            del cp["logo"]

        return cp

    def add_job_id(self, job_meta_object: dict) -> dict:
        entity_urn = job_meta_object["entityUrn"]
        job_id = int(entity_urn.split(":")[-1])
        cp = copy.deepcopy(job_meta_object)
        cp["job_id"] = job_id
        return cp

    def extract_paging_info(self, jobs_meta: dict) -> dict:
        paging = jobs_meta["data"]["paging"]
        del paging["links"]
        return paging

    def flatten(self, l):
        return [item for sublist in l for item in sublist]

    def process_jobs_for_ingestion(self, all_jobs_meta: list[dict]) -> list[dict]:
        job_postings = [
            self.extract_raw_jobs_metadata_list(raw_job_meta)
            for raw_job_meta in all_jobs_meta
        ]

        job_postings_flattened = self.flatten(job_postings)

        simplified_job_postings = [
            self.simplify_job_meta_object(job_posting)
            for job_posting in job_postings_flattened
        ]

        with_id = [
            self.add_job_id(job_posting)
            for job_posting in simplified_job_postings
        ]

        return with_id