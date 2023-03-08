from pprint import pprint

from processor import Processor
import json

from scraper import Scraper


class TestProcessor:
    processor = Processor()
    metadata_json = None

    def get_test_metadata_json(self):
        if self.metadata_json is None:
            with open("jobs_metadata.json", 'r') as meta:
                self.metadata_json = json.load(meta)

        return self.metadata_json

    def get_test_metadata_job(self):
        return {
            "dashEntityUrn": "urn:li:fsd_jobPosting:3342557643",
            "companyDetails": {
                "company": "urn:li:fs_normalized_company:145751",
                "*companyResolutionResult": "urn:li:fs_normalized_company:145751",
                "$recipeTypes": ["com.linkedin.voyager.deco.jserp.WebJobPostingWithCompanyName"],
                "$type": "com.linkedin.voyager.jobs.JobPostingCompany"
            },
            "formattedLocation": "Veneto, Italy",
            "listedAt": 1667580654000,
            "title": "Cloud Engineer",
            "$recipeTypes": ["com.linkedin.voyager.deco.jserp.WebSearchJobJserpJobPostingLite"],
            "$type": "com.linkedin.voyager.jobs.JobPosting",
            "entityUrn": "urn:li:fs_normalized_jobPosting:3342557643",
            "workRemoteAllowed": False
        }

    def get_test_job_metadata_no_logo(self):
        return {
            '$type': 'com.linkedin.voyager.jobs.JobPosting',
            'dashEntityUrn': None,
            'entityUrn': 'urn:li:fs_miniJob:3306082304',
            'listDate': 1665100800000,
            'listedAt': 1665100800000,
            'location': 'Turin, Piedmont, Italy',
            'objectUrn': 'urn:li:job:3306082304',
            'title': 'DATA ENGINEER / focus T-SQL',
            'trackingId': 'l5PdEUBwQ0C3QnC7m+3XuQ=='
        }

    def test_extract_raw_jobs_metadata_list(self):
        jobs_metadata_list = self.processor.extract_raw_jobs_metadata_list(self.get_test_metadata_json())
        assert jobs_metadata_list[0] == self.get_test_metadata_job()

    def test_simplify_job_meta_object(self):
        single_job_meta = self.get_test_metadata_job()

        assert self.processor.simplify_job_meta_object(single_job_meta) == {
            'dashEntityUrn': 'urn:li:fsd_jobPosting:3342557643',
            'companyDetails': {'company': 'urn:li:fs_normalized_company:145751',
                               '*companyResolutionResult': 'urn:li:fs_normalized_company:145751',
                               '$recipeTypes': ['com.linkedin.voyager.deco.jserp.WebJobPostingWithCompanyName'],
                               '$type': 'com.linkedin.voyager.jobs.JobPostingCompany'},
            'formattedLocation': 'Veneto, Italy', 'listedAt': 1667580654000, 'title': 'Cloud Engineer',
            '$recipeTypes': ['com.linkedin.voyager.deco.jserp.WebSearchJobJserpJobPostingLite'],
            '$type': 'com.linkedin.voyager.jobs.JobPosting', 'entityUrn': 'urn:li:fs_normalized_jobPosting:3342557643',
            'workRemoteAllowed': False}

    def test_add_job_id(self):
        single_job_meta = self.get_test_job_metadata_no_logo()
        assert self.processor.add_job_id(single_job_meta) == {
            '$type': 'com.linkedin.voyager.jobs.JobPosting',
            'dashEntityUrn': None,
            'entityUrn': 'urn:li:fs_miniJob:3306082304',
            'job_id': 3306082304,
            'listDate': 1665100800000,
            'listedAt': 1665100800000,
            'location': 'Turin, Piedmont, Italy',
            'objectUrn': 'urn:li:job:3306082304',
            'title': 'DATA ENGINEER / focus T-SQL',
            'trackingId': 'l5PdEUBwQ0C3QnC7m+3XuQ=='
        }

    def get_test_list_of_search_result(self):
        return [
            {
                "included": [
                    {'objectUrn': 'urn:li:company:162479', 'entityUrn': 'urn:li:fs_miniCompany:162479', 'name': 'Apple',
                     'showcase': False, 'active': True, 'logo': {'artifacts': [{'width': 200,
                                                                                'fileIdentifyingUrlPathSegment': '200_200/0/1595530301220?e=1675900800&v=beta&t=tWuShH4224SCy7mWqn36U9qeFkhem057ybaJOyAjT2A',
                                                                                'expiresAt': 1675900800000,
                                                                                'height': 200,
                                                                                '$type': 'com.linkedin.common.VectorArtifact'},
                                                                               {'width': 100,
                                                                                'fileIdentifyingUrlPathSegment': '100_100/0/1595530301220?e=1675900800&v=beta&t=GHi-25naPC1yOxlqydDZHLvY2LCGsIOcL8sCKcNSTdw',
                                                                                'expiresAt': 1675900800000,
                                                                                'height': 100,
                                                                                '$type': 'com.linkedin.common.VectorArtifact'},
                                                                               {'width': 400,
                                                                                'fileIdentifyingUrlPathSegment': '400_400/0/1595530301220?e=1675900800&v=beta&t=C-GaXEPEURcOi-V2yYS8OfQKcvswxYYDo7bOoU_y3B8',
                                                                                'expiresAt': 1675900800000,
                                                                                'height': 400,
                                                                                '$type': 'com.linkedin.common.VectorArtifact'}],
                                                                 'rootUrl': 'https://media-exp1.licdn.com/dms/image/C560BAQHdAaarsO-eyA/company-logo_',
                                                                 '$type': 'com.linkedin.common.VectorImage'},
                     'universalName': 'apple', 'dashCompanyUrn': 'urn:li:fsd_company:162479',
                     'trackingId': 'KDmSQhQySSq5UXls9mLHrw==',
                     '$type': 'com.linkedin.voyager.entities.shared.MiniCompany'},
                    {'objectUrn': 'urn:li:company:10080368', 'entityUrn': 'urn:li:fs_miniCompany:10080368',
                     'name': 'Sympower', 'showcase': False, 'active': True, 'logo': {'artifacts': [{'width': 200,
                                                                                                    'fileIdentifyingUrlPathSegment': '200_200/0/1638531261417?e=1675900800&v=beta&t=R2gY61q5_CQJr4__5p_6yJG-OntISoozeA-OQGf8dF8',
                                                                                                    'expiresAt': 1675900800000,
                                                                                                    'height': 200,
                                                                                                    '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                                   {'width': 100,
                                                                                                    'fileIdentifyingUrlPathSegment': '100_100/0/1638531261417?e=1675900800&v=beta&t=iSt7zFiOwtIAvlD6sBZpvaTBtxKeWoZeQ_KDU4AggrU',
                                                                                                    'expiresAt': 1675900800000,
                                                                                                    'height': 100,
                                                                                                    '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                                   {'width': 400,
                                                                                                    'fileIdentifyingUrlPathSegment': '400_400/0/1638531261417?e=1675900800&v=beta&t=1p9aiV_c0kivKwcY9DywRF9i1-e-fjsddDNWuVB-D7o',
                                                                                                    'expiresAt': 1675900800000,
                                                                                                    'height': 400,
                                                                                                    '$type': 'com.linkedin.common.VectorArtifact'}],
                                                                                     'rootUrl': 'https://media-exp1.licdn.com/dms/image/C4D0BAQEq5bNXUJkIAA/company-logo_',
                                                                                     '$type': 'com.linkedin.common.VectorImage'},
                     'universalName': 'sympower', 'dashCompanyUrn': 'urn:li:fsd_company:10080368',
                     'trackingId': 'Uu5PCMAZQTm8UiU9qlzv8A==',
                     '$type': 'com.linkedin.voyager.entities.shared.MiniCompany'},
                    {'listDate': 1666480966000, 'dashEntityUrn': None, 'objectUrn': 'urn:li:job:3325943638',
                     'entityUrn': 'urn:li:fs_miniJob:3325943638', 'logo': {'artifacts': [{'width': 200,
                                                                                          'fileIdentifyingUrlPathSegment': '200_200/0/1657054972290?e=1675900800&v=beta&t=dbRt-nO-Y1Wsubz8sm21jakNtc2n5SH3ezVEr4RN5ms',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 200,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                         {'width': 100,
                                                                                          'fileIdentifyingUrlPathSegment': '100_100/0/1657054972290?e=1675900800&v=beta&t=YxqzC9Tewk2-AaMObD7lQLzqYyd7QHD9scAchkoKsO0',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 100,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                         {'width': 400,
                                                                                          'fileIdentifyingUrlPathSegment': '400_400/0/1657054972290?e=1675900800&v=beta&t=A9hzTm_jkQ_xfhP7PLAqpCTkJYYaBAjXE3YC9eyeXD4',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 400,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'}],
                                                                           'rootUrl': 'https://media-exp1.licdn.com/dms/image/C560BAQE9wp87-KDfwg/company-logo_',
                                                                           '$type': 'com.linkedin.common.VectorImage'},
                     'location': 'Segrate, Lombardy, Italy', 'title': 'Data Engineer - Business Intelligence - IBM CIC',
                     'listedAt': 1666480966000, 'trackingId': 'MkzCgvSFRqaOGJXLQP0oPg==',
                     '$type': 'com.linkedin.voyager.jobs.JobPosting'}]
            },
            {
                "included": [
                    {'objectUrn': 'urn:li:company:162479', 'entityUrn': 'urn:li:fs_miniCompany:162479', 'name': 'Apple',
                     'showcase': False, 'active': True, 'logo': {'artifacts': [{'width': 200,
                                                                                'fileIdentifyingUrlPathSegment': '200_200/0/1595530301220?e=1675900800&v=beta&t=tWuShH4224SCy7mWqn36U9qeFkhem057ybaJOyAjT2A',
                                                                                'expiresAt': 1675900800000,
                                                                                'height': 200,
                                                                                '$type': 'com.linkedin.common.VectorArtifact'},
                                                                               {'width': 100,
                                                                                'fileIdentifyingUrlPathSegment': '100_100/0/1595530301220?e=1675900800&v=beta&t=GHi-25naPC1yOxlqydDZHLvY2LCGsIOcL8sCKcNSTdw',
                                                                                'expiresAt': 1675900800000,
                                                                                'height': 100,
                                                                                '$type': 'com.linkedin.common.VectorArtifact'},
                                                                               {'width': 400,
                                                                                'fileIdentifyingUrlPathSegment': '400_400/0/1595530301220?e=1675900800&v=beta&t=C-GaXEPEURcOi-V2yYS8OfQKcvswxYYDo7bOoU_y3B8',
                                                                                'expiresAt': 1675900800000,
                                                                                'height': 400,
                                                                                '$type': 'com.linkedin.common.VectorArtifact'}],
                                                                 'rootUrl': 'https://media-exp1.licdn.com/dms/image/C560BAQHdAaarsO-eyA/company-logo_',
                                                                 '$type': 'com.linkedin.common.VectorImage'},
                     'universalName': 'apple', 'dashCompanyUrn': 'urn:li:fsd_company:162479',
                     'trackingId': 'tGZ0/mz6Tx6+P70epq3aDQ==',
                     '$type': 'com.linkedin.voyager.entities.shared.MiniCompany'},
                    {'listDate': 1667007962000, 'dashEntityUrn': None, 'objectUrn': 'urn:li:job:3332076982',
                     'entityUrn': 'urn:li:fs_miniJob:3332076982', 'logo': {'artifacts': [{'width': 200,
                                                                                          'fileIdentifyingUrlPathSegment': '200_200/0/1583426039703?e=1675900800&v=beta&t=ah6jYCxBUdjx5bmA7Psg_PoLG6yT1iBSu2ETPJhAzaI',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 200,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                         {'width': 100,
                                                                                          'fileIdentifyingUrlPathSegment': '100_100/0/1583426039703?e=1675900800&v=beta&t=YvnswZgq3gbVd-iztdc5ulIRdJ_qfjCxsCw4kEtf9qU',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 100,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                         {'width': 400,
                                                                                          'fileIdentifyingUrlPathSegment': '400_400/0/1583426039703?e=1675900800&v=beta&t=kwbJpePkF2iHP3dYiTxSy1YHBxz83eeg-Kjt-6QjY0A',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 400,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'}],
                                                                           'rootUrl': 'https://media-exp1.licdn.com/dms/image/C4D0BAQHYFP0dgznPyg/company-logo_',
                                                                           '$type': 'com.linkedin.common.VectorImage'},
                     'location': 'Milan, Lombardy, Italy', 'title': 'Data Engineer | Full Remote',
                     'listedAt': 1667007962000, 'trackingId': 'xCnJNUdXRAutQ+buY1720w==',
                     '$type': 'com.linkedin.voyager.jobs.JobPosting'},
                    {'listDate': 1666130524000, 'dashEntityUrn': None, 'objectUrn': 'urn:li:job:3321059630',
                     'entityUrn': 'urn:li:fs_miniJob:3321059630', 'logo': {'artifacts': [{'width': 200,
                                                                                          'fileIdentifyingUrlPathSegment': '200_200/0/1578411053895?e=1675900800&v=beta&t=RMHkrkfCYRTSizLg_3pcD-LX049SyuwYQrALm4YMSg4',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 200,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                         {'width': 100,
                                                                                          'fileIdentifyingUrlPathSegment': '100_100/0/1578411053895?e=1675900800&v=beta&t=JPmo8H6z6MH7-9G_wcGCoDv76Ebk9UJD17w4xY-Ltxw',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 100,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'},
                                                                                         {'width': 400,
                                                                                          'fileIdentifyingUrlPathSegment': '400_400/0/1578411053895?e=1675900800&v=beta&t=4pLCJtfAnTtT1dpJwEO3aNnzev73ObSpaupSBKXmBCw',
                                                                                          'expiresAt': 1675900800000,
                                                                                          'height': 400,
                                                                                          '$type': 'com.linkedin.common.VectorArtifact'}],
                                                                           'rootUrl': 'https://media-exp1.licdn.com/dms/image/C560BAQFO0zsi4Ey0DQ/company-logo_',
                                                                           '$type': 'com.linkedin.common.VectorImage'},
                     'location': 'Valenzano, Puglia, Italia', 'title': 'Data Engineer', 'listedAt': 1666130524000,
                     'trackingId': 'rhuyBWAhTb20MnSEmNcDiA==', '$type': 'com.linkedin.voyager.jobs.JobPosting'}]
            }
        ]

    def test_process_jobs_for_ingestion(self):
        all_stuff_from_paginated_search = self.get_test_list_of_search_result()

        assert self.processor.process_jobs_for_ingestion(all_stuff_from_paginated_search) == \
               [
                   {
                       'listDate': 1666480966000, 'dashEntityUrn': None, 'objectUrn': 'urn:li:job:3325943638',
                       'entityUrn': 'urn:li:fs_miniJob:3325943638', 'location': 'Segrate, Lombardy, Italy',
                       'title': 'Data Engineer - Business Intelligence - IBM CIC', 'listedAt': 1666480966000,
                       'trackingId': 'MkzCgvSFRqaOGJXLQP0oPg==',
                       '$type': 'com.linkedin.voyager.jobs.JobPosting',
                       'job_id': 3325943638
                   },
                   {
                       'listDate': 1667007962000, 'dashEntityUrn': None, 'objectUrn': 'urn:li:job:3332076982',
                       'entityUrn': 'urn:li:fs_miniJob:3332076982', 'location': 'Milan, Lombardy, Italy',
                       'title': 'Data Engineer | Full Remote', 'listedAt': 1667007962000,
                       'trackingId': 'xCnJNUdXRAutQ+buY1720w==',
                       '$type': 'com.linkedin.voyager.jobs.JobPosting', 'job_id': 3332076982
                   },
                   {
                       'listDate': 1666130524000, 'dashEntityUrn': None, 'objectUrn': 'urn:li:job:3321059630',
                       'entityUrn': 'urn:li:fs_miniJob:3321059630', 'location': 'Valenzano, Puglia, Italia',
                       'title': 'Data Engineer', 'listedAt': 1666130524000, 'trackingId': 'rhuyBWAhTb20MnSEmNcDiA==',
                       '$type': 'com.linkedin.voyager.jobs.JobPosting', 'job_id': 3321059630
                   }
               ]
