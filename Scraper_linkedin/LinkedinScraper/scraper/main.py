from linkedin_api import Linkedin

from linkedin_custom_search import LinkedinCustomSearch
from processor import Processor
from scraper import Scraper, ScraperConfig

api = Linkedin('projectfreedom95@gmail.com', 'Danis$1324')

processor = Processor()
searcher = LinkedinCustomSearch(api)
scraper = Scraper(searcher, processor, ScraperConfig())

res = scraper.fetch_job_descriptions_by_scraping_plan()
print("z")

"""
 - addattare la job search al pagination plan
 - usare pagination plan per parallelizzare le richieste
 - fare le richiesta in base alla location
 - criterio di parallelizzazione customizzabile: parallelizza per paginazione, parallelizza per nazione
 - limite 
 - evade
 - parametrizzare per backfilling, creare cli
 - come funziona listed at per backfilling
 - scraped date
"""
