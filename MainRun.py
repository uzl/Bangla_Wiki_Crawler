from UzlCrawler import BdWikiCrawler, IterableQueue, UzlUtill
import queue
from concurrent.futures import ThreadPoolExecutor as Executor


ROOT_URL = 'https://bn.wikipedia.org/wiki/%E0%A6%95%E0%A7%8D%E0%A6%B0%E0%A7%80%E0%A6%A1%E0%A6%BC%E0%A6%BE'

QUEUE = queue.Queue()
QUEUE.put(ROOT_URL)
utill = UzlUtill()

# hold all the urls that had been processed. This helps not use
PROCESSED_URL_SET = utill.get_previous_processed_url()


def fetch(url):
    print('Fetching ' + url)
    bc = BdWikiCrawler()
    return bc.crawle(url)


MAX_NUM = 100
cntr = 0

with Executor(max_workers=50) as exe:

    while cntr<MAX_NUM:
        url_list = []
        for url in IterableQueue(QUEUE):
            url_list.append(url)
            PROCESSED_URL_SET.add(url) # actually process starts from exe.submit() in below

        # clear the queue
        with QUEUE.mutex:
            QUEUE.queue.clear()

        jobs = [exe.submit(fetch, u) for u in url_list]
        links = [job.result() for job in jobs]

        all_url_flat_list = [item for sublist in links for item in sublist]

        for url in all_url_flat_list:
            if url not in PROCESSED_URL_SET:
                QUEUE.put(url)
                cntr += 1
                print('')
            else:
                print('\nDuplicate URL. Skipping '+url+'\n')

        utill.save_current_state(PROCESSED_URL_SET)

