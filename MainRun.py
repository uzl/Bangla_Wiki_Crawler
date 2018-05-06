from UzlCrawler import BdWikiCrawler

ROOT_URL = 'https://bn.wikipedia.org/wiki/%E0%A6%95%E0%A7%8D%E0%A6%B0%E0%A7%80%E0%A6%A1%E0%A6%BC%E0%A6%BE'

# bc = BdWikiCrawler()
# links = bc.crawle(ROOT_URL)
# print(links)

# for link in links:
#     bc.crawle(link)



import queue
import time
from concurrent.futures import ThreadPoolExecutor as Executor

QUEUE = queue.Queue()
QUEUE.put(ROOT_URL)
all_url_map = {}

#######################################
class IterableQueue():
    def __init__(self,source_queue):
            self.source_queue = source_queue
    def __iter__(self):
        while True:
            try:
               yield self.source_queue.get_nowait()
            except queue.Empty:
               return
###########################################################################


def fetch(url):
    print('Fetching...')
    bc = BdWikiCrawler()
    return bc.crawle(url)


MAX_NUM = 200
cntr = 0
with Executor(max_workers=50) as exe:

    while cntr<MAX_NUM:
        url_list = []
        cntr += 1
        print(cntr)
        for url in IterableQueue(QUEUE):
            url_list.append(url)
            all_url_map[url] = 1

        print('Trying to execute in parallel')
        # for url in url_list:
        #     job = exe.submit(fetch, url)
        #
        # links = job.result()
        # if links != False:

        jobs = [exe.submit(fetch, u) for u in url_list]
        links = [job.result() for job in jobs]
        if links != False:
            for link in links:
                if isinstance(link, list):
                    for l in link:
                        if l not in all_url_map:
                            QUEUE.put(l)
                        else:
                            print('Skipping ', l)
                else:
                    if link not in all_url_map:
                        QUEUE.put(link)
                    else:
                        print('Skipping ', link)


