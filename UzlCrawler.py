#!/usr/bin/env python


from bs4 import BeautifulSoup, SoupStrainer
from urllib import error
import urllib.request as urllib2
import urllib.parse
import queue
import pickle


class BdWikiCrawler:

    OUTPUT_DIR = 'output/'

    def __init__(self):
        import os
        if not os.path.exists(self.OUTPUT_DIR):
            os.makedirs(self.OUTPUT_DIR)

    def fetch_url_data(self, url):
        try:
            data = urllib2.urlopen(url).read()

            return (True,  data)
        except error.HTTPError as e:
            print(e)
            return (False, e)

    def extrate_hyperlinks(self, response_data):

        hyperlink_dic = {}
        soup = BeautifulSoup(response_data, 'html.parser')
        links = soup.find_all('a')

        for link in links:
            url = str(link.get('href'))

            if url.startswith('/wiki/'):
                decoded_url = (urllib.parse.unquote(url[5:]))
                if ':' not in decoded_url:
                    final_url = 'https://bn.wikipedia.org/wiki' + urllib.parse.quote(decoded_url)
                    hyperlink_dic[final_url] = (0, decoded_url[1:])

        return hyperlink_dic

    def parse_data(self, response_data):
        soup = BeautifulSoup(response_data, 'html.parser')
        page_title =  soup.find('title').string


# reflist columns
        rigt_tab = soup.find_all("div", {"class": "thumb tright"})
        for tab in rigt_tab:
            tab.decompose()

        all_reference = soup.find_all("div", {"class": "reflist columns"})
        for ref in all_reference:
            ref.decompose()

        rigt_table = soup.find_all("table", {"class": "infobox"})
        for table in rigt_table:
            table.decompose()

        all_tagle_link = soup.find_all("span", {"class": "toctoggle"})
        for link in all_tagle_link:
            link.decompose()

        all_tagle_link = soup.find_all("span", {"class": "mw-editsection"})
        for link in all_tagle_link:
            link.decompose()

        all_external_link = soup.find_all("a", {"class": "external text"})
        for link in all_external_link:
            link.decompose()

        all_external_link = soup.find_all("sup", {"class": "reference"})
        for link in all_external_link:
            link.decompose()

        result = soup.find("div", {"id": "mw-content-text"})

        return page_title, result.text


    def save_data_to_disk(self, file_name, data_str):
        import time
        ts = time.time()
        import datetime
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H %M %S')
        full_file_name = self.OUTPUT_DIR + file_name + '___'+ st + '.txt'

        with open(full_file_name, "wb") as text_file:
            text_file.write(data_str.encode('utf-8'))

    def crawle(self, url):
        try:
            status, response_data = self.fetch_url_data(url)
            if status:
                links = [ x for x in self.extrate_hyperlinks(response_data)]
                title, content = self.parse_data(response_data)

                print(str(float("%0.2f" % (len(response_data) / 1024))) + ' KB Response Received from ' + url +
                      '\nPage Title: ' + title +'\n')

                self.save_data_to_disk( title, content)
                return links
            else:
                print('Something Wrong. Target URL was '+ url)
                return []
        except:
            print('Failed to Connect with '+ url)
            return []



########################################################################
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

class UzlUtill:
    PROCESSED_SET_FILE_NAME = 'PROCESSED_URL_MEMORY.pickel'

    def __init__(self):
        pass

    def get_previous_processed_url(self):
        import os
        if os.path.exists(self.PROCESSED_SET_FILE_NAME):
            f = open(self.PROCESSED_SET_FILE_NAME, 'rb')
            return pickle.load(f)
        else:
            return set()

    def save_current_state(self, url_set):
        with open( self.PROCESSED_SET_FILE_NAME, 'wb') as handle:
            pickle.dump(url_set, handle, protocol=pickle.HIGHEST_PROTOCOL)
