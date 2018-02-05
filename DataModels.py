import Crawler
from urllib.parse import urlparse
import re

class Conference(object):
    def __init__(self, conference_name="", location="", date="", partOfSeries=""):
        self.name = conference_name
        self.location = location
        self.date = date
        self.part_of_series = partOfSeries
    def __parse_netloc(self, parsed_url):
        spl = parsed_url.netloc.split(".")
        spl.remove(spl[len(spl)-1])
        return " ".join(spl)
    def __str__(self):
        return self.name
    def __get_date(self, link):
        parsed_url = urlparse(Crawler.getRelocatedLink(link))
        if(re.search("\d{4}", parsed_url.netloc)) or re.search("\d{4}", parsed_url.path):
            return re.search("\d{4}").group(0) 
        else:
            return Crawler.search_website_for_date(link)
    def extract_conference_informations(self, link):
        parsed_url = urlparse(Crawler.getRelocatedLink(link))
        regex_result = re.search("\d{4}", parsed_url.path)
        self.name = self.__parse_netloc(parsed_url)
        self.location = Crawler.getLocation(link, Crawler.get_location_dict())
        self.date = self.__get_date(link)
class Talk(object):
    def __init__(self, talk_title="", talk_language=""):
        self.title = talk_title
        self.language = talk_language
    def extract_information_from_title(self, title):
        talk = Talk()
        talk.title = title
        talk.language = Crawler.check_language_of_string(title, Crawler.get_translator())
        return talk
class Topic(object):
    def __init__(self, name, tags):
        self.name = name
        self.tags = tags
class Conference_Series(object):
    def __init__(self, name, frequency):
        self.name = name
        self.frequency = frequency

