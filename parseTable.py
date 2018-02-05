import pandas
import urllib.request
import re
from selenium import webdriver

class TableParser:
    def __init__(self, schedule_link, path_to_blacklist="data/exceptions.csv", divider="  "):
        self.schedule_link = schedule_link
        self.path_to_blacklist = path_to_blacklist
        self.divider = divider
    def readTables(self, schedule_link, js=False):
        if(js):
            browser = webdriver.Firefox()
            browser.get(self.schedule_link)
            html = browser.page_source
            browser.quit()
            dataframes = pandas.read_html(urllib.request.urlopen(schedule_link).read())
        else:
            dataframes = pandas.read_html(urllib.request.urlopen(schedule_link).read())
        return dataframes
    def get_cells_list(self, dataframe):
        dataframe.columns = range(dataframe.shape[1])
        dataframe.fillna(False, inplace=True)
        cells = list()
        for index, row in dataframe.iterrows():
            for cell in row:
                cells.append(cell)
        else:
            return cells
    def clean_cell_list(self, cells_list, path_to_blacklist="data/exceptions.csv"):
        c = list()
        with open("data/exceptions.csv") as exceptions_file:
            words = exceptions_file.read().split(',')
            for cell in cells_list:
                if (cell == False) or (re.match('\d{2}:\d{2}\w{2}', cell))  or (re.match('\d{2}:\d{2}', cell)) or ("Keynote" in cell):
                    continue
                for word in words:
                    if(cell.lower() == word.lower()):
                        break
                else:
                    c.append(cell)
            return c
    def remember_value(self, value, path='data/exceptions.csv'):
        try:
            f = open(path, "r+")
        except (IOError, OSError) as e:
            print(inspect.currentframe().f_code.co_name+"////"+e)
            return False
        if(value is not None) and (f is not None):
            value = value.strip()
            for l in f:
                ar = l.split(",")
                for w in ar:
                    if(value.lower() == w.lower()):
                        return None
            f.write(value.lower()+",")
        else:
            raise Exception('Value or File is None')
            return False
    def parse_cell(self, cell_string, divider="  "):
        talk = cell_string.split(divider)
        if (len(talk) < 2):
            self.remember_value(talk[0], self.path_to_blacklist)
            return False
        else:
            return talk
    def parse(self):
        talks = dict()
        for dataframe in self.readTables(self.schedule_link):
            for cell in self.clean_cell_list(self.get_cells_list(dataframe)):
                parsed_cell = self.parse_cell(cell)
                if(parsed_cell == False):
                    continue
                else:
                    talks[parsed_cell[1]] = parsed_cell[0]
        else:
            return talks
