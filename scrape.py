import requests

from bs4 import BeautifulSoup
import pandas as pd
import re

import json
import uuid
import pickle
import time
import random
from collections import OrderedDict

cached_filename = "./cache/cache"
cached_directory = "./cache"

def cached_request(root_url, headers, data):

    hassh = "".join([json.dumps(d, sort_keys=True) for d in [root_url, data]])
    

    for line in open(cached_filename):
        if hassh in line:
            h, filename = line.split("|")
            filename = filename.strip()
            print("Using cached request!")
            return pickle.load(open(filename, 'br'))

    response = requests.post(root_url, headers=headers, data=data)
    filename = cached_directory + "/" + str(uuid.uuid4())

    pickle.dump(response, open(filename,'bw'))
    fp = open(cached_filename, 'a')
    fp.write(f"{hassh}|{filename}\n")

    time.sleep(5 + random.random())
    
    return response
    

root_url = "https://www.brewersfriend.com/search/index.php"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'}
data = {'keyword': '',
        'method': '',
        'units': 'metric',
        'style': '',
        'oglow': '',
        'oghigh': '',
        'fglow': '',
        'fghigh': '',
        'ibulow': '',
        'ibuhigh': '',
        'abvlow': '',
        'abvhigh': '',
        'colorlow': '',
        'colorhigh': '',
        'batchsizelow': '',
        'batchsizehigh': '',
        'sort': '',
        'page': 2}



def store_data(page):
    data['page'] = page
    response = cached_request(root_url, headers, data)

    doc = BeautifulSoup(response.text, features="html.parser")
    
    columns = []
    for match in doc.find_all("th"):
        column_name = match.find("span").contents[0]
        column_name = re.sub("[\t\s]+", " ", column_name)
        columns.append(column_name)
    columns.append("expand-id")
    columns.append("href")


    rows = []
    expanded_jsons = []
    for match in doc.find_all("tr", {'class': ['odd', 'even']}):
        row = []
        for i, td in enumerate(match.find_all("td")):
            
            if columns[i] == "Title":
                content = td.find("a","recipetitle").contents[0]
            elif columns[i] == "Style":
                link = td.find("a")
                if link is not None:
                    content = td.find("a").contents[0]
                else:
                    content = td.contents[0]
            else:
                content = td.contents[0]

            content = re.sub("[\t\s]+", " ", content)

            content = content.strip()
            row.append(content)
        
        expand_id = match.find("span", "expand-recipe")['data-expand-id']
        href = match.find("a","recipetitle")['href']
        
        expand_id = re.sub("[\t\s]+", " ", expand_id)
        href = re.sub("[\t\s]+", " ", href)
    
        row.append(expand_id)
        row.append(href)
        rows.append(row)


        expanded_info = doc.find("tr", {'id': expand_id})
        if (expanded_info):
            expanded_json = {}
            table = expanded_info.find("table")
            if table:
                for info in list(table.find_all("tr"))[2:]:
                    for td in info.find_all("td"):
                        what = td.find("b").contents[0]
                        content = td.contents[1].strip()
                        
                        what = what.replace("\n", " ")
                        content = content.replace("\n", " ")

                        what = re.sub("[\t\s:]+", " ", what)
                        content = re.sub("[\t\s]+", " ", content)
                        
                        what = what.strip()
                        content = content.strip()
                        
                        expanded_json[what] = content
                        
            expanded_json["expand-id"] = expand_id
            expanded_jsons.append(expanded_json)

    #print(doc)
    #print(pd.DataFrame(rows, columns=columns))
    #print(pd.DataFrame(expanded_jsons))
    df = pd.DataFrame(rows, columns=columns).set_index("expand-id").join(pd.DataFrame(expanded_jsons).set_index("expand-id"), lsuffix="_").reset_index()

    df.to_feather(f"./data/per-page/page_{data['page']}.feather")
    df.to_csv(f"./data/per-page/page_{data['page']}.csv")


nums = list(range(1, 4746))
random.shuffle(nums)
nums = [62,50]
for page in nums:
    print(page)
    store_data(page)
