#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 15:42:57 2018

@author: rafman
"""

import requests
from termcolor import colored
from bs4 import BeautifulSoup
import pandas as pd
import re

website_= "open-medicaments.fr"


def get_product():
    finalDF = pd.DataFrame()
    for i in range(1,5):
        website_ = "https://www.open-medicaments.fr/api/v1/medicaments?query=paracetamol&page="+str(i)+"&limit=100"
        r = requests.get(website_, headers={'Accept': 'application/json'})
        dftable = pd.read_json(r.text)

        finalDF = pd.concat([finalDF, dftable], ignore_index=True)
    
    return (finalDF[finalDF.columns[1]])

def main():
    print (colored("*******  CRAWLING of "+website_+" *********", "red"))
    ciss = get_product()
    
    dfParacetamol = pd.DataFrame(columns=["Pharma", "QTE", "MS", "Type"])
    for c in ciss.values:
       # print(c)
        m = re.match(r"(PARACETAMOL) (\w+ ?\w*) (\d+) (.+), (\w+)", c)
        if (m):

            df = pd.DataFrame([[m.group(2), m.group(3), m.group(4), m.group(5)]],  columns=["Pharma", "QTE", "MS", "Type"])
                          
            dfParacetamol =  pd.concat([dfParacetamol, df], ignore_index=True)
            
    
    print(dfParacetamol)


if __name__ == '__main__':
    main()