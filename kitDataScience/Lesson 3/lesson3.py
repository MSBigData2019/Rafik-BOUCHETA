#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 17:53:07 2018

@author: rafman
"""

import requests
from termcolor import colored
from bs4 import BeautifulSoup
import pandas as pd
from IPython.display import display_html
from collections import OrderedDict

website_ = "https://gist.github.com/paulmillr/2657075"
token = " 551c2c53292bb7d3d6194fb7e88aa7014a157bcd"
maxPage = 10
headers = {'Authorization' : 'token '+ token}
dictUserStars = {}

def _handle_request_result_and_build_soup(page_url):
  request_result = requests.get(page_url)
  if request_result.status_code == 200:
    html_doc =  request_result.text
    soup = BeautifulSoup(html_doc,"html.parser")
    return soup

def get_top_users(soup):

    dftable = pd.read_html(website_)[0]
    psudos = dftable["User"].str.split(expand=True)

    return psudos[psudos.columns[0]]

    
def get_mean_repos_user(psudo):
    res = requests.get("https://api.github.com/users/" + psudo + "/repos" , headers=headers )
    if (res.ok):
        dfRepos = pd.read_json(res.content)
        if ("stargazers_count" in dfRepos.columns):
            return dfRepos["stargazers_count"].mean()
   
    return 0

def main():
    print (colored("*******  CRAWLING of "+website_+" *********", "red"))
    psudos = get_top_users(_handle_request_result_and_build_soup(website_))
    
    for psudo in psudos:
        print(colored("******  get repos : " + psudo + " ********", "green"))
        dictUserStars[psudo] = get_mean_repos_user(psudo)
        
    dictUserStarsSorted = OrderedDict(sorted(dictUserStars.items(), key=lambda x: x[1] ))
    for user, stars in dictUserStarsSorted.items():
        
        print (user + "==> " + str(stars))
    

if __name__ == '__main__':
    main()