#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 09:27:43 2018

@author: rafman
"""

import requests
from termcolor import colored
from bs4 import BeautifulSoup

website_prefix = "https://www.reuters.com/finance/stocks/financial-highlights/"
pages = ['LVMH.PA', 'AIR.PA', 'DANO.PA']

def _handle_request_result_and_build_soup(page_url):
  request_result = requests.get(page_url)
  if request_result.status_code == 200:
    html_doc =  request_result.text
    soup = BeautifulSoup(html_doc,"html.parser")
    return soup

def get_Shares_Owned(soup):
      result =  soup.find('td', text = "% Shares Owned:").parent.findAll('td')[1].text
      #table = soup("table", {'class' : 'dataTable' })
      print ("shres owned : "+result)
      
def get_dividend_yield(soup):
    result = soup.find('td', text = "Dividend Yield").parent.findAll('td')
    print("Dividend Yield of company : "+result[1].text)
    print("Dividend Yield of Industry : "+result[2].text)
    print("Dividend Yield of sector : "+result[3].text)
    
def get_price_change(soup):
    result = soup.find(class_ = 'nasdaqChangeHeader').parent.findAll('span')[1].text.strip()
    print("price now : "+result+" €")
    result = soup.find(class_ = 'valueContentPercent').text.strip()[1:-1]
    print("and his change : "+result)
      
def get_sales_quarter(soup):
    res = soup.find('div', class_ = 'column1').findAll('div', class_ = 'module')[1].findAll('tr')[2]
    tds = res.findAll('td')
    print(tds[0].text + ":")
    print("Estimation : "+tds[1].text)
    print("Mean : "+tds[2].text)
    print ("hight : "+tds[3].text)
    print("Low : "+tds[4].text)
    
def main():
    for page in pages :
        print (colored("******* "+page+" *********", "red"))
        soup = _handle_request_result_and_build_soup(website_prefix+page)
        print (colored("#### sale per quarter ####", "green"))
        get_sales_quarter(soup)
        print (colored("\n#### price and change ####", "green"))
        get_price_change(soup)
        print (colored("\n#### dividend yield ####", "green"))
        get_dividend_yield(soup)
        print (colored("\n#### shares owned  ####", "green"))
        get_Shares_Owned(soup)
    
    

if __name__ == '__main__':
    main()

