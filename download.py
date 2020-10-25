# -*- coding: utf-8 -*-
"""
@author: kali
"""

import requests
from bs4 import BeautifulSoup
import os
import zipfile   
import csv 
from io import TextIOWrapper, BufferedReader
import numpy as np


class DataDownloader:
    
    
    def __init__(self, url='https://ehw.fit.vutbr.cz/izv/', folder='data', cache_filename='data_{}.pkl.gz'):
        
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
  
        # vyres uvodni adresar
        self.target = os.path.join(os.getcwd(), self.folder)
        if not os.path.exists(self.target):
           os.makedirs(self.target)

        # fake hlavicka
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        
        # seznam kraju
        self.kraje = {'PHA': '00.csv',
                 'STC': '01.csv',
                 'JHC': '02.csv',
                 'PLK': '03.csv',
                 'ULK': '04.csv',
                 'HKK': '05.csv',
                 'JHM': '06.csv',
                 'MSK': '07.csv',
                 'OLK': '14.csv',
                 'ZLK': '15.csv',
                 'VYS': '16.csv',
                 'PAK': '17.csv',
                 'LBK': '18.csv',
                 'KVK': '19.csv'}           
    
    def download_data(self):
        
        home = requests.get(self.url, headers=self.headers)
        
        # vezmi linky
        soup = BeautifulSoup(home.text, 'html.parser')
        
        # vezmi vsechny tr krome posledniho
        trs_links = []
        
        trs = soup.findAll('tr')
        trs_links.append(trs[11].findAll('a')[-1].get('href'))
        trs_links.append(trs[23].findAll('a')[-1].get('href'))
        trs_links.append(trs[35].findAll('a')[-1].get('href'))
        trs_links.append(trs[47].findAll('a')[-1].get('href'))
        
        # najdi posledni tr s odkazem
        i = -1
        while True:
            
            a = trs[i].findAll('a')
            if a:
                trs_links.append(a[-1].get('href'))
                break
            i -= 1
        
        # postupne stahni kazdy soubor
        for link in trs_links:
            with requests.get(self.url + link, stream=True) as r:
                r.raise_for_status()
                with open(os.path.join(self.target, link.split('/')[-1]), 'wb') as file:
                    for chunk in r.iter_content(chunk_size=8192):
                        file.write(chunk)
            
            
    # ziskej data jednoho z regionu
    def parse_region_data(self, region):
        
        # zjisti kolik existuje zipu stazenych
        zips = [f for f in os.listdir(self.target) if f.endswith('.zip')]
        
        # pokud chybi nektery ze zipu, spravne by tam nemel byt ani jeden (pokud jsem ho teda sam nesmazal). stahni je radsi znovu vsechny at je jistota ze je to ok
        if len(zips) != 5:        
            self.download_data()
            
            # a aktualizuj seznam
            zips = [f for f in os.listdir(self.target) if f.endswith('.zip')]
    
        # seznam vsech prazdnych ndarrays
        ndlist = []
        for i in range(64):
            ndlist.append(np.array([]))
            
        # pro konkretni region vytahni vsechna data ze vsech souboru
        for z in zips:
            
            try:
            
                with zipfile.ZipFile(os.path.join(self.target, z)) as zf:
                
                    # nacti konkretni soubor kraje
                    with zf.open(self.kraje[region], 'r') as f:

                        # vyres pocet radku
                        #lc = sum(1 for i in f)
                        
                        # prejdi zpatky na zacatek
                        #f.seek(0)
                        
                        # vyrob csv reader
                        reader = csv.reader(TextIOWrapper(f), delimiter=';')
                        
                        # preved data na numpy pole
                        array = np.asarray([data for data in reader], dtype=str)
                        
                        # vyrad prazdna mista a XXka
                        array[array==''] = -1
                        array[array=='XX'] = -1
                        
                        # tady nekde musis zmenit typ tech polozek aby se tam daly nacpat
                        
                        # nastrkej je to hlavniho pole
                        for i in range(64):
                            # pro id musis nechat str
                            if i == 1:
                                ndlist[i] = np.concatenate([ndlist[i], array[:, i].astype('str')])   
                                
                            else:
                                ndlist[i] = np.concatenate([ndlist[i], array[:, i].astype('str')])                            
                            
                            
            # kdyby nahodou ten soubor mezitim nekdo smazal
            except (OSError, KeyError) as e:
                print(e)

        # vytvor header pro cache
        header = ['region', 'p1', 'p36', 'p37', 'p2a', 'weekday(p2a)', 'p2b', 'p6', 'p7', 'p8', 'p9', 'p10', 'p11', 'p12', 'p13a', 'p13b', 'p13c', 'p14', 'p15', 'p16', 'p17', 'p18', 'p19', 'p20', 'p21', 'p22', 'p23', 'p24', 'p27', 'p28', 'p34', 'p35', 'p39', 'p44', 'p45a', 'pr7', 'p48a', 'p49', 'p50a', 'p50b', 'p51', 'p52', 'p53', 'p55a', 'p57', 'p58', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q', 'r', 's', 't', 'p5a']
    
        # vrat tuple se zpracovanym jednim krajem
        return (header, ndlist)
    
    def get_list(self, regions=None):
        
        if not regions:
            # vsechny kraje
            regions = self.kraje
            
        try:    
    
            for region in regions:
                
                # pokud je regio platny
                if region in self.kraje:
                    self.parse_region_data(region)
                    
                # region neexistuje
                else:
                    print('Region neexistuje.')
                    
        except TypeError:
            print('Spatne zadnay argument [seznam regionu].')
    
    
if __name__ == "__main__":
    
    dd = DataDownloader()
    
    dd.parse_region_data('PHA')
    
    # pokud se spousti samostatne tak dmeo funkcionalita
    #dd.get_list()
