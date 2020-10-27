# -*- coding: utf-8 -*-
"""
@author: kali
"""

import requests
from bs4 import BeautifulSoup
import os
import zipfile   
import csv 
from io import TextIOWrapper, BufferedReader, StringIO
import numpy as np
from datetime import datetime


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

                # nazev tohoto zipu i s cestou
                filename = os.path.join(self.target, link.split('/')[-1])

                # konkretni stahovany zip
                with open(filename, 'wb') as file:
                    for chunk in r.iter_content(chunk_size=8192):
                        file.write(chunk)

                # rozbal zip do jedne slozky
                with zipfile.ZipFile(filename, 'r') as zf:

                    # pokud jeste neexistuje slozka s tim nazvem
                    if not os.path.exists(filename.split('.')[0]):
                        os.makedirs(filename.split('.')[0])

                    # rozbal vsechny soubory
                    zf.extractall(filename.split('.')[0])

            
    # ziskej data jednoho z regionu
    def parse_region_data(self, region):

        # zjisti kolik existuje slozek
        dirs = [f for f in os.listdir(self.target) if os.path.isdir(os.path.join(self.target, f))]

        # pokud chybi nektery z adresaru, spravne by tam nemel byt ani jeden (pokud jsem ho teda sam nesmazal). stahni je radsi znovu vsechny at je jistota ze je to ok
        if len(dirs) != 5:
            self.download_data()
            
            # a aktualizuj seznam
            dirs = [f for f in os.listdir(self.target) if os.path.isdir(os.path.join(self.target, f))]

        # prazdne docasne ndarray do ktereho budes appendit
        ndwhole = np.empty([0, 64])

        # vysledne pole sloupcu
        ndresult = []

        # pro konkretni region vytahni vsechna data ze vsech slozek
        for d in dirs:

            try:

                with open(os.path.join(self.target, d, self.kraje[region]), 'r') as f:

                    # vyrob csv reader
                    reader = csv.reader(f, delimiter=';')
                    # preved data na numpy pole
                    array = np.asarray([data for data in reader], dtype=str)

                    # strc data do celkoveho pole
                    ndwhole = np.concatenate([ndwhole, array])

            # kdyby nahodou ten soubor mezitim nekdo smazal
            except (OSError, KeyError) as e:
                print(e)

        # KROK 2 - zpracuj celkove pole a rozdel ho na jednotlive sloupce

        # vyrad prazdna mista a XXka
        ndwhole[ndwhole == ''] = -1
        ndwhole[ndwhole == 'XX'] = -1

        # nastrc tam kraje
        ndresult.append(np.full((ndwhole.shape[0]), region))

        # nastrkej je do hlavniho pole, sloupce 0-63 z csv
        for i in range(64):
            # uint8
            if i in {1, 4, 6, 7, 8, 9, 10, 11, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                     32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 43, 44, 63}:
                ndresult.append(ndwhole[:, i].astype('uint8'))
            # stringy, co nadelas
            elif i in {0, 3, 45, 46, 49, 50, 51, 52, 54, 55, 57, 58, 59, 56, 62}:
                ndresult.append(ndwhole[:, i].astype('str'))
            # delsi inty, ale ne moc dlouhe
            elif i in {12, 16, 41}:
                ndresult.append(ndwhole[:, i].astype('uint16'))
            # dlouhe inty do 4294967295
            elif i in {2, 60, 61}:
                ndresult.append(ndwhole[:, i].astype('uint32'))
            # souradnice s jistotou
            elif i in {47, 48}:
                ndresult.append(ndwhole[:, i].astype('str'))
            # over cas jestli je validni
            elif i == 5:
                pass
    
        # vrat tuple se zpracovanym jednim krajem
        return (['region', 'p1', 'p36', 'p37', 'p2a', 'weekday(p2a)', 'p2b', 'p6', 'p7', 'p8', 'p9',
                 'p10', 'p11', 'p12', 'p13a', 'p13b', 'p13c', 'p14', 'p15', 'p16', 'p17', 'p18', 'p19',
                 'p20', 'p21', 'p22', 'p23', 'p24', 'p27', 'p28', 'p34', 'p35', 'p39', 'p44', 'p45a', 'pr7',
                 'p48a', 'p49', 'p50a', 'p50b', 'p51', 'p52', 'p53', 'p55a', 'p57', 'p58', 'a', 'b', 'c',
                 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q', 'r', 's', 't', 'p5a'], ndresult)
    
    def get_list(self, regions=None):

        print('start')
        print(datetime.now())

        if not regions:
            # vsechny kraje
            regions = [*self.kraje]
            
        try:    
    
            for region in regions:
                
                # pokud je region platny
                if region in self.kraje:
                    self.parse_region_data(region)
                    
                # region neexistuje
                else:
                    print('Region neexistuje.')
                    
        except TypeError:
            print('Spatne zadany argument [seznam regionu].')

        print(datetime.now())
        print('END')
    
if __name__ == "__main__":
    
    dd = DataDownloader()
    dd.get_list()
