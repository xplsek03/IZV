# -*- coding: utf-8 -*-
"""
@author: kali
"""

import requests
from bs4 import BeautifulSoup
import os
import zipfile   
import csv 
from io import TextIOWrapper
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

        # predpripravena hlavicka dat
        self.header = ['region', 'p1', 'p36', 'p37', 'p2a', 'weekday(p2a)', 'p2b', 'p6', 'p7', 'p8', 'p9',
         'p10', 'p11', 'p12', 'p13a', 'p13b', 'p13c', 'p14', 'p15', 'p16', 'p17', 'p18', 'p19',
         'p20', 'p21', 'p22', 'p23', 'p24', 'p27', 'p28', 'p34', 'p35', 'p39', 'p44', 'p45a', 'pr7',
         'p48a', 'p49', 'p50a', 'p50b', 'p51', 'p52', 'p53', 'p55a', 'p57', 'p58', 'a', 'b', 'c',
         'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q', 'r', 's', 't', 'p5a']

        # cache v pameti: tabulky
        self.cache = {}


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

        # prazdne docasne ndarray do ktereho budes appendit
        ndwhole = np.empty([0, 64])

        # vysledne pole sloupcu
        ndresult = []

        # zjisti kolik existuje zipu stazenych
        zips = [f for f in os.listdir(self.target) if f.endswith('.zip')]

        # pokud chybi nektery ze zipu, spravne by tam nemel byt ani jeden (pokud jsem ho teda sam nesmazal). stahni je radsi znovu vsechny at je jistota ze je to ok
        if len(zips) != 5:
            self.download_data()

            # a aktualizuj seznam
            zips = [f for f in os.listdir(self.target) if f.endswith('.zip')]

        # pro konkretni region vytahni vsechna data ze vsech souboru
        for z in zips:

            try:

                with zipfile.ZipFile(os.path.join(self.target, z)) as zf:

                    # nacti konkretni soubor kraje
                    with zf.open(self.kraje[region], 'r') as f:

                        # vyrob csv reader
                        reader = csv.reader(TextIOWrapper(f, encoding='windows-1250'), delimiter=';')

                        # strc data do celkoveho pole
                        ndwhole = np.concatenate([ndwhole, np.asarray([data for data in reader], dtype=str)])

            # kdyby nahodou ten soubor mezitim nekdo smazal
            except (OSError, KeyError) as e:
                print(e)

        # KROK 2 - zpracuj celkove pole a rozdel ho na jednotlive sloupce

        # vyrad prazdna mista
        ndwhole[ndwhole == ''] = -1

        # nastrc tam kraje
        ndresult.append(np.full((ndwhole.shape[0]), region))

        # vytvor sety
        aset = {1, 4, 6, 7, 8, 9, 10, 11, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 35, 36, 37, 38, 39, 40, 42, 43, 44, 63, 53}
        bset = {0, 3, 45, 46, 49, 50, 51, 52, 54, 55, 57, 58, 59, 56, 62}
        cset = {12, 16, 41}
        dset = {2, 60, 61}
        eset = {47, 48}

        # nastrkej je do hlavniho pole, sloupce 0-63 z csv
        for i in range(64):
            # uint8
            if i in aset:
                ndresult.append(ndwhole[:, i].astype('uint8'))
            # stringy, co nadelas
            elif i in bset:
                ndresult.append(ndwhole[:, i].astype('str'))
            # delsi inty, ale ne moc dlouhe
            elif i in cset:
                ndresult.append(ndwhole[:, i].astype('uint16'))
            # dlouhe inty do 4294967295
            elif i in dset:
                ndresult.append(ndwhole[:, i].astype('uint32'))
            # souradnice s jistotou
            elif i in eset:
                ndwhole[:, i][ndwhole[:, i] == 'D:'] = -1
                ndwhole[:, i][ndwhole[:, i] == 'E:'] = -1
                ndresult.append(np.core.defchararray.replace(ndwhole[:, i],',', '.').astype('complex64'))
            # over XX
            elif i  == 34:
                ndwhole[:, i][ndwhole[:, i] == 'XX'] = -1
                ndresult.append(ndwhole[:, i].astype('uint8'))
            # over cas
            elif i == 5:
                ndwhole[:, i][np.logical_or(np.char.startswith(ndwhole[:, i], '25'), np.char.endswith(ndwhole[:, i], '60'))] = -1
                ndresult.append(ndwhole[:, i].astype('uint16'))

        # vrat tuple se zpracovanym jednim krajem
        return (self.header, ndresult)


    # slouceni dvou velkych poli - provizorni
    def _merge(self, dataset, x):

        # sluc kazdy sloupec
        for i in range(64):
            dataset[i] = np.concatenate([dataset[i], x[i]])


    # overeni jestli existuje cache pickle.gz soubor
    def _cachefile_exists(self, region):

        if os.path.join


    # nahrani obsahu cache do pameti
    def _cachefile_load(self, region):
        pass


    # ulozeni neceho do cache a zaroven i nacteni do pameti
    def _cachefile_save(self, table, region):
        pass


    def get_list(self, regions=None, frommain=False):

        # vystup
        dataset = []

        if not regions:
            # vsechny kraje
            regions = [*self.kraje]
            
        try:    

            once = True

            for region in regions:

                # CACHE

                # pokud neni v pameti
                if region not in self.cache:

                    # pokud existuje cache soubor
                    if self._cachefile_exists(region):
                        # nacti soubor do pameti
                        self._cachefile_load(region)

                    # musis zavolat funkci
                    else:
                        # tohle jsou data pro kazdy region
                        _, table = self.parse_region_data(region)

                        # funkce to ulozi do cache a zaroven i do pameti
                        self._cachefile_save(table, region)

                # pridej do datasetu
                self._merge(dataset, self.cache[region])

                # pokud se to vola z main
                if frommain:

                    # vytiskni seznam sloupcu
                    if once:
                        print('SLOUPCE:')
                        print(self.header)
                        print('\n')
                        once = False

                    # vytiskni seznam kraju a pocet radku
                    print(region + ':\t' + str(table[0].shape[0]) + ' radku')

        except OSError:
            print('Spatne zadany argument [seznam regionu] nebo region.')

    
if __name__ == "__main__":

    print(datetime.now())

    dd = DataDownloader()
    dd.get_list(['PHA', 'VYS', 'OLK'], frommain=True)

    print(datetime.now())
