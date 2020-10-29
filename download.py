# -*- coding: utf-8 -*-
"""
@author: xplsek03
"""

import requests
from bs4 import BeautifulSoup
import gzip
import os
import zipfile   
import csv 
from io import TextIOWrapper
import numpy as np
from datetime import datetime
import pickle


class DataDownloader:
    
    
    def __init__(self, url='https://ehw.fit.vutbr.cz/izv/', folder='data', cache_filename='data_{}.pkl.gz'):

        '''
        Metoda nastavuje nutne atributy pro vytvoreni instance DataDownloader.

        Argumenty:
        url: adresa, ze ktere se stahuji archivy
        folder: absolutni nebo relativni cesta k adresari, kam se maji ukladat cache a stazene soubory
        cache_filename: sablona nazvu cache souboru
        '''

        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
  
        ##BUG
        self.target = os.path.join(os.getcwd(), self.folder)

        # pokud cesta neexistuje, vytvor ji
        if not os.path.exists(self.target):
           os.makedirs(self.target)

        ##BUG - pokud cesta neni adresar

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

        # cache v pameti: slovnik kraj:tabulka
        self.cache = {}


    def download_data(self):

        '''
        Metoda provede stazeni dat ze zadane adresy.
        '''

        # GET request
        home = requests.get(self.url, headers=self.headers)

        # Soup objekt
        soup = BeautifulSoup(home.text, 'html.parser')

        # seznam pro konkretni odkazy A v ramci prvku TR
        trs_links = []

        trs = soup.findAll('tr')
        trs_links.append(trs[11].findAll('a')[-1].get('href'))
        trs_links.append(trs[23].findAll('a')[-1].get('href'))
        trs_links.append(trs[35].findAll('a')[-1].get('href'))
        trs_links.append(trs[47].findAll('a')[-1].get('href'))

        # najdi posledni dostupne TR s odkazem A
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


    def parse_region_data(self, region):

        '''
        Metoda ziska data z jednoho konkretniho regionu.

        Argumenty:
        region: string oznacujici REGION

        Navraci:
        tuple([seznam sloupcu tabulky], [seznam jednotlivych numpy poli reprezentujicich sloupce tabulky])
        '''

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
        bset = {0, 45, 46, 49, 50, 51, 52, 54, 55, 57, 58, 59, 56, 62}
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
            # konvertuj datum
            elif i == 3:
                ndresult.append(ndwhole[:, i].astype('str')) # datetime64
            # over cas
            elif i == 5:
                ndwhole[:, i][np.logical_or(np.char.startswith(ndwhole[:, i], '25'), np.char.endswith(ndwhole[:, i], '60'))] = -1
                ndresult.append(ndwhole[:, i].astype('uint16'))

        # vrat tuple se zpracovanym jednim krajem
        return (self.header, ndresult)


    def _merge(self, dataset, x):

        '''
        Pomocna metoda pro slouceni dat z prave nacteneho souboru a vysledneho pole sloupcu

        Argumenty:
        dataset: vysledne pole sloupcu
        x: data z prave nacteneho souboru
        '''

        # dataset je zatim prazdny, inicializuj
        if not dataset:
            dataset = x

        # uz v nem neco je
        else:
            # sluc kazdy sloupec
            for i in range(64):
                dataset[i] = np.concatenate([dataset[i], x[i]])


    def _cachefile_exists(self, region):

        '''
        Metoda overi zda existuje cache pickle soubor.

        Argumenty:
        region: kod REGIONU

        Navraci:
        True: soubor existuje
        False: soubor neexistuje
        '''

        # zkontroluj jeslti soubor existuje
        if os.path.isfile(os.path.join(self.target, self.cache_filename.format(region))):
            return True
        return False


    def _cachefile_load(self, region):

        '''
        Metoda nahraje obsah cache souboru do pameti.

        Argumenty:
        region: kod REGIONU
        '''

        # rozbal a vezmi obsah souboru
        with gzip.open(os.path.join(self.target, self.cache_filename.format(region)), 'rb') as f:
            # pickle je nebezpecnej..
            # nahrej soubor do pameti
            self.cache[region] = pickle.load(f)


    def _cachefile_save(self, table, region):

        '''
        Metoda ulozi do pickle cache souboru tabulku s daty jednoho kraje. Zaroven nacte data do cache v pameti.

        Argumenty:
        table: pole sloupcu tabulky s daty kraje
        region: kod REGIONU
        '''

        # uloz ty data do pameti
        self.cache[region] = table

        # otevri si cilovej soubor
        with gzip.open(os.path.join(self.target, self.cache_filename.format(region)), 'ab') as f:
            # pickle je nebezpecnej..
            # uloz data do souboru
            pickle.dump(table, f)


    def get_list(self, regions=None, frommain=False):

        '''
        Metoda umoznuje ziskat data pro konkretni regiony.

        Argumenty:
        regions: seznam kodu REGIONU. Vychozi hodnota None (vsechny dostupne regiony).
        frommain: jestli je funkce volana z __main__. Vychozi hodnota: False (neni volana z __main__).

        Navraci:
        tuple([seznam sloupcu tabulky], [seznam jednotlivych numpy poli reprezentujicich sloupce tabulek danych regionu])
        '''

        # vystup vsech dat zadanych kraju
        dataset = []

        if not regions:
            # vsechny kraje
            regions = [*self.kraje]
            
        try:    

            # tiskni hlavicku tabulky pouze jednou
            once = True

            # pro kazdy region
            for region in regions:

                # CACHE START

                # pokud neni v pameti
                if region not in self.cache:

                    # pokud existuje cache soubor
                    if self._cachefile_exists(region):
                        # nacti soubor do pameti
                        self._cachefile_load(region)

                    # musis zavolat funkci parse_region_data
                    else:
                        # tohle jsou data pro kazdy region
                        _, table = self.parse_region_data(region)

                        # funkce to ulozi do cache a zaroven i do pameti
                        self._cachefile_save(table, region)

                # CACHE END

                # pridej do datasetu, pokud uz v nem neco je
                if dataset:
                    # sluc kazdy sloupec
                    for i in range(64):
                        dataset[i] = np.concatenate([dataset[i], self.cache[region][i]])
                # pokud v datasetu jeste nic neni
                else:
                    dataset = self.cache[region]

                # pokud se to vola z main
                if frommain:

                    # vytiskni seznam sloupcu
                    if once:
                        print('SLOUPCE:')
                        print(self.header)
                        print('\n')
                        once = False

                    # vytiskni seznam kraju a pocet radku
                    print(region + ':\t' + str(self.cache[region][0].shape[0]) + ' radku')

            # vrat dataset
            return (self.header, dataset)

        except TypeError:
            print('Spatne zadany argument [seznam regionu] nebo region.')

    
if __name__ == '__main__':

    # pokud je spousteno samostatne, proved nacteni vybranych kraju
    dd = DataDownloader()
    # nacti tri vybrane kraje
    dd.get_list(region=['VYS', 'STC', 'JHC'], frommain=True)
