# -*- coding: utf-8 -*-
"""
@author: xplsek03
"""

import matplotlib.pyplot as plt
from download import DataDownloader
import numpy as np
import os
import argparse


def plot_stat(data_source, kraje, fig_location=None, show_figure=False):

    '''
    Motoda, ktera vytvori grafy z dostupnych dat pro zadane kraje.

    Argumenty:
    data_source: zdroj dat - sloupce tabulky - pro konkretni kraje
    kraje: seznam retezcu oznacujicich kody konkretnich REGIONU
    fig_location: adresa souboru do ktereho se ma vygenerovany graf ulozit. Vychozi None: nevytvaret
    show_figure: zda se ma graf zobrazit. Vychozi None (nema se zobrazit)
    '''

    # nastav rozmery 16:9
    plt.rcParams["figure.figsize"] = [16, 9]

    # seznam let pro ktere se to ma vypisovat
    roky = ['2016', '2017', '2018', '2019', '2020']

    # pocet nehod v kazdem kraji
    mrtvoly = [[] for i in range(len(roky))]

    # proved pro kazdy rok
    for i in range(len(roky)):

        # vyfiltruj konkretni kraj
        for kraj in kraje:
            # celkovy pocet nehod
            pocet = data_source[0][ (np.char.strip(data_source[0]) == kraj) & np.char.startswith(data_source[4], roky[i]) ]

            # dopln seznam pro tvorbu grafu
            mrtvoly[i].append(np.size(pocet,0))

    # vytvor pocet grafu podle roku
    fig, ax = plt.subplots(len(roky))
    fig.tight_layout()

    # pro kazdy graf z peti
    for i in range(len(roky)):

        # nastav okraje
        ax[i].margins(0.10, 0.10)

        # smaz horni a prave okraje
        ax[i].spines['right'].set_visible(False)
        ax[i].spines['top'].set_visible(False)

        # sloupce pro jeden sloupcovy graf
        sloupce = ax[i].bar(kraje, mrtvoly[i], color='red')

        # nalezeni poradi v ramci skupiny
        u = sorted(mrtvoly[i])
        poradi = [u.index(x) for x in mrtvoly[i]]

        for k,sloupec in enumerate(sloupce):

            # y pozice
            yval = sloupec.get_height()

            # pridej text s poradim do kazdeho grafu
            ax[i].text(sloupec.get_x() + sloupec.get_width() / 2, yval, poradi[k]+1, va='bottom')  # va: vertical alignment y positional argument

    # pokud se ma graf zobrazit
    if show_figure:
        plt.show()

    # pokud se ma ulozit
    if fig_location:

        # kdyby nahodou mezitim smazal ten vytvoreny adresar
        try:
            os.makedirs(os.path.dirname(fig_location), exist_ok=True)
            fig.savefig(fig_location, dpi=fig.dpi)

        except OSError:
            print('Neco s epokazilo pri vytvareni ciloveho souboru.')



if __name__ == '__main__':

    # parser argumentu z cmd
    parser = argparse.ArgumentParser(description='IZV1 - xplsek03')
    parser.add_argument("--show_figure", action="store_true")
    parser.add_argument("--fig_location", action="store", dest="fig_location", default=None)

    # argumenty prejate z prikazove radky
    arguments = parser.parse_args()

    dd = DataDownloader()

    # vybrane kraje
    kraje = ['VYS', 'STC', 'JHC']

    # pokud byla zadana cesta k ulozeni souboru
    if arguments.fig_location:

        # rozdel na nazev souboru a cestu
        path, file = os.path.split(arguments.fig_location)

        # validace 1
        if not path:
            print('Neni zadana cesta.')
            exit()

        # validace 2
        if not file:
            print('Neni zadan nazev souboru.')
            exit()

        # over spravnost cesty, pripadne ji vytvor
        try:
            if not os.path.exists(path):
                os.makedirs(path)

        # neco se pokazilo
        except OSError:
            print('Spatne zadana cesta nebo nema prava k zapisu.')
            exit()

    plot_stat(dd.get_list(kraje)[1], kraje, show_figure=arguments.show_figure, fig_location=arguments.fig_location)
