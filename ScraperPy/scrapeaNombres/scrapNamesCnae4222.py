"""
Scrapea nombres de empresas para cnae 2413 de iberinform.es
Escribe el resultado en archivo cnae2413.csv
Dependencia: necesita instalar beautifulsoup4

             pip install beautifulsoup4
"""
#Utiles varios______________________
import random
import time
#___________________________________
import csv #Manejo de archivo csv
import urllib.request #Realizar peticiones HTTP

from bs4 import BeautifulSoup

def construye_url(pagina):
    '''Construye la url de la web usando la estructura de paginacion de ellos

        Args:
            pagina(int): numero de la pagina que nos toca escrapear

        Returns:
            _(string): cadena con la url de la pagina
    '''
    return "https://www.iberinform.es/es/productos/directorio-empresas/"\
        "directorio-cnae/4222/construccion-de-redes-electricas-y-de-telecom"\
        "unicaciones/paginacion/"+str(pagina)

#Construimos primera url
pag = 1
url = construye_url(pag)

html = urllib.request.urlopen(url)
soup = BeautifulSoup(html)
tags = soup('span')
tags = tags[29:-24] #Eliminamos residuo html

#Inicializa lista donde guardaremos los nombres de empresas
nombres_empresas = []
while(tags != []):
    for tag in tags:
        nombres_empresas.extend([tag.contents[0]])
    print('Numero de nombres encontrados: '+str(len(nombres_empresas)))

    pag+=1
    url = construye_url(pag)
    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html)
    tags = soup('span')
    tags = tags[29:-24]

#Escribe en .csv
name_csv = 'cnae4222.csv'
with open(name_csv, 'w', encoding='utf-8') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL) #para entrecomillar pa despue
    wr.writerow(nombres_empresas)
