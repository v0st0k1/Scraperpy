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

#Devuelve url para paginacion
def construye_url(pagina):
    ''' Devuelve la url con la pagina 'pagina' del directorio de empresas

        Args:
            pagina(int): numero de pagina

        Returns:
            _(string): url de la pagina
    '''
    return "https://www.iberinform.es/es/productos/directorio-empresas/"\
        "directorio-cnae/4121/construccion-de-edificios-residenciales"\
        "/paginacion/"+str(pagina)

#Construimos primera url, cambiar 'pag' para empezar por otra pagina
pag = 1
url = construye_url(pag)

html = urllib.request.urlopen(url)
soup = BeautifulSoup(html)
tags = soup('span')
tags = tags[29:-24] #Eliminamos residuo html

#Inicializa lista donde guardaremos los nombres de empresas
nombres_empresas = []
while(tags != [] ):
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
name_csv = 'cnae4121_5.csv' #cambiar X en <name> _X para escribir poco a poco
with open(name_csv, 'w', encoding='utf-8') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL) #Entrecomilla util pa despues
    wr.writerow(nombres_empresas)
