"""
Script que toma nombres de empresas de archivos de la forma cnaeXXXX.csv,
donde XXXX es cnae al cual pertenecen las empresas nombradas en dicho archivo,
luego a partir del nombre y la web infocif, escrapea la informacion pertinente
y guarda los datos en una base de datos dynamoDB mediante aws.

Esta versión utiliza proxies rotativos, escrepeados de la web free-proxy-list
y la libreria fake_useragent con la que podemos obtener un user-agent aleatorio
/!\ Warning: puedes haber fallos con los proxies al ser gratuitos y dificultar
mucho el desarrollo del script

Para escrapear usaremos urllib y BeautifulSoup4
#   pip install urllib
#   pip install BeautifulSoup

Para acceder a la base de datos boto3
#   pip install boto3

Para manejar datos usaremos el formato JSON
#   pip install json

Para falsificar el user-agent, si se usa
#   pip install fake-useragent

Para manejar el error 10054, 10060... cuando el servidor nos cierre
#   pip install https://github.com/saltycrane/retry-decorator/archive/v0.1.2.tar.gz
#   from retry_decorator import retry
Nota: metemos mas abajo la funcion tal cual porque funciona mejor
"""

import decimal
import random
import time

import boto3 #Manejo de DynamoDB con AWS
import json #Manejar formato JSON

from boto3.dynamodb.conditions import Key, Attr #Manejo de DynamoDB con AWS

from bs4 import BeautifulSoup #Stepping en codigo html

import csv #Manejo de archivos csv
import uuid #Crear clave HASH para base de datos

import requests #Realizar peticiones HTTP

from lxml.html import fromstring #Scrapear lista de proxies

from functools import wraps #Scrapear lista de proxies

from itertools import cycle #Para hacer pool de proxies
import traceback

from fake_useragent import UserAgent

def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:20]:
        #if i.xpath('.//td[7][contains(text(),"yes")]'):
        #if i.xpath('.//td[3][contains(text(),"ES")]') or i.xpath('.//td[3][contains(text(),"NL")]') :
            #Grabbing IP and corresponding PORT
        proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
        proxies.add(proxy)
    print(proxies)
    return proxies

def resuelve_con_json(js):
    '''Obtiene la informacion a partir de un json

    Args:
        js(json): Json obtenido por un script en la pagina

    Returns:
        resultado(diccionario): Diccionario con la informacion pertinente
    '''
    resultado = {
        "telefono" : js['telephone'] ,
        "localidad" : js['address']['addressLocality'] ,
        "provincia" : js['address']['addressRegion'] ,
        "direccion" : js['address']['streetAddress'] ,
        "n_empleados" : js['numberOfEmployees'] ,
        "cif" : js['taxID'] ,
        "f_fundacion" : js['foundingDate'] ,
        "conseguido" : True ,
        }
    return resultado

def resuelve_con_html(soup):
    '''Obtiene informacion a partir del codigo html formateado con BeautifulSoup

    Args:
        soup(bs4.BeautifulSoup): html de la pagina

    Raises:
        AttributeError: no se haya encontrado informacion para ese campo

        IndexError: fallida la obtencion de la fecha de fundacion

    Returns:
        resultado(diccionario): Diccionario con la informacion pertinente
    '''
    try:
        Cif = soup.find("h2", {"class" : "editable col-md-10 col-sm-9 col-xs-12 mb10 text-right"}).get_text()
    except AttributeError as ae:
        Cif = '-'#Salimos directamente, porque sin Cif no nos interesa
        resultado = {"conseguido" : False}
        return resultado
    try:
        fecha = soup.findAll("p",{"class" : "editable col-md-10 col-sm-9 col-xs-12 mb10 text-right"})[0].get_text().strip().replace('\r','').replace('\n','')
        fecha = fecha[fecha.find("(")+1:fecha.find(")")]
    except AttributeError as ae:
        fecha = '-'
    try:
        domicilio = soup.findAll("p",{"class" : "editable col-md-10 col-sm-9 col-xs-12 mb10 text-right"})[1].get_text()
    except AttributeError as ae:
        domicilio = '-'
    try:
        domicilio_div = domicilio.split('\n')
        direccion = domicilio_div[1].strip()
        localidad = domicilio_div[4].strip()
    except IndexError:
        direccion = domicilio.strip().replace('\n','').replace('\r','')
        localidad = "-"
    try:
        tlf = soup.findAll("p",{"class" : "editable col-md-10 col-sm-9 col-xs-12 mb10 text-right"})[2].get_text().replace('\r','').replace('\n','')
        tlf = tlf.strip()
    except AttributeError as ae:
        tlf = '-'
    try:
        n_empleados = soup.findAll("p",{"class" : "editable col-md-8 col-sm-8 col-xs-12 mb10 text-right"})[2].get_text().replace('\r','').replace('\n','').strip()
    except AttributeError as ae:
        n_empleados = '-'

    resultado = {
        "telefono" : tlf ,
        "localidad" : localidad ,
        "provincia" : "-" ,
        "direccion" : direccion ,
        "n_empleados" : n_empleados ,
        "cif" : Cif ,
        "f_fundacion" : fecha ,
        "conseguido" : True ,
        }
    return resultado

def formatea(resultado):
    ''' Formatea la informacion para hacerla mas legible y mejor al usarla
        junto a DynamoDB con boto3

        Args:
            resultado(diccionario): Diccionario con la informacion pertinente

        Raises:
            ValueError: error al sacar entero de la cadena de num. empleados
                            porque contiene caracter '.', solucion reemplazar

        Returns:
            resultado(diccionario): Diccionario con la informacion formateada
    '''
    if resultado["n_empleados"] == '-' or resultado["n_empleados"] == '':
        resultado["n_empleados"] = 0
    try:
        resultado["n_empleados"] = int(resultado["n_empleados"])
    except ValueError as ve:
        resultado["n_empleados"] = int(resultado["n_empleados"].replace('.',''))
    if resultado["telefono"] == '+34' or resultado["telefono"] == '':
        resultado["telefono"] = '-'
    if resultado["f_fundacion"] == '':
        resultado["f_fundacion"] = '-'
    if resultado["localidad"] == '':
        resultado["localidad"] = "-"
    if resultado["provincia"] == '':
        resultado["provincia"] = "-"
    if resultado["direccion"] == '':
        resultado["direccion"] = "-"
    if resultado["cif"] == '':
        resultado["cif"] = "-"

    return resultado

#Para usar decorador retry!
def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

#@retry(Exception, tries=15, delay=3, backoff=2)
def get_html(url, proxy):

    try:
        ua = UserAgent()
        head = {'User-Agent': ua.random}
        url = url.replace('Ñ','%F1').replace('Ç','%C7').replace('º','%BA').replace('ª','%AA')
        resp = requests.get(url, proxies={"http":proxy, "https":proxy}, headers = head)
    except requests.HTTPError as err: #Error 500 supongo que no se podra salir con decorador retry
        resp = requests.get("http://www.infocif.es/general/empresas-informacion-listado-empresas.asp?Buscar=E-29%20CONSTRUCCIONES%20INTEGRALES")
        f = open("Log_fails.txt", "w+")
        leido = f.read()
        f.write(leido+"\n"+url)

    return resp

#@retry(Exception, tries=15, delay=3, backoff=2)
def get_soup(html):
    return BeautifulSoup(html, 'html.parser')


def resuelve_ventas(html, soup, resultado, proxy):
    ''' Obtiene la informacion correspondiente al volumen de ventas.
        Para ello, comprueba que exista dicha informacion en la web y en caso
        afirmativo la obtiene a partir de una nueva url y actualiza diccionario.

        Args:
            soup(bs4.BeautifulSoup): html de la pagina
            resultado(diccionario): Diccionario con la informacion

        Raises:
            urllib.error.HTTPError: error HTTP obtenido al ingresar en alguna
                                    url, normalmente sera un 500. Solucionamos
                                    esperando un tiempo t = (0.0 5.9999] aleat.
                                    y reingresando. Cambio: mejor guardar nombre y seguir

            urllib.error.URLError: error por denegacion de conexion por parte
                                   del servidor, seguramente por un bloqueo
                                   automatico por parte de este, solucionamos
                                   esperando entre dos y tres minutos, y
                                   volviendo a intentarlo de forma persistente

        Returns:
            resultado(diccionario): Diccionario con la informacion ya con ventas
    '''
    #Para no meter en ventas algo que no sea, vemos si existe info.
    if soup.find("span", {"class": "fwb cp colorred"}) != None :
        if(soup.find("span", {"class": "fwb cp colorred"}).get_text() == 'Sin información'):
            resultado["n_ventas"] = 0
        else:
            format_nombre = html.url.split('/')[4]
            new_url = "http://www.infocif.es/balance-cuentas-anuales/"+format_nombre

            html2 = get_html(new_url, proxy)
            soup2 = get_soup(html2.text)

            #A partir de la celda Ingresos de explotacion vemos que la siguiente
            #es la correspondiente a la cifra
            i = 0
            flag_0 = True
            while flag_0:
                if soup2.findAll('td')[i].get_text() == 'Ingresos de explotación':
                    flag_0 = False
                i += 1

            n_ventas = int(soup2.findAll('td')[i].get_text().replace('.',''))
            if soup2.findAll('th', {"class" : "w40 text-center roboto fs16"})[0].get_text().find('miles') != -1 :
                n_ventas *= 1000
            if soup2.findAll('th', {"class" : "w40 text-center roboto fs16"})[0].get_text().find('millones') != -1 :
                n_ventas *= 1000000
            resultado["n_ventas"] = n_ventas
        return resultado
    else:
        format_nombre = html.url.split('/')[4]
        new_url = "http://www.infocif.es/balance-cuentas-anuales/"+format_nombre

        html2 = get_html(new_url, proxy)
        soup2 = BeautifulSoup(html2.text)

        #antes 141
        i = 0
        flag_0 = True
        while flag_0:
            if soup2.findAll('td')[i].get_text() == 'Ingresos de explotación':
                flag_0 = False
            i += 1
        n_ventas = int(soup2.findAll('td')[i].get_text().replace('.',''))
        if soup2.findAll('th', {"class" : "w40 text-center roboto fs16"})[0].get_text().find('miles') != -1 :
            n_ventas *= 1000
        if soup2.findAll('th', {"class" : "w40 text-center roboto fs16"})[0].get_text().find('millones') != -1 :
            n_ventas *= 1000000
        resultado["n_ventas"] = n_ventas
        return resultado

def busqueda_con_nombre(nombre, proxy):
    ''' A partir del nombre obtiene la informacion deseada del site infocif.es

        Args:
            nombre(str): cadena con el nombre de la empresa

        Raises:
            json.JSONDecodeError: error al obtener el json de la web.
                                  se resuelve realizando la busqueda usando el
                                  metodo busqueda_con_html

            urllib.error.HTTPError: error HTTP obtenido al ingresar en alguna
                                    url, normalmente sera un 500. Solucionamos
                                    esperando un tiempo t = (0.0 5.9999] aleat.
                                    y reingresando.

            urllib.error.URLError: error por denegacion de conexion por parte
                                   del servidor, seguramente por un bloqueo
                                   automatico por parte de este, solucionamos
                                   esperando entre dos y tres minutos, y
                                   volviendo a intentarlo de forma persistente

        Returns:
            resultado(diccionario): diccionario con la informacion
                                    con campo conseguido True si se consiguise
                                    False de lo contrario


    '''
    url_actual = "http://www.infocif.es/general/empresas-informacion-listado-empresas.asp?Buscar="+nombre.replace(" ","%20").replace("Ñ","%D1")

    #Abrimos la url actual, con la empresa buscada, y la cargamos para BeatfSoup

    #Curarnos en salud:
    #VAMOS A ESPERAR UNOS SEGUNDOS CON CADA BUSQUEDA PARA QUE NO PETE MIENTRAS
    #NO ESTOY CUAL CENTINELA: borra esto mas adelante o especifica or something
    #time.sleep(random.random()+random.randrange(3))
    html = get_html(url_actual, proxy)
    soup = get_soup(html.text)
    #3 opciones al buscar: Resultado ambiguo, sin resultado y con Resultado
    #Si no hay res. o es ambiguo se retorna diccionario con conseguido = False
    #si se consigue se pasa la info en diccionario y conseguido = True
    sin_res = soup.find("div", {"class": "title-lg roboto mt50"}) != None
    res_ambiguo = soup.find("h1", {"class": "col-xs-12 fs18 roboto nopaddinglateral mb5"}) != None

    if(not sin_res and not res_ambiguo):
        #La pagina, muy cutremente, carga directamente los datos de dos JSONs
        #usando un script en js. Tomamos el ultimo, que tiene mas info
        tags = soup.findAll("script", {"type": "application/ld+json"})

        for tag in tags:
            try:
                js = json.loads(tag.get_text())
            except json.JSONDecodeError as jex:
                tags = []

        #Dos posibilidades, con json, y sin json
        if tags != []:
            resultado = resuelve_con_json(js)
            if not resultado["conseguido"]:
                return resultado
            #Formateamos mejor el resultado, y eliminamos emptys que da problemas con dynamodb
            resultado = formatea(resultado)

        else:
            resultado = resuelve_con_html(soup)
            if not resultado["conseguido"]:
                return resultado
            #Formateamos mejor el resultado, y eliminamos emptys que da problemas con dynamodb
            resultado = formatea(resultado)

        #Toma ventas de otra URL
        resultado = resuelve_ventas(html, soup, resultado, proxy)
        return resultado

    else:
        resultado = {"conseguido" : False ,}
        return resultado

def prueba_proxy(proxy):
    url = 'http://www.infocif.es/'
    try:
        ua = UserAgent()
        head = {'User-Agent': ua.random}
        response = requests.get(url,proxies={"http": proxy, "https": proxy}, headers=head, timeout=1.5)
        return True
    except:
        return False

def main(fichero, cnae):
    contador = 0
    proxies_skipping = 0
    proxy = None
    with open(fichero, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            for name in row:
                if(contador <= 0):
                    contador = 20
                    proxies_skipping = 0
                    proxies = get_proxies()
                    print(proxies)
                    proxy_pool = cycle(proxies)
                    proxy = next(proxy_pool)
                    #Busca proxy bueno, hace 20 y repetimos
                    proxies_skipping = 0
                    while True:
                        if prueba_proxy(proxy):
                            print("Proxy encontrado")
                            break
                        else:
                            print("Skipping. Posible fallo proxy")
                            proxy = next(proxy_pool)
                            proxies_skipping += 1
                            if proxies_skipping > 10:
                                proxies_skipping = 0
                                proxies = get_proxies()
                                proxy_pool = cycle(proxies)
                                proxy = next(proxy_pool)
                try:
                    res = busqueda_con_nombre(name, proxy)
                    camon = True
                except:
                    f=open("scrapy_log.txt","w+")
                    leido = f.read()
                    f.write(leido+"\n"+name)
                    f.close()
                    camon = False
                if camon:
                    id = uuid.uuid4()

                    if(res["conseguido"]):
                        table.put_item(
                            Item={
                                'empresa_id': str(id),
                                'nombre': name,
                                'cnae': cnae,
                                'conseguido': True,
                                'telefono': res["telefono"],
                                'localidad': res["localidad"],
                                'provincia': res["provincia"],
                                'direccion': res["direccion"],
                                'n_empleados': int(res["n_empleados"]),
                                'cif': res["cif"],
                                'f_fundacion': res["f_fundacion"],
                                'ventas': res["n_ventas"],

                            }
                        )
                    else:
                        table.put_item(
                            Item={
                                'empresa_id': str(id),
                                'nombre': name,
                                'cnae': cnae,
                                'conseguido': False,
                                'ventas' : -1
                            }
                        )
                    print("Metido "+name)
                    contador -= 1

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
table = dynamodb.Table('info_9499')

#for i in range(5): Remind: cambiado manualmente porque x_1, 2 ya resuelto
"""for i in [4]:
    nombre_f = "cnae4121_"+str(i+1)+".csv"
    main(nombre_f, 4121)
f=open("4121_success.txt","w")
f.close()"""

#Done: 4291
for cnae in [9499]:#, 4122, 4211, 4212, 4213, 4221, 4222, 4299, 4321, 7111, 7112]:
    nombre_f = "cnae"+str(cnae)+"copy.csv"
    main(nombre_f, cnae)
    f=open(str(cnae)+"_success.txt","w")
    f.close()
