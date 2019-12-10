"""
Script que toma nombres de empresas de archivos de la forma cnaeXXXX.csv,
donde XXXX es cnae al cual pertenecen las empresas nombradas en dicho archivo,
luego a partir del nombre y la web infocif.es, escrapea la informacion pertinente
y guarda los datos en una base de datos dynamoDB en AWS.

/!\ Esta es la version a cara descubierta /!\
.No se contempla el bloqueo de IP.

Para escrapear usaremos urllib y BeautifulSoup4
#   pip install urllib
#   pip install BeautifulSoup

Para acceder a la base de datos boto3
#   pip install boto3

Para manejar datos usaremos el formato JSON
#   pip install json

Para manejar el error 10054, 10060... cuando el servidor nos cierre
#   pip install https://github.com/saltycrane/retry-decorator/archive/v0.1.2.tar.gz
#   from retry_decorator import retry
Nota: metemos mas abajo la funcion tal cual porque funciona mejor
"""
#Utiles varios______________________
import decimal
import random
import time
#Manejo de formato JSON_____________
import json
#Manejo de DynamoDB con AWS_________
from boto3.dynamodb.conditions import Key, Attr
import boto3
#___________________________________
#Sacar HTML, hacer peticiones HTTP y parsear
from bs4 import BeautifulSoup
import urllib.request
#___________________________________
import csv #Manejo archivos .csv
import uuid #Para obtener clave irrepetible

from functools import wraps #Requerido por decorador @retry

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

#Definicion del decorador (funciona mejor que importar libreria)
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

@retry(Exception, tries=15, delay=3, backoff=2)
def open_url(url):
    ''' Abre la URL y devuelve el resultado obtenido

        Args:
            url(string): string con la direccion a extraer informacion

        Raises:
            urllib.error.HTTPError: error HTTP, muy raro que suceda, solucionamos
                                    abriendo una pagina que sabemos que no obtendra
                                    resultado y guardando la url en un archivo log

        Returns:
            resp(http.client.HTTPResponse): respuesta con la informacion de la peticion
                                            y el codigo html

    '''
    try:
        url = url.replace('Ñ','%F1').replace('Ç','%C7').replace('º','%BA').replace('ª','%AA')
        resp = urllib.request.urlopen(url)
    except urllib.error.HTTPError as err: #Si se manejase con retry habria bucle infinto
        resp = urllib.request.urlopen("http://www.infocif.es/general/empresas-informacion-listado-empresas.asp?Buscar=E-29%20CONSTRUCCIONES%20INTEGRALES")
        f = open("Log_fails.txt", "w+", encoding='utf-8')
        leido = f.read()
        f.write(leido+"\n"+url)
    return resp

@retry(Exception, tries=15, delay=3, backoff=2)
def get_soup(html):
    ''' Parsea el codigo html para trabajar con el mas facil, mediante la
        libreria BeautifulSoup4

        Args:
            html(http.client.HTTPResponse): respuesta con la informacion de la peticion
                                            y el codigo html

        Returns:
            _(bs4.BeautifulSoup): objeto de clase BeautifulSoup con informacion
    '''
    return BeautifulSoup(html)

def get_html(url):
    '''Abre direccion url y devuelve el codigo html

        Args:
            url(string): cadena a sacar informacion

        Returns:
            html(http.client.HTTPResponse): respuesta de la peticion HTTP
    '''
    html = open_url(url)
    return html


def resuelve_ventas(html, soup, resultado):
    ''' Obtiene la informacion correspondiente al volumen de ventas.
        Para ello, comprueba que exista dicha informacion en la web y en caso
        afirmativo la obtiene a partir de una nueva url y actualiza diccionario.

        Args:
            html(http.client.HTTPResponse): informacion y codigo html

            soup(bs4.BeautifulSoup): objeto bs4 con la informacion parseada

            resultado(diccionario): Diccionario con la informacion

        Returns:
            resultado(diccionario): Diccionario con la informacion ya con ventas
    '''
    #Para no meter en ventas algo que no sea, vemos si existe info.
    if soup.find("span", {"class": "fwb cp colorred"}) != None :
        if(soup.find("span", {"class": "fwb cp colorred"}).get_text() == 'Sin información'):
            resultado["n_ventas"] = 0
        else:
            format_nombre = html.geturl().split('/')[4]
            new_url = "http://www.infocif.es/balance-cuentas-anuales/"+format_nombre

            html2 = get_html(new_url)
            soup2 = get_soup(html2)

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
        format_nombre = html.geturl().split('/')[4]
        new_url = "http://www.infocif.es/balance-cuentas-anuales/"+format_nombre

        html2 = get_html(new_url)
        soup2 = get_soup(html2)

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

def busqueda_con_nombre(nombre):
    ''' A partir del nombre obtiene la informacion deseada del site infocif.es

        Args:
            nombre(str): cadena con el nombre de la empresa

        Raises:
            json.JSONDecodeError: error al obtener el json de la web.
                                  se resuelve realizando la busqueda usando el
                                  metodo busqueda_con_html

        Returns:
            resultado(diccionario): diccionario con la informacion
                                    con campo conseguido True si se consiguise
                                    False de lo contrario


    '''
    url_actual = "http://www.infocif.es/general/empresas-informacion-listado-empresas.asp?Buscar="+nombre.replace(" ","%20").replace("Ñ","%D1")

    #Abrimos la url actual, con la empresa buscada, y la cargamos para BeatfSoup
    html = get_html(url_actual)
    soup = get_soup(html)

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
        resultado = resuelve_ventas(html, soup, resultado)
        return resultado

    else:
        resultado = {"conseguido" : False ,}
        return resultado

def main(fichero, cnae):
    ''' Estructura principal que a partir del nombre de un fichero, perteneciente
        a un cnae, lee los nombres de empresas almacenados en el y llama a los
        metodos de extraccion de la informacion pertinentes

        Args:
            fichero(string): nombre del fichero con los nombres de empresas

            cnae(int): numero cnae al que pertenecen las empresas de dicho
                        fichero
    '''
    with open(fichero, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            for name in row:
                res = busqueda_con_nombre(name)
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

class DecimalEncoder(json.JSONEncoder):
    '''Scada de la web de amazon y supuestamente necesaria para codear y decodear
    los numeros decimales. Al parecer DynamoDB guarda todo como strings, incluidos
    los tipos numericos y decimales, aunque luego puedas realizar operaciones
    matematicas con ellos
    '''
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

#Cargamos bbdd y abrimos la tabla
dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
table = dynamodb.Table('info_empresasFinal')


#Done: 4121_1, 4121_2, 4121_3, 4121_4, 4121_5, 4291
#/!\ Warning: los archivos 4121_X hay que manejarlos aparte (abajo p.ej)
for cnae in [4122, 4211, 4212, 4213, 4221, 4222, 4299, 4321, 7111, 7112, 9499]:
    nombre_f = "cnae"+str(cnae)+".csv"
    main(nombre_f, cnae)
    f=open(str(cnae)+"_success.txt","w", encoding='utf-8')
    f.close()

"""for i in [4]:
    nombre_f = "cnae4121_"+str(i+1)+".csv"
    main(nombre_f, 4121)
f=open("4121_success.txt","w")
f.close()"""
