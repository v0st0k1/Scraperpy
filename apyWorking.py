""" Se busca en la tabla antigua los Cifs que nos interesan
    y usando la API eInforma se guarda en otra tabla la informacion, esta es
    mucho mas veraz que la obtenida en la tabla antigua usando escrapeo
"""
import http.client
import boto3
import json
import csv
from boto3.dynamodb.conditions import Key, Attr

import time

#Para pasar de float a Decimal
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def usa_API(cif, conn, payload, headers):
    ''' Busca una empresa usando la API en funcion de su CIF

        Args:
            cif(string): identificativo de la empresa

            conn(http.client.HTTPSConnection): conexion con la API

            payload(string): cadena con payload, en nuestro caso vacía

            headers(string): cadena con el header, especifica en nuestro caso
                             que la informacion obtenida por el get sea un JSON
                             y el token de autentificacion

        Raises:
            KeyError: si no tiene alguno de los parametros clave, se aborta

            decimal.OverFlow: sobrecarga, numero demasiado grande, muy raro
                              se deberia a un fallo en la base de datos de la API

            http.client.RemoteDisconnected: se sobrecarga la API o algo produce
                                            que el servidor cierre conexion
                                            Solucion: se reconecta en 5 segundos

        Returns:
            empresa(dictionary): diccionario con los datos de la empresa
                                 obtenido por la API, directamente para insertar
                                 en base de datos

    '''
    #Pide info. a la API
    conn.request("GET", "/api/v1/companies/"+cif+"/report", payload, headers)

    try:
        res = conn.getresponse()
    except http.client.RemoteDisconnected as rd:
        print("Excepcion desconexion remota, intentamos de nuevo en 5 segundos")
        time.sleep(5)
        conn.request("GET", "/api/v1/companies/"+cif+"/report", payload, headers)
        res = conn.getresponse()
    data = res.read()

    #Lo cargamos en json para facilitar el asunto
    js = json.loads(data.decode("utf-8"))
    #print(js)
    #Guardamos en archivo dentro de carpeta de logs para no perder las llamadas
    #en caso fatal
    f=open("./calls/"+cif+".txt","w", encoding='utf-8')
    f.write(str(js))
    f.close()

    #Creacion del Item. uso de try-except para asegurarnos de las clave y campos
    empresa = {}
    try:
        empresa['Cif'] = js['identificativo']
        flag_key = True
    except KeyError as ke:
        print("No tiene Cif, abortamos posiblemente ya ni exista")
        flag_key = False
    try:
        empresa['Ventas'] = Decimal(str(js['ventas']))
    except KeyError as ke:
        print("No tiene Ventas, abortamos posiblemente ya ni exista")
        flag_key = False
    except decimal.OverFlow:
        print("Overflow con ventas!")
        flag_key = False

    if(flag_key):
        try:
            empresa['Empleados'] = Decimal(str(js['empleados']))
        except KeyError as ke:
            empresa['Empleados'] = -1 #Code error number: Desconocido
        """except decimal.OverFlow:
            print("Overflow con empleados!")
            empresa['Empleados'] = -2 #Code error number: Overflow"""
        try:
            empresa['Cnae'] = js['cnae']
        except KeyError as ke:
            empresa['Cnae'] = "Desconocido"
        try:
            empresa['Nombre'] = js['denominacion']
        except KeyError as ke:
            empresa['Nombre'] = "Desconocido"
        try:
            empresa['Nombre comercial'] = str(js['nombreComercial'])
        except KeyError as ke:
            empresa['Nombre comercial'] = "Desconocido"
        try:
            empresa['Email'] = js['email']
        except KeyError as ke:
            empresa['Email'] = "Desconocido"
        try:
            empresa['Telefono'] = str(js['telefono'])
        except KeyError as ke:
            empresa['Telefono'] = "Desconocido"
        try:
            empresa['Fecha fundacion'] = js['fechaConstitucion']
        except KeyError as ke:
            empresa['Fecha fundacion'] = "Desconocido"
        try:
            empresa['Situacion'] = js['situacion']
        except KeyError as ke:
            empresa['Situacion'] = "Desconocido"
        try:
            empresa['Web'] = str(js['web'])
        except KeyError as ke:
            empresa['Web'] = "Desconocido"
        try:
            empresa['Año ventas'] = str(js['anioVentas'])
        except KeyError as ke:
            empresa['Año ventas'] = "Desconocido"
        try:
            empresa['Capital social'] = Decimal(str(js['capitalSocial']))
        except KeyError as ke:
            empresa['Capital social'] = -1 #Code error number: Desconocido
        """except decimal.OverFlow:
            print("Overflow con capital social!")
            empresa['Capital social'] = -2 #Code error number: Overflow"""
        try:
            empresa['Domicilio social'] = js['domicilioSocial']
        except KeyError as ke:
            empresa['Domicilio social'] = "Desconocido"
        try:
            empresa['Localidad'] = js['localidad']
        except KeyError as ke:
            empresa['Localidad'] = "Desconocido"
        try:
            empresa['Forma juridica'] = js['formaJuridica']
        except KeyError as ke:
            empresa['Forma juridica'] = "Desconocido"
        try:
            empresa['Fecha ult. balance'] = js['fechaUltimoBalance']
        except KeyError as ke:
            empresa['Fecha ult. balance'] = "Desconocido"
        try:
            empresa['Cargo principal'] = js['cargoPrincipal']
        except KeyError as ke:
            empresa['Cargo principal'] = "Desconocido"
        try:
            empresa['Fax'] = str(js['fax'])
        except KeyError as ke:
            empresa['Fax'] = "Desconocido"

        return empresa

#Este metodo no se usa, ya que corresponde a una version antigua de este script
def csv_working(f_nombre, conn, payload, headers):
    ''' A partir de un fichero, obtenido de DynamoDB con los Cifs de las empresas
        a los que sacaremos informacion usando la API

        Args:
            f_nombre(string): nombre del fichero csv que tendra los cifs

            conn(http.client.HTTPSConnection): conexion con la API

            payload(string): cadena con payload, en nuestro caso vacía

            headers(string): cadena con el header, especifica en nuestro caso
                             que la informacion obtenida por el get sea un JSON
                             y el token de autentificacion

    '''
    with open(f_nombre, encoding = "utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            id = row['empresa_id (S)']
            print(type(id))
            ventas_key = row['ventas (N)']
            print(ventas_key)
            cif = row['cif (S)']
            #Algunos cif se guardan con CIF antes, eliminar en ese caso
            cif = cif.replace('CIF','').replace('cif','')
            empresa = usa_API(cif, conn, payload, headers)
            table.put_item(Item=empresa)
            table_old.update_item(
                Key = {
                    'empresa_id' : id,
                    'ventas' : int(ventas_key)
                },
                UpdateExpression="set sincronizado = :s",
                ExpressionAttributeValues={
                    ':s': True
                },
                ReturnValues="UPDATED_NEW"
            )

def no_existe(tabla, cif):
    ''' Comprueba si en tabla existe ya la empresa con cif (cif)

        Args:
            tabla(dynamodb.Table): tabla en la que verificar

            cif(string): identificativo con el que comprobar

        Returns:
            _(boolean): si se ha encontrado o no
    '''
    cif = cif.replace('CIF','').replace('Cif','').replace('cif','')
    respuesta = tabla.query(
        KeyConditionExpression=Key('Cif').eq(cif) & Key('Ventas').gte(0)
    )

    return respuesta['Items'] == []

def pertenencia(ventas, empleados):
    ''' Funcion auxiliar que nos devuelve si una empresa es pequeña, mediana o
        grande, en funcion de sus ventas y empleados.
        El criterio es el siguiente:
            Si una empresa factura ventas O tiene empleados como grande, es grande
            Si no es grande, entonces se ve si es mediana o pequeña en funcion del
            numero de empleados, y si no existiese en funcion de las ventas

        Args:
            ventas(float): numero de ventas en euros

            empleados(int): numero fisico de empleados

        Return:
            tipo(string): cadena con el tipo de empresa (grande, mediana o pequeña)

    '''
    #Se usa el mas restrictivo, si una empresa factura como grande pero tiene
    #empleados como mediana, nos interesa lo que factura
    #Paradigma 2: se ve si es mediana o pequeña por su n_empleados, en caso de no haber
    #el de ventas
    criterio_peq = {'min_ventas' : 0,
            'max_ventas' : 1E7-0.01,
            'min_empleados' : 0,
            'max_empleados' : 49
           }
    criterio_med = {'min_ventas' : 1E7,
            'max_ventas' : 5E7,
            'min_empleados' : 50,
            'max_empleados' : 250
           }
    tipo = None
    #REVISAR segun la pespectiva a usar en la practica del tipado
    #Actual: si ventas o empleados como grande pues grande.
    #luego pequeña o mediana en funcion de empleados, o ventas si no viniese
    #RECUERDA: empleados o ventas pueden ser 0 si no se tiene alguna de esas
    if empleados > criterio_med['max_empleados'] or ventas > criterio_med['max_ventas']:
        tipo = "grande"
    elif empleados > criterio_peq['max_empleados']:
        tipo = "mediana"
    elif empleados == 0 and ventas > criterio_peq['max_ventas']:
        tipo = "mediana"
    else:
        tipo = "pequeña"
    return tipo

def busqueda(tabla_busq, tabla_insertar, conn, payload, headers):
    ''' Scanea la tabla tabla_busq completamente, las empresas que cumplan los
        requisitos seran utilizadas mediante la API para obtener una informacion
        mas veraz. Luego se almacenara esta informacion en archivos (logs) para
        no desperdiciar las consultas, y tambien seran guardadas en la tabla de
        datos correspondiente tabla_insertar

        Args:
            tabla_busq(boto3.resources.factory.dynamodb.Table): tabla busqueda

            tabla_insertar(boto3.resources.factory.dynamodb.Table): tabla donde insertar

            conn(http.client.HTTPSConnection): conexion con la API

            payload(string): cadena con payload, en nuestro caso vacía

            headers(string): cadena con el header, especifica en nuestro caso
                             que la informacion obtenida por el get sea un JSON
                             y el token de autentificacion

    '''
    #Hacemos scan de la tabla completa, y paginando vamos avanzando en ella
    #si encontramos una que cumpla los requisitos la introducimos en su correspondiente
    respuesta = tabla_busq.scan()
    items = respuesta['Items']
    print("Se va a leer "+str(len(respuesta))+" instancias")

    desechadas = 0

    while True:
        for empresa in items:
            #Vemos que se haya conseguido y que ademas tenga alguno de los dos datos importantes
            if empresa['conseguido']:
                print("Se va a intentar empresa "+empresa['nombre'])
                print("Con ventas "+str(empresa['ventas'])+" y empleados "+str(empresa['n_empleados']))
                #Aqui podemos poner los requisitos de empleados y/o ventas
                if empresa['ventas'] > 1000000 or empresa['n_empleados'] > 15:
                    #Vemos si no se ha metido ya
                    if no_existe(tabla_insertar, empresa['cif']):
                        tipo = pertenencia(empresa['ventas'], empresa['n_empleados'])
                        """if not tipo == 'grande':"""
                        empresa_ok = usa_API(empresa['cif'], conn, payload, headers)
                        empresa_ok['tipo'] = tipo
                        tabla_insertar.put_item(Item=empresa_ok)
                        print("Insertada empresa "+empresa_ok['Nombre']+" con Cif "+empresa_ok['Cif'])
                        #input("Pulsa Enter para continuar...")
                        """else:
                            desechadas += 1
                            print("Empresa "+empresa['nombre']+" no cumple requisitos, es GRANDE")"""
                    else:
                        print("Ya se ha metido anteriormente empresa "+empresa['nombre']+" con cif "+empresa['cif'])
                else:
                    print('No cumple requisitos de venta/empleados')
                    desechadas += 1
        if respuesta.get('LastEvaluatedKey'):
            respuesta = tabla_busq.scan(ExclusiveStartKey=respuesta['LastEvaluatedKey'])
            items = respuesta['Items']
            print("Se va a leer "+str(len(respuesta))+" instancias")
        else:
            print("Empresas desechadas por grande: "+str(desechadas))
            break

conn = http.client.HTTPSConnection("developers.einforma.com")

#Conexion base de datos
dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
#tabla_test = dynamodb.Table('API_working')
tabla_old = dynamodb.Table('info_empresasFinal')
tabla_insertar = dynamodb.Table('using_API')
#________________________
print(type(tabla_old))
payload = ""

#/!\ Warning: el access-token (bearer) caduca a la hora, hay que retomar codigo
#de Insomnia, si fuese necesario, usar librerias de python para automatizar
#informacion. Web oficial de OAuth2.0 https://oauth.net/code/python/
headers = {
    'accept': "application/json",
    'authorization': "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJkZDMxYzRlZjYxZTMzMGEwY2QzN2ZiNGQ2MTEwOWNkYyJ9.OvWnHSoMGPeXylqIqc5X0EMLN05XAS8LI4qcDIds4Kdw0wfnL4WFU96-q2EQcFrIRbVZjzk9wEri5tM3Kk-05A"
    }

busqueda(tabla_old, tabla_insertar, conn, payload, headers)
