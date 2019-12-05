# Scraperpy
Escrapea información de empresas según CNAEs

## Escrapea nombres

Mediante la web [Iberinform.es](https://www.iberinform.es/) *escrapeamos* los nombres de las empresas que pertenezcan a un cnae que le indiquemos, tendremos un script por cada cnae para facilitar un poco el trabajo. Además, guardaremos dichos nombres en tablas dentro de ficheros *csv*, cada fichero corresponderá a un cnae. 

---

Para esta función, usaremos los *scripts* scrapNamesCnaeXXXX.py donde *XXXX* es el cnae correspondiente. Los nombres se almacenarán en ficheros de la forma cnaeXXXX.csv, donde *XXXX* es el cnae correspondiente. Excepto para cnaes con un alto número de empresas, en las que para no saturar la memoria, iremos almacenando en ficheros particionados.

## Escrapea información

Usamos la web [Infocif.es](http://www.infocif.es/) para, a partir de los nombres antes obtenidos, buscar las empresas y sacar la información pertinente relativa a las mismas, si fuese posible. La información que extraeremos consta de:

+ **Cif**: Cif identificativo de la empresa
+ **Volumen de ventas**: volumen de ventas en euros (€), es posible que la web no contenga este campo, en tal caso guardaremos el valor 0
+ **Dirección**: dirección física de la empresa, si no viniese especificada se almacena un carácter '-'
+ **Fecha de fundación**: fecha en que la empresa fue constituida, si no viniese especificada se almacena un carácter '-'
+ **Localidad**: municipio en que reside la empresa, si no viniese especificada se almacena un carácter '-'
+ **Provincia**: provincia en que reside la empresa, si no viniese especificada se almacena un carácter '-'
+ **Número de empleados**: número de empleados que trabajan en la empresa, si no constara se guardaría el valor 0
+ **Teléfono**: número de teléfono relativo a la empresa, si no viniese especificada se almacena un carácter '-'

Una vez vayamos *escrapeando* la información, guardaremos cada *Item* correspondiente a una empresa en una base de datos *no-sql* usando DynamoDB en Amazon Web Service. Dicha tabla será usada para filtrar las empresas que nos interesen en función de sus características antes descritas y así luego mediante la API de *eInforma*, obtener información mas fidedigna de aquellas seleccionadas.

:dancer:

---

Para esta función podemos usar dos *scripts*, scraperpy_infocif.py, el cual no oculta nuestra dirección IP o user-agent. Y la versión que utiliza *proxies* rotativos para mejorar el anonimato y los posibles bloqueos por parte del servidor, y además usando la libreria fake-useragent nos proporcionamos de un user-agent aleatorio que mejora dicho anonimato.
