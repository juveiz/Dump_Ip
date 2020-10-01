# Parseo de Datos de .dump a codificación one hot

Si queremos acceder al virtual enviroment lo que tenemos que hacer es ejecutar desde la carpeta descragada el comando:

~~~

source bin/activate

~~~

El programa transforma los archivos .dump en matrices de codificación one hot, además de seleccionar solo las ips que aparecen más.

Para realizar esto tenemos que ejecutar el programa. Tiene dos modos de ejecución principal:

#### Método de archivos

El primer método es introducir las rutas de los archivos una a una tras la flag -a, de la siguiente forma:

~~~

python3 parse_data.py -a /home/juveiz/Documentos/PoC_Machine_Learning/Datos/Dump/nfcapd.202009081615.dump 

~~~

Esto es mejor si solo se van a transformar un par de archivos. si no es así, es mejor usar el método de ficheros.

#### Método de ficheros

El segundo método consiste en poner todas las rutas en un único fichero y dejar que el programa lo lea del fichero con la flag -f, de la siguiente forma:

~~~

python3 parse_data.py -f ./fichero.txt 

~~~

Adicionalmente, para ambos métodos se puede utilizar la flag -v para que se muestre por pantalla información de por donde va el programa.