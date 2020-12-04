import pandas as pd
import numpy as np
import pickle
import bz2
import argparse

"""
Funcion to_pandas

Transforma un archivo en un dataframe de pandas

Entrada: path al fichero

Salida: dataframe

"""
def to_pandas(path):
    try:
        dump_data = pd.read_csv(path,delimiter=',',header=0,
                                names=['start_time','start_time.1','start_time.2','protocol','route_address',
                                    'source_address','destination_address','source_port','dest_port','input_interfaz_num',
                                    'output_interfaz_num','input_packets','output_packages','input_bytes','output_bytes',
                                    'flows','bits_per_second','packets_per_second','bits_per_packet'],dtype=str)
        dump_data = dump_data.drop(columns=['start_time.1','start_time.2','protocol',
                                        'route_address','input_interfaz_num','output_interfaz_num',
                                        'input_packets','output_packages','input_bytes','output_bytes','flows',
                                        'bits_per_second','packets_per_second','bits_per_packet'])
    
    except:
        error = 'No se ha podido leer el fichero ' + path
        print(error)
        return 

    return dump_data

"""
Funcion get_ips_ports

Toma todos los dataframes de la lista  y selecciona las ips y puertos que más aparecen

Entrada: lista de dataframes

Salida: dos listas: una con las ips y otro con los puertos

"""
def get_ips_ports(lista):
    dic_ip = {}
    dic_ports = {}

    for item in lista:
        temp = to_pandas(item)
        ips = np.append(temp['source_address'],temp['destination_address'])
        unique, counts = np.unique(ips,return_counts=True)

        for A,B in zip(unique,counts):
            if A in dic_ip:
                dic_ip[A] = dic_ip[A] + B
            else:
                dic_ip[A] = B

        ports = np.append(temp['source_port'],temp['dest_port'])
        unique, counts = np.unique(ports,return_counts=True)

        for A,B in zip(unique,counts):
            if int(float(A)) in dic_ports:
                dic_ports[int(float(A))] = dic_ports[int(float(A))] + B
            else:
                dic_ports[int(float(A))] = B
    

    count_data = np.array(list(dic_ip.items()))

    # Ordenamos de mayor a menor
    count_order = np.sort(count_data[:,-1])
    count_order = np.unique(count_order)[::-1]
    ordered = np.array([])
    # Obtenemos las ips que corresponden a esos valores
    for i in count_order:
        index = np.where(count_data[:,-1] == i)[0]
        for j in index:
            ordered = np.append(ordered,count_data[j,:],axis=0)
    ordered.resize((int(ordered.shape[0]/2),2))
    

    count_ports = np.array(list(dic_ports.items()))
    count_orderp = np.sort(count_ports[:,-1])
    count_orderp = np.unique(count_orderp)[::-1]
    orderedp = np.array([])
    for i in count_orderp:
        index = np.where(count_ports[:,-1] == i)[0]
        for j in index:
            orderedp = np.append(orderedp,count_ports[j,:],axis=0)
    orderedp.resize((int(orderedp.shape[0]/2),2))
    orderedp = orderedp.astype('int64')
    max_ips = ordered[np.where(ordered[:,1].astype(int) > 20000 * len(lista))[0],0]
    max_ports = orderedp[:np.where(orderedp[:,1] >= 20000 * len(lista))[0][-1],0]
    max_ports = max_ports.astype(str)
    return max_ips,max_ports

"""
Funcion to_one_hot

Transforma un dataframe de pandas en una codificacción one hot 

Entrada: dataframe, ips a utilizar y puertos a utilizar

Salida: matriz de one hot

"""
def to_one_hot(dataframe, max_ips, max_ports):
    # Creamos una matriz del tamanio adecuado y la llenamos de 0
    num_filas = dataframe.shape[0]
    num_columnas = 2 * (max_ips.shape[0] + 1) + 2 * (max_ports.shape[0] + 1)
    res = np.zeros(shape=(num_filas,num_columnas + 1),dtype=int)
    # Para cada fila seleccionamos los indices adecuados 
    # y ponemos un uno en los mismos
    
    data = dataframe.values[:,1:]
    for i in range(max_ips.shape[0]):
        fila, columna = np.where(data == max_ips[i])
        columna = columna*(max_ips.shape[0] + 1 + max_ports.shape[0] + 1) + i
        res[fila,columna] = 1
    
    data = dataframe.values[:,3:]
    for i in range(max_ports.shape[0]):
        fila, columna = np.where(data == max_ports[i])
        columna = columna*(max_ips.shape[0] + 1 + max_ports.shape[0] + 1) + i + max_ips.shape[0] + 1
        res[fila,columna] = 1
    
    res[np.where(~res[:,:max_ips.shape[0]].any(axis=1))[0],max_ips.shape[0]] = 1
    
    res[np.where(~res[:,max_ips.shape[0] + 1:max_ips.shape[0] + 1 + max_ports.shape[0]].any(axis=1))[0],
        max_ips.shape[0] + max_ports.shape[0] + 1] = 1
    
    res[np.where(~res[:,max_ips.shape[0] + 1 + max_ports.shape[0] + 1:max_ips.shape[0] + 
                         1 + max_ports.shape[0] + 1 + max_ips.shape[0]].any(axis=1))[0],
                         max_ips.shape[0] + 1 + max_ports.shape[0] + 1 + max_ips.shape[0]] = 1
    
    res[np.where(~res[:,max_ips.shape[0] + 1 + max_ports.shape[0] + 1 + max_ips.shape[0] + 1:-2].any(axis=1))[0],
                         -2] = 1
    
    fecha = dataframe.values[0,0]
    fecha = fecha.replace('-','')
    fecha = fecha.replace('T','')
    fecha = fecha.replace(':','')
    fecha = int(fecha)
    res[:,-1] = fecha
        
    return res

"""
Funcion to_one_hot_list

Transforma una lista dataframe de pandas en una lista de codificacción one hot 
y los guarda en pickle en la misma carpeta

Entrada: lista de dataframes, ips a utilizar y puertos a utilizar

Salida: lista de matrices de one hot

"""
def to_one_hot_list(lista,max_ips,max_ports,verbose):
    L = []
    for item in lista:
        temp = to_pandas(item)
        if verbose:
            print('Codificando: ' + temp.values[0,0])
        one_hot = to_one_hot(temp,max_ips,max_ports)
        L.append(one_hot)
        if verbose:
            print('Codificado: ' + temp.values[0,0])
    return L

"""
Funcion to_one_hot_count

Transforma un one_hot del creado anteriormente y cuenta el numero de unos por cada ip 

Entrada: one hot, ips a utilizar 

Salida: one_hot_count

"""
def to_one_hot_count(lista,max_ips,max_ports):
    L = []
    for item in lista:
        old = item.sum(axis=0)[:-1]
        new = np.zeros(shape=(old.shape[0]+1,),dtype=int)
        new[:-1] = old 
        new[-1] = item[0,-1]
        L.append(new)
    one_hot_count = np.array(L)
    return one_hot_count

"""
Funcion to_pickle

Transforma un lista de datos en archvios pickle en el path indicado

Entrada: lista, path donde guardar los pickles

"""
def to_pickle(lista,max_ips,max_ports,path):
    for i in range(len(lista)):
        filename = path + str(lista[i][0][-1]) + '.bz2'
        outfile = bz2.BZ2File(filename, 'w')
        pickle.dump(lista[i],outfile)
        outfile.close()
    
    filename = path +'_max_ips.bz2'
    outfile = bz2.BZ2File(filename, 'w')
    pickle.dump(max_ips,outfile)
    outfile.close()

    filename = path +'_max_ports.bz2'
    outfile = bz2.BZ2File(filename, 'w')
    pickle.dump(max_ports,outfile)
    outfile.close()

def main():
    # Parseo de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--archivos", type=str, nargs='+',
                        help="lista de rutas de los archivos")
    parser.add_argument("-f","--fichero", type=str,
                        help="ruta del fichero con la lista de archivos separados por salto de linea")  
    parser.add_argument("-v","--verbose", help="imprime por pantalla como va funcionando el programa",
                        action="store_true")   
    parser.add_argument("-c","--count", help="cuenta cuantos hay de cada fila en vez de generar todos los datos",
                        action="store_true")  

    args = parser.parse_args()
    if args.archivos and args.fichero:
        print('Introduzca solo uno')
        return
    if not args.archivos and not args.fichero:
        print('Introduzca al menos uno')
        return
    
    # Cargamos los datos
    data = []
    if args.archivos:
        data = args.archivos

    if args.fichero:
        paths = open(args.fichero,'r')
        for item in paths.readlines():
            data.append(item.strip('\n'))
        paths.close()
        
    # Obtenemos las ips y puertos
    if args.verbose:
        print('Comienzo de seleccion de Ips y Puertos')
    max_ips, max_ports = get_ips_ports(data)
    if args.verbose:
        print('Ips y Puertos seleccionados')
    
    # Transformamos los datos en codificacion one hot y los guardamos en pickle
    one_hot_list = to_one_hot_list(data,max_ips,max_ports,args.verbose)

    if args.count:
        count = to_one_hot_count(one_hot_list,max_ips,max_ports)
        if args.verbose:
            print('Count generado')
        to_pickle([count],max_ips,max_ports,'./Pickle')
    else:
        to_pickle(one_hot_list,max_ips,max_ports,'./Pickle')
    return

if __name__ == "__main__":
    main()