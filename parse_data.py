import pandas as pd
import numpy as np
import pickle
import bz2
import argparse
import sys
import multiprocessing

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
    temp = pd.concat(lista,ignore_index=True)
    # Obtenemos todas las ips de todos los datos
    ips = np.append(temp['source_address'],temp['destination_address'])
    # Obtenemos la cantidad de ips que hay en total
    unique, counts = np.unique(ips,return_counts=True)
    count_data = np.asarray((unique, counts)).T
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
    # Obtenemos las ips con mas valores
    busqueda = ordered[np.where(ordered[:,1] > 20000 * len(lista))[0],0]
    # Obtenemos los puertos que corresponden a estas ips y seleccionamos 
    # los que mas aparecen de la misma forma
    L = []
    for item in busqueda:
        L.append(temp.loc[temp.source_address == item])
        L.append(temp.loc[temp.destination_address == item])
    result = pd.concat(L)
    result = result.drop_duplicates()
    ports = np.append(result['source_port'],result['dest_port'])
    uniquep, countsp = np.unique(ports,return_counts=True)
    uniquep = uniquep.astype('float64')
    uniquep = uniquep.astype('int64')
    count_ports = np.asarray((uniquep, countsp)).T
    count_orderp = np.sort(count_ports[:,-1])
    count_orderp = np.unique(count_orderp)[::-1]
    orderedp = np.array([])
    for i in count_orderp:
        index = np.where(count_ports[:,-1] == i)[0]
        for j in index:
            orderedp = np.append(orderedp,count_ports[j,:],axis=0)
    orderedp.resize((int(orderedp.shape[0]/2),2))
    orderedp = orderedp.astype('int64')
    max_ips = ordered[np.where(ordered[:,1] > 20000 * len(lista))[0],0]
    max_ports = orderedp[:np.where(orderedp[:,1] >= 1000 * len(lista))[0][-1],0]
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
    for i in range(num_filas):
        fila = dataframe.iloc[i].values[1:].astype(str)
        try:
            indice_1 = np.where(max_ips == fila[0])[0][0]
        except:
            indice_1 = max_ips.shape[0]

        try:
            indice_2 = np.where(max_ports == fila[2])[0][0]
        except:
            indice_2 = max_ports.shape[0]
        indice_2 = indice_2 + max_ips.shape[0] + 1

        try:
            indice_3 = np.where(max_ips == fila[1])[0][0]
        except:
            indice_3 = max_ips.shape[0]
        indice_3 = indice_3 + max_ips.shape[0] + 1 + max_ports.shape[0] + 1

        try:
            indice_4 = np.where(max_ports == fila[3])[0][0]
        except:
            indice_4 = max_ports.shape[0]
        indice_4 = indice_4 + max_ips.shape[0] + 1 + max_ports.shape[0] + 1 + max_ips.shape[0] + 1

        res[i,np.array([indice_1,indice_2,indice_3,indice_4])] = 1
        
        fecha = dataframe.iloc[i].values[0]
        fecha = fecha.replace('-','')
        fecha = fecha.replace('T','')
        fecha = fecha.replace(':','')
        fecha = int(fecha)
        res[i,-1] = fecha

    return res

"""
Funcion to_one_hot_list

Transforma una lista dataframe de pandas en una lista de codificacción one hot 

Entrada: lista de dataframes, ips a utilizar y puertos a utilizar

Salida: lista de matrices de one hot

"""
def to_one_hot_list(lista,max_ips,max_ports,verbose):
    res = []
    for item in lista:
        if verbose:
            print('Codificando: ' + item.values[0,0])
        res.append(to_one_hot(item,max_ips,max_ports))
    return res

"""
Funcion to_one_hot_count

Transforma un one_hot del creado anteriormente y cuenta el numero de unos por cada ip 

Entrada: one hot, ips a utilizar 

Salida: matriz de one hot

"""
def to_one_hot_count(one_hot,max_ips):
    one_hot_count = np.zeros(shape=(len(max_ips) + 1,len(max_ips) + 2),dtype=int)
    counts = np.count_nonzero(one_hot == 1,axis=0)[:len(max_ips) + 1]
    for i in range(len(max_ips) + 1):
        one_hot_count[i,i] = 1
        one_hot_count[i,-1] = counts[i]
    return one_hot_count

"""
Funcion to_pickle

Transforma un lista de datos en archvios pickle en el path indicado

Entrada: lista, path donde guardar los pickles

"""
def to_pickle(lista,max_ips,max_ports,path):
    for i in range(len(lista)):
        filename = path + str(i) + '.bz2'
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


"""
Funcion multiprocessing_func

Ejecuta el programa principal en formato multiprocessing

Entrada: dataframe, max_ips, max_ports, path donde se guardaran los pickles

"""
def multiprocessing_func(dataframe,max_ips,max_ports,path):
    one_hot = to_one_hot(dataframe,max_ips,max_ports)
    filename = path + str(dataframe.values[0,0]) + '.bz2'
    outfile = bz2.BZ2File(filename, 'w')
    pickle.dump(one_hot,outfile)
    outfile.close()

def main():
    # Parseo de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--archivos", type=str, nargs='+',
                        help="lista de rutas de los archivos")
    parser.add_argument("-f","--fichero", type=str,
                        help="ruta del fichero con la lista de archivos separados por salto de linea")
    parser.add_argument("-m","--multiprocessing", help="ejecuta el programa en múltiples procesadores",
                        action="store_true")   
    parser.add_argument("-v","--verbose", help="imprime por pantalla como va funcionando el programa",
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
        for item in args.archivos:
            data.append(to_pandas(item))

    if args.fichero:
        paths = open(args.fichero,'r')
        for item in paths.readlines():
            data.append(to_pandas(item.strip('\n')))
        paths.close()
    if args.verbose:
        print('Datos leidos')

    # Obtenemos las ips y puertos
    max_ips, max_ports = get_ips_ports(data)
    if args.verbose:
        print('Ips y Puertos seleccionados')

    # Si activamos el multiprocesamiento lo ejecutamos de esa forma
    if args.multiprocessing:
        processes = []
        for item in data:
            p = multiprocessing.Process(target=multiprocessing_func, args=(item,max_ips,max_ports,'./Pickle'))
            processes.append(p)
            p.start()
        
        for process in processes:
            process.join()
        
        print('One hot y pickle creado para todos los dataframes')
        filename = './Pickle_max_ips.bz2'
        outfile = bz2.BZ2File(filename, 'w')
        pickle.dump(max_ips,outfile)
        outfile.close()

        filename = './Pickle_max_ports.bz2'
        outfile = bz2.BZ2File(filename, 'w')
        pickle.dump(max_ports,outfile)
        outfile.close()

        return

    # Transformamos los datos en codificacion one hot
    one_hot_list = to_one_hot_list(data,max_ips,max_ports,args.verbose)
    if args.verbose:
        print('One hot creado para todos los dataframes')

    # Por ultimo lo pasamos a pickle y lo guardamos en la misma carpeta de la ejecucion
    to_pickle(one_hot_list,max_ips,max_ports,'./Pickle')
    if args.verbose:
        print('Datos guardados en pickle')
    return

if __name__ == "__main__":
    main()