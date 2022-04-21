import pandas as pd
import numpy as np

#Creamos la función
def concatenar(start,end,verbose=True):
    #Decimos la ruta de los archivos
    #basepath = "/media/jonathan/JHON/Tesis/DatosTesis/raw_data/%.4d%.2d%.2d_maestra_1_mitma_distrito.csv"
    basepath = "/media/jonathan/JHON/Tesis/DatosTesis/MatrizDeViajes/Distritos/Periodos/%.4d%.2d%.2d_maestra_1_mitma_distrito.csv"
    #Definimos una lista con las fehas de inicio y final que se analizarán, esta lista se guardará en "dates"
    dates = pd.date_range(start, end)

    #El simbolo % es el operador de modulo(residuo de un problema de división)
    #El simbolo % en cadenas compara si dos textos son iguales y arroja un True or False(Carro=Caro->False)
    #Se hace un ciclo for para capturar cada timestamp y luego se pasa cada una de las fechas al basepath
    paths = [basepath % (timestamp.year, timestamp.month, timestamp.day) for timestamp in dates]

    n = 0

    #Creamos 2 dataframe vacios
    timeseries_o = pd.DataFrame({})
    timeseries_d = pd.DataFrame({})

    #Hacemos un recorrido por cada uno de los paths (nombre completo del archivo)
    for path in paths:

        #Verifica si la variable de función "verbose esta en True", Si es asi entonces imprime el porcentaje de archivos concatenados
        if verbose: print(path, "%.2f%%" % (float(n) * 100 / len(paths)))

        #Leemos cada uno de los archivos csv y lo guardamos en la variable "data"
        #Le datos a las columnas de fEcha, origen y destino un formato tipo string

        data = pd.read_csv(path, dtype={"fecha": str,
                                        "origen": str,
                                        "destino": str}, sep='|')


        #Acotamos los datos y solo utlizamos aquellos datos que tienen una distancia mayor a 002-005
        #data = data[data.distancia >= "010-050"]
        #print(data)

        data = data[data.distancia >= "002-005"]

        #El método pivot_table permite reorganizar en forma de tabla un dataframe,las columnas de esta tabla
        #ahora son los distritos de "origen", el periodo (hora del dia) ahora es el indice y representa las filas
        #de la tabla y los datos que se presentan corresponden al # de viajes en determinada hora del día (00-23)
        #dependiendo del distrito de origen.
        #table_o = data.pivot_table("viajes", index="periodo", columns=["origen"], aggfunc=np.sum)



        table_o = data.pivot_table("viajes", index="periodo",aggfunc=np.sum)
        #print(table_o)


        #Cambiamos a tipo flotante los valores de la tabla creada
        table_o = table_o.astype(np.float32)

        #Consulta la forma del indice, en este caso el indice es la variable periodo (00-23) tiene que dar 24,
        #por las 24 horas del día.
        #Se hace para proteger de una excepción si el distrito en cuestion no tiene registros de viajes

        if table_o.index.shape[0] != 24:
            j = 0
            for i in range(24):
                if table_o.index[i] != (i+j):
                    print (data.fecha.iloc[0],i+j,"origen sin registros!")
                    table_o.loc[i+j] = 0
                    j += 1
                if (i + j) == 23: break
            table_o = table_o.sort_index() #sort_index devuelve un nuevo datafRame ordenado por etiqueta si la table es False, de lo contrario actualiza la tabla y retorna un NONE

        #Crea una nueva columa "ds" con la fecha, fecha.iloc[0] devuelve "20200214"-"20200215"... y la hora en formato 24h
        table_o["ds"] = pd.date_range(data.fecha.iloc[0], "%s 23:00:00" % (data.fecha.iloc[0]), freq="H")

        #Fija como indice la columna ds
        table_o = table_o.set_index("ds")

        #los datos que no tengan registros y aparezcan como NaN el metodo fillna los pondrá en 0
        table_o = table_o.fillna(0)

        #Añade los datos de los diferentes archivos a una sola variable llamada timeseries_o
        timeseries_o = timeseries_o.append(table_o)


    return timeseries_o


start = "20200214"
#end = "20200215"
end = "20201031"
timeseries_o = concatenar(start,end)
#print(timeseries_o)

timeseries_o.to_csv("periodoviajes24h.csv")
#timeseries_d.to_csv("2dointento_d.csv")
