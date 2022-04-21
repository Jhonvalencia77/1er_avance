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

        data = data[data.distancia >= "002-005"]

        columnas=["19024","19046_AM","19053_AM","19058_AM","19071_AM","19086_AM","1913001","1913002","1913003","1913004","1913005","19151_AM","19156_AM","19160_AM","19171","19190_AM","19192_AM","19220_AM","19245_AM","19257_AM","19274_AM","19280_AM","19319","19326_AM","19331_AM","28002","28004","2800501","2800502","2800503","2800504","2800505","2800601","2800701","2800702","2800703","2800704","28008_AM","28009","28010","2801301","2801302","2801303","2801304","28014","28015","28018","28022","28023","28026","28027_AM","28030_AM","28031_AM","28032_AM","28033","28038_AM","28040","28041","28043","28044","28045","28046","28047","2804901","2804902","2804903_AD","28050_AM","28052_AM","28053","28054","2805801","2805802","2805803","2805804","2805805","2805806","2805807","2805808","2805809","28059","28060_AM","28061","2806501","2806502","2806503","2806504","28066","28067_AM","28068","28072","28073","2807401","2807402","2807403","2807404","2807405","2807406","2807407","28075","2807901","2807902","2807903","2807904","2807905","2807906","2807907","2807908","2807909","2807910","2807911","2807912","2807913","2807914","2807915","2807916","2807917","2807918","2807919","2807920","2807921","2808001","2808002","28082","28083","28084","28085_AM","28086","28087","28089","28090","28091","2809201","2809202","2809203","2809204","28095_AM","28096","28099_AM","28100","28104","2810601","28108","28110_AM","2811301","2811302","2811501","2811502","28119_AM","2812301","28125_AM","2812701","28129","28130","28131","28132","28133_AM","2813401","28137_AM","28140_AM","28141","28144","28145_AM","2814801","2814802","2814803","2814804","28149","28150","28151_AM","28152","28154","28160","2816101","2816102","28162_AM","28164","28165_AM","28167","28171","28172","28176","28177","28180","28181","28901_AM","28903"]
        filtrado = pd.DataFrame({})
        for col in columnas:
            filtrar = data.loc[data.loc[:,'origen'] == col]
            filtrado=filtrado.append(filtrar)

        print(filtrado.shape)

        #table_o = data.pivot_table("viajes", index="periodo", columns=["origen"], aggfunc=np.sum)

        table_o = filtrado.pivot_table("viajes", index="periodo",aggfunc=np.sum)
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
end = "20201031"
timeseries_o = concatenar(start,end)
#print(timeseries_o)

timeseries_o.to_csv("periodoviajes24h_Madrid.csv")
