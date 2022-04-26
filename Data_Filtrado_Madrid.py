import pandas as pd
import numpy as np
import time
import ray

ray.init()
#Creamos la función

def Añadir_Paths(start,end):
    basepath = "/media/jonathan/JHON/Tesis/DatosTesis/MatrizDeViajes/Distritos/Periodos/%.4d%.2d%.2d_maestra_1_mitma_distrito.csv"
    dates = pd.date_range(start, end)
    paths = [basepath % (timestamp.year, timestamp.month, timestamp.day) for timestamp in dates]
    return paths


@ray.remote
def fun_filtrar(path,verbose=True):
    global timeseries_o
    n = 0
    #Verifica si la variable de función "verbose esta en True", Si es asi entonces imprime el porcentaje de archivos concatenados
    if verbose: print(path, "%.2f%%" % (float(n) * 100 / len(Rutas)))

    data = pd.read_csv(path, dtype={"fecha": str,
                                    "origen": str,
                                    "destino": str}, sep='|',index_col=0)

    data = data[data.distancia >= "002-005"]


    columnas=["19024","19046_AM","19053_AM","19058_AM","19071_AM","19086_AM","1913001","1913002","1913003","1913004","1913005","19151_AM","19156_AM","19160_AM","19171","19190_AM","19192_AM","19220_AM","19245_AM","19257_AM","19274_AM","19280_AM","19319","19326_AM","19331_AM","28002","28004","2800501","2800502","2800503","2800504","2800505","2800601","2800701","2800702","2800703","2800704","28008_AM","28009","28010","2801301","2801302","2801303","2801304","28014","28015","28018","28022","28023","28026","28027_AM","28030_AM","28031_AM","28032_AM","28033","28038_AM","28040","28041","28043","28044","28045","28046","28047","2804901","2804902","2804903_AD","28050_AM","28052_AM","28053","28054","2805801","2805802","2805803","2805804","2805805","2805806","2805807","2805808","2805809","28059","28060_AM","28061","2806501","2806502","2806503","2806504","28066","28067_AM","28068","28072","28073","2807401","2807402","2807403","2807404","2807405","2807406","2807407","28075","2807901","2807902","2807903","2807904","2807905","2807906","2807907","2807908","2807909","2807910","2807911","2807912","2807913","2807914","2807915","2807916","2807917","2807918","2807919","2807920","2807921","2808001","2808002","28082","28083","28084","28085_AM","28086","28087","28089","28090","28091","2809201","2809202","2809203","2809204","28095_AM","28096","28099_AM","28100","28104","2810601","28108","28110_AM","2811301","2811302","2811501","2811502","28119_AM","2812301","28125_AM","2812701","28129","28130","28131","28132","28133_AM","2813401","28137_AM","28140_AM","28141","28144","28145_AM","2814801","2814802","2814803","2814804","28149","28150","28151_AM","28152","28154","28160","2816101","2816102","28162_AM","28164","28165_AM","28167","28171","28172","28176","28177","28180","28181","28901_AM","28903"]

    filtrado = pd.DataFrame({})
    for col in columnas:
        filtrar = data.loc[data.loc[:,'origen'] == col]
        #print(filtrar.head(10))
        filtrado=filtrado.append(filtrar)


    return filtrado

start = "20200214"
end = "20200215"

timeseries_o = []
filtrado2 = pd.DataFrame({})


Rutas = Añadir_Paths(start,end)

for path in Rutas:
    timeseries_o.append(fun_filtrar.remote(path))

VarRay = ray.get(timeseries_o)

for df in VarRay:
    filtrado2 = filtrado2.append(df)

filtrado2.to_csv("DataFiltrado_Madrid3.csv")
