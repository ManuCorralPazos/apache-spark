#!/usr/bin/python3

import sys, os

from pyspark.sql import SparkSession 
 
#inicializacion 
spark = SparkSession.builder.appName('personasGastosConTarjetaCredito').getOrCreate()  

entrada = sys.argv[1] 
salida = sys.argv[2] 

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(os.path.join(entrada, "[0-9]*.txt"))


#obtenemos la categoria y las visitas de cada entrada
def categoriaYVisitas (record):
  video,user,ndias,categoria,longitud,nvisitas,resto = record.split("\t", 6)
  return (categoria, int(nvisitas))

mapeo = datosEntrada.map(categoriaYVisitas)

#sumamos las visitas de cada categoria
suma = mapeo.reduceByKey (lambda x, y: x+y)

#calculamos la categoria con el minimo de visitas

minimo = suma.reduceByKey(lambda x, y: min(x, y)).min(lambda x: x[1])

finalRDD = spark.sparkContext.parallelize(minimo)

finalRDD.coalesce(1).saveAsTextFile(salida)
#guardamos la salida 
#minimo.saveAsTextFile(salida) 