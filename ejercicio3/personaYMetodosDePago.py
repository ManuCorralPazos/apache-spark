#!/usr/bin/python3

import sys

from pyspark.sql import SparkSession 
 
#inicializacion 
spark = SparkSession.builder.appName('personaYMetodosDePago').getOrCreate()  

entrada = sys.argv[1] 
salida1 = sys.argv[2] 
salida2 = sys.argv[3]

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

#obtenemos la cantidad de pagos sin tdc mayores de 1500
def pagoSinTDCMayorDe1500 (record):
  nombre, formaPago, dinero = record.split(";", 2)
  if (formaPago == "Tarjeta de crédito"):
    return (nombre, 0)
  else:
    if (int(dinero) > 1500):
      return (nombre, 1)
    else:
      return (nombre, 0)

#obtenemos la cantidad de pagos sin tdc menores/iguales a 1500
def pagoSinTDCMenorIgualDe1500 (record):
  nombre, formaPago, dinero = record.split(";", 2)
  if (formaPago == "Tarjeta de crédito"):
    return (nombre, 0)
  else:
    if (int(dinero) <= 1500):
      return (nombre, 1)
    else:
      return (nombre, 0)

sinTDCMayorDe1500 = datosEntrada.map(pagoSinTDCMayorDe1500)

sinTDCMenorIgualDe1500 = datosEntrada.map(pagoSinTDCMenorIgualDe1500)

#sumamos los pagos mayores de 1500 sin tdc
sumaMayorDe1500 = sinTDCMayorDe1500.reduceByKey (lambda x, y: x+y)
#sumamos los pagos menores/iguales a 1500 sin tdc
sumaMenorIgualDe1500 = sinTDCMenorIgualDe1500.reduceByKey (lambda x, y: x+y)

#guardamos la salida 
sumaMayorDe1500.map(lambda x: ';'.join(map(str, x))).saveAsTextFile(salida1)
sumaMenorIgualDe1500.map(lambda x: ';'.join(map(str, x))).saveAsTextFile(salida2)





