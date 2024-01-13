 #!/usr/bin/python3

import sys 

from pyspark.sql import SparkSession 
 
#inicializacion 
spark = SparkSession.builder.appName('personasGastosConTarjetaCredito').getOrCreate()  

entrada = sys.argv[1] 
salida = sys.argv[2] 

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

#obtenemos el dinero gastado con tarjeta de crédito en cada compra
def dineroConTarjeta (record):
  nombre, formaPago, dinero = record.split(";", 2)
  if (formaPago == "Tarjeta de crédito"):
    return (nombre, int(dinero))
  else:
    return (nombre, 0)

mapeo = datosEntrada.map(dineroConTarjeta)

#sumamos el dinero pagado con tarjeta por persona
suma = mapeo.reduceByKey (lambda x, y: x+y)
  
#guardamos la salida 
suma.saveAsTextFile(salida) 