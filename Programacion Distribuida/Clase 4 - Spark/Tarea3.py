#######################################
################ TAREA 3 ##############
#######################################

#######################################
####### OPERACIONES BASICAS ###########
#######################################

# Ejercicio 1.

textRDD = spark.sparkContext.textFile("/user/tarea3/datos/war-and-peace.txt")

# Ejercicio 2.

textRDD.count()

# Ejercicio 3.

textRDD.take(10)

# Ejercicio 4.

#Verificar el split por palabras
textRDD.flatMap(lambda line: line.split(" ")).take(20) 

textRDD.flatMap(lambda line: line.split(" ")).count()

#######################################
############## TITANIC ################
#######################################

# Ejercico 1.

titanictext = spark.sparkContext.textFile("/user/tarea3/datos/titanic.csv")

# Ejercicio 2

titanic2 = titanictext.map(lambda line: line.split(",")) 

titanic3 = titanic2.map(lambda tuple: (tuple[3], tuple[5])) 

max_fare_by_sex = titanic3.reduceByKey(lambda x,y: x if x>y else y)

# Resultado
[(u'male', u'91.0792'), (u'female', u'93.5')]

# Ejercicio 3
min_fare_by_sex = titanic3.reduceByKey(lambda x,y: x if x<y else y)

# Resultado
[(u'male', u'0.0'), (u'female', u'10.4625')]

# Ejercicio 4

titanic4 = titanic2.map(lambda tuple: (tuple[3]+'_'+tuple[1], 1))
count_by_sex_survived = titanic4.reduceByKey(lambda x,y: x + y)

# Resultado
[(u'male_0', 468),
 (u'female_1', 233),
 (u'female_0', 81),
 (u'male_1', 109)]

# Ejercicio 5

#punto a)

titanic_map_5_a = titanic2.map(lambda tuple: (tuple[3], float(tuple[5]), 1))

average_by_sex = titanic_map_5_a.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+ y[0], x[1]+y[1])).mapValues(lambda x: round(x[0]/x[1],2))

# Otra manera de hacer el group by utilizando map en vez mapValues
average_by_sex = titanic_map_5_a.map(lambda x: (x[0], (x[1],1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (str(x[0]),x[1][0]/x[1][1]))

average_by_sex.take(10)

# Resultado
[(u'male', 25.52), (u'female', 44.48)]

#punto b)

titanic_map_5_b = titanic2.map(lambda tuple: (tuple[2], float(tuple[5]), 1))

average_by_pclass = titanic_map_5_b.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+ y[0], x[1]+y[1])).mapValues(lambda x: round(x[0]/x[1],2))

average_by_pclass.take(10)

# Resultado
[(u'1', 84.15), (u'3', 13.68), (u'2', 20.66)]


#######################################
############## MOVIELENS ##############
#######################################

# Ejercicio 1.
moviesRDD = spark.sparkContext.textFile("/user/tarea3/datos/ratings.csv")
moviesRDD2 = moviesRDD.map(lambda line: line.split(",")) 
moviesRDD2.take(10)

# Resultado


# Ejercicio 2.
movies_map = moviesRDD2.map(lambda tuple: (tuple[0], tuple[2]))
average_by_id = movies_map.mapValues(lambda x: (int(x),1)).reduceByKey(lambda x, y: (x[0]+ y[0], x[1]+y[1])).mapValues(lambda x: float(x[0]/x[1]))
average_by_id.take(10)

# Resultado

# Ejercicio 3.
movies_map = moviesRDD2.map(lambda tuple: (tuple[3], 1))
count_by_day = movies_map.reduceByKey(lambda x,y: x + y)
count_by_day_sort = count_by_day.sortBy(lambda tuple: tuple[1], False)
count_by_day_sort.take(10)

# Resultado
 




