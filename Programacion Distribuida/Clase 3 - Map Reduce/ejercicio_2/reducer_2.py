import sys

subproblema = None
fare_minimo = None

for claveValor in sys.stdin:
	sex, fare = claveValor.split("\t", 1)
	
	#El primer subproblema es el primer sex de reducer	
	if subproblema == None:
		subproblema = sex
		fare_minimo = fare

	#si el sex es del subproblema actual, comprobamos si es el fare minimo
	if subproblema == sex:
		if fare < fare_minimo:
			fare_minimo = fare
	else: #si ya acabamos con el subproblema, emitimos    
		print("%s\t%s" % (subproblema, fare_minimo))
        
		#Pasamos al siguiente subproblema
		subproblema = sex
		fare_minimo = fare

#el anterior bucle no emite el ultimo subproblema    
print("%s\t%s" % (subproblema, fare_minimo))