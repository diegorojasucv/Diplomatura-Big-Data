#!/usr/bin/python
import sys

curr_word = None
curr_fare = 0
curr_count = 0
word = None

for line in sys.stdin:
    word, fare, count = line.split("\t") 
    
    # Corregimos el formato del precio
    fare = fare.replace("\n", "")
    fare = float(fare)

    # pasamos los count a enteros
    count = int(count)
    
    if curr_word == word:
      curr_fare += fare
      curr_count += count
    else:    
      if curr_word:
        print("%s\t%s" % (curr_word, (curr_fare / curr_count)))
      curr_word = word
      curr_fare = fare
      curr_count = count

if curr_word == word:
    print("%s\t%s" % (curr_word, (curr_fare / curr_count)))