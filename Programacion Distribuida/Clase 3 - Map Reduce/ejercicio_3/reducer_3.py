#!/usr/bin/python
import sys

curr_word = None
curr_count = 0
word = None

for line in sys.stdin:
    word, count = line.split("\t") 
    
    count = int(count)
    
    if curr_word == word:
      curr_count += count
    else:    
      if curr_word:
        print("%s\t%s" % (curr_word, curr_count))
      curr_word = word
      curr_count = count

if curr_word == word:
    print ('{0}\t{1}'.format(curr_word, curr_count))