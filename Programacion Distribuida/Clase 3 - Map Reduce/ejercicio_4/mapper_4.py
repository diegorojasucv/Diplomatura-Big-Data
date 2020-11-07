import sys

# Nos quedamos con el age y fare
for line in sys.stdin:
    line = line.strip()
    survived, pclass, name, sex, age, siblings, parents, fare  = line.split(",")
    print("%s\t%s\t%s" % (age, fare, '1'))


