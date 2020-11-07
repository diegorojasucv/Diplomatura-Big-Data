import sys

# Nos quedamos con el sex y fare
for line in sys.stdin:
    line = line.strip()
    survived, pclass, name, sex, age, siblings, parents, fare  = line.split(",")
    print("%s\t%s" % (sex, fare))