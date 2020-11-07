import sys

# Nos quedamos con el sex y age de los que sobrevivieron
for line in sys.stdin:
    line = line.strip()
    survived, pclass, name, sex, age, siblings, parents, fare  = line.split(",")
    # Filtramos solo los que sobrevivieron
    if survived == '1':
        print("%s\t%s" % (sex + ' ' + age, survived))


