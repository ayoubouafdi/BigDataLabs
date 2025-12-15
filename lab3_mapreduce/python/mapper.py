#!/usr/bin/env python3
import sys

# Le mapper lit depuis l'entrée standard (STDIN)
for line in sys.stdin:
    # Supprimer les espaces au début et à la fin
    line = line.strip()
    # Découper la ligne en mots
    words = line.split()
    
    # Émettre chaque mot avec le nombre 1
    for word in words:
        # Le format est : clé \t valeur
        print('%s\t%s' % (word, 1))