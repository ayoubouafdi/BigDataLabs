#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

# Le reducer lit depuis l'entrée standard
for line in sys.stdin:
    line = line.strip()

    # Récupérer la clé et la valeur
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        # Ignorer les lignes mal formées
        continue

    # Hadoop trie les clés avant de les envoyer au reducer.
    # Si le mot change, on affiche le résultat du mot précédent.
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Écrire le résultat pour le mot précédent
            print('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# Ne pas oublier d'afficher le dernier mot
if current_word == word:
    print('%s\t%s' % (current_word, current_count))