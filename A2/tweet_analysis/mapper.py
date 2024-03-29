#!/usr/bin/env python3
"""mapper.py"""

import sys, re, json

"""
Reads input from STDIN and loads each line with the attribute text (which should be a dict) as a json-file if the line 
is none empty. This files is then added to a list. Then the duplications are removed from the list. 
"""
temp_data = []
for line in sys.stdin:
    if not line.isspace():
        data = json.loads(line)
        temp_data.append(data["text"])

temp_data = set(temp_data)

# Creates a regex that looks for everything that's not a swedish letter.
regex = re.compile('[^a-öA-Ö]')
pronouns_list = ["han", "hon", "denna", "det", "denne", "den", "hen"]
for line in temp_data:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
   
    # The sets a list with pronouns that should be search for.
    for word in words:
        # Splits the word into a list of words on the occurrence of the regex
        words_split = re.split(regex, word)
        """
        Loops through the split word list and looks for lower case occurrence of in the pronouns list. If it finds a 
        occurrence then it writes the result together with the amount of occurrence to STDOUT.
        """
        for split_word in words_split:
            if split_word.lower() in pronouns_list: 
                print("%s\t%s" % (split_word.lower(), 1))
                
                
