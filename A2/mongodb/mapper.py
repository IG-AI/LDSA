#!/usr/bin/env python3
"""mapper.py"""

import re

def mapper(collection):
    output = []
    # input comes from STDIN (standard input)
    for document in collection:
        # remove leading and trailing whitespace
        document = str(document['text']).strip()
        # split the line into words
        words = document.split()
        # Creates a regex that looks for everything that's not a swedish letter.
        regex = re.compile('[^a-öA-Ö]')
        # The sets a list with pronouns that should be search for.
        pronouns_list = ["han", "hon", "denna", "det", "denne", "den", "hen"]
        for word in words:
            # Splits the word into a list of words on the occurrence of the regex
            words_split = re.split(regex, word)
            # Loops through the split word list and looks for lower case occurrence of in the pronouns list
            # If it finds a occurrence then it writes the result togheter with the amount of occurrence to STDOUT
            for split_word in words_split:
                if split_word.lower() in pronouns_list:
                    output.append("%s" % (split_word.lower()))

    return output


