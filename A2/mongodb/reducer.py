#!/usr/bin/env python3
"""reducer.py"""


def reducer(input):
    current_word = None
    current_count = 0
    word = None
    output = ""
    # input comes from STDIN
    input.sort()
    count = 1
    for word in input:
        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_word == word:
            count += 1
        else:
            if current_word:
                # write result to STDOUT
                output = output + ('%s\t%s\n' % (current_word, count))
            count = 1
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word:
        output = output + ('%s\t%s\n' % (current_word, count))

    return output