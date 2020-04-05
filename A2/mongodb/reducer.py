#!/usr/bin/env python3
"""reducer.py"""


def reducer(input):
    current_word = None
    current_count = 0
    word = None
    output = ""
    # input comes from STDIN
    for line in input:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper.py
        word, count = line.split('\t', 1)

        # convert count (currently a string) to int
        try:
            count = int(count)
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            continue

        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_word == word:
            current_count += count
        else:
            if current_word:
                # write result to STDOUT
                output = output + ('%s\t%s\n' % (current_word, current_count))
            current_count = count
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word:
        output = output + ('%s\t%s\n' % (current_word, current_count))

    return output