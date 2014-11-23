def makenested(dictionary, separator):
    """ Converts a dictionary consisting of key-value pair into a nested
    structure. All keys must be strings and no values may be dicts."""
    keys = dictionary.keys()
    for key in keys:
        if separator in key:
            value = dictionary[key]
            del dictionary[key]
            index = key.index(separator)
            parent = key[:index]
            childkey = key[index + 1:]
            if parent not in dictionary:
                dictionary[parent] = {}
            dictionary[parent][childkey] = value
    for key in dictionary:
        if isinstance(dictionary[key], dict):
            makenested(dictionary[key], separator)

def makeflat(dictionary, separator):
    keys = dictionary.keys()
    for key in keys:
        if isinstance(dictionary[key], dict):
            makeflat(dictionary[key], separator)
            for childkey in dictionary[key]:
                dictionary[key + separator + childkey] = dictionary[key][childkey]
            del dictionary[key]
