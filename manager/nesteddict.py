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

def mergenesteddicts(base, overrides):
    """ Merges OVERRIDES into BASE, overriding properties where necessary. If
    additional dictionaries are contained as values, they are recursively
    merged. """
    for key in overrides:
        if key in base and isinstance(base[key], dict) and isinstance(overrides[key], dict):
                mergenesteddicts(base[key], overrides[key])
        else:
            base[key] = overrides[key]

def deepequals(dict1, dict2):
    keys1 = dict1.keys()
    keys2 = dict2.keys()
    if keys1 == keys2:
        for key in keys1:
            if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                if not deepequals(dict1[key], dict2[key]):
                    return False
            elif dict1[key] != dict2[key]:
                return False
        return True
    else:
        return False

def deepcopy(dictionary):
    newdict = {}
    for key in dictionary:
        if isinstance(dictionary[key], dict):
            newdict[key] = deepcopy(dictionary[key])
        else:
            newdict[key] = dictionary[key]
    return newdict
