import os, json, sys

in_file = sys.argv[1]
cmap_key = sys.argv[2]

with open(in_file, 'r') as f:
    cmap = json.load(f)[cmap_key]


def hasDecimal(cur_bin):
    minv = cur_bin.get('minValue')
    maxv = cur_bin.get('maxValue')
    if minv and '.' in minv:
        return True
    if maxv and '.' in maxv:
        return True
    return False


new_bins = []

for cur_bin in cmap:
    float_type = hasDecimal(cur_bin)
    new_bin = {}
    if float_type:
        new_bin.update(
            {k: float(cur_bin[k])
             for k in 'minValue maxValue'.split()})
    else:
        new_bins.append(
            {k: int(cur_bin[k])
             for k in 'minValue maxValue'.split()})
    new_bin.update({k: int(cur_bin[k]) for k in 'r g b'.split()})
    new_bin['description'] = cur_bin['description']
    new_bins.append(
        {k: new_bin[k]
         for k in 'minValue maxValue r g b description'.split()})

with open(in_file, 'w') as f:
    out = {cmap_key: new_bins}
    f.write(json.dumps(out, indent=4))
