def flatten_dict(structure):
    output={}
    for k, v in structure.items():
      if isinstance(v, dict):
        tmp = flatten_dict(v)
        for k1, v1 in tmp.items():
          new_key = k + '.' + k1
          output[new_key] = v1
      else:
        output[k] = v
    return output