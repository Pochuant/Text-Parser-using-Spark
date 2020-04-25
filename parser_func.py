dict_output = {}
list_output = []

class parser_func:
  def flatten_dict(input_dic):
    for k,v in input_dic.items():
      if isinstance(v, str):
        dict_output[k] = v
      elif isinstance(v, dict):
        parser_func.flatten_dict({k+'.'+ i:j for i,j in v.items()})
      elif isinstance(v, list):
        dict_output[k] = []
        for listelement in v:
          if isinstance(listelement,str):
            dict_output[k].append(listelement)
          elif isinstance(listelement, dict):
            parser_func.flatten_dict({k+'.'+ i : j for i,j in listelement.items()})


  def flatten_list(input_list):
    for i in input_list:
      if isinstance(i, int):
        list_output.append(i)
      elif isinstance(i, list):
        parser_func.flatten_list(i)