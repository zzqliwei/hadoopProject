
def get_rdd_result(name, rdd):
    return "{0} = {1}".format(name,rdd.collect())