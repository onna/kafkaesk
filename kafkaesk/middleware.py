import functools


def m1(ob, handler):
    ob1= handler(ob)
    ob1["1"] = 1
    return ob1

def m2(ob, handler):
    return handler(ob)

def handler(ob):
    return {"ob": ob}

def execute(ob):
    h = handler
    for m in [m1, m2]:
        h = functools.partial(m, handler=h)

    return h(ob)
