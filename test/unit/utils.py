from inspect import signature


def call_method(obj, name: str):
    """
    Helper function to call any method of a class
    providing `None` instead of every parameter.
    """
    method = getattr(obj, name)
    func_sign = signature(method)
    return method(*(
        None for _ in range(len(func_sign.parameters))
    ))
