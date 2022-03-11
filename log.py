
import logging

def setup_logger(name):
    l = logging.getLogger(name)
    l.setLevel(logging.INFO)
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    l.addHandler(h)

    return l
