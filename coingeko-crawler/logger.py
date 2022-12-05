import logging
import logging.handlers
import os


handler = logging.handlers.RotatingFileHandler(\
    filename=os.path.join(os.path.dirname(__file__), 'log/test.log'))

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(handler)

try:
    1 / 0
except Exception as e:
    logging.exception(e)
    exit(1)
