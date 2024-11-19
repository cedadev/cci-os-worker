# Logger setup
import logging

logging.basicConfig(level=logging.DEBUG)
logstream = logging.StreamHandler()

formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
logstream.setFormatter(formatter)
