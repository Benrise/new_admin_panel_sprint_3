import os
import logging
from logging.handlers import RotatingFileHandler

logs_dir = './logs'

if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('etl_python_process')

fh = RotatingFileHandler(
    os.path.join(logs_dir, 'etl_logs.log'),
    maxBytes=20_000_000,
    backupCount=5 
)

formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)-16s:%(lineno)-5d] %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)
