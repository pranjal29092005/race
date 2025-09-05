import os, sys
import er_db
import shutil

cur_dir = os.path.dirname(os.path.abspath(__file__))
keys = [s.lower() for s in er_db.get_db_keys()]
rscript = shutil.which('Rscript')
