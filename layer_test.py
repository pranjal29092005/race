#!/usr/bin/env ipython
import er_db
# query to get exposures with layer custom enums
# select * from "getAllExposuresWithCustomLayersNFR"()

layers = pd.read_csv(os.path.join(dl_dir,
                                  'intLayers.csv')).astype({
                                      'out_audit_id': 'int64',
                                      'out_portfolio_id': 'int64'
                                  })

with er_db.get_db_conn('Integration') as conn:
    for idx, row in layers.iterrows():
        aid = row['out_audit_id']
        pid = row['out_portfolio_id']
        query = f'select * from f_layer_{aid}'
        res = pd.read_sql(query, conn)
        if 'layer_custom_enum_1' in res.columns:
            len_items = len(res.loc[res['layer_custom_enum_1'].str.len() > 0])
            if len_items > 0:
                print(f'{pid}, {len_items}')
        # cursor = conn.cursor(cursor_factory=DictCursor)
        # cursor.execute(query)
        # res = cursor.fetchall()
        # print(res)

# import pandas as pd, json
# import os, sys

# sys.path.insert(0, '/home/surya/Projects/SourceArea/race/Helpers')
# os.chdir('/home/surya/Projects/SourceArea/race/Helpers')
# import er_db
# from psycopg2.extras import DictCursor
