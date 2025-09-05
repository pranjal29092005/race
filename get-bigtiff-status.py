import er_db, er_utils
import pandas as pd

if __name__ == '__main__':
    stmt = 'select "ID",filename,status,"ImportSetName" from race.c_audit where userid in (5, 596) order by "ID" desc limit 10'
    with er_db.get_db_conn('prod') as conn:
        df = pd.read_sql(stmt, conn)
        er_utils.tabulate_df(df)
