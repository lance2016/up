import os
import uuid
import pandas as pd
from urllib.parse import quote
from sqlalchemy import create_engine


DB_URI = "postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}".format(
    username='gpadmin',
    password=quote('gpadmin', '', "utf-8", None),
    hostname="",
    port=5432,
    database='szcs_idm'
)
engine = create_engine(DB_URI)


# generate uuid
def get_uuid():
    random_uuid = uuid.uuid4()
    return ''.join(str(random_uuid).split('-'))


# save file
def save_file(df, file_name):
    save_path = f"./csv"
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    full_path = f"{save_path}{os.sep}{file_name}.csv"
    df.to_csv(full_path, index=False, header=False, encoding='utf-8')
    print(f"save {full_path} success, length:{df.shape[0]}")




def test(conn):
    sql = '''
    select * from szcs_idm.i_zhrc limit 0,1
    '''
    relation_df = get_df_content(sql, conn)
    save_file(relation_df, 'lb_bq')


if __name__ == '__main__':
    with engine.connect() as conn:
        test(conn)
        