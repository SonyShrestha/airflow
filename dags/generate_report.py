from utils.db_con import *
from utils.logger import get_logger
from utils.global_vars import *
import os
import pandas as pd
import argparse

current_dir,current_file=os.path.split(os.path.abspath(__file__))
print(current_dir)
def generate_csv(mysql_session):
   get_tables_query="""select db_name,table_name,filter_logic,csv_file_name from MASTER_DATA.fc_monitor_tables"""
   get_tables_query_result=mysql_session.execute(get_tables_query)
   df_db_table=pd.DataFrame(get_tables_query_result.fetchall())
   df_db_table.columns=get_tables_query_result.keys()
   #print(df_db_table)
   for index,i in df_db_table.iterrows():
       db_name=i['db_name']
       table_name=i['table_name']
       filter_logic=i['filter_logic']
       csv_file_name=i['csv_file_name']
       #print(filter_logic)
       if filter_logic:
           table_query="""select * from {0}.{1} where {2}""".format(db_name,table_name,filter_logic)
       else:
           table_query="""select * from {0}.{1}""".format(db_name,table_name)
       table_result=mysql_session.execute(table_query)
       df=pd.DataFrame(table_result.fetchall())
       df.columns=table_result.keys()
       df.to_csv(current_dir+'/monitor_result/'+csv_file_name+'.csv')
   #df.to_csv('monitor_result/control_total_clientend.csv')
   #print(df)

   

def main(action):
   mysql_session=create_session(mysql_url)
   if action=='generate_csv':
       generate_csv(mysql_session)

if __name__ == "__main__":
    actions=["generate_csv"]
    parser=argparse.ArgumentParser()
    parser.add_argument('action',type=str, choices=actions,help="Actions that can be performed")
    args = parser.parse_args()
    action=args.action

    try:

       main(action)
    except Exception as e:
       print(str(e))

