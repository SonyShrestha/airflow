
# Description: Creation of mysql session
# 25th September, 2020

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

def create_session(connection_url):    
    global session
    en_flag = 0
    ses_flag = 0
    try:
        if type(connection_url) is str:
            engine = create_engine(connection_url)
            en_flag = 1
        else:
            engine = connection_url
        Session = sessionmaker(bind=engine)
        session = Session()
        ses_flag = 1
    except Exception as e:
        if en_flag == 1:
            engine.dispose()
        if ses_flag == 1:
            session.close()
        raise e
    return session

