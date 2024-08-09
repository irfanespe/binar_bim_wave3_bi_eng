import pandas as pd
from sqlalchemy import create_engine # bikin connection antara code dengan db
from sqlalchemy import text

db_user = 'postgres'
db_pass = 'admin'
db_location = 'localhost'
db_port = '5432'
db_name = 'latihan_binar'

def create_engine_pg(db_user, db_pass, db_location, db_port, db_name):
    # create engine for postgresql connection
    # template -> create_engine('postgresql://user_id:pass@location:port/database_name')
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_location}:{db_port}/{db_name}') #f string format

    return engine

# define extraction process from postgresql

def extract_from_postgres(query):
    # fungsi : meng-ekstraksi data dari postgresql dengan query yang diinput
    # input : 
    #   query : sql query untuk mengambil data
    # output :
    #   df_temp : pandas dataframe dari query yang diinput 

    # running query
    result = connection.execute(text(query))

    # casting query result into pandas dataframe
    df_temp = pd.DataFrame(result)

    return df_temp

def create_surrogate(df):
    # create new column to get surrogate key
    df['order_detail_id'] = df['order_id'].map(str) + '_' + df['product_id'].map(str)

    return df

def load_data_to_pg(df, table_name):
    df.to_sql( 
                table_name, 
                con=eng,
                schema='dim_fact_layer',
                if_exists='replace',
                index=False,
                method='multi'
            )
    
    return None

if __name__ == "__main__":
    print("task created")
    # create engine
    eng = create_engine_pg(db_user, db_pass, db_location, db_port, db_name)
    print("engine created")

    # connect to db
    connection = eng.connect()
    print("connection to pg success")

    # extract data from postgre
    # create query
    q = """
        select *
        from mp_dataset.order_details
        """

    df_order_details = extract_from_postgres(q)
    print("extract data order_details")


    # create surrogate key
    fact_order_details = create_surrogate(df_order_details)
    print("create surrogate key for order_details")

    # load data to postgresql
    load_data_to_pg(fact_order_details, 'fact_ord_new')
    print("load to fact_order_details")