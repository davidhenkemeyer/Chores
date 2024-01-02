import mysql.connector

def perform_sql_query(query):
    print("Performing DB query: {}".format(query))
    # TODO: Figure out the best pattern for NOT passing in the plain text password here
    cnx = mysql.connector.connect(user='david', password='opHeron1370!',
                                host='127.0.0.1',
                                database='chores')
    cursor = cnx.cursor(buffered=True)
    return_code=False
    return_data = None
    try:
        cursor.execute(query)
        cnx.commit()
        return_code = True
        return_data = cursor.fetchall()
    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        return_code = False
        
    # user_id = cursor.lastrowid
    cursor.close()
    cnx.close()
    return return_code, return_data

def get_insert_query(table_name, kwargs):
    if table_name not in ['chores', 'families', 'users']:
        print("Invalid table name specified: {}".format(table_name))
        return False
    field_names = []
    field_values = []
    for item in kwargs.keys():
        field_names.append(item)
        field_values.append("'" + kwargs[item] + "'")
    return "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, ",".join(field_names), ",".join(field_values))
    
def get_update_query(table_name, id, kwargs):
    if table_name not in ['chores', 'families', 'users']:
        print("Invalid table name specified: {}".format(table_name))
        return False
    field_value_pairs = []
    for item in kwargs.keys():
        field_value_pairs.append("{0} = '{1}'".format(item, kwargs[item]))
    return "UPDATE {0} SET {1} WHERE id = {2}".format(table_name, ",".join(field_value_pairs), id)

def get_delete_query(table_name, id):
    return "DELETE FROM {0} WHERE id={1}".format(table_name, id)

def get_all_items(table):
    return perform_sql_query("SELECT * FROM {}".format(table))

def get_id_from_name(table, name):
    ret_val, ret_data = perform_sql_query("SELECT id FROM {0} WHERE name = '{1}'".format(table, name))
    if ret_val == True:
        # Name should be unique
        assert len(ret_data) == 1
        return ret_data[0][0]
    else:
        return None
         

def add_record(table, **kwargs):
    print("Adding {}".format(table))
    #for item in kwargs.keys():
        # TODO: Make a sql query call to the DB to get the column names, rather than hardcoding the list here
        #if item not in ['name', 'pin', 'created_at']:
        #    print("Invalid argument passed to 'add_family': {}".format(item))
        #    exit (1)
    return (perform_sql_query(get_insert_query(table, kwargs)))
    
def delete_record(table, id):
    print("Deleting {}".format(table))
    return (perform_sql_query(get_delete_query(table, id)))

def update_record(table, id, **kwargs):
    print("Updating {}".format(table))
    #for item in kwargs.keys():
        # TODO: Make a sql query call to the DB to get the column names, rather than hardcoding the list here
        #if item not in ['name', 'pin', 'created_at']:
        #    print("Invalid argument passed to 'update_family': {}".format(item))
        #    exit (1)
    return (perform_sql_query(get_update_query(table, id, kwargs)))

#if add_user(name="mary", password="password123"):
#    print("Add_user successful")
#else:
#    print("Add user failed")
#if update_user(1, email_addr="david.henkemeyer@gmail.com", capacity="4"):
#    print("Update successful")
#else:
#    print("Update failed")
    
print(get_all_items("users"))
print(get_id_from_name("users", "David"))
