import mysql.connector
from ReusableCodes.PythonLogging import setup_logging
import logging
import os
from datetime import datetime
import json
import traceback
import requests
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


def fetch_orders_to_extract_data(db_config):
    try:
        setup_logging()
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        pending_order_query = f"select receipt_no,registration_no,company_name,id,workflow_status from orders where process_status = 'InProgress' and LOWER(workflow_status) in ('extraction_pending','loader_pending') and (locked_by IS NULL or locked_by ='' or locked_by LIKE '%{os.environ.get('SystemName')}%' or locked_by LIKE '%{os.environ.get('Machinename')}%')"
        logging.info(pending_order_query)
        cursor.execute(pending_order_query)
        pending_order_results = cursor.fetchall()
        print(pending_order_results)
        cursor.close()
        connection.close()
        return pending_order_results
    except Exception as e:
        logging.error(f"Exception {e} occurred")
        return []


def get_db_credentials(config_dict):
    host = config_dict['Host']
    db_user = config_dict['User']
    password = config_dict['Password']
    database = config_dict['Database']
    db_config = {
        "host": host,
        "user": db_user,
        "password": password,
        "database": database,
        "connect_timeout": 6000,
        "charset": 'utf8mb4'
    }
    return db_config


def update_locked_by(dbconfig, registration_id):
    setup_logging()
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        user = os.environ.get('SystemName')
        update_locked_query = f"update orders set locked_by = '{user}' where id ='{registration_id}'"
        cursor.execute(update_locked_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_locked_by_empty(dbconfig, registration_id):
    setup_logging()
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        update_locked_query = f"update orders set locked_by = '' where id ='{registration_id}'"
        cursor.execute(update_locked_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_modified_date(db_config, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        current_date = datetime.now()
        today_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
        update_locked_query = f"update orders set modified_date = '{today_date}' where id = {database_id}"
        logging.info(update_locked_query)
        cursor.execute(update_locked_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_workflow_status(db_config, reg_id, status):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"Update orders set workflow_status = '{status}' where id = {reg_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating workflow status {e}")


def update_process_status(db_config, database_id, status):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        query = f"Update orders set process_status = '{status}' where id = {database_id}"
        logging.info(query)
        cursor.execute(query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error in updating process status {e}")


def update_retry_count(db_config, registration_no, retry_counter, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_retry_counter_query = f"update orders set retry_counter = {retry_counter} where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_retry_counter_query)
        cursor.execute(update_retry_counter_query)
        connection.commit()
    except Exception as e:
        print(f"Exception occurred while updating retry counter by {e}")
    finally:
        cursor.close()
        connection.close()


def get_retry_count(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        retry_counter_query = f"select retry_counter from orders where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(retry_counter_query)
        cursor.execute(retry_counter_query)
        result = cursor.fetchone()[0]
        logging.info(f"Retry count {result}")
        return result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def get_documents_to_extract(db_config, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        extract_documents_query = f"select * from documents where registration_no = '{registration_no}' and document_extraction_status = 'Pending' and document_extraction_needed = 'Y'"
        logging.info(extract_documents_query)
        cursor.execute(extract_documents_query)
        result = cursor.fetchall()
        print(result)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error while fetching results {e}")
        raise Exception(e)
    else:
        return result


def extraction_pending_files(db_config, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        pending_files_query = f"select * from documents where registration_no = '{registration_no}' and document_extraction_status = 'Pending' and document_extraction_needed = 'Y'"
        logging.info(pending_files_query)
        cursor.execute(pending_files_query)
        pending_files = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error in fetching number of pending files {e}")
    else:
        return pending_files


def update_extraction_status(db_config, document_id, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set document_extraction_status = 'Success' where registration_no = '{registration_no}' and id = {document_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_database_single_value(db_config, table_name, registration_no_column_name, registration_no, column_name, column_value):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}'".format(table_name, registration_no_column_name, registration_no)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    print(result)
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}'".format(table_name, column_name,
                                                                                      column_value, registration_no_column_name,
                                                                                      registration_no)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}) VALUES ('{}', '{}')".format(table_name, registration_no_column_name,
                                                                                      column_name,
                                                                                      registration_no,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def insert_datatable_with_table_director(config_dict, db_config, sql_table_name, column_names_list, df_row, field_name):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)
    registration_column_name = config_dict['registration_no_Column_name']
    registration_no = result_dict[registration_column_name]

    if sql_table_name == 'authorized_signatories':
        name_column_name = config_dict['name_column_name_in_db_directors']
        name = result_dict[name_column_name]
        designation_column_name = config_dict['designation_column_name']
        designation = result_dict[designation_column_name]
        # try:
        #     secretary_name = f'''SELECT company_secretary_name FROM related_entity'''
        #     logging.info(secretary_name)
        #     db_cursor.execute(secretary_name)
        #     secretary_name_result = db_cursor.fetchall()
        #     company_secretary_query = f'''
        #             SELECT company_secretary_name FROM related_entity
        #             WHERE LOWER(company_secretary_name) = "{str(name).lower()}"
        #         '''
        #     logging.info(company_secretary_query)
        #     db_cursor.execute(company_secretary_query)
        #     company_secretary_result = db_cursor.fetchall()
        #     if company_secretary_result:
        #         designation = 'Company Secretary'
        #     else:
        #         company_secretary_name = secretary_name_result
        #         designation = 'Company Secretary'
        # except:
        #     logging.info("There is no company secretary name available.")
        select_query = (
            f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND LOWER({name_column_name})'
            f' = "{str(name).lower()}"')
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        print(result)
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
            # Insert the record
            insert_query = f'''
            INSERT INTO {sql_table_name}
            SET {', '.join([f"{col} = %s" for col in column_names_list])};
            '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
            # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
        else:
            result_dict.pop(registration_column_name)
            result_dict.pop(name_column_name)
            result_dict.pop(designation_column_name)
            column_names_list = list(column_names_list)
            column_names_list.remove(registration_column_name)
            column_names_list.remove(name_column_name)
            column_names_list.remove(designation_column_name)
            update_query = f'''UPDATE {sql_table_name}
                                                        SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                                        WHERE {registration_column_name} = "{registration_no}" AND LOWER({name_column_name}) = "{str(name).lower()}"'''
            logging.info(update_query)
            db_cursor.execute(update_query)
            logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    elif sql_table_name == 'principal_business_activities':
        main_activity_group_description_column_name = config_dict['main_activity_group_description_column_name']
        main_activity_group_description = result_dict[main_activity_group_description_column_name]
        select_query = (
                f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND LOWER({main_activity_group_description_column_name})'
                f' = "{str(main_activity_group_description).lower()}"')
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        print(result)
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
                        # Insert the record
            insert_query = f'''
                    INSERT INTO {sql_table_name}
                    SET {', '.join([f"{col} = %s" for col in column_names_list])};
                    '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
        else:
                result_dict.pop(registration_column_name)
                result_dict.pop(main_activity_group_description_column_name)
                column_names_list = list(column_names_list)
                column_names_list.remove(registration_column_name)
                column_names_list.remove(main_activity_group_description_column_name)
                update_query = f'''UPDATE {sql_table_name}
                                                            SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                                            WHERE {registration_column_name} = "{registration_no}" AND LOWER({main_activity_group_description_column_name}) = "{str(main_activity_group_description).lower()}"'''
                logging.info(update_query)
                db_cursor.execute(update_query)
                logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
                # Assuming 'company' table contains 'legal_name' as the column for the company's official name.
                # You'll need to add this before performing the select query for 'name_history'
    # elif sql_table_name == 'name_history':
    #             # Extract the name from the "a)" section (This is assumed to be part of the input data)
    #             name_column_name = config_dict['name_column_name_in_name_history']
    #             name = result_dict[name_column_name]
    #
    #             # Step 1: Get the company's legal name from the 'company' table.
    #             company_select_query = f'SELECT legal_name FROM Company WHERE {registration_column_name} = "{registration_no}"'
    #             logging.info(company_select_query)
    #             db_cursor.execute(company_select_query)
    #             company_result = db_cursor.fetchone()
    #
    #             # Step 2: Compare the name from "a)" section with the current company's legal name
    #             if company_result:
    #                 legal_name = company_result['legal_name']  # Extracting the company's legal name.
    #
    #                 if str(name).lower() != str(
    #                         legal_name).lower():  # If they do not match, save the name in 'name_history'
    #                     select_query = (
    #                         f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND LOWER({name_column_name})'
    #                         f' = "{str(name).lower()}"')
    #                     logging.info(select_query)
    #                     db_cursor.execute(select_query)
    #                     result = db_cursor.fetchall()
    #                     print(result)
    #                     logging.info(len(result))
    #
    #                     if len(result) == 0:  # If no matching record found in 'name_history'
    #                         # Insert the record into 'name_history'
    #                         insert_query = f'''
    #                                     INSERT INTO {sql_table_name}
    #                                     SET {', '.join([f"{col} = %s" for col in column_names_list])};
    #                                     '''
    #                         logging.info(insert_query)
    #                         logging.info(tuple(df_row.values))
    #                         db_cursor.execute(insert_query, tuple(df_row.values))
    #                     else:
    #                         result_dict.pop(registration_column_name)
    #                         result_dict.pop(name_column_name)
    #                         column_names_list = list(column_names_list)
    #                         column_names_list.remove(registration_column_name)
    #                         column_names_list.remove(name_column_name)
    #
    #                         update_query = f'''UPDATE {sql_table_name}
    #                                             SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])}
    #                                             WHERE {registration_column_name} = "{registration_no}" AND LOWER({name_column_name}) = "{str(name).lower()}"'''
    #                         logging.info(update_query)
    #                         db_cursor.execute(update_query)
    #                         logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    #                 else:
    #                     logging.info(
    #                         f"Name '{name}' matches the current legal name '{legal_name}'. No update necessary.")
    #             else:
    #                 logging.error(f"Legal name for company with registration number '{registration_no}' not found.")
    elif sql_table_name == 'address_history':
        address_line_column_name = config_dict['address_line_column_name']
        address_line = result_dict[address_line_column_name]
        current_registered_address = get_registered_address(db_config, registration_no)

        if str(current_registered_address).lower() == str(address_line).lower():
            logging.info(f"Skipping this {address_line} as it is equal to current address {current_registered_address}")
            db_cursor.close()
            db_connection.close()
            return

        select_query = (
            f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND LOWER({address_line_column_name})'
            f' = "{str(address_line).lower()}"')
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        print(result)
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
            # Insert the record
            insert_query = f'''
                        INSERT INTO {sql_table_name}
                        SET {', '.join([f"{col} = %s" for col in column_names_list])};
                        '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
        else:
            logging.info(f"{address_line} already present in database")
            # result_dict.pop(registration_column_name)
            # result_dict.pop(address_line_column_name)
            # column_names_list = list(column_names_list)
            # column_names_list.remove(registration_column_name)
            # column_names_list.remove(address_line_column_name)
            # update_query = f'''UPDATE {sql_table_name}
            #                                                     SET {address_line_column_name} = "{address_line}"
            #                                                    WHERE {registration_column_name} = "{registration_no}" '''
            # logging.info(update_query)
            # db_cursor.execute(update_query)
            # logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    elif sql_table_name == 'auditors':
        name_column_name = config_dict['name_column_name_in_db_auditors']
        name = result_dict[name_column_name]
        auditor_firm_name_column_name = config_dict['auditors_firm_name_column_name']
        auditor_firm_name = result_dict[auditor_firm_name_column_name]
        nature_column_name = config_dict['auditor_nature_column_name']
        nature = result_dict[nature_column_name]
        year_column_name = config_dict['auditor_year_column_name']
        year = result_dict[year_column_name]
        select_query = (
            f'SELECT * FROM {sql_table_name} '
            f'WHERE {registration_column_name} = "{registration_no}" '
            f'AND LOWER({name_column_name}) = "{str(name).lower()}" '
            f'AND LOWER({nature_column_name}) = "{str(nature).lower()}" '
            f'AND {year_column_name} = "{year}"'
        )
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        print(result)
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
            # Insert the record
            insert_query = f'''
                    INSERT INTO {sql_table_name}
                    SET {', '.join([f"{col} = %s" for col in column_names_list])};
                    '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
            # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
        else:
            logging.info("Auditor details already exist in the Database")
    else:
        if sql_table_name == 'current_shareholdings':
            name_column_name = config_dict['name_column_name_in_db_shareholders']
        else:
            raise Exception("Invalid table")
        name = result_dict[name_column_name]
        select_query = (f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND {name_column_name}'
                        f' = "{name}"')
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
            # Insert the record
            insert_query = f'''
            INSERT INTO {sql_table_name}
            SET {', '.join([f"{col} = %s" for col in column_names_list])};
            '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
            # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
        else:
            if field_name != 'previous_business_address':
                result_dict.pop(registration_column_name)
                result_dict.pop(name_column_name)
                column_names_list = list(column_names_list)
                column_names_list.remove(registration_column_name)
                column_names_list.remove(name_column_name)
                update_query = f'''UPDATE {sql_table_name} SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                WHERE {registration_column_name} = "{registration_no}" AND {name_column_name} = "{name}"'''
                logging.info(update_query)
                db_cursor.execute(update_query)
                logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
            else:
                logging.info(f"Business address already there in registered previous address so not updating")
    db_cursor.close()
    db_connection.close()


def update_database_single_value_financial(db_config, table_name, registration_no_column_name, registration_no, column_name, column_value, year, nature):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}'".format(table_name, registration_no_column_name, registration_no, 'year', year, 'nature', nature)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}'".format(table_name, column_name,
                                                                                      column_value, registration_no_column_name,
                                                                                      registration_no,
                                                                                      'Year',
                                                                                      year,
                                                                                      'nature',
                                                                                       nature)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, registration_no_column_name,
                                                                                      column_name,
                                                                                      'nature',
                                                                                      registration_no,
                                                                                      column_value,
                                                                                      nature)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def form_check(db_config, config_dict, registration_no, document_date):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = str(config_dict['Form6_check_query']).format(document_date, registration_no)
        logging.info(query)
        cursor.execute(query)
        result = cursor.fetchone()
        logging.info(result)
        status = result[0]
        form15_date = result[1]
        return status, form15_date
    except Exception as e:
        logging.info(f"Error occurred while checking form 6 {e}")
        return None


# def update_form_extraction_status(db_config, registration_no, config_dict):
#     errors = []
#     setup_logging()
#     try:
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()
#         connection.autocommit = True
#         update_query_form15 = str(config_dict['form15_extraction_needed_update_query']).format(registration_no, registration_no)
#         logging.info(update_query_form15)
#         cursor.execute(update_query_form15)
#         update_query_form10 = str(config_dict['form10_extraction_needed_update_query']).format(registration_no)
#         logging.info(update_query_form10)
#         cursor.execute(update_query_form10)
#         update_query_form40 = str(config_dict['form40_extraction_needed_update_query']).format(registration_no, registration_no)
#         logging.info(update_query_form40)
#         cursor.execute(update_query_form40)
#         update_query_form20 = str(config_dict['form20_extraction_needed_update_query']).format(registration_no, registration_no)
#         logging.info(update_query_form20)
#         cursor.execute(update_query_form20)
#         update_query_form6 = str(config_dict['form6_extraction_needed_update_query']).format(registration_no, registration_no)
#         logging.info(update_query_form6)
#         cursor.execute(update_query_form6)
#         update_query_financial = str(config_dict['financial_update_query']).format(registration_no)
#         logging.info(update_query_financial)
#         cursor.execute(update_query_financial)
#         cursor.close()
#         connection.close()
#     except Exception as e:
#         logging.error(f"Error updating form extraction status {e}")
#         tb = traceback.extract_tb(e.__traceback__)
#         for frame in tb:
#             if frame.filename == __file__:
#                 errors.append(f"File - {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
#         raise Exception(errors)
#     else:
#         return True


def update_extraction_needed_status_to_n(db_config, document_id, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set document_extraction_needed = 'N' where registration_no = '{registration_no}' and id = {document_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_bot_comments_empty(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_comments_query = f"update orders set bot_comments = '',retry_counter = '',exception_type = '' where registration_no = '{registration_no}' and id ='{database_id}'"
        cursor.execute(update_comments_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def get_financial_status(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        retry_counter_query = f"select financial_status,profit_and_loss_status,auditors_status from documents where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(retry_counter_query)
        cursor.execute(retry_counter_query)
        result = cursor.fetchall()[0]
        financial_result = result[0]
        profit_and_loss_result = result[1]
        auditors_result = result[2]
        logging.info(f"financial status {result}")
        return financial_result, profit_and_loss_result, auditors_result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_finance_status(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set financial_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_pnl_status(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set profit_and_loss_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_auditors_status(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set auditors_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_database_single_value_with_one_column_check(db_config, table_name, registration_no_column_name, registration_no, column_name, column_value, column_to_check, value_to_check):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, registration_no_column_name, registration_no, column_to_check, value_to_check)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, registration_no_column_name,
                                                                                      registration_no,column_to_check,value_to_check)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}) VALUES ('{}', '{}')".format(table_name, registration_no_column_name,
                                                                                      column_name,
                                                                                      registration_no,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def get_legal_name_form15(db_config, registration_no):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        legal_name_query = f"select legal_name from Company where registration_no = '{registration_no}'"
        logging.info(legal_name_query)
        cursor.execute(legal_name_query)
        result = cursor.fetchone()[0]
        return result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_completed_status_api(orderid, config_dict):
    setup_logging()
    try:
        url = os.environ.get('update_api_url')

        payload = json.dumps({
            "receiptnumber": orderid,
            "status": 2
        })
        headers = {
            'Authorization': os.environ.get('update_api_authorization'),
            'Content-Type': 'application/json',
            'Cookie': os.environ.get('update_api_cookie')
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        logging.info(response.text)
    except Exception as e:
        logging.info(f"Error in updating status in API {e}")
        return False
    else:
        return True


def update_end_time(db_config, registration_no, database_id):
    try:
        setup_logging()
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        check_query = f"SELECT end_time FROM orders WHERE registration_no = '{registration_no}' and id = {database_id}"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if result is not None and result[0] is None:
            update_query = f"update orders set end_time = '{current_datetime}' where registration_no = '{registration_no}' and id = {database_id}"
            logging.info(update_query)
            cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.info(f"Error updating end time {e}")


def get_extraction_status(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        retry_counter_query = f"select directors_extraction_status,other_than_directors_extraction_status from documents where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(retry_counter_query)
        cursor.execute(retry_counter_query)
        result = cursor.fetchall()[0]
        financial_result = result[0]
        profit_and_loss_result = result[1]
        logging.info(f"Extraction status {result}")
        return financial_result, profit_and_loss_result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_extraction_status_directors(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set directors_extraction_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_extraction_status_other_than_directors(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set other_than_directors_extraction_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")

def financial_data_availability_check(db_config, registration_no, year, nature, column_name):
    setup_logging()
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        # Query to check if financial data is available
        select_query = (f"SELECT {column_name} FROM financials WHERE registration_no = '{registration_no}' "
                        f"AND year = '{year}' AND nature = '{nature}'")
        logging.info(select_query)
        cursor.execute(select_query)
        result = cursor.fetchone()
        if result is None:
            # print("result", result)
            data_exists = False
        else:
            # print("result", result)
            column_data = result[column_name]
            if column_data is None:
                data_exists = False
            else:
                data_exists = True
        cursor.close()
        connection.close()
        return data_exists
    except Exception as e:
        logging.error(f"Error checking financial data availability: {e}")
        return None

def get_split_status(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        split_status_query = f"select split_status from documents where registration_no = '{registration_no}' and category='Financial' and id={database_id}"
        logging.info(split_status_query)
        cursor.execute(split_status_query)
        result = cursor.fetchone()[0]
        logging.info(f"Retry count {result}")
        return result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_split_status_and_split_pdf_path(db_config, registration_no, database_id, finance_split_pdf_path, pnl_split_pdf_path):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_split_status_path__query = f"UPDATE documents SET split_status = 'Y', finance_split_pdf_path = '{finance_split_pdf_path}',pnl_split_pdf_path = '{pnl_split_pdf_path}' WHERE registration_no = '{registration_no}' AND id = {database_id}"
        logging.info(update_split_status_path__query)
        cursor.execute(update_split_status_path__query)
        connection.commit()
    except Exception as e:
        print(f"Exception occurred while updating retry counter by {e}")
    finally:
        cursor.close()
        connection.close()


def get_split_finance_and_pnl_path(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        get_split_file_path__query = f"SELECT finance_split_pdf_path, pnl_split_pdf_path FROM documents WHERE registration_no = '{registration_no}' AND id = {database_id}"
        logging.info(get_split_file_path__query)
        cursor.execute(get_split_file_path__query)
        result = cursor.fetchone()
        finance_split_pdf_path, pnl_split_pdf_path = result
        return finance_split_pdf_path, pnl_split_pdf_path
    except Exception as e:
        logging.error(f"Exception occurred while updating retry counter by {e}")
        return None, None
    finally:
        cursor.close()
        connection.close()


def get_split_pdf_path(db_config, registration_no,database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        query = (
            f"SELECT split_status, finance_split_pdf_path, pnl_split_pdf_path, finance_pdf_to_excel_conversion_status, finance_excel_path ,pnl_excel_path, pnl_pdf_to_excel_conversion_status FROM documents WHERE registration_no = '{registration_no}' AND category= 'Financial' and id='{database_id}' "
        )
        logging.info(query)
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            split_status = result[0]  # Assign split_status
            finance_split_pdf_path = result[1]  # Assign split_pdf_path
            pnl_split_pdf_path = result[2]
            finance_pdf_to_excel_conversion_status = result[3]  # Assign pdf_to_excel_conversion_status
            finance_excel_file_path = result[4]
            pnl_excel_file_path = result[5]
            pnl_pdf_to_excel_conversion_status = result[6]

            logging.info(f"Fetched values - split_status: {split_status}, finance split_pdf_path: {finance_split_pdf_path}, pnl split pdf path - {pnl_split_pdf_path}"
                         f"finance_pdf_to_excel_conversion_status: {finance_pdf_to_excel_conversion_status}, finance_pdf_to_excel_conversion_status: {pnl_pdf_to_excel_conversion_status}")
            return split_status, finance_split_pdf_path, pnl_split_pdf_path, finance_pdf_to_excel_conversion_status, finance_excel_file_path, pnl_excel_file_path, pnl_pdf_to_excel_conversion_status
        else:
            logging.info("No record found for the given registration_no and database_id.")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching document status: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def update_financials(db_config, registration_no):
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        # Step 1: Execute the first query and capture the year column
        cursor.execute(f"""SELECT * FROM financials
            WHERE registration_no = '{registration_no}' AND financials_bs_subTotals IS NULL
        """)

        # Fetch the result (assuming 'year' is one of the columns in the result set)
        result_first_query = cursor.fetchall()

        if not result_first_query:
            print(f"No records found for registration number {registration_no} in the first query.")
            return
        for i in result_first_query:
            print(i)
        # Assuming that the 'year' column is the first column in the result set
            year = i[6]
            pnldata = i[15]
            id=i[0]
            print(f"""DELETE FROM financials
                            WHERE id='{id}'""")

            # Step 2: Execute the second query and check if `financials_bs_subTotals` is NULL
            cursor.execute(f"""
                UPDATE financials
                SET financials_pnl_lineitems = '{pnldata}'
                WHERE registration_no = '{registration_no}' AND year = '{year}'
            """)
            result_second_query = cursor.fetchall()

            cursor.execute(f"""
                DELETE FROM financials
                WHERE id={id}
            """)
            print(f"""DELETE FROM financials
                WHERE id='{id}'""")
            result_third_query = cursor.fetchall()

        connection.commit()
        print(f"Updated financials_pnl_lineitems for registration number {registration_no} and year {year}.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection.is_connected():
            # Close the connection
            cursor.close()
            connection.close()


def insert_new_tags(db_config, registration_no, database_id, all_tags_data, column_name):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Ensure all_tags_data is a string or serialize it to JSON if needed
        if isinstance(all_tags_data, (list, dict)):
            all_tags_data = json.dumps(all_tags_data)  # Convert to JSON if it's a list or dictionary

            all_tags_data_safe = f"'{all_tags_data}'"if all_tags_data else "NULL"

            # Create the dynamic update query
            update_query = (
                    f"UPDATE documents SET {column_name} = {all_tags_data_safe} "
                    f"WHERE registration_no = '{registration_no}' AND category = 'Financial_File'"
                )

        logging.info(update_query)
        cursor.execute(update_query)
        connection.commit()
        logging.info(f"Updated {column_name} with new tags.")
        return True  # Return True to indicate the update was successful
    except Exception as e:
        logging.error(f"Exception occurred while updating {column_name}: {e}")
        return False  # Return False if an error occurred
    finally:
        cursor.close()
        connection.close()


def update_excel_status_and_path(db_config, registration_no, database_id, excel_path, excel_file_column_name, status_column_name):
    """
    Update pdf_to_excel_conversion_status to 'Y' and set the excel_path in the documents table.
    """
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Update pdf_to_excel_conversion_status to 'Y' and set the excel_path
        update_query = (
            f"UPDATE documents SET {status_column_name} = 'Y', {excel_file_column_name} = '{excel_path}' "
            f"WHERE registration_no = '{registration_no}' AND id = {database_id}"
        )
        logging.info(update_query)
        cursor.execute(update_query)
        connection.commit()  # Commit the update

        logging.info(f"Updated pdf_to_excel_conversion_status to 'Y' and excel_path to {excel_path}")
        return True  # Return True to indicate the update was successful
    except Exception as e:
        logging.error(f"Exception occurred while updating document status: {e}")
        return False  # Return False if an error occurred
    finally:
        cursor.close()
        connection.close()


def get_registered_address(db_config, registration_no):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        get_registered_address_query = f"SELECT registered_full_address FROM Company WHERE registration_no = '{registration_no}'"
        logging.info(get_registered_address_query)
        cursor.execute(get_registered_address_query)
        registered_address = cursor.fetchone()[0]
        return registered_address
    except Exception as e:
        logging.error(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()