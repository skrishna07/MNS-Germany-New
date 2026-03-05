import os
from ReusableCodes.PythonLogging import setup_logging
from ReusableCodes.ReadExcelConfig import create_main_config_dictionary
from ReusableCodes.GetConfigFromSharepoint import download_config_from_sharepoint
from ReusableCodes.DatabaseQueries import fetch_orders_to_extract_data
from ReusableCodes.DatabaseQueries import update_locked_by
from ReusableCodes.DatabaseQueries import get_db_credentials
from ReusableCodes.ExceptionManager import exception_handler
from ReusableCodes.ExceptionManager import exception_handler_main
from MasterFunctions import data_extraction_and_insertion
from MasterFunctions import json_loader_and_tables
from ReusableCodes.DatabaseQueries import update_process_status
from ReusableCodes.DatabaseQueries import update_workflow_status
from ReusableCodes.DatabaseQueries import update_bot_comments_empty
from ReusableCodes.DatabaseQueries import update_end_time
from ReusableCodes.TransactionalLog import generate_transactional_log
from ReusableCodes.DatabaseQueries import update_locked_by_empty
from ReusableCodes.DatabaseQueries import update_completed_status_api
from ReusableCodes.SendEmail import send_email
import logging


def main():
    main_config_sharepoint_url = os.environ.get('relative_url_main_config')
    main_config_local_path = 'Germany_MainConfig.xlsx'
    #download_config_from_sharepoint(main_config_sharepoint_url, main_config_local_path)
    run_environment = os.environ.get('RunEnvironment')
    system_name = os.environ.get('SystemName')
    if 'dev' in run_environment.lower():
        sheet_name = "DEV"
    else:
        sheet_name = "PROD"
    try:
        setup_logging()
        config_dict, config_status = create_main_config_dictionary(main_config_local_path, sheet_name)
        if config_status == "Pass":
            logging.info("Config Read successfully")
            db_config = get_db_credentials(config_dict)
            while True:
                registration_no = None
                receipt_no = None
                company_name = None
                database_id = None
                pending_orders_data = fetch_orders_to_extract_data(db_config)
                if len(pending_orders_data) == 0:
                    logging.info(f"No more orders to extract")
                    break
                for pending_order in pending_orders_data:
                    try:
                        attachments = []
                        database_id = pending_order[3]
                        update_locked_by(db_config, database_id)
                        receipt_no = pending_order[0]
                        registration_no = pending_order[1]
                        company_name = pending_order[2]
                        workflow_status = pending_order[4]
                        # company_type = pending_order[5]
                        if str(workflow_status).lower() == 'extraction_pending':
                            data_extraction = data_extraction_and_insertion(db_config, registration_no, config_dict, company_name)
                            if data_extraction:
                                logging.info(f"Successfully extracted data for Reg no -{registration_no}")
                                update_workflow_status(db_config, database_id, 'Loader_pending')
                        if str(workflow_status).lower() == 'loader_pending':
                            loader_status, final_email_table, json_file_path, financial_table, tags_table = json_loader_and_tables(db_config, main_config_local_path, registration_no, receipt_no, config_dict, database_id)
                            if loader_status:
                                logging.info(f"Successfully extracted JSON Loader for reg no - {registration_no}")
                                update_workflow_status(db_config, database_id, 'Loader_generated')
                                update_process_status(db_config, database_id, 'Completed')
                                update_bot_comments_empty(db_config, registration_no, database_id)
                                update_end_time(db_config, registration_no, database_id)
                                transactional_log_file_path = generate_transactional_log(db_config, config_dict)
                                completed_subject = str(config_dict['cin_Completed_subject']).format(registration_no,
                                                                                                     receipt_no)
                                completed_body = str(config_dict['cin_Completed_body']).format(registration_no,receipt_no, company_name,final_email_table, financial_table, tags_table, system_name)
                                business_mails = str(config_dict['business_mail']).split(',')
                                support_mails = str(config_dict['support_mail']).split(',')
                                attachments.append(json_file_path)
                                attachments.append(transactional_log_file_path)
                                # api_update_status = update_completed_status_api(receipt_no, config_dict)
                                # if api_update_status:
                                #     logging.info(f"Successfully updated in API for Receipt No - {receipt_no}")
                                try:
                                    # send_email(completed_subject, completed_body, business_mails,
                                    #            attachments)
                                    send_email(completed_subject, completed_body, support_mails)
                                except Exception as e:
                                    logging.error(f"Error sending mail {e}")
                            update_locked_by_empty(db_config, database_id)
                    except Exception as e:
                        logging.error(f"Exception occurred for Reg no - {registration_no} \n {e}")
                        exception_handler(e, registration_no, config_dict, receipt_no, company_name, database_id, db_config)
    except Exception as e:
        logging.error(f"Exception {e} occurred while executing master script")
        exception_handler_main(e)


if __name__ == "__main__":
    main()
