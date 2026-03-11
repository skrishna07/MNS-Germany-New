from ReusableCodes.PythonLogging import setup_logging
from ReusableCodes.DatabaseQueries import get_documents_to_extract
import logging
import traceback
from ReusableCodes.DatabaseQueries import extraction_pending_files
# from ReusableCodes.DatabaseQueries import update_financials
from ExtractionCodes.RegistryDocumentExtraction import registry_document_main
from ReusableCodes.DatabaseQueries import update_extraction_status
from ExtractionReusableCodes.AddressSplit import split_address
from ReusableCodes.GetConfigFromSharepoint import download_config_from_sharepoint
from ReusableCodes.ReadExcelConfig import create_main_config_dictionary
from JSONGenerationCodes.JSONLoaderGeneration import json_loader
from JSONGenerationCodes.OrderJson import order_json
from ReusableCodes.FinalEmailTable import final_table
from ReusableCodes.FinalEmailTable import financials_table
from ExtractionReusableCodes.Holding_Entities import get_holding_entities
from ReusableCodes.DatabaseQueries import get_financial_status
from ReusableCodes.DatabaseQueries import update_finance_status
from ReusableCodes.DatabaseQueries import update_pnl_status
from ReusableCodes.DatabaseQueries import update_auditors_status
from ReusableCodes.DatabaseQueries import get_split_status
from ReusableCodes.DatabaseQueries import get_split_finance_and_pnl_path
from ReusableCodes.Split_Scanned_Pdf import split_pdf_based_on_headers_and_fields
from ReusableCodes.DatabaseQueries import update_split_status_and_split_pdf_path
from ReusableCodes.split_finance_pnl_pdf import create_two_pdfs_ocr_only
from New_tags_table import new_tags_table
from ExtractionCodes.Financial_Document_Extraction import finance_main
from ExtractionCodes.RegistryDocumentExtraction import auditor_details_main
import os
from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())


def data_extraction_and_insertion(db_config, registration_no, config_dict, company_name):
    setup_logging()
    error_count = 0
    errors = []
    try:
        documents_to_extract = get_documents_to_extract(db_config, registration_no)
        document_name = None
        document_download_path = None
        for document in documents_to_extract:
            try:
                document_id = document[0]
                document_name = document[2]
                document_download_path = document[6]
                category = document[4]
                output_path = str(document_download_path).replace('.PDF', '.pdf').replace('.pdf', '.xlsx')
                if 'registry' in str(category).lower():
                    if not os.path.exists('Config'):
                        os.makedirs('Config')
                    #download_config_from_sharepoint(os.environ.get('relative_url_extraction_CD_config'), config_dict['registry_config_path_CD'])
                    #download_config_from_sharepoint(os.environ.get('relative_url_extraction_AD_config'), config_dict['registry_config_path_AD'])
                    #download_config_from_sharepoint(os.environ.get('relative_url_extraction_DK_config'), config_dict['registry_config_path_DK'])
                    registry_document_extraction_directors = registry_document_main(db_config, config_dict, document_download_path, output_path, registration_no,document_name)
                    if registry_document_extraction_directors:
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
                elif 'financial' in str(category).lower():
                    split_status = get_split_status(db_config, registration_no, document_id)
                    if str(split_status).lower() != 'y':
                        temp_pdf_directory = os.path.dirname(document_download_path)

                        # call split code , if split success update 'y' in database along with split_pdf_path
                        # is_split_successful = split_pdf_based_on_headers_and_fields(document_download_path, split_pdf_path, header_keywords, field_keywords, content_keywords)
                        finance_split_pdf_path, pnl_split_pdf_path, is_split_successful = create_two_pdfs_ocr_only(document_download_path,str(config_dict['standalone_headers_finance']).split(','), str(config_dict['financial_fields']).split(','), str(config_dict['standalone_headers_profit_and_loss']).split(','),
                                                                       str(config_dict['profit_and_loss_fields']).split(','),str(config_dict['stop_keywords']).split(','), str(config_dict['contents']).split(',') +
                                str(config_dict['germany_contents']).split(','), temp_pdf_directory)
                        print("successfully splitted pdf")
                        if is_split_successful:
                            update_split_status_and_split_pdf_path(db_config, registration_no, document_id, finance_split_pdf_path, pnl_split_pdf_path)
                    else:
                        finance_split_pdf_path, pnl_split_pdf_path = get_split_finance_and_pnl_path(db_config, registration_no, document_id)
                    temp_pdf_directory = os.path.dirname(document_download_path)
                    pdf_document_name = os.path.basename(document_download_path)
                    pdf_document_name = str(pdf_document_name).replace('.pdf', '')

                    finance_output_file_name = 'finance_' + pdf_document_name
                    if '.xlsx' not in finance_output_file_name:
                        finance_output_file_name = finance_output_file_name + '.xlsx'
                    finance_output_file_path = os.path.join(temp_pdf_directory, finance_output_file_name)
                    finance_status, profit_and_loss_status, auditors_status = get_financial_status(db_config, registration_no,
                                                                                  document_id)
                    finance_input = config_dict['financial_input']
                    #download_config_from_sharepoint(os.environ.get('relative_url_financial_config'),
                    #                               config_dict['Financial_config'])
                    if str(finance_status).lower() != 'y':

                        # success_count_finance = 0
                        # for item in ['Standalone','Consolidated']:
                        #     main_finance_extraction = False
                        #     if item == 'Consolidated':
                        #         temp_pdf_name_finance = 'temp_finance_consolidated' + pdf_document_name
                        #         finance_output_file_name = 'finance_consolidated' + pdf_document_name
                        #     else:
                        #         finance_output_file_name = 'finance_standalone' + pdf_document_name
                        #         temp_pdf_name_finance = 'temp_finance_standalone' + pdf_document_name
                        #     if '.pdf' not in temp_pdf_name_finance:
                        #         temp_pdf_name_finance = temp_pdf_name_finance + '.pdf'
                        #     temp_pdf_path_finance = os.path.join(temp_pdf_directory, temp_pdf_name_finance)
                        #     if '.xlsx' not in finance_output_file_name:
                        #         finance_output_file_name = finance_output_file_name + '.xlsx'
                        #     finance_output_file_path = os.path.join(temp_pdf_directory, finance_output_file_name)
                        main_finance_extraction = finance_main(db_config, config_dict, document_download_path, registration_no, finance_output_file_path, finance_input, finance_split_pdf_path, document_id)
                        if main_finance_extraction:
                        #         success_count_finance += 1
                        # if success_count_finance == 2:
                            logging.info(f"Successfully extracted for assets and liabilities")
                            update_finance_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted for assets and liabilities")
                    pnl_output_file_name = 'pnl_' + pdf_document_name
                    # if item == 'Consolidated':
                    #     pnl_output_file_name = 'pnl_consolidated_' + pdf_document_name
                    # else:
                    #     pnl_output_file_name = 'pnl_standalone_' + pdf_document_name
                    if '.xlsx' not in pnl_output_file_name:
                        pnl_output_file_name = pnl_output_file_name + '.xlsx'

                    pnl_output_path = os.path.join(temp_pdf_directory, pnl_output_file_name)
                    pnl_input = config_dict['pnl_input']
                    if str(profit_and_loss_status).lower() != 'y':
                        # success_pnl = 0
                        # for item in ['Standalone', 'Consolidated']:
                        #     pnl_extraction = False
                        #     if item == 'Consolidated':
                        #         temp_pdf_name_pnl = 'temp_pnl_consolidated' + pdf_document_name
                        #     else:
                        #         temp_pdf_name_pnl = 'temp_pnl_standalone' + pdf_document_name
                        #     if '.pdf' not in temp_pdf_name_pnl:
                        #         temp_pdf_name_pnl = temp_pdf_name_pnl + '.pdf'
                        #     temp_pdf_path_pnl = os.path.join(temp_pdf_directory, temp_pdf_name_pnl)
                        #     if item == 'Consolidated':
                        #         pnl_output_file_name = 'pnl_consolidated' + pdf_document_name
                        #     else:
                        #         pnl_output_file_name = 'pnl_standalone' + pdf_document_name
                        #     if '.xlsx' not in pnl_output_file_name:
                        #         pnl_output_file_name = pnl_output_file_name + '.xlsx'
                        #     pnl_output_path = os.path.join(temp_pdf_directory, pnl_output_file_name)
                        #     pnl_input = config_dict['pnl_input']
                        if pnl_split_pdf_path is not None:
                            if str(pnl_split_pdf_path).lower() == 'none':
                                update_pnl_status(db_config, registration_no, document_id)
                            else:
                                pnl_extraction = finance_main(db_config, config_dict, document_download_path,
                                                              registration_no, pnl_output_path, pnl_input,
                                                              pnl_split_pdf_path, document_id)
                                if pnl_extraction:
                                    logging.info(f"Successfully extracted Profit and Loss")
                                    update_pnl_status(db_config, registration_no, document_id)
                        else:
                            update_pnl_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted Profit and Loss")

                    if str(auditors_status).lower() != 'y':
                        auditors_output_file_name = 'auditors' + pdf_document_name
                        if '.xlsx' not in auditors_output_file_name:
                            auditors_output_file_name = auditors_output_file_name + '.xlsx'
                        auditors_output_file_name = os.path.join(temp_pdf_directory, auditors_output_file_name)
                        #download_config_from_sharepoint(os.environ.get('relative_url_auditor_config'),
                        #                           config_dict['auditors_config_path'])
                        main_auditor_extraction = auditor_details_main(db_config, config_dict, document_download_path, auditors_output_file_name, registration_no)
                        if main_auditor_extraction:
                            update_auditors_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted Auditors")
                    updated_finance_status, updated_pnl_status, updated_auditors_status = get_financial_status(db_config, registration_no,
                                                                                      document_id)
                    if str(updated_finance_status).lower() == 'y' and str(updated_pnl_status).lower() == 'y' and str(updated_auditors_status).lower() == 'y':
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
            except Exception as e:
                logging.error(f"Error {e} occurred while extracting for file - {document_name} at path - {document_download_path}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"File - {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        try:
            split_address(registration_no, config_dict, db_config)
        except Exception as e:
            logging.error(f"Error in splitting address {e}")
        try:
            get_holding_entities(db_config, registration_no, config_dict)
        except Exception as e:
            logging.error(f"Error in getting holding entities{e}")
    except Exception as e:
        logging.error(f"Error occurred while extracting for Reg no - {registration_no}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        pending_files = extraction_pending_files(db_config, registration_no)
        if len(pending_files) == 0:
            return True
        else:
            raise Exception(f"Multiple exceptions occurred while extracting:\n\n" + "\n".join(errors))


def json_loader_and_tables(db_config, config_excel_path, registration_no, receipt_no, config_dict, database_id):
    errors = []
    try:
        config_json_file_path = config_dict['config_json_file_path']
        #download_config_from_sharepoint(os.environ.get('relative_url_config_json'), config_json_file_path)
        root_path = config_dict['Root path']
        sheet_name = 'JSON_Loader_SQL_Queries'
        final_email_table = None
        financial_table = None
        json_loader_status, json_file_path, json_nodes = json_loader(db_config, config_json_file_path, registration_no, root_path, config_excel_path, sheet_name, receipt_no)
        if json_loader_status:
            order_sheet_name = "JSON Non-LLP Order"
            config_dict_order, status = create_main_config_dictionary(config_excel_path, order_sheet_name)
            for json_node in json_nodes:
                try:
                    json_order_status = order_json(config_dict_order, json_node, json_file_path)
                    if json_order_status:
                        logging.info(f"Successfully ordered json for {json_node}")
                except Exception as e:
                    logging.error(f"Error occurred while ordering for {json_node} {e}")
            # handle_json_structure=update_financials(db_config,registration_no)
            final_email_table = final_table(db_config, registration_no, database_id)
            financial_table = financials_table(db_config, registration_no)
            tags_table = new_tags_table(db_config, registration_no, database_id)
    except Exception as e:
        logging.error(f"Exception occurred while generating json loader {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        return True, final_email_table, json_file_path, financial_table, tags_table

