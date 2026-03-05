from ReusableCodes.PythonLogging import setup_logging
import logging
import os
import pandas as pd
import traceback
from ExtractionReusableCodes.OpenAI import split_openai
from ExtractionReusableCodes.OpenAI import split_claude
import re
import json
from ReusableCodes.DatabaseQueries import update_database_single_value_financial
from ExtractionReusableCodes.AzureOCRExtraction import extract_whole_pdf_data
from ReusableCodes.DatabaseQueries import financial_data_availability_check
from ExtractionReusableCodes.OpenAI import process_pdf_with_claude
from ExtractionReusableCodes.GetFinancialExcelData import get_excel_data
from ExtractionReusableCodes.ExtractReadablePDF import extract_text_from_readable_pdf
from ExtractionReusableCodes.OpenAI import process_pdf_with_openai
from ReusableCodes.DatabaseQueries import update_pnl_status
from ReusableCodes.DatabaseQueries import get_split_pdf_path
from ExtractionReusableCodes.Azure_Document_Intelligence import azure_pdf_to_excel_conversion
from ExtractionReusableCodes.mapping_and_comparison import mapping_and_comp
from ReusableCodes.DatabaseQueries import insert_new_tags
from ReusableCodes.DatabaseQueries import update_excel_status_and_path
from pathlib import Path
import unicodedata
import time


def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text


def normalize(text):
    text = str(text)

    # Normalize unicode (Excel + OCR hidden chars)
    text = unicodedata.normalize("NFKD", text)

    text = text.lower()
    text = text.replace("&", "and")

    # Remove punctuation EXCEPT hyphen
    text = re.sub(r"[^\w\s-]", "", text)

    # Remove extra spaces
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalize_dict_keys(data):
    if isinstance(data, dict):
        return {
            normalize(k): normalize_dict_keys(v)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [normalize_dict_keys(item) for item in data]
    else:
        return data


def finance_main(db_config, config_dict, pdf_path, registration_no, output_file_path, financial_type, temp_pdf_path, database_id):
    setup_logging()
    error_count = 0
    errors = []
    try:
        # company_name='Beximco Pharmaceuticals Limited'
        if financial_type == 'finance':
            header_keywords = str(config_dict['standalone_headers_finance']).split(',')
            field_keywords = str(config_dict['financial_fields']).split(',')
            column_name = config_dict['subtotals_column_name']
            germany_contents = (config_dict['germany_contents'].split(','))
            is_pnl = False
            # if financial_data_type == 'Consolidated':
            #     print("Consolidated finance")
            #     header_keywords = str(config_dict['consolidated_headers_finance']).split(',')
            #     negative_header = 'standalone'
            # else:
            #     negative_header = 'consolidated'
            #     print("Standalone finance")
            #     header_keywords = str(config_dict['standalone_headers_finance']).split(',')
            # field_keywords = str(config_dict['financial_fields']).split(',')
            # germany_contents = (config_dict['germany_contents'].split(','))
            # header_keywords = str(config_dict['standalone_headers']).split(',')
            # field_keywords = str(config_dict['standalone_fields']).split(',')
            # company_header = company_name
        elif financial_type == 'pnl':
            column_name = config_dict['pnl_column_name']
            header_keywords = str(config_dict['standalone_headers_profit_and_loss']).split(',')
            field_keywords = str(config_dict['profit_and_loss_fields']).split(',')
            germany_contents = (config_dict['germany_contents'].split(','))
            is_pnl = True
            # if financial_data_type == 'Consolidated':
            #     print("Consolidated pnl")
            #     header_keywords = str(config_dict['consolidated_headers_profit_and_loss']).split(',')
            #     negative_header = 'standalone'
            # else:
            #     print("Standalone pnl")
            #     negative_header = 'consolidated'
            #     header_keywords = str(config_dict['standalone_headers_profit_and_loss']).split(',')
            # field_keywords = str(config_dict['profit_and_loss_fields']).split(',')
            # germany_contents = (config_dict['germany_contents'].split(','))
        else:
            raise Exception("No Input financial type provided")

        currency = 'EURO'
        config_file_path = config_dict['Financial_config']
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(config_file_path):
            raise Exception("Main Mapping File not found")
        try:
            df_map = pd.read_excel(config_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map['Value'] = None
        open_ai_df_list = []
        registration_no_column_name = config_dict['registration_no_Column_name']
        if financial_type == 'finance':
            financial_df = df_map[(df_map['Type_of_financial'] == config_dict['financial_keyword']) | (df_map['Type_of_financial'] == config_dict['common_keyword'])]
        elif financial_type == 'pnl':
            financial_df = df_map[(df_map['Type_of_financial'] == config_dict['profit_and_loss_keyword']) | (
                        df_map['Type_of_financial'] == config_dict['common_keyword'])]
        else:
            raise Exception("No input financial type provided")
        straight_df = financial_df[(financial_df[financial_df.columns[1]] == config_dict['financial_straight_keyword']) &
                            (financial_df['Node'].notna()) &
                            (financial_df['Node'] != '') &
                            (financial_df['Node'] != 'null')]
        main_field_nodes = straight_df['main_dict_node'].unique()
        open_ai_dict = {}
        for field_node in main_field_nodes:
            straight_nodes_list = straight_df[straight_df['main_dict_node'] == field_node]['Node'].unique()
            open_ai_dict[field_node] = {field_name: '' for field_name in straight_nodes_list}
        straight_field_nodes = straight_df[(straight_df['main_dict_node'] == '') | (straight_df['main_dict_node'].isna())]['Node'].unique()
        exclude_fields = ['year', 'financial_year', 'nature', 'filing_type', 'filing_standard','Currency']
        master_dict = {"Group": [{'YYYY-MM-DD': ""}], "Company": [{'YYYY-MM-DD': ""}]}
        # if financial_data_type == 'Consolidated':
        #     master_dict = {"Group": [{'YYYY-MM-DD': ""}]}
        # else:
        #     master_dict = {"Company": [{'YYYY-MM-DD': ""}]}
        open_ai_dict_straight = {field_name: '' for field_name in straight_field_nodes if field_name not in exclude_fields}
        open_ai_dict.update(open_ai_dict_straight)
        # split_status, split_pdf_path, pdf_to_excel_conversion_status, excel_file_path = get_split_pdf_path(db_config,registration_no,database_id)
        # if financial_data_type == 'Consolidated':
        master_dict["Group"][0]["YYYY-MM-DD"] = str(open_ai_dict)
        # else:
        master_dict["Company"][0]["YYYY-MM-DD"] = str(open_ai_dict)
        logging.info(master_dict)
        if financial_type == 'finance':
            prompt = config_dict['financial_prompt'] + '\n' + str(master_dict) + '\n' + '\n' + str(config_dict['financial_example_prompt'])
        elif financial_type == 'pnl':
            prompt = config_dict['profit_and_loss_prompt'] + '\n' + str(master_dict) + '\n' + '\n' + str(config_dict['financial_example_prompt'])
        else:
            raise Exception("No input financial type provided")

        print(f"Prompt - {prompt}")
        # Call get_split_status function after obtaining output
        split_status, finance_split_pdf_path, pnl_split_pdf_path, finance_pdf_to_excel_conversion_status, finance_excel_file_path, pnl_excel_file_path, pnl_pdf_to_excel_conversion_status = get_split_pdf_path(
            db_config, registration_no, database_id)

        if financial_type == 'finance':
            split_pdf_path = finance_split_pdf_path
            excel_file_path = finance_excel_file_path
            excel_path_column_name = 'finance_excel_path'
            pdf_to_excel_conversion_status = finance_pdf_to_excel_conversion_status
            status_column_name = "finance_pdf_to_excel_conversion_status"
        elif financial_type == 'pnl':
            split_pdf_path = pnl_split_pdf_path
            excel_file_path = pnl_excel_file_path
            excel_path_column_name = 'pnl_excel_path'
            pdf_to_excel_conversion_status = pnl_pdf_to_excel_conversion_status
            status_column_name = "pnl_pdf_to_excel_conversion_status"
        else:
            raise ValueError("Invalid financial_type. Expected 'finance' or 'pnl'.")

        # Log the results from get_split_status
        logging.info(
            f"Fetched split_status: {split_status}, split_pdf_path: {split_pdf_path}, pdf_to_excel_conversion_status: {pdf_to_excel_conversion_status}, excel_path: {excel_file_path}")
        if str(pdf_to_excel_conversion_status).lower() != 'y' and (
                excel_file_path == '' or excel_file_path is None):
            # Example usage
            print("split_pdf_path", split_pdf_path)
            excel_file_path = os.path.splitext(split_pdf_path)[0] + '.xlsx'
            # Standardize the path using pathlib (optional, but more robust)
            excel_file_path = Path(excel_file_path).as_posix()  # Converts to use forward slashes
            output_directory = os.path.dirname(excel_file_path)
            print("output_directory", output_directory)
            table_dataframes, conversion_status = azure_pdf_to_excel_conversion(split_pdf_path, excel_file_path)
            if conversion_status:
                update_excel_status_and_path(db_config, registration_no, database_id, excel_file_path,
                                             excel_path_column_name, status_column_name)

        extracted_text = get_excel_data(excel_file_path)
        # output = process_pdf_with_openai(temp_pdf_path, prompt)
        # # output = process_pdf_with_claude(temp_pdf_path, prompt)
        # print("pdf image processed")
        # print("output:",output)
        # if 'sorry' in output or 'unable' in output:
        #     print(f"Unable to process with image so going with extracted text")
        #     # extracted_text = extract_whole_pdf_data(temp_pdf_path)
        #     extracted_text = extract_text_from_readable_pdf(temp_pdf_path)
        #     extracted_text = extracted_text.replace(',', '')
        #     # extracted_text = extracted_text.replace('-', '0')
        #     logging.info(extracted_text)
        #     if extracted_text:
        #         extracted_text = extracted_text.replace(',', '')
        #         logging.info(f"Normal extraction text length: {len(extracted_text)}")
        #     else:
        #         logging.info("Normal extraction returned None or empty")
        #     if (
        #             extracted_text is None or
        #             extracted_text.strip() == "" or
        #             len(extracted_text.strip()) < 500
        #     ):
        #         logging.info(f"Going for azure ocr extraction as before extraction not happened properly")
        #         # extracted_text = analyze_read(pdf_path, header_keywords, field_keywords, germany_contents)
        #         extracted_text = extract_whole_pdf_data(temp_pdf_path)
        #     if extracted_text is not None:
        #         temp_pdf_directory = os.path.dirname(pdf_path)
        #         pdf_document_name = os.path.basename(pdf_path)
        #         pdf_document_name = str(pdf_document_name).replace('.pdf', '.txt')
        #         if financial_type == 'finance':
        #             temp_text_name = 'temp_translated_finance_' + pdf_document_name
        #         else:
        #             temp_text_name = 'temp_translated_pnl_' + pdf_document_name
        #         if '.pdf' not in temp_text_name:
        #             temp_text_name += '.txt'
        #         temp_text_path = os.path.join(temp_pdf_directory, temp_text_name)
        #         with open(temp_text_path, 'w', encoding='utf-8') as file:
        #             file.write(extracted_text)
        #     if extracted_text is None and is_pnl:
        #         logging.info(f"No pnl data found for {registration_no}")
        #         update_pnl_status(db_config, registration_no, database_id)
        #         return True
        #     extracted_text = extracted_text.replace(',', '')
        #     output = split_openai(extracted_text, prompt)

        output = split_openai(extracted_text, prompt)
        # output = split_claude(extracted_text, prompt)
        try:
            output = re.sub(r'(?<=: ")(\d+(,\d+)*)(?=")', lambda x: x.group(1).replace(",", ""), output)
        except:
            pass
        output = remove_text_before_marker(output, "```json")
        output = remove_string(output, "```")
        logging.info(output)
        group_output = {}
        company_output = {}
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        if financial_type == 'finance':
            output_directory = os.path.dirname(pdf_path)  # Get the directory of the PDF file

            # Define the JSON file name and path
            open_ai_json_file_path = os.path.join(output_directory, "open_ai_finance.json")

            # Save the processed output to the JSON file
            with open(open_ai_json_file_path, 'w') as json_file:
                json.dump(output, json_file, indent=4)
            print(f"Processed output saved to {open_ai_json_file_path}")
        elif financial_type == 'pnl':
            output_directory = os.path.dirname(pdf_path)  # Get the directory of the PDF file

            # Define the JSON file name and path
            open_ai_json_file_path = os.path.join(output_directory, "open_ai_pnl.json")

            # Save the processed output to the JSON file
            with open(open_ai_json_file_path, 'w') as json_file:
                json.dump(output, json_file, indent=4)
            print(f"Processed output saved to {open_ai_json_file_path}")
        else:
            raise ValueError("Invalid financial_type. Expected 'finance' or 'pnl'.")

        try:
            # Handle Group Output
            consolidated_keywords = (config_dict['consolidated_keywords'].split(','))
            if len(output["Group"]) != 0:
                # If the first structure is detected (list of dictionaries per year)
                if isinstance(output["Group"][0], dict):
                    # For first structure where years are inside dictionaries
                    group_output = {}
                    for item in output["Group"]:
                        group_output.update(item)
                else:
                    # For second structure where years are keys within the first dictionary
                    group_output = output["Group"][0]
            else:
                group_output = {}
        except:
            group_output = {}

        try:
            # Handle Company Output
            if len(output["Company"]) != 0:
                # If the first structure is detected (list of dictionaries per year)
                if isinstance(output["Company"][0], dict):
                    # For first structure where years are inside dictionaries
                    company_output = {}
                    for item in output["Company"]:
                        company_output.update(item)
                else:
                    # For second structure where years are keys within the first dictionary
                    company_output = output["Company"][0]
            else:
                company_output = {}
        except:
            company_output = {}

        company_output = normalize_dict_keys(company_output)
        group_output = normalize_dict_keys(group_output)

        main_group_df = financial_df.copy()
        print("success")
        main_company_df = financial_df.copy()
        df_list = []
        for key, value in company_output.items():
            company_year_df = main_company_df.copy()
            nature = 'Standalone'
            financial_value = None
            field_name = None
            intangible_found = False
            tangible_found = False
            financial_investments_found = False
            stocks_found = False
            provisions_found = False
            inventories_found = False
            receivables_and_other_assets_found = False
            equity_found = False
            equity_capital_found = False
            depreciation_found = False
            equity_total_found = False
            for index,row in company_year_df.iterrows():
                try:
                    field_name = str(row.iloc[0]).strip()
                    print("field_name:",field_name)
                    main_node = row['main_dict_node']
                    value_type = str(row.iloc[1]).strip()
                    if value_type.lower() == 'straight':
                        node = row['Node']
                        if field_name == 'year':
                            financial_value = key
                        elif field_name == 'nature':
                            financial_value = nature
                        elif field_name == 'Currency':
                            financial_value = currency
                        elif field_name == 'filing_type':
                            financial_value = 'Annual return'
                        else:
                            if pd.notna(main_node) and main_node != '' and main_node != 'nan':
                                financial_value = value[normalize(main_node)][normalize(node)]
                            else:
                                financial_value = value[normalize(node)]
                        try:
                            if field_name != 'year':
                                financial_value = float(financial_value)
                                if financial_type == 'pnl' and financial_value < 0:
                                    financial_value = abs(financial_value)
                        except:
                            pass
                except Exception as e:
                    logging.info(f"Error while processing {field_name} {e}")
                    financial_value = None

                if field_name == 'Intangible_assets':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        intangible_found = True
                        print(intangible_found)
                elif (field_name == 'Tangible_assets') or (field_name == 'Sachanlagen') :
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        tangible_found = True
                        print(tangible_found)
                elif field_name == 'Financial_investments':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        financial_investments_found = True
                        print(financial_investments_found)
                elif field_name == 'Stocks':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        stocks_found = True
                        print(stocks_found)
                elif field_name == 'Provisions':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        provisions_found = True
                        print(provisions_found)
                elif field_name == 'Inventories':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        inventories_found = True
                        print(inventories_found)
                elif field_name == 'Receivables_and_other_assets':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        receivables_and_other_assets_found = True
                        print(receivables_and_other_assets_found)
                elif field_name == 'Equity':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        equity_found = True
                        print(equity_found)
                elif field_name == 'Equity_capital':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        equity_capital_found = True
                        print(equity_capital_found)
                elif field_name == 'Depreciation':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        depreciation_found = True
                        print(depreciation_found)
                elif field_name == 'Total_Equity':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        equity_total_found = True
                        print(equity_total_found)
                else:
                    pass

                if field_name in [
                                        'Concessions_industrial_property_rights_and_similar',
                                        'Goodwill',
                                        'Advance_payments_made',
                                        'Concessions_and_similar_rights_acquired_against_payment',
                                        'Self_created_industrial_property_rights_and_similar_rights_and_values',
                                        'other_intangible_assets',
                                        'concessions_industrial_property_rights_and_similar_rights_and_values_acquired_against_payment_as_well_as_licenses_to_such_rights_and_values',
                                        'Self_created_development_services',
                                        'Rights_and_licenses_acquired_for_a_fee',
                                        'purchased_industrial_property_rights_and_similar_rights_and_values',
                                        'Concessions_industrial_property_rights_and_similar_rights_and_assets_acquired_against_payment_as_well_as_licences_to_such_rights_and_assets',
                                        'Concessions_industrial_property_rights_and_similar_rights_and_assets_acquired_for_a_fee_as_well_as_licenses_to_such_rights_and_assets',
                                        'Concessions_industrial_property_rights_and_similar_rights_and_assets_acquired_for_a_fee__as_well_as_licenses_to_such_rights_and_assets'
                                    ]:
                    if intangible_found:
                        logging.info(f"Intangible assets already found so skipping {field_name}")
                        financial_value = None
                elif field_name in [
                            'Technical_equipment_and_machinery',
                            'Other_equipment_fixtures_and_fittings',
                            'Advance_payments_and_assets_under_construction',
                            'Land_and_buildings',
                            'Land_and_buildings_including_buildings_on_third_party_land',
                            'Buildings_on_third_party_land',
                            'Other_facilities_operating_and_business_equipment',
                            'Land_rights_equivalent_to_land_and_buildings_including_buildings_on_third_party_land',
                            'property_plant_and_equipment',
                            'Facilities_under_construction',
                            'Land__land_rights_and_buildings__including_buildings_on_third_party_land'
                        ]:
                    if tangible_found:
                        logging.info(f"Tangible assets already found so skipping {field_name}")
                        financial_value = None
                elif field_name in [
                            'Investments',
                            'Shares_in_affiliated_companies',
                            'Loans_to_affiliated_companies',
                            'Securities_held_as_fixed_assets',
                            'Other_loans',
                            'Investments_in_associated_companies',
                            'Investments_in_associates_and_joint_ventures',
                            'Investment_properties',
                            'Loans_to_companies_with_which_a_shareholding_relationship_exists',
                            'Investments_accounted_for_using_the_equity_method',
                            'Other_financial_investments',
                            'Shares_in_associated_companies',
                            'Loans_to_associated_companies'
                        ]:
                    if financial_investments_found:
                        logging.info(f"Financial investments already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Inventories', 'Raw_materials_consumables_and_supplies',
                                    'Work_in_progress',
                                    'Advance_payments_made',
                                    'Advance_payments_received_on_orders',
                                    'Raw_materials_auxiliary_materials_and_operating_supplies',
                                    'Finished_products_and_goods',
                                    'Unfinished_services',
                                    'Goods',
                                    'Requests_from_deliveries_and_services',
                                    'Work_in_progress_work_in_progress',
                                    'Auxiliary_and_operating_materials',
                                    'Deposit_paid',
                                    'Advance_payments_received']:
                    if stocks_found:
                        logging.info(f"Stocks already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Other_provisions', 'Provisions_for_pensions_and_similar_obligations',
                                    'Other_long_term_provisions', 'Tax_provisions', 'Provisions_for_other_long_term_employee_benefits']:
                    if provisions_found:
                        logging.info(f"Provisions already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Trade_receivables',
                                    'Other_assets', 'Trade_accounts_receivable'
                                    , 'Trade_accounts_receivable_and_other_receivables']:
                    if receivables_and_other_assets_found:
                        logging.info(f"Receivables_and_other_assets already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Equity_capital', 'Total_Equity']:
                    if equity_found:
                        financial_value = None
                elif field_name in ['Equity', 'Total_Equity']:
                    if equity_capital_found:
                        financial_value = None
                elif field_name in ['Equity', 'Equity_capital']:
                    if equity_total_found:
                        financial_value = None
                elif field_name in [
                                    'Depreciation_of_intangible_assets_and_property_plant_and_equipment',
                                    'Depreciation_on_financial_assets_and_on_current_securities',
                                    'depreciation_on_financial_assets_and_on_securities_held_as_current_assets',
                                    'Depreciation_of_intangible_assets_and_tangible_assets'
                                ]:
                    if depreciation_found:
                        financial_value = None
                else:
                    pass
                if field_name in ['Raw_materials_consumables_and_supplies',
                                    'Work_in_progress',
                                    'Advance_payments_made',
                                    'Advance_payments_received_on_orders',
                                    'Raw_materials_auxiliary_materials_and_operating_supplies',
                                    'Finished_products_and_goods',
                                    'Unfinished_services',
                                    'Goods',
                                    'Requests_from_deliveries_and_services',
                                    'Work_in_progress_work_in_progress',
                                    'Auxiliary_and_operating_materials',
                                    'Deposit_paid',
                                    'Advance_payments_received']:
                    if inventories_found:
                        logging.info(f"Inventories already found so skipping {field_name}")
                        financial_value = None
                company_year_df.at[index, 'Value'] = financial_value
            df_list.append(company_year_df)
        for key, value in group_output.items():
            group_year_df = main_group_df.copy()
            nature = 'Consolidated'
            financial_value = None
            field_name = None
            intangible_found = False
            tangible_found = False
            financial_investments_found = False
            stocks_found = False
            provisions_found = False
            inventories_found = False
            receivables_and_other_assets_found = False
            equity_found = False
            equity_capital_found = False
            depreciation_found = False
            equity_total_found = False
            for index,row in group_year_df.iterrows():
                try:
                    field_name = str(row.iloc[0]).strip()
                    main_node = row['main_dict_node']
                    value_type = str(row.iloc[1]).strip()
                    if value_type.lower() == 'straight':
                        node = row['Node']
                        if field_name == 'year':
                            financial_value = key
                        elif field_name == 'nature':
                            financial_value = nature
                        elif field_name == 'Currency':
                            financial_value = currency
                        elif field_name == 'filing_type':
                            financial_value = 'Annual return'
                        else:
                            if pd.notna(main_node) and main_node != '' and main_node != 'nan':
                                financial_value = value[normalize(main_node)][normalize(node)]
                            else:
                                financial_value = value[normalize(node)]
                        try:
                            if field_name != 'year':
                                financial_value = float(financial_value)
                                if financial_type == 'pnl' and financial_value < 0:
                                    financial_value = abs(financial_value)
                        except:
                            pass
                except Exception as e:
                    logging.info(f"Error while processing {field_name} {e}")
                    financial_value = None
                if field_name == 'Intangible_assets':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        intangible_found = True
                        print(intangible_found)
                elif (field_name == 'Tangible_assets') or (field_name == 'Sachanlagen'):
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        tangible_found = True
                        print(tangible_found)
                elif field_name == 'Financial_investments':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        financial_investments_found = True
                        print(financial_investments_found)
                elif field_name == 'Stocks':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        stocks_found = True
                        print(stocks_found)
                elif field_name == 'Provisions':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        provisions_found = True
                        print(provisions_found)
                elif field_name == 'Inventories':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        inventories_found = True
                        print(inventories_found)
                elif field_name == 'Receivables_and_other_assets':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        receivables_and_other_assets_found = True
                        print(receivables_and_other_assets_found)
                elif field_name == 'Equity':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        equity_found = True
                        print(equity_found)
                elif field_name == 'Equity_capital':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        equity_capital_found = True
                        print(equity_capital_found)
                elif field_name == 'Depreciation':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        depreciation_found = True
                        print(depreciation_found)
                elif field_name == 'Total_Equity':
                    print(financial_value)
                    if financial_value is not None and financial_value != "":
                        equity_total_found = True
                        print(equity_total_found)
                else:
                    pass

                if field_name in [
                    'Concessions_industrial_property_rights_and_similar',
                    'Goodwill',
                    'Advance_payments_made',
                    'Concessions_and_similar_rights_acquired_against_payment',
                    'Self_created_industrial_property_rights_and_similar_rights_and_values',
                    'other_intangible_assets',
                    'concessions_industrial_property_rights_and_similar_rights_and_values_acquired_against_payment_as_well_as_licenses_to_such_rights_and_values',
                    'Self_created_development_services',
                    'Rights_and_licenses_acquired_for_a_fee',
                    'purchased_industrial_property_rights_and_similar_rights_and_values',
                    'Concessions_industrial_property_rights_and_similar_rights_and_assets_acquired_against_payment_as_well_as_licences_to_such_rights_and_assets',
                    'Concessions_industrial_property_rights_and_similar_rights_and_assets_acquired_for_a_fee_as_well_as_licenses_to_such_rights_and_assets',
                    'Concessions_industrial_property_rights_and_similar_rights_and_assets_acquired_for_a_fee__as_well_as_licenses_to_such_rights_and_assets'
                ]:
                    if intangible_found:
                        logging.info(f"Intangible assets already found so skipping {field_name}")
                        financial_value = None
                elif field_name in [
                    'Technical_equipment_and_machinery',
                    'Other_equipment_fixtures_and_fittings',
                    'Advance_payments_and_assets_under_construction',
                    'Land_and_buildings',
                    'Land_and_buildings_including_buildings_on_third_party_land',
                    'Buildings_on_third_party_land',
                    'Other_facilities_operating_and_business_equipment',
                    'Land_rights_equivalent_to_land_and_buildings_including_buildings_on_third_party_land',
                    'property_plant_and_equipment',
                    'Facilities_under_construction',
                    'Land__land_rights_and_buildings__including_buildings_on_third_party_land'
                ]:
                    if tangible_found:
                        logging.info(f"Tangible assets already found so skipping {field_name}")
                        financial_value = None
                elif field_name in [
                    'Investments',
                    'Shares_in_affiliated_companies',
                    'Loans_to_affiliated_companies',
                    'Securities_held_as_fixed_assets',
                    'Other_loans',
                    'Investments_in_associated_companies',
                    'Investments_in_associates_and_joint_ventures',
                    'Investment_properties',
                    'Loans_to_companies_with_which_a_shareholding_relationship_exists',
                    'Investments_accounted_for_using_the_equity_method',
                    'Other_financial_investments',
                    'Shares_in_associated_companies',
                    'Loans_to_associated_companies'
                ]:
                    if financial_investments_found:
                        logging.info(f"Financial investments already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Inventories', 'Raw_materials_consumables_and_supplies',
                                    'Work_in_progress',
                                    'Advance_payments_made',
                                    'Advance_payments_received_on_orders',
                                    'Raw_materials_auxiliary_materials_and_operating_supplies',
                                    'Finished_products_and_goods',
                                    'Unfinished_services',
                                    'Goods',
                                    'Requests_from_deliveries_and_services',
                                    'Work_in_progress_work_in_progress',
                                    'Auxiliary_and_operating_materials',
                                    'Deposit_paid',
                                    'Advance_payments_received']:
                    if stocks_found:
                        logging.info(f"Stocks already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Other_provisions', 'Provisions_for_pensions_and_similar_obligations',
                                    'Other_long_term_provisions', 'Tax_provisions',
                                    'Provisions_for_other_long_term_employee_benefits']:
                    if provisions_found:
                        logging.info(f"Provisions already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Trade_receivables',
                                    'Other_assets', 'Trade_accounts_receivable',
                                    'Trade_accounts_receivable_and_other_receivables']:
                    if receivables_and_other_assets_found:
                        logging.info(f"Receivables_and_other_assets already found so skipping {field_name}")
                        financial_value = None
                elif field_name in ['Equity_capital', 'Total_Equity']:
                    if equity_found:
                        financial_value = None
                elif field_name in ['Equity', 'Total_Equity']:
                    if equity_capital_found:
                        financial_value = None
                elif field_name in ['Equity', 'Equity_capital']:
                    if equity_total_found:
                        financial_value = None
                elif field_name in [
                    'Depreciation_of_intangible_assets_and_property_plant_and_equipment',
                    'Depreciation_on_financial_assets_and_on_current_securities',
                    'depreciation_on_financial_assets_and_on_securities_held_as_current_assets',
                    'Depreciation_of_intangible_assets_and_tangible_assets'
                ]:
                    if depreciation_found:
                        financial_value = None
                else:
                    pass
                if field_name in ['Raw_materials_consumables_and_supplies',
                                  'Work_in_progress',
                                  'Advance_payments_made',
                                  'Advance_payments_received_on_orders',
                                  'Raw_materials_auxiliary_materials_and_operating_supplies',
                                  'Finished_products_and_goods',
                                  'Unfinished_services',
                                  'Goods',
                                  'Requests_from_deliveries_and_services',
                                  'Work_in_progress_work_in_progress',
                                  'Auxiliary_and_operating_materials',
                                  'Deposit_paid',
                                  'Advance_payments_received']:
                    if inventories_found:
                        logging.info(f"Inventories already found so skipping {field_name}")
                        financial_value = None
                group_year_df.at[index, 'Value'] = financial_value
            df_list.append(group_year_df)
        for i, df in enumerate(df_list):
            formula_df = df[df[df.columns[1]] == config_dict['Formula_Keyword']]
            for _, row in formula_df.iterrows():
                company_formula = row['Node']
                company_formula_field_name = row['Field_Name']
                subtype = row['main_dict_node']
                for field_name in df['Field_Name']:
                    try:
                        field_name = str(field_name)
                        pattern = r'\b' + re.escape(field_name) + r'\b'
                        # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
                        if pd.notna(subtype) and subtype != '' and subtype != 'nan':
                            replacement_value = str(
                                df[(df['Field_Name'] == field_name) & (df['main_dict_node'] == subtype)]['Value'].values[0])
                        else:
                            replacement_value = str(
                                df[df['Field_Name'] == field_name]['Value'].values[0])
                        replacement_value = str(replacement_value) if replacement_value != '' else '0'
                        company_formula = re.sub(pattern, replacement_value,company_formula)
                    except Exception as e:
                        continue
                logging.info(company_formula_field_name + ":" + company_formula)
                try:
                    # Calculate the value using the provided formula and insert it
                    if 'None' in company_formula:
                        company_formula = company_formula.replace('None', '0')
                    if pd.notna(subtype) and subtype != '' and subtype != 'nan':
                        df.at[
                            df[(df['Field_Name'] == company_formula_field_name) & (df['main_dict_node'] == subtype)].index[
                                0], 'Value'] = round(eval(company_formula), 2)
                    else:
                        df.at[
                            df[df['Field_Name'] == company_formula_field_name].index[
                                0], 'Value'] = round(eval(company_formula), 2)
                except (NameError, SyntaxError):
                    # Handle the case where the formula is invalid or contains a missing field name
                    logging.info(f"Invalid formula for {company_formula_field_name}: {company_formula}")
            df_list[i] = df
        logging.info(df_list)
        for df_to_insert in df_list:
            sql_tables_list = df_to_insert[df_to_insert.columns[3]].unique()
            logging.info(sql_tables_list)
            year_value = df_to_insert[df_to_insert['Field_Name'] == 'year']['Value'].values[0]
            nature_value = df_to_insert[df_to_insert['Field_Name'] == 'nature']['Value'].values[0]
            logging.info(year_value)
            # Check if financial data is available for the given registration number, year, and nature
            data_exists = financial_data_availability_check(db_config, registration_no, year_value, nature_value,column_name)
            if not data_exists:  # Only proceed if no data is found
                for table_name in sql_tables_list:
                    table_df = df_to_insert[df_to_insert[df_to_insert.columns[3]] == table_name]
                    columns_list = table_df[table_df.columns[4]].unique()
                    logging.info(columns_list)
                    for column_name in columns_list:
                        logging.info(column_name)
                        # filter table df with only column value
                        column_df = table_df[table_df[table_df.columns[4]] == column_name]
                        logging.info(column_df)
                        # create json dict with keys of field name and values for the same column name entries
                        json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                        # Convert the dictionary to a JSON string
                        json_string = json.dumps(json_dict)
                        logging.info(json_string)
                        try:
                            update_database_single_value_financial(db_config, table_name, registration_no_column_name,
                                                                   registration_no, column_name, json_string, year_value,
                                                                   nature_value)
                        except Exception as e:
                            logging.info(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                         f"with data {json_string}")
            else:
                logging.info(f"Data already exists in the 'financials' table for registration_no={registration_no}, "
                             f"year={year_value}, and nature={nature_value}. Skipping data insertion/update.")
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in df_list:
                # logging.info(dataframe)
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2
    except Exception as e:
        logging.error(f"Error in extracting financial data for reg no - {registration_no}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 6")
            return True
        else:
            raise Exception(f"Multiple exceptions occurred:\n\n" + "\n".join(errors))
    finally:
        time.sleep(10)