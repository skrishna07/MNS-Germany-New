import pandas as pd
import json
from ReusableCodes.PythonLogging import setup_logging
import os
import logging
from ExtractionReusableCodes.AmazonOCRAllPages import process_textract_document
from ExtractionReusableCodes.OpenAI import split_openai
from ReusableCodes.DatabaseQueries import update_database_single_value
from ReusableCodes.DatabaseQueries import insert_datatable_with_table_director
from ExtractionReusableCodes.ExtractReadablePDF import extract_text_from_readable_pdf
from ExtractionReusableCodes.ExtractReadablePDF import extract_text_by_type
import traceback
from datetime import datetime
from auditor_details import analyze_read_auditors

def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text


def get_age(DOB):
    # Given date in the "dd/mm/yyyy" format
    try:
        given_date_string = DOB

        # Parse the given date string
        given_date = datetime.strptime(given_date_string, "%Y-%m-%d")

        # Get the current date
        current_date = datetime.now()

        # Calculate the age
        age = current_date.year - given_date.year - (
                (current_date.month, current_date.day) < (given_date.month, given_date.day))
        return age
    except Exception as e:
        logging.info(f"Error in calculating age {e}")
        return None


def registry_document_main(db_config, config_dict, pdf_path, output_file_path, registration_no,document_name):
    setup_logging()
    error_count = 0
    errors = []
    try:
        print(document_name)
        if 'cd' in str(document_name).lower():
            extraction_config = config_dict['registry_config_path_CD']
        elif 'ad' in str(document_name).lower():
            extraction_config = config_dict['registry_config_path_AD']
        elif 'dk' in str(document_name).lower():
            extraction_config = config_dict['registry_config_path_DK']
        else:
            raise Exception("Invalid document name")
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(extraction_config):
            raise Exception("Main Mapping File not found")
        try:
            df_map = pd.read_excel(extraction_config, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map['Value'] = None
        output_dataframes_list = []
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        single_nodes = single_df['Node'].unique()
        open_ai_dict = {field_name: '' for field_name in single_nodes}
        for index, row in group_df.iterrows():
            node_values = str(row['Node']).split(',')
            sub_dict = {field_name: '' for field_name in node_values}
            main_node = row['main_dict_node']
            sub_list = {main_node: [sub_dict]}
            open_ai_dict.update(sub_list)
        if 'cd' in str(document_name).lower():
            # pdf_text = process_textract_document(pdf_path)
            pdf_text = extract_text_by_type(pdf_path)
            registry_prompt = config_dict['CD_prompt'] + '\n' + str(open_ai_dict)
        elif 'ad' in str(document_name).lower():
            pdf_text = extract_text_from_readable_pdf(pdf_path)
            registry_prompt = config_dict['AD_prompt'] + '\n' + str(open_ai_dict)
        elif 'dk' in str(document_name).lower():
            pdf_text = extract_text_from_readable_pdf(pdf_path)
            registry_prompt = config_dict['DK_prompt'] + '\n' + str(open_ai_dict)

        else:
            raise Exception("Invalid input file type")
        output = split_openai(pdf_text, registry_prompt)
        output = remove_text_before_marker(output, "```json")
        output = remove_string(output, "```")
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        for index, row in df_map.iterrows():
            dict_node = str(row.iloc[2]).strip()
            type = str(row.iloc[1]).strip()
            main_group_node = str(row.iloc[6]).strip()
            if type.lower() == 'single':
                value = output.get(dict_node)
                value = str(value).replace("'", "")
            elif type.lower() == 'group':
                value = output.get(main_group_node)
            else:
                value = None
            df_map.at[index, 'Value'] = value
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        output_dataframes_list.append(single_df)
        output_dataframes_list.append(group_df)
        registration_no_column_name = config_dict['registration_no_Column_name']
        sql_tables_list = single_df[single_df.columns[3]].unique()
        for table_name in sql_tables_list:
            table_df = single_df[single_df[single_df.columns[3]] == table_name]
            columns_list = table_df[table_df.columns[4]].unique()
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
                    update_database_single_value(db_config, table_name,registration_no_column_name,
                                                                           registration_no,
                                                                           column_name, json_string)
                except Exception as e:
                    logging.error(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                  f"with data {json_string}")
                    error_count += 1
                    tb = traceback.extract_tb(e.__traceback__)
                    for frame in tb:
                        if frame.filename == __file__:
                            errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        for index, row in group_df.iterrows():
            try:
                field_name = str(row.iloc[0]).strip()
                nodes = str(row.iloc[2]).strip()
                sql_table_name = str(row.iloc[3]).strip()
                column_names = str(row.iloc[4]).strip()
                main_group_node = str(row.iloc[6]).strip()
                value_list = row['Value']
                if value_list is not None:
                    if len(value_list) == 0:
                        logging.info(f"No value for {field_name} so going to next field")
                        continue
                else:
                    logging.info(f"No value for {field_name} so going to next field")
                    continue
                table_df = pd.DataFrame(value_list)
                logging.info(table_df)
                column_names_list = column_names.split(',')
                column_names_list = [x.strip() for x in column_names_list]
                table_df = table_df.fillna('')
                if sql_table_name == 'authorized_signatories' and 'other' in str(document_name).lower():
                    table_df['designation'] = None
                    column_names_list.append('designation')
                    if len(value_list) > 1:
                        designation_value = 'Director'
                    else:
                        designation_value = 'Proprietor'
                    for index_director, row_director in table_df.iterrows():
                        table_df.at[index_director, 'designation'] = designation_value
                # if sql_table_name == 'current_shareholdings':
                #     table_df['percentage_holding'] = None
                #     column_names_list.append('percentage_holding')
                #     try:
                #         paidup_capital = single_df[single_df['Field_Name'] == 'paidup_capital']['Value'].values[0]
                #         paidup_capital = str(paidup_capital)
                #         paidup_capital = str(paidup_capital).replace(',','')
                #         paidup_capital = float(paidup_capital)
                #         for index_share, row_share in table_df.iterrows():
                #             try:
                #                 no_of_shares = str(row_share['no_of_shares'])
                #                 no_of_shares = no_of_shares.replace(',', '')
                #                 no_of_shares = float(no_of_shares)
                #                 percentage_holding = (no_of_shares / paidup_capital)*100
                #                 percentage_holding = round(percentage_holding, 2)
                #             except Exception as e:
                #                 logging.error(f"Error fetching percentage holding {e}")
                #                 percentage_holding = None
                #                 error_count += 1
                #                 tb = traceback.extract_tb(e.__traceback__)
                #                 for frame in tb:
                #                     if frame.filename == __file__:
                #                         errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                #             table_df.at[index_share, 'percentage_holding'] = percentage_holding
                #     except Exception as e:
                #         logging.error(f"Error in fetching percentage holding {e}")
                #         error_count += 1
                #         tb = traceback.extract_tb(e.__traceback__)
                #         for frame in tb:
                #             if frame.filename == __file__:
                #                 errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                table_df[registration_no_column_name] = registration_no
                column_names_list.append(registration_no_column_name)
                column_names_list = [x.strip() for x in column_names_list]
                table_df.columns = column_names_list
                for _, df_row in table_df.iterrows():
                    try:
                        insert_datatable_with_table_director(config_dict, db_config, sql_table_name, column_names_list,
                                                             df_row, field_name)
                    except Exception as e:
                        logging.info(
                            f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                            df_row)
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
            except Exception as e:
                logging.error(f"Exception occurred while inserting for group values {e}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in output_dataframes_list:
                # logging.info(dataframe)
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2
        output_dataframes_list.clear()
    except Exception as e:
        logging.error(f"Error in extracting data from Form 40 {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 40")
            return True
        else:
            raise Exception(f"Multiple exceptions occurred:\n\n" + "\n".join(errors))


def auditor_details_main(db_config, config_dict, pdf_path, output_file_path, registration_no):
    setup_logging()
    error_count = 0
    errors = []
    try:
        # Load configuration for auditor documents
        auditor_config = config_dict['auditors_config_path']
        map_file_sheet_name = config_dict['config_sheet']

        # Verify existence of the mapping file and load it
        if not os.path.exists(auditor_config):
            raise Exception("Mapping File not found for auditor details")
        try:
            df_map = pd.read_excel(auditor_config, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Error reading mapping file: {e}")

        df_map['Value'] = None
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]

        # Prepare dictionary nodes for OpenAI prompt
        open_ai_dict = {field_name: '' for field_name in single_df['Node'].unique()}
        for index, row in group_df.iterrows():
            node_values = str(row['Node']).split(',')
            sub_dict = {field_name: '' for field_name in node_values}
            main_node = row['main_dict_node']
            sub_list = {main_node: [sub_dict]}
            open_ai_dict.update(sub_list)

        # Process PDF based on document type
        pdf_text = analyze_read_auditors(pdf_path)
        if pdf_text is None:
            return True
        if not pdf_text:
            raise Exception("Invalid input file type for auditor details")

        # Generate OpenAI prompt for auditor details and extract data
        auditor_prompt = config_dict['auditor_prompt'] + '\n' + str(open_ai_dict)
        output = split_openai(pdf_text, auditor_prompt)
        output = json.loads(remove_string(remove_text_before_marker(output, "```json"), "```"))
        # output = remove_text_before_marker(output, "```json")
        # output = remove_string(output, "```")
        logging.info(output)
        # Map extracted values to DataFrame
        for index, row in df_map.iterrows():
            dict_node = row.iloc[2].strip()
            type = row.iloc[1].strip()
            main_group_node = row.iloc[6].strip()
            value = output.get(dict_node) if type.lower() == 'single' else output.get(main_group_node)
            df_map.at[index, 'Value'] = value

        # Prepare DataFrames for output
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        output_dataframes_list = [single_df, group_df]
        registration_no_column_name = config_dict['registration_no_Column_name']

        # Insert group values into the database
        for index, row in group_df.iterrows():
            try:
                field_name = row.iloc[0].strip()
                sql_table_name = row.iloc[3].strip()
                column_names = row.iloc[4].strip()
                main_group_node = row.iloc[6].strip()
                value_list = row['Value']

                if not value_list:
                    logging.info(f"No value for {field_name} so skipping")
                    continue

                table_df = pd.DataFrame(value_list)
                table_df[registration_no_column_name] = registration_no
                column_names_list = [x.strip() for x in column_names.split(',')]
                column_names_list.append(registration_no_column_name)
                table_df.columns = column_names_list

                for _, df_row in table_df.iterrows():
                    try:
                        insert_datatable_with_table_director(config_dict, db_config, sql_table_name,
                                                             column_names_list, df_row, field_name)
                    except Exception as e:
                        logging.error(f"Exception {e} occurred while inserting row in {sql_table_name}: {df_row}")
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
            except Exception as e:
                logging.error(f"Exception occurred while processing group values: {e}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")

        # Export extracted values to Excel
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in output_dataframes_list:
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2

    except Exception as e:
        logging.error(f"Error extracting auditor details: {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info("Successfully extracted auditor details")
            return True
        else:
            raise Exception("Errors occurred:\n" + "\n".join(errors))
