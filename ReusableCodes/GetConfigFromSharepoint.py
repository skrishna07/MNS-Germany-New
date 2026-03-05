from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential
import os
from ReusableCodes.PythonLogging import setup_logging
import logging
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
# Replace these variables with your SharePoint site and credentials


def download_config_from_sharepoint(config_url, local_file_path):
    setup_logging()
    try:
        sharepoint_site_url = os.environ.get('sharepoint_site_url')
        sharepoint_username = os.environ.get('sharepoint_username')
        sharepoint_password = os.environ.get('sharepoint_password')

        user_credentials = UserCredential(sharepoint_username, sharepoint_password)
        # Authenticate and create a context for the SharePoint site
        ctx = ClientContext(sharepoint_site_url).with_credentials(user_credentials)
        # Get the file from SharePoint
        response = File.open_binary(ctx, config_url)
        # Save the file locally
        if response.status_code == 200:
            with open(local_file_path, "wb") as local_file:
                local_file.write(response.content)

            logging.info(f"File downloaded and saved to {local_file_path}")
        else:
            raise Exception(f"Unable to download file from sharepoint")
    except Exception as e:
        raise Exception(e)
