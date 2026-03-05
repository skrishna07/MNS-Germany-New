import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from pdf2image import convert_from_path
from io import BytesIO
import base64
from openai import OpenAI

def process_pdf_with_openai(pdf_path, prompt, dpi=300):
    def encode_image(image):
        """Encodes a PIL image to Base64."""
        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    # Convert PDF to images
    images = convert_from_path(pdf_path, dpi=dpi)

    # Encode images to Base64
    image_base64_list = [encode_image(img) for img in images]

    # Initialize OpenAI client
    client = OpenAI(api_key =os.environ.get('OPENAI_API_KEY_New'))

    # Send images and prompt to OpenAI
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": [
                               {"type": "text", "text": prompt}
                           ] + [
                               {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"}}
                               for img_base64 in image_base64_list
                           ]
            }
        ],
    )

    return completion.choices[0].message.content
prompt='''gIVE ME ALL the address of OORJA TECHINCAL SERVICES AND INVOICE NUMBER?
WRITE IT format {'address:''}'''
path=r"C:\Users\BRADSOL\Downloads\2420041683.pdf"
result=process_pdf_with_openai(path,prompt)
print(result)