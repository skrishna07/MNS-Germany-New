import json
import os
import requests
import logging
from pdf2image import convert_from_path
from io import BytesIO
import base64
from openai import OpenAI
import anthropic
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())



def split_openai(text, initial_prompt):
    url = os.environ.get('url')
    prompt = str(text) + ' ' + '\n' + '--------------------------------' + '\n' + initial_prompt
    logging.info(prompt)
    payload = json.dumps({
        "model": "gpt-5.2",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.7,
        "top_p": 1,
        "frequency_penalty": 0,
        "presence_penalty": 0
    })
    headers = {
        'Authorization': os.environ.get('OPENAI_API_KEY_Vietnam'),
        'Content-Type': 'application/json',
        'Cookie': os.environ.get('cookie')
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    json_response = response.json()
    print(json_response)
    content = json_response['choices'][0]['message']['content']
    print(content)
    return content


def process_pdf_with_openai(pdf_path, prompt, dpi=300):
    def encode_image(image):
        """Encodes a PIL image to Base64."""
        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")


    print(f"Prompt {prompt}")

    # Convert PDF to images
    images = convert_from_path(pdf_path, dpi=dpi)

    # Encode images to Base64
    image_base64_list = [encode_image(img) for img in images]

    # Initialize OpenAI client
    client = OpenAI(api_key =os.environ.get('OPENAI_API_KEY_New'))

    # Send images and prompt to OpenAI
    completion = client.chat.completions.create(
        model="gpt-5.2",
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


def process_pdf_with_claude(pdf_path, prompt, dpi=300):
    def encode_image(image):
        """Encodes a PIL image to Base64."""
        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    # Convert PDF to images
    images = convert_from_path(pdf_path, dpi=dpi)

    # Encode images
    image_base64_list = [encode_image(img) for img in images]

    # Initialize Claude client
    client = anthropic.Anthropic(
        api_key=os.environ.get("claude_api_key")
    )

    # Build content array (text + images)
    content = [
        {
            "type": "text",
            "text": prompt
        }
    ]

    for img_base64 in image_base64_list:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": img_base64
            }
        })
    print(f"Content from PDF {content}")
    # Send request to Claude
    response = client.messages.create(
        model="claude-opus-4-6",  # Vision-capable
        max_tokens=10000,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": content
            }
        ]
    )

    return response.content[0].text


def split_claude(text, initial_prompt):
    import os, json, requests

    url = "https://api.anthropic.com/v1/messages"

    prompt = str(text) + '\n--------------------------------\n' + initial_prompt

    payload = json.dumps({
        "model": "claude-opus-4-6",
        "max_tokens": 10000,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    })

    headers = {
        "x-api-key": os.environ.get("claude_api_key"),
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    response = requests.post(url, headers=headers, data=payload)
    json_response = response.json()
    logging.info(json_response)
    content = json_response["content"][0]["text"]

    return content