import os
import csv
import json
import base64
import openai
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage

# Loading environment variables
load_dotenv()

# Sub function to convert images to base64
def encode_image_to_base64(logger, image_path):
    logger.info(f"Ariflow - encode_image_to_base64 - Encoding image to base64")
    try:
        with open(image_path, 'rb') as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
        logger.info(f"Ariflow - encode_image_to_base64 - Image encoded to base64 successfully")
        return img_base64
    except Exception as e:
        logger.error(f"Ariflow - encode_image_to_base64 - Error encoding image to base64: {e}")
        return None
 
# Sub function to summarize images using OpenAI  
def image_summarize(logger, img_base64, prompt):
    logger.info(f"Ariflow - image_summarize - Summarizing image with GPT")
    try:

        chat = ChatOpenAI(
            model       = "gpt-4o", 
            max_tokens  = 1024,
            api_key     = os.getenv("OPENAI_API_KEY")
        )

        msg = chat.invoke(
            [
                HumanMessage(
                    content=[
                        {
                            "type": "text", 
                            "text": prompt
                        },
                        {
                            "type"      : "image_url",
                            "image_url" : {"url": f"data:image/jpeg;base64,{img_base64}"},
                        },
                    ]
                )
            ]
        )
        logger.info(f"Ariflow - image_summarize - Summary generated successfully")
        return msg.content
    
    except Exception as e:
            logger.error(f"Ariflow - image_summarize - Error generating summary with GPT-4o: {e}")
            return None

# Function to parse images and extract contents
def parse_images(logger, image_path):
    logger.info(f"Ariflow - parse_images - Generating summaries for images in {image_path}")
    prompt = (
        "You are an assistant tasked with summarizing images for retrieval via RAGs. "
        "These summaries will be embedded and used to retrieve the raw image via RAGs. "
        "Give a concise summary of the image that is well optimized for retrieval via RAGs."
    )
    
    if os.path.isfile(image_path):
        logger.info(f"Ariflow - parse_images - Processing image")

        # Encode image to base64
        image_base64 = encode_image_to_base64(logger, image_path)
        if image_base64:
            image_summary = image_summarize(logger, image_base64, prompt)
            logger.info(f"Ariflow - parse_images - Image {image_path} summary: {image_summary}")
        
            if image_summary:
                return image_summary
            else:
                return "Failed to summarize image"
        else:
            return f"Failed to encode image {image_path}"

# Function to parse CSV files and extract contents
def parse_csv_files(logger, csv_file_path):
    logger.info(f"Ariflow - parse_csv_files - Extarcting contents from csv file: {csv_file_path}")

    extracted_contents = ""

    try:
        # Open the CSV file for reading
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                # Join the row contents and append to the extracted contents
                extracted_contents += ", ".join(row) + "\n"
    except Exception as e:
        logger.error(f"Airflow - parse_csv_files - Error processing CSV file: {e}")
        extracted_contents = f"Error processing CSV file {csv_file_path}: {str(e)}"

    return extracted_contents

# Function to parse JSON files and extract contents
def parse_json_files(logger, json_file_path):
    def flatten_json(y, prefix=''):
        out = {}
        if isinstance(y, dict):
            for k, v in y.items():
                out.update(flatten_json(v, prefix + k + '_'))
        elif isinstance(y, list):
            for i, v in enumerate(y):
                out.update(flatten_json(v, prefix + str(i) + '_'))
        else:
            out[prefix[:-1]] = y  # Remove the trailing underscore
        return out

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        flat_json = flatten_json(data)
        # Concatenate all values as a single string
        text_content = " ".join([str(value) for value in flat_json.values()])
        return text_content
    except Exception as e:
        print(f"Error processing JSON file {json_file_path}: {e}")
        return None