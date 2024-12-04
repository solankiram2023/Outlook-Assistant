import chardet
from bs4 import BeautifulSoup
from unidecode import unidecode

# Function to process email JSON contents and format them
def decode_content(content):
    # detected = chardet.detect(content.encode())
    # encoding = detected.get('encoding') or 'utf-8'
    # return content.encode().decode(encoding, errors='replace')

    return unidecode(content)

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', '').strip()

def extract_text_and_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Replace <a> tags with their text and link inline (e.g., "Text (URL)")
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.get_text(strip=True)
        if a_tag.get('originalsrc', None):
            href = a_tag['originalsrc']
        else:
            href = a_tag['href']
        a_tag.replace_with(f"{link_text} ({href})")

    # Extract the cleaned text
    return soup.get_text(separator='\n', strip=True)