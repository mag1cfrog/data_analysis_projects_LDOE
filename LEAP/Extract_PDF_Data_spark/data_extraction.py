 
import fitz 

def extract_data(page, keyword, replace_list=None):
    """
    Extracts data from the page based on the keyword and optional replacements.

    :param page: The page from which to extract data.
    :param keyword: The keyword based on which data is extracted.
    :param replace_list: A list of tuples with replacement rules.
    :return: Extracted data as a string.
    """
    words = get_words(page, keyword)
    if words and replace_list:
        for old, new in replace_list:
            words = words.replace(old, new)
    return words.strip() if words else words

def get_words(page, name):
    """
    Gets words from the page based on the name (coordinates).

    :param page: The page from which to get words.
    :param name: The name which maps to specific coordinates on the page.
    :return: Words as a string.
    """
    rect = fitz.Rect(
        coordinate_mappings[name]
    )  # Assuming coordinate_mappings is globally accessible or passed as an argument
    words = page.get_textbox(rect)
    return words

