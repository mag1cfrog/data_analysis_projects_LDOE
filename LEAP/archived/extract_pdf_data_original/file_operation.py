import os 
import time
from pdfrw import PdfReader, PdfWriter
from tqdm import tqdm

def combine_pdfs(Directory, Subjects_in_Headlines, Report_Type):
    """
    Combine multiple PDF files into a single PDF file based on specified criteria.

    This function searches for PDF files in the given directory that match specific criteria
    (including subject and report type), and combines these files into a single PDF. The combined
    PDF is saved in a 'cache' subdirectory within the provided directory. The function also
    reports the time taken to combine the PDF files.

    Args:
    - Directory (str): The directory where PDF files are located and where the combined PDF will be saved.
    - Subjects_in_Headlines (list of str): List of subjects to be included in the filenames of the PDFs to combine.
    - Report_Type (str): Report type that needs to be included in the filenames of the PDFs to combine.

    Returns:
    - None: The function does not return a value but saves the combined PDF in the specified directory.
    """
    start_time = time.time()
    # Get a list of all the PDF files in the directory
    pdf_files = [f for f in os.listdir(Directory) if f.endswith('.pdf') and any(Subject in f for Subject in Subjects_in_Headlines) and Report_Type in f]
    # Create a new PDF writer object
    writer = PdfWriter()
    # Iterate over the PDF files
    for filename in tqdm(pdf_files, desc="Combining PDFs"):
        # Open the PDF file
        reader = PdfReader(os.path.join(Directory, filename))
        # Append the pages to the new PDF file
        writer.addpages(reader.pages)
    # Create the cache directory if it doesn't exist
    os.makedirs(os.path.join(Directory, 'cache'), exist_ok=True)
    # Save the new PDF file
    writer.write(os.path.join(Directory, 'cache', 'combined_file.pdf'))
    # Get the end time
    end_time = time.time()
    # Print the running time
    print(f"Running time: {end_time - start_time} seconds")


def check_and_combine_pdfs(config):
    """Check if the combined PDF exists; if not, combine PDFs."""
    combined_file_path = os.path.join(config["Directory"], 'cache', 'combined_file.pdf')
    if not os.path.exists(combined_file_path):
        combine_pdfs(config["Directory"], config['Subjects_in_Headlines'], config['Report_Type'])
