import os
import time
from pdfrw import PdfReader, PdfWriter
from rich.progress import Progress


def delete_file(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)
        print(f"The cached file has been deleted.")
    else:
        print(f"File {filepath} does not exist.")


def combine_pdfs(PDF_DIRECTORY, Subjects_in_Headlines, Report_Type):
    """
    Combines PDFs in the directory into a single PDF file.
    :param PDF_DIRECTORY: The directory where the PDF files are stored.
    :param Subjects_in_Headlines: The list of subjects to be included in the headlines.
    :param Report_Type: The type of report to be combined.
    """
    start_time = time.time()

    # Get a list of all the PDF files in the directory
    pdf_files = [
        f
        for f in os.listdir(PDF_DIRECTORY)
        if f.endswith(".pdf")
        and any(Subject in f for Subject in Subjects_in_Headlines)
        and Report_Type in f
    ]

    # Create a new PDF writer object
    writer = PdfWriter()

    # Create a progress bar with Rich
    with Progress() as progress:
        task = progress.add_task("[red]Combining PDFs...", total=len(pdf_files))

        # Iterate over the PDF files
        for filename in pdf_files:
            # Update the progress bar
            progress.update(
                task, advance=1, description=f"[green]Processing {filename}..."
            )

            # Open the PDF file
            reader = PdfReader(os.path.join(PDF_DIRECTORY, filename))

            # Append the pages to the new PDF file
            writer.addpages(reader.pages)

    # Create the cache directory if it doesn't exist
    os.makedirs(os.path.join(PDF_DIRECTORY, "cache"), exist_ok=True)

    # Save the new PDF file
    writer.write(os.path.join(PDF_DIRECTORY, "cache", "combined_file.pdf"))

    # Get the end time
    end_time = time.time()

    # Print the running time
    print(f"Running time: {end_time - start_time} seconds")


