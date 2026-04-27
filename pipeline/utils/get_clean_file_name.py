def get_clean_file_name(file_path):
    """
    Extracts a clean file name from a given file path by removing directories and file extensions.

    Args:
        file_path (str): The full path to the file. Example: '../data/diagnoses_icd10.csv'
    Returns:
        str: A clean file name without directories and extensions. Example: 'diagnoses_icd10'
    """
    import os
    # Get the base name (file name with extension)
    base_name = os.path.basename(file_path)
    # Remove the file extension
    clean_name = os.path.splitext(base_name)[0]
    return clean_name
