version = '0.0.1'
update_date = '2023/12/20'


class RetiredIDProcessor:
    """
    A processor to handle the retirement of inactive IDs and map them to their new active IDs.

    Parameters:
    - excel_path (str): Path to the Excel file containing the mapping between active and retired IDs.
    - active_id_col (str): Column name in the Excel file for active IDs. Default is 'ACTIVE_UID'.
    - retired_id_col (str): Column name in the Excel file for retired IDs. Default is 'RETIRED_UID'.
    - numeric_ids (bool): Indicates if the IDs are numeric (True) or not (False). Default is True.

    The processor reads an Excel file to create a mapping and provides a method to process an array of IDs.
    It replaces retired IDs with their corresponding active IDs.

    Usage:
    - Initialize the processor with the path to the Excel file and the nature of IDs (numeric or not).
      Optionally, specify the column names for active and retired IDs if they are different from the default.
    - Process an array of IDs using the 'process' method, which returns a new numpy array with updated IDs.

    Example:
    retired_id_processor = RetiredIDProcessor('path/to/excel/file', numeric_ids=True)
    new_ids = retired_id_processor.process(old_ids_array)
    """

    def __init__(self, excel_path, active_id_col='ACTIVE_UID', retired_id_col='RETIRED_UID', numeric_ids=True):
        self.numeric_ids = numeric_ids
        self.id_mapping = self._load_data(excel_path, active_id_col, retired_id_col)

    def _load_data(self, excel_path, active_id_col, retired_id_col):
        df = xw_excel_to_df(excel_path, print_details=False)

        if self.numeric_ids:
            # Convert IDs to float64 if numeric
            id_map = dict(zip(df[retired_id_col].astype('float64'), df[active_id_col].astype('float64')))
        else:
            # Convert IDs to string if not numeric
            id_map = dict(zip(df[retired_id_col].astype('str'), df[active_id_col].astype('str')))

        print('ID Processor Initialized.')
        return id_map
    
    
    def process(self, id_array):
        """
        Process an array of IDs, replacing retired IDs with active IDs.

        Parameters:
        - id_array (numpy.array): An array of IDs to be processed. Should be of type float64 if numeric_ids is True, otherwise of type str.

        Returns:
        - A new numpy array with retired IDs replaced by their corresponding active IDs.
        """
        if self.numeric_ids and id_array.dtype != np.float64:
            raise TypeError(f"ID array expected to be of type float64 for numeric IDs, yet got a type of {id_array.dtype}")
        elif not self.numeric_ids and id_array.dtype != np.object:
            raise TypeError(f"ID array expected to be of type object (string) for non-numeric IDs, yet got a type of {id_array.dtype}")

        # Replace retired IDs with active IDs using vectorized operations
        vectorized_replace = np.vectorize(lambda x: self.id_mapping.get(x, x))
        return vectorized_replace(id_array)