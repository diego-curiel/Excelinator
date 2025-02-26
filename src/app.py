import pandas as pd

import argparse as ap
import magic as mg
import os
from pathlib import Path
from typing import Any


def get_system_args() -> ap.Namespace:
    """
    Description
    -----------
    Generates the script system arguments.
    ---
    Return
    ------
    ap.Namespace = Parsed system arguments.
    """
    parser = ap.ArgumentParser(
            prog='The Excelinator',
            description= '''Compare two spreadsheets, mark matches and merge by
            left join.'''
    )
    # First Spreadsheet 
    parser.add_argument(
        '-o', '--origin', 
        help='Left spreadsheet or dataset file path.', 
        required=True, 
        type=Path
    )
    # First Spreadsheet Index Column
    parser.add_argument(
        '-x',
        '--origin-index', 
        help='Name or position of the index column in the origin spreadsheet.',
        required=True
    )
    # Second Spreadsheet
    parser.add_argument(
        '-p', '--partner', 
        help='Right spreadsheet or dataset file path.', 
        required=True, 
        type=Path
    )
    # # Second Spreadsheet Index Column
    parser.add_argument(
        '-y', 
        '--partner-index', 
        help='Name or position of the index column in the partner file.',
        required=True
    )
    # Match Symbol
    parser.add_argument('-m', '--match-marker', 
                        help='Symbol or text to mark matches.', 
                        default="1")
    # Missmatch Symbol
    parser.add_argument('-n', '--missmatch-marker',
                        help='Symbol or text to mark missmatches.', 
                        default="0")
    # Results column name
    parser.add_argument('-r', '--results-column',
                        help='Name for the results column.', default="RESULTS")
    # Column in reference spreadsheet to be copied
    parser.add_argument(
        '-c', 
        '--copy-columns',
        help='Columns in the partner file to be copied into the save file.',
        nargs='*',
        default=list(),
    )
    # Remove missmatches
    parser.add_argument(
        '-d',
        '--delete-missmatches',
        help='Delete the rows that did not match in both files.',
        action='store_true'
    )
    # Set textfields to uppercase
    parser.add_argument(
        '-u',
        '--uppercase',
        help='Set all textfields (including the header) to uppercase.',
        action='store_true'
    )
    # Save file path
    parser.add_argument(
        '-f',
        '--save-file',
        help='Path to the save file',
        type=Path,
        required=True
    )
    # Save file as an Excel spreadsheet
    parser.add_argument(
        '--xlsx',
        help='Save the output file as an Excel spreadsheet.',
        action='store_true'
    )
    return parser.parse_args()


def avoid_similar_columns(column_name: str, column_list: list) -> str:
    """
    avoid_similar_columns
    ---------------------
    Pandas really hates when two columns have the same name, this function is 
    here to avoid that (as the name says), returning a brand new name for every
    time this exact name repeats. E.G:
        repeated_name | repeated_name_2 | repeated_name_3

    params:
    -------
    column_name: str - The name of the column that may cause conflicts.
    column_list: str - A list containing the actual set of columns of the dataset
    """
    
    ult_name = column_name
    copy_count = 1 # one means original :), the name won't change
    while ult_name in column_list:
        copy_count += 1
        ult_name = f"{column_name}_{copy_count}"

    return ult_name
        

def mark_matches(dataset_a: pd.DataFrame, dataset_b: pd.DataFrame,
                 index_column_a: str, index_column_b: str, 
                 result_column_name: str, match_marker: str, 
                 missmatch_marker: int|str, 
                 drop_missmatches: bool=False) -> pd.DataFrame:
    """
    mark_matches
    ------------
    Takes two datasets and maps all matches with the corresponding symbol adding
    a column with a personalized name.

    params:
    -------
    dataset_a: pd.DataFrame - Origin Dataset
    dataset_b: pd.Dataframe - Partner Dataset
    index_column_a: str - Name of the column where the origin objects index exists
    index_column_b: str - Name of the column where the partner objects index exist
    result_column_name: str - Name to asign to the results column
    match_marker: str - Symbol or number to use to mark matches
    missmatch_marker: str - Symbol or number to use to mark missmatches
    """
    # Check if both index columns for origin and partner exist
    if not index_column_a in dataset_a:
        raise SystemExit(
                "The index column name for the origin file is invalid"
            )

    if not index_column_b in dataset_b:
        raise SystemExit(
                "The index column name for the partner file is invalid"
            )

    mapping = {
            True: match_marker,
            False: missmatch_marker,
        }
    
    index_a = dataset_a.loc[:,index_column_a]
    index_b = dataset_b.loc[:, index_column_b]
    dataset_a[result_column_name] = index_a.isin(index_b).map(
        lambda x: mapping.get(bool(x), x)
    )

    # Remove missmatches
    if drop_missmatches:
        match_mask = dataset_a[result_column_name] == match_marker
        dataset_a = dataset_a.loc[match_mask]

    return dataset_a.copy()


def merge_datasets(dataset_a: pd.DataFrame, dataset_b: pd.DataFrame,
                   index_column_a: str, index_column_b: str, 
                   copy_columns: list[str|None]):
    """
    merge_datasets
    --------------
    This function takes two datasets, their index and the columns that will be
    conserved before mergin both datasets. 
    
    args
    ----
    dataset_a: pd.DataFrame - The "left" table of the merge
    dataset_b: pd.DataFrame - The "right" table of the merge
    index_column_a: str - The position of the index in the left table
    index_column_b: str - The position of the index in the right table
    copy_columns: str - The array of columns to be copied
    """
    # Half an hour debuging just to be solved by adding .copy(), smh
    needed_columns = copy_columns.copy()
    needed_columns.append(index_column_b)
    # Check if the columns exist in the partner file
    nonexistent_columns = [
            x for x in needed_columns if x not in dataset_b.columns
        ]
    if nonexistent_columns:
        error_message = "The following columns do not appear in the partner file: {cols}"
        raise SystemExit(
                error_message.format(cols=nonexistent_columns)
            )
    # Drop unnecessary columns from the partner dataset
    unneeded_partner_columns = [
            x for x in dataset_b.columns if x not in needed_columns
        ]
    # This declaration is just a way to create a copy of the object
    dataset_b = dataset_b.drop(columns=unneeded_partner_columns, inplace=False)
    dataset_b.drop_duplicates(subset=index_column_b, inplace=True, 
                              ignore_index=True)
    # Small fix to avoid duplicated columns
    new_columns_set = list()
    # Remove the index column from this proccess, otherwise the name may change 
    for column in dataset_b.columns:
        if column in copy_columns:
            column = avoid_similar_columns(column_name=str(column), 
                                           column_list=list(dataset_a.columns))
        new_columns_set.append(column)

    dataset_b.columns = new_columns_set

    # Let's merge
    result_df = pd.merge(left=dataset_a, right=dataset_b, left_on=index_column_a,
                      right_on=index_column_b, how="left")
    return result_df


def get_filename_extension(file_path:Path) -> str:
    """Description:
    Takes a file path and returns the extension that is contained in the file
    name. This function does not needs the file to be readable but raises an 
    SystemExit exception if the file does not exist.
    
    Parameters:
    file_path (Path): Path to the file whose extension will be extracted.

    Returns:
    str: Name of the file extension (e.g., 'xlsx', 'csv').
    """
    return file_path.suffix.strip('.') 


def get_real_extension(file_path:Path) -> str:
    """Description:
    Takes a file path and returns the real extension of the file using libmagic.
    Raises a SystemExit exception if the file does not exist or it is not readable.

    Parameters:
    file_path (Path): Path to the file whose extension will be guessed.
    
    Raise:
    SystemExit: If the file does not exist, or it is not accessable.
    SystemExit: If the filetype is not supported. 

    Returns:
    str: Name of the file extension (e.g., 'xlsx', 'csv').
    """
    # Dictionary of mime and extensions
    MIME_EXTENSIONS = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "text/csv": "csv"
    }
    # Check if the file is readable
    if not file_path.exists or not os.access(file_path, os.R_OK):
        raise SystemExit(
                "The file: {f} does not exist, or it is not readable.".format(
                    f=file_path
                )
        )
    # Get file extension
    file_mime = mg.from_file(file_path, mime=True)
    file_extension = MIME_EXTENSIONS.get(file_mime, None)
    # Exception if the mimetype is invalid
    if file_extension is None:
        raise SystemExit(
            "The file type of: {f} is not supported. Supported file types: {t}".format(
                f=file_path,
                t="Excel 2007+, CSV"
            )
        )

    return file_extension


def get_read_kwargs(file_path:Path|str, file_extension:str, 
                    lazy_load:bool=False) -> dict:
    """
    Description:
    Takes the file extension and returns the appropriate parameters for the 
    Pandas read function, whether it's `pd.read_csv` or `pd.read_excel`.
    Returns an empty dictionary if the file extension is invalid.

    Parameters:
    file_path (Path|str): Path to the file to be passed inside the kwargs as input.
    file_extension (str): Input file extension (e.g., 'xlsx'). Case-insensitive.
    lazy_load (bool, default: False): If True, sets `chunksize` in kwargs for lazy loading.

    Returns:
    dict: The kwargs for the appropriate Pandas read function.
    dict: An empty dictionary if the file extension is invalid.
    """

    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Set extension to lowercase always
    file_extension = file_extension.lower()    
    read_kwargs = dict()
    # Select the correct parameter for the type file
    if file_extension == "xlsx":
        read_kwargs["io"] = file_path

    elif file_extension == "csv": 
        read_kwargs["filepath_or_buffer"] = file_path
        # Change chunksize if the origin file size exceeds the max size
        if lazy_load:
            read_kwargs["chunksize"] = 100000 # A hundred thousand!
            read_kwargs["low_memory"] = False # Not necessary

    return read_kwargs


def main():
    # App Constants
    SYS_ARGS = get_system_args()

    # Get both origin and partner files location
    ORIGIN_PATH = SYS_ARGS.origin 
    PARTNER_PATH = SYS_ARGS.partner 
    
    # Save file path
    SAVE_PATH = SYS_ARGS.save_file

    # Dictionary that contains a read function acording to the filetype
    READ_FUNCTIONS = {
        "csv": pd.read_csv,
        "xlsx": pd.read_excel
    }

    # AKA 14MiB
    MAX_BYTES_SIZE = 14680064

    # Check the file extension for both the origin and partner files.
    # This function checks if the file exists and if it is readable, too.
    # Also checks if the file mime is compatible (Too many responsabilities...).
    ORIGIN_EXTENSION = get_real_extension(ORIGIN_PATH)
    PARTNER_EXTENSION = get_real_extension(PARTNER_PATH)
    # Read the origin file size in bytes.
    ORIGIN_BYTESIZE = os.path.getsize(ORIGIN_PATH)
    # If the origin file should be read with lazy loading
    ORIGIN_LAZY_LOAD = ORIGIN_BYTESIZE >= MAX_BYTES_SIZE

    # Select the correct kwargs for the origin type file
    origin_read_kwargs = get_read_kwargs(file_path=ORIGIN_PATH, 
                                         file_extension=ORIGIN_EXTENSION,
                                         lazy_load=ORIGIN_LAZY_LOAD) 

    # Apply kwargs and select the correct function according to the file type
    print("Loading the origin file...")
    origin_df = READ_FUNCTIONS[ORIGIN_EXTENSION](**origin_read_kwargs)

    # Set the correct kwargs for the partner type file 
    partner_read_kwargs = get_read_kwargs(file_path=PARTNER_PATH,
                                          file_extension=PARTNER_EXTENSION,
                                          lazy_load=False) # <- Not supported
      
    print("Loading the partner file...")
    partner_df = READ_FUNCTIONS[PARTNER_EXTENSION](**partner_read_kwargs)
    
    mark_matches_kwargs: dict[Any, Any] = {
            "dataset_b":partner_df,
            "index_column_a":SYS_ARGS.origin_index,
            "index_column_b":SYS_ARGS.partner_index,
            "result_column_name":SYS_ARGS.results_column,
            "match_marker":SYS_ARGS.match_marker,
            "missmatch_marker":SYS_ARGS.missmatch_marker,
            "drop_missmatches":SYS_ARGS.delete_missmatches,
        }

    merge_datasets_kwargs: dict[Any, Any] = {
            "dataset_b":partner_df,
            "index_column_a":SYS_ARGS.origin_index,
            "index_column_b":SYS_ARGS.partner_index,
            "copy_columns":SYS_ARGS.copy_columns
        }

    result_df = pd.DataFrame()

    if ORIGIN_LAZY_LOAD:
        print("Your origin file seems heavy, reading in lazy load mode...")
        for chunk in origin_df:
            # Find the chunk matches
            mark_matches_kwargs["dataset_a"] = chunk
            chunk_matches = mark_matches(**mark_matches_kwargs)
            if SYS_ARGS.copy_columns:
                # Merge the chunk with the needed columns
                merge_datasets_kwargs["dataset_a"] = chunk_matches
                chunk_matches = merge_datasets(**merge_datasets_kwargs)

            # Append the chunk to the final result
            result_df = pd.concat([result_df, chunk_matches])
    else:
        print("Reading origin file in normal mode...")
        # Find the origin file matches
        mark_matches_kwargs["dataset_a"] = origin_df
        origin_matches = mark_matches(**mark_matches_kwargs)
        if SYS_ARGS.copy_columns:
            # Merge the chunk with the needed columns
            merge_datasets_kwargs["dataset_a"] = origin_matches
            origin_matches = merge_datasets(**merge_datasets_kwargs)
        result_df = origin_matches

    # Change all strings to uppercase (random requirement, lol)
    if SYS_ARGS.uppercase:
        to_upper = lambda x: str(x).upper() if isinstance(x, str) else x
        result_df = result_df.map(to_upper)
        result_df.columns = [to_upper(x) for x in result_df.columns]
   
    print("Saving file...")
    if SYS_ARGS.xlsx:
        save_file_ext = get_filename_extension(SAVE_PATH)
        save_path = SAVE_PATH
        if not save_file_ext == 'xlsx':
            save_path = "{p}.xlsx".format(p=SAVE_PATH)
        with pd.ExcelWriter(save_path, engine='xlsxwriter') as excel_writer:
            result_df.to_excel(excel_writer, index=False)

    else:
        result_df.to_csv(SAVE_PATH, index=False, encoding="UTF-8")


if __name__ == "__main__":
    main()
