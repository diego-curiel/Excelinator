import pandas as pd

import argparse as ap
import time
import os
from pathlib import Path
from typing import Any, Dict


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
                 drop_missmatches: bool) -> pd.DataFrame:
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
 
    dataset_a[result_column_name] = dataset_a[index_column_a].isin(
                dataset_b[index_column_b]
            ).map(lambda x: mapping.get(bool(x), x))

    # Remove missmatches
    if drop_missmatches:
        dataset_a = dataset_a[dataset_a[result_column_name] == match_marker]


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
                      right_on=index_column_b)
    return result_df
      

def main():
    parser = ap.ArgumentParser(
            prog='The Excelinator',
            description= 'Compare two spreadsheets and mark matches',
    )
    # First Spreadsheet 
    parser.add_argument('-o', '--origin', help='First spreadsheet', 
                        required=True)
    # First Spreadsheet Index Column
    parser.add_argument(
        '-x',
        '--origin-index', 
        help='Name or position of the index column in the origin spreadsheet',
        required=True
    )
    # Second Spreadsheet
    parser.add_argument('-p', '--partner', help='Second spreadsheet', 
                        required =True)
    # # Second Spreadsheet Index Column
    parser.add_argument(
        '-y', 
        '--partner-index', 
        help='Name or position of the index column in the partner spreadsheet',
        required=True
    )
    # Match Symbol
    parser.add_argument('-m', '--match-marker', 
                        help='Symbol or string to mark matches', default="1")
    # Missmatch Symbol
    parser.add_argument('-n', '--missmatch-marker',
                        help='Symbol to mark missmatches', default="0")
    # Results column name
    parser.add_argument('-r', '--results-column',
                        help='Name of the results column', default="RESULTS")
    # Column in reference spreadsheet to be copied
    parser.add_argument(
        '-c', 
        '--copy-columns',
        nargs='*',
        default=list(),
        help='Columns in the partner spreadsheet to be copied'
    )

    # Remove missmatches
    parser.add_argument(
        '-d',
        '--delete-missmatches',
        help='Delete rows that did not match in both files',
        action='store_true'
    )

    args = parser.parse_args()

    # Get both origin and partner files location
    origin_path = Path(args.origin)
    partner_path = Path(args.partner)
    
    # Check if both the origin_path and partner_path exist
    if not origin_path.exists():
        raise SystemExit("The origin file doesn't exist")
    if not partner_path.exists():
        raise SystemExit("The partner file doesn't exist")
    
    # Check the file extension for both the origin and partner files
    # I should replace this with libmagic, but I don't see the necessity
    origin_file_extension = origin_path.name.split(".")[-1]
    partner_file_extension = partner_path.name.split(".")[-1]
    # Dictionary that contains a read function acording to the filetype
    read_functions = {
            "csv": pd.read_csv,
            "xlsx": pd.read_excel
    }
    # Check if both origin and partner files have a compatible extension
    if not origin_file_extension in read_functions:
        raise SystemExit(
                "The origin file is neither an xlsx nor an csv"
            )
    if not partner_file_extension in read_functions:
        raise SystemExit(
                "The partner file is neither an xlsx nor a csv"
            )

    origin_read_function: Dict[Any, Any] = {}
    partner_read_function: Dict[Any, Any] = {}
    # AKA 14MiB
    MAX_BYTES_SIZE = 14680064
    lazy_load = False
    # Select the correct kwargs for each read_x function
    if origin_file_extension == "xlsx":
        origin_read_function["io"] = origin_path
    else:
        origin_read_function["filepath_or_buffer"] = origin_path

    # Change chunksize if the origin file size exceeds the max size
    origin_file_size = os.path.getsize(origin_path)
    if origin_file_extension == "csv" and origin_file_size >= MAX_BYTES_SIZE:
        origin_read_function["chunksize"] = 100000 # A hundred thousand!
        origin_read_function["low_memory"] = False # Not necessary to have this
        lazy_load = True

    # Summon the correct pd.read_x function according to the file extension
    # Add kwargs previously selected
    print("Reading the origin file...")
    origin_df = read_functions[origin_file_extension](**origin_read_function)

    # Remove the "chunksize" kwarg in order to read the partner file (not needed)
    # Change the io path
    if "chunksize" in origin_read_function:
        del origin_read_function["chunksize"]
    
    if partner_file_extension == "xlsx":
        partner_read_function["io"] = partner_path
    else:
        partner_read_function["filepath_or_buffer"] = partner_path
      
    print("Reading the partner file...")
    partner_df = read_functions[partner_file_extension](**partner_read_function)
    
    mark_matches_kwargs: Dict[Any, Any] = {
            "dataset_b":partner_df,
            "index_column_a":args.origin_index,
            "index_column_b":args.partner_index,
            "result_column_name":args.results_column,
            "match_marker":args.match_marker,
            "missmatch_marker":args.missmatch_marker,
            "drop_missmatches":args.delete_missmatches,
        }

    merge_datasets_kwargs: Dict[Any, Any] = {
            "dataset_b":partner_df,
            "index_column_a":args.origin_index,
            "index_column_b":args.partner_index,
            "copy_columns":args.copy_columns
        }

    result_df = pd.DataFrame()
    if lazy_load:
        print("Your origin file seems heavy, reading in lazy load mode...")
        for chunk in origin_df:
            # Find the chunk matches
            mark_matches_kwargs["dataset_a"] = chunk
            chunk_matches = mark_matches(**mark_matches_kwargs)
            # Merge the chunk with the needed columns
            merge_datasets_kwargs["dataset_a"] = chunk_matches
            ult_chunk = merge_datasets(**merge_datasets_kwargs)
            # Append the chunk to the final result
            result_df = pd.concat([result_df, ult_chunk])
    else:
        print("Reading origin file in normal mode...")
        # Find the origin file matches
        mark_matches_kwargs["dataset_a"] = origin_df.copy()
        origin_matches = mark_matches(**mark_matches_kwargs)
        # Merge the origin file with the needed columns
        merge_datasets_kwargs["dataset_a"] = origin_matches
        result_df = merge_datasets(**merge_datasets_kwargs)

   
    print("Saving file...")
    
    save_date = time.strftime(r"%y%m%d-%H-%M-%S", time.localtime())
    save_path = origin_path.parent.joinpath(f"RESULTS-{save_date}.xlsx")
    result_df.to_excel(save_path, index=False)


if __name__ == "__main__":
    main()
