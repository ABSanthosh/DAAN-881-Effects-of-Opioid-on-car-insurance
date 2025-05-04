# This file helps you to generate a context file from the datasets folder.
# It reads the first 2 lines of each file in the datasets folder and saves them to a context file.
# The context file is saved in the ".context" folder.
# The context file is used to provide context to the LLM when generating code.

# read all the files in the directory "./datasets" and read the first 2 lines of each file.
# write the data to a string in the format
# "file_name: \n line1 \n line2 \n"
# save the string to a file called "context.txt"
import os
import glob
import pandas as pd

def read_first_n_lines(file_path, n=4):
    """Read the first n lines of a file."""
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        lines = []
        for _ in range(n):
            try:
                lines.append(next(file))
            except StopIteration:
                break
    return lines

def read_files_in_directory(directory, n=2):
  """Read all files in a directory and return their first n lines."""
  context = ""
  files = {}
  
  # Get all subdirectories in the datasets folder
  subdirs = glob.glob(os.path.join(directory, '*'))
  
  # Iterate through each subdirectory
  for subdir in subdirs:
    if os.path.isdir(subdir):
      # Get all files in the subdirectory
      file_list = os.listdir(subdir)
      
      # Process each file
      for filename in file_list:
        full_path = os.path.join(subdir, filename)
        
        if os.path.isfile(full_path):
          try:
            if full_path.endswith('.csv'):
              files[full_path] = pd.read_csv(full_path).head(n).to_string(index=False)
            elif full_path.endswith('.xlsx'):
              files[full_path] = pd.read_excel(full_path).head(n).to_string(index=False)
            elif full_path.endswith('.txt'):
              lines = read_first_n_lines(full_path, n)
              files[full_path] = ''.join(lines)
          except Exception as e:
            print(f"Error reading {full_path}: {e}")
    
  # Save the files with their full path
  for file_path, content in files.items():
    context += f"{file_path}:\n{content}\n\n"
  
  return context

def load_dataset(file_path, **read_kwargs):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path, **read_kwargs, low_memory=False)
    elif file_path.endswith(".xlsx"):
        return pd.read_excel(file_path, **read_kwargs)
    elif file_path.endswith(".txt"):
        return pd.read_csv(file_path, delimiter="|", **read_kwargs, encoding='ISO-8859-1', low_memory=False)
    else:
        raise ValueError(f"Unsupported file format for {file_path}")

def PredStatisticsContext():
    # directory = "./scripts/pred/statistics"
    # read all files in the directory
    # get all contents in all the files
    # write it to .context/statistics-context.txt
    context = ""
    directory = "./scripts/pred/statistics"
    files = glob.glob(os.path.join(directory, '*'))
    for file_path in files:
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                    context += f"{file_path}:\n{content}\n\n"
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
    # Save the context to a file called "statistics_context.txt"
    context_file_path = os.path.join(directory, ".context", "statistics_context.txt")
    os.makedirs(os.path.dirname(context_file_path), exist_ok=True)
    with open(context_file_path, "w", encoding='utf-8') as f:
        f.write(context)

if __name__ == "__main__":
    # Read the first 4 lines of each file in the "./datasets" directory
    # directory = "./datasets"
    # context = read_files_in_directory(directory, n=2)

    # # just take "PA_PROCESSED.csv", "SC_PROCESSED.csv", "TN_PROCESSED.csv" files
    # files = ["PA/PA_PROCESSED.csv", "SC/SC_PROCESSED.csv", "TN/TN_PROCESSED.csv"]
    # # read the first 4 lines of each file in the "./datasets" directory
    # for file in files:
    #     file_path = os.path.join(directory, file)
    #     if os.path.isfile(file_path):
    #         try:
    #             if file_path.endswith('.csv'):
    #                 context += f"{file_path}:\n{pd.read_csv(file_path).head(2).to_string(index=False)}\n\n"
    #             elif file_path.endswith('.xlsx'):
    #                 context += f"{file_path}:\n{pd.read_excel(file_path).head(2).to_string(index=False)}\n\n"
    #             elif file_path.endswith('.txt'):
    #                 lines = read_first_n_lines(file_path, 2)
    #                 context += f"{file_path}:\n{''.join(lines)}\n\n"
    #         except Exception as e:
    #             print(f"Error reading {file_path}: {e}")

    # # Save the context to a file called "context.txt"
    # context_file_path = os.path.join(directory, ".context", "processed_dataset_context.txt")
    # os.makedirs(os.path.dirname(context_file_path), exist_ok=True)
    # with open(context_file_path, "w", encoding='utf-8') as f:
    #     f.write(context)


    # # Save the context to a file called "context.txt"
    # with open("./.context/dataset_context.txt", "w", encoding='utf-8') as f:
    #     f.write(context)
    PredStatisticsContext()
    pass
