"""Generate Jupyter notebooks and Markdown files for NumPy 100 exercises.

Provides utilities to parse exercise data and generate various output
formats (Jupyter notebooks and Markdown files) for NumPy exercises.
"""

import os

import mdutils
import nbformat as nbf


def ktx_to_dict(input_file, keystarter='<'):
    """Parse keyed text file to a python dictionary.
    
    Args:
        input_file: Path to the keyed text file.
        keystarter: Character that marks the start of a key (default: '<').
    
    Returns:
        dict: Dictionary mapping keys to their values.
    """
    answer = dict()

    with open(input_file, 'r+', encoding='utf-8') as f:
        lines = f.readlines()

    k, val = '', ''
    for line in lines:
        if line.startswith(keystarter):
            k = line.replace(keystarter, '').strip()
            val = ''
        else:
            val += line

        if k:
            answer.update({k: val.strip()})

    return answer


def dict_to_ktx(input_dict, output_file, keystarter='<'):
    """Store a python dictionary to a keyed text file.
    
    Args:
        input_dict: Dictionary to save.
        output_file: Path to the output file.
        keystarter: Character to mark key starts (default: '<').
    """
    with open(output_file, 'w+') as f:
        for k, val in input_dict.items():
            f.write(f'{keystarter} {k}\n')
            f.write(f'{val}\n\n')


# Load exercise data from source files
HEADERS = ktx_to_dict(os.path.join('source', 'headers.ktx'))
QHA = ktx_to_dict(os.path.join('source', 'exercises100.ktx'))


def create_jupyter_notebook(destination_filename='100_Numpy_exercises.ipynb'):
    """Create Jupyter notebook with all exercises.
    
    Programmatically creates a notebook with questions and empty cells
    for answers, loaded from source files.
    
    Args:
        destination_filename: Output notebook file path
                             (default: '100_Numpy_exercises.ipynb').
    """
    # Create cells sequence
    nb = nbf.v4.new_notebook()
    nb['cells'] = []

    # Add header
    nb['cells'].append(nbf.v4.new_markdown_cell(HEADERS["header"]))
    nb['cells'].append(nbf.v4.new_markdown_cell(HEADERS["sub_header"]))
    nb['cells'].append(
        nbf.v4.new_markdown_cell(HEADERS["jupyter_instruction"])
    )

    # Add initialization
    nb['cells'].append(nbf.v4.new_code_cell('%run initialise.py'))

    # Add questions and empty code cells for answers
    for n in range(1, 101):
        nb['cells'].append(
            nbf.v4.new_markdown_cell(f'#### {n}. ' + QHA[f'q{n}'])
        )
        nb['cells'].append(nbf.v4.new_code_cell(""))

    # Delete file if one with the same name exists
    if os.path.exists(destination_filename):
        os.remove(destination_filename)

    # Write sequence to file
    nbf.write(nb, destination_filename)


def create_jupyter_notebook_random_question(
        destination_filename='100_Numpy_random.ipynb'):
    """Create Jupyter notebook with a random question.
    
    Programmatically creates a notebook that picks a random exercise
    from the exercises100.ktx file.
    
    Args:
        destination_filename: Output notebook file path
                             (default: '100_Numpy_random.ipynb').
    """
    # Create cells sequence
    nb = nbf.v4.new_notebook()
    nb['cells'] = []

    # Add header
    nb['cells'].append(nbf.v4.new_markdown_cell(HEADERS["header"]))
    nb['cells'].append(nbf.v4.new_markdown_cell(HEADERS["sub_header"]))
    nb['cells'].append(
        nbf.v4.new_markdown_cell(HEADERS["jupyter_instruction_rand"])
    )

    # Add initialization and random question picker
    nb['cells'].append(nbf.v4.new_code_cell('%run initialise.py'))
    nb['cells'].append(nbf.v4.new_code_cell("pick()"))

    # Delete file if one with the same name exists
    if os.path.exists(destination_filename):
        os.remove(destination_filename)

    # Write sequence to file
    nbf.write(nb, destination_filename)


def create_markdown(destination_filename='100_Numpy_exercises',
                    with_hints=False, with_solutions=False):
    """Create Markdown file with exercises.
    
    Args:
        destination_filename: Output markdown file path (without extension).
        with_hints: Include hints for each exercise (default: False).
        with_solutions: Include solutions for each exercise
                       (default: False).
    """
    # Build output filename
    if with_hints:
        destination_filename += '_with_hints'
    if with_solutions:
        destination_filename += '_with_solutions'

    # Initialize Markdown file
    mdfile = mdutils.MdUtils(file_name=destination_filename)

    # Add headers
    mdfile.write(HEADERS["header"] + '\n')
    mdfile.write(HEADERS["sub_header"] + '\n')

    # Add questions (and hint or answers if required)
    for n in range(1, 101):
        mdfile.new_header(
            title=f"{n}. {QHA[f'q{n}']}",
            level=4,
            add_table_of_contents="n"
        )
        if with_hints:
            mdfile.write(f"`{QHA[f'h{n}']}`")
        if with_solutions:
            mdfile.insert_code(QHA[f'a{n}'], language='python')

    # Delete file if one with the same name exists
    if os.path.exists(destination_filename):
        os.remove(destination_filename)

    # Write sequence to file
    mdfile.create_md_file()


def create_rst(destination_filename, with_ints=False, with_answers=False):
    """Create reStructuredText file with exercises.
    
    TODO: Use rstdoc python library. Also see possible integrations with
    https://github.com/rougier/numpy-100/pull/38
    
    Args:
        destination_filename: Output file path.
        with_ints: Include integration notes (default: False).
        with_answers: Include answers (default: False).
    """
    pass


if __name__ == '__main__':
    create_jupyter_notebook()
    create_jupyter_notebook_random_question()
    create_markdown()
    create_markdown(with_hints=False, with_solutions=True)
    create_markdown(with_hints=True, with_solutions=False)
    create_markdown(with_hints=True, with_solutions=True)
